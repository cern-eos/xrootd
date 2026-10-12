#include "XrdHttpS3Handler.hh"

#include "XrdHttp/XrdHttpReq.hh"
#include "XrdHttpS3Paths.hh"
#include "XrdHttpS3SigV4.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdVersion.hh"

#include <cerrno>
#include <cstring>
#include <ctime>
#include <stdexcept>

XrdVERSIONINFO(XrdHttpGetExtHandler, HttpS3);
XrdVERSIONINFO(XrdHttpExtClaim, HttpS3);

namespace XrdHttpS3 {
namespace {

const std::string *HeaderOf(const XrdHttpReq &req, const char *name) {
  auto it = req.allheaders.find(name);
  if (it == req.allheaders.end())
    return nullptr;
  return &it->second;
}

bool HeaderAuth(const XrdHttpReq &req) {
  const std::string *auth = HeaderOf(req, "authorization");
  return auth && auth->compare(0, 16, "AWS4-HMAC-SHA256") == 0;
}

bool Presigned(const XrdHttpReq &req) {
  if (!req.opaque)
    return false;
  const char *alg = req.opaque->Get("X-Amz-Algorithm");
  return alg && std::strncmp(alg, "AWS4-HMAC-SHA256", 16) == 0;
}

}

Handler::Handler(XrdSysError *log, const char *config, XrdOucEnv *env) : m_log(*log) {
  if (!env)
    throw std::runtime_error("S3 handler requires the server environment");
  if (!LoadConfig(config, m_log, m_cfg))
    throw std::runtime_error("S3 handler configuration failed");
  void *raw = env->GetPtr("XrdSfsFileSystem*");
  if (!raw)
    throw std::runtime_error("S3 handler did not receive a filesystem");
  m_sfs = static_cast<XrdSfsFileSystem *>(raw);
  m_meta.reset(new Meta(*m_sfs, m_cfg, m_log));
  m_ops.reset(new Ops(*m_meta, m_cfg, m_log));
}

Handler::~Handler() = default;

bool Handler::MatchesPath(const char *, const char *) {
  return false;
}

int Handler::Init(const char *) {
  return 0;
}

bool Handler::Claim(const XrdHttpReq &req) const {
  if (!m_cfg.enabled)
    return false;
  return HeaderAuth(req) || Presigned(req);
}

int Handler::ProcessReq(XrdHttpExtReq &req) {
  if (!m_cfg.enabled)
    return m_ops->Fail(req, 403, "AccessDenied", "S3 is switched off");

  auto authIt = req.headers.find("authorization");
  bool headerAuth = authIt != req.headers.end() &&
                    authIt->second.compare(0, 16, "AWS4-HMAC-SHA256") == 0;

  VerifyInput vin;
  vin.method = req.verb;
  vin.resource = req.resource;
  vin.query = req.headers.count("xrd-http-query") ? req.headers["xrd-http-query"] : "";
  vin.headers = req.headers;
  vin.region = m_cfg.region;
  vin.skew = m_cfg.skew;

  ParsedAuth parsed;
  std::string detail;
  AuthFail why = AuthFail::Ok;
  if (headerAuth) {
    vin.authorization = authIt->second;
    parsed = ParseAuthorization(vin.authorization);
    if (!parsed.ok) {
      detail = "Authorization header is not AWS SigV4";
      why = AuthFail::Malformed;
    }
  } else {
    auto query = ParseQuery(vin.query);
    auto cred = query.find("X-Amz-Credential");
    size_t slash = cred == query.end() ? std::string::npos : cred->second.find('/');
    if (cred == query.end() || slash == std::string::npos || slash == 0) {
      why = AuthFail::Malformed;
      detail = "presigned URL is missing a SigV4 query parameter";
    } else {
      parsed.ok = true;
      parsed.accessKey = cred->second.substr(0, slash);
    }
  }

  KeyRecord key;
  if (why == AuthFail::Ok) {
    if (!ValidAccessKey(parsed.accessKey))
      return m_ops->Fail(req, 403, "InvalidAccessKeyId", "The access key is not valid");
    int err = 0;
    if (m_meta->LoadKey(parsed.accessKey, key, err) != 0) {
      if (err == ENOENT || err == EINVAL)
        return m_ops->Fail(req, 403, "InvalidAccessKeyId", "The access key ID does not exist");
      return m_ops->Fail(req, 500, "InternalError", "Could not read the access key");
    }
    if (key.status != "active")
      return m_ops->Fail(req, 403, "InvalidAccessKeyId", "The access key is not active");
    if (key.expires > 0 && static_cast<long long>(time(nullptr)) > key.expires)
      return m_ops->Fail(req, 403, "InvalidAccessKeyId", "The access key has expired");
    if (headerAuth)
      why = VerifySigV4(vin, key.secret, detail);
    else
      why = VerifyPresign(vin, key.secret, parsed, detail);
  }
  if (why != AuthFail::Ok) {
    const char *code = "SignatureDoesNotMatch";
    int http = 403;
    if (why == AuthFail::Malformed) {
      code = "AuthorizationHeaderMalformed";
      http = 400;
    } else if (why == AuthFail::Skew) {
      code = "RequestTimeTooSkewed";
      http = 403;
    } else if (why == AuthFail::MissingHash) {
      code = "InvalidArgument";
      http = 400;
    } else if (why == AuthFail::Expired) {
      code = "AccessDenied";
      http = 403;
    }
    if (m_cfg.traceInfo)
      m_log.Emsg("S3", detail.c_str());
    return m_ops->Fail(req, http, code, detail);
  }
  return m_ops->Dispatch(req, key);
}

}

extern "C" {

XrdHttpExtHandler *XrdHttpGetExtHandler(XrdSysError *log, const char *config,
                                        const char *, XrdOucEnv *env) {
  try {
    return new XrdHttpS3::Handler(log, config, env);
  } catch (const std::exception &ex) {
    if (log)
      log->Emsg("S3", ex.what());
    return nullptr;
  }
}

bool XrdHttpExtClaim(XrdHttpExtHandler *self, const XrdHttpReq *req) {
  if (!self || !req)
    return false;
  return static_cast<XrdHttpS3::Handler *>(self)->Claim(*req);
}

}
