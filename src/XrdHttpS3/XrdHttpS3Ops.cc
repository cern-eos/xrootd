#include "XrdHttpS3Ops.hh"

#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdHttpS3Client.hh"
#include "XrdHttpS3Fs.hh"
#include "XrdHttpS3Paths.hh"
#include "XrdHttpS3SigV4.hh"
#include "XrdHttpS3Xml.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysError.hh"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <map>
#include <set>
#include <sys/stat.h>
#include <vector>

namespace XrdHttpS3 {
namespace {

std::string Trim(const std::string &in) {
  size_t a = 0;
  while (a < in.size() && (in[a] == ' ' || in[a] == '\t'))
    a++;
  size_t b = in.size();
  while (b > a && (in[b - 1] == ' ' || in[b - 1] == '\t' || in[b - 1] == '\r'))
    b--;
  return in.substr(a, b - a);
}

bool MetaNameOk(const std::string &name) {
  if (name.empty() || name.size() > 128)
    return false;
  for (unsigned char c : name) {
    if (!(std::isalnum(c) || c == '-' || c == '_' || c == '.'))
      return false;
  }
  return true;
}

std::string CanonEtag(std::string s) {
  s = Trim(s);
  if (s.size() >= 2 && (s[0] == 'W' || s[0] == 'w') && s[1] == '/')
    s = Trim(s.substr(2));
  if (s.size() >= 2 && s.front() == '"' && s.back() == '"')
    s = s.substr(1, s.size() - 2);
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

bool EtagListed(const std::string &header, const std::string &etag, bool &any) {
  any = false;
  std::string want = CanonEtag(etag);
  size_t i = 0;
  while (i < header.size()) {
    size_t comma = header.find(',', i);
    if (comma == std::string::npos)
      comma = header.size();
    std::string tok = Trim(header.substr(i, comma - i));
    i = comma + 1;
    if (tok.empty())
      continue;
    if (tok == "*") {
      any = true;
      continue;
    }
    if (CanonEtag(tok) == want)
      return true;
  }
  return false;
}

bool ParseHttpDate(const std::string &in, time_t &out) {
  std::tm tm{};
  const char *rest = strptime(Trim(in).c_str(), "%a, %d %b %Y %H:%M:%S", &tm);
  if (!rest)
    return false;
  out = timegm(&tm);
  return out != static_cast<time_t>(-1);
}

const std::string *Header(const std::map<std::string, std::string> &headers, const char *name) {
  auto it = headers.find(name);
  if (it == headers.end())
    return nullptr;
  return &it->second;
}

}

int CollectUserMeta(const std::map<std::string, std::string> &headers,
                    std::map<std::string, std::string> &user) {
  user.clear();
  size_t bytes = 0;
  for (const auto &kv : headers) {
    if (kv.first.compare(0, 11, "x-amz-meta-") != 0)
      continue;
    std::string name = kv.first.substr(11);
    std::string value = kv.second;
    if (!MetaNameOk(name) || value.size() > 1024 || value.find('\r') != std::string::npos ||
        value.find('\n') != std::string::npos)
      return 1;
    bytes += name.size() + value.size();
    if (bytes > 2048 || user.size() >= 32)
      return 2;
    user[name] = value;
  }
  return 0;
}

std::string UserMetaHeaders(const std::map<std::string, std::string> &user) {
  std::string out;
  for (const auto &kv : user) {
    if (!MetaNameOk(kv.first) || kv.second.find('\r') != std::string::npos ||
        kv.second.find('\n') != std::string::npos)
      continue;
    out += "\r\nx-amz-meta-" + kv.first + ": " + kv.second;
  }
  return out;
}

ReadCond EvalReadCond(const std::map<std::string, std::string> &headers, const std::string &etag,
                      time_t mtime, const char *ifMatch, const char *ifNone, const char *ifUnmod,
                      const char *ifMod) {
  const std::string *match = Header(headers, ifMatch);
  const std::string *none = Header(headers, ifNone);
  const std::string *unmod = Header(headers, ifUnmod);
  const std::string *mod = Header(headers, ifMod);
  if (match) {
    bool any = false;
    if (!EtagListed(*match, etag, any) && !any)
      return ReadCond::Failed;
  } else if (unmod) {
    time_t when = 0;
    if (ParseHttpDate(*unmod, when) && mtime > when)
      return ReadCond::Failed;
  }
  if (none) {
    bool any = false;
    if (EtagListed(*none, etag, any) || any)
      return ReadCond::NotModified;
  } else if (mod) {
    time_t when = 0;
    if (ParseHttpDate(*mod, when) && mtime <= when)
      return ReadCond::NotModified;
  }
  return ReadCond::Ok;
}

int EvalWriteCond(const std::map<std::string, std::string> &headers, bool exists,
                  const std::string &etag) {
  const std::string *none = Header(headers, "if-none-match");
  const std::string *match = Header(headers, "if-match");
  if (none) {
    if (Trim(*none) != "*")
      return 400;
    if (exists)
      return 412;
  }
  if (match) {
    bool any = false;
    if (!exists || (!EtagListed(*match, etag, any) && !any))
      return 412;
  }
  return 0;
}

namespace {

const char *kSubresources[] = {
    "acl", "lifecycle", "location", "logging", "uploads", "versioning", "website", nullptr};

const std::string *Hdr(const XrdHttpExtReq &req, const char *name) {
  auto it = req.headers.find(name);
  if (it == req.headers.end())
    return nullptr;
  return &it->second;
}

std::string QueryOf(const XrdHttpExtReq &req) {
  auto it = req.headers.find("xrd-http-query");
  if (it == req.headers.end())
    return std::string();
  return it->second;
}

bool WantsContinue(const XrdHttpExtReq &req) {
  const std::string *exp = Hdr(req, "expect");
  return exp && exp->find("100-continue") != std::string::npos;
}

int Drain(XrdHttpExtReq &req) {
  if (req.length <= 0 || WantsContinue(req))
    return 0;
  long long left = req.length;
  while (left > 0) {
    char *data = nullptr;
    int want = static_cast<int>(std::min<long long>(left, 65536));
    int n = req.BuffgetData(want, &data, true);
    if (n <= 0)
      return -1;
    left -= n;
  }
  return 0;
}

bool HeaderValueOk(const std::string &value) {
  if (value.size() > 512)
    return false;
  for (unsigned char c : value) {
    if (c < 0x20 || c == 0x7f)
      return false;
  }
  return true;
}

std::string CleanType(const std::string &in) {
  std::string out;
  for (char c : in) {
    if (c == '\r' || c == '\n')
      break;
    out.push_back(c);
  }
  if (out.empty())
    return "application/octet-stream";
  if (out.size() > 256)
    out.resize(256);
  return out;
}

bool ParseRange(const std::string &h, long long size, long long &start, long long &end) {
  if (h.compare(0, 6, "bytes=") != 0 || h.find(',') != std::string::npos)
    return false;
  std::string spec = h.substr(6);
  size_t dash = spec.find('-');
  if (dash == std::string::npos)
    return false;
  try {
    std::string a = spec.substr(0, dash);
    std::string b = spec.substr(dash + 1);
    if (a.empty() && b.empty())
      return false;
    if (a.empty()) {
      long long n = std::stoll(b);
      if (n <= 0)
        return false;
      if (n > size)
        n = size;
      start = size - n;
      end = size - 1;
      return size > 0;
    }
    start = std::stoll(a);
    end = b.empty() ? size - 1 : std::stoll(b);
  } catch (...) {
    return false;
  }
  if (start < 0 || start >= size || end < start)
    return false;
  if (end >= size)
    end = size - 1;
  return true;
}

struct FileInfo {
  std::string key;
  long long size{0};
  time_t mtime{0};
};

void Walk(Fs &fs, const XrdSecEntity *who, const std::string &dir, const std::string &rel,
          std::vector<FileInfo> &out, int budget, bool &truncated, int &err) {
  if (static_cast<int>(out.size()) >= budget) {
    truncated = true;
    return;
  }
  std::vector<std::string> names;
  if (fs.List(dir, who, names, err) != 0) {
    if (err == ENOENT) {
      err = 0;
      return;
    }
    return;
  }
  for (const std::string &name : names) {
    if (static_cast<int>(out.size()) >= budget) {
      truncated = true;
      return;
    }
    if (name == ".s3" || name == ".s3folder") {
      if (name == ".s3folder" && !rel.empty()) {
        struct stat st;
        int sterr = 0;
        fs.Stat(JoinPath(dir, name), who, st, sterr);
        FileInfo info;
        info.key = rel;
        info.size = 0;
        info.mtime = sterr == 0 ? st.st_mtime : 0;
        out.push_back(info);
      }
      continue;
    }
    std::string path = JoinPath(dir, name);
    struct stat st;
    int sterr = 0;
    if (fs.Stat(path, who, st, sterr) != 0)
      continue;
    if (S_ISDIR(st.st_mode))
      Walk(fs, who, path, rel + name + "/", out, budget, truncated, err);
    else {
      FileInfo info;
      info.key = rel + name;
      info.size = st.st_size;
      info.mtime = st.st_mtime;
      out.push_back(info);
    }
    if (err && err != ENOENT)
      return;
  }
  err = 0;
}

}

Ops::Ops(Meta &meta, const Config &cfg, XrdSysError &log)
    : m_meta(meta), m_cfg(cfg), m_log(log) {}

int Ops::Fail(XrdHttpExtReq &req, int http, const char *code, const std::string &message) {
  std::string body = ErrorXml(code, message, req.resource);
  std::string hdr = "Content-Type: application/xml";
  if (Reply(req, http, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

bool Ops::Owned(const KeyRecord &key, const BucketRecord &bucket) const {
  return bucket.owner == key.user;
}

int Ops::LoadOwned(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                   BucketRecord &out) {
  int err = 0;
  if (m_meta.LoadBucket(bucket, out, err) != 0) {
    if (err == ENOENT || err == EINVAL)
      return Fail(req, 404, "NoSuchBucket", "The specified bucket does not exist");
    return Fail(req, 500, "InternalError", "Could not read the bucket record");
  }
  if (!Owned(key, out))
    return Fail(req, 403, "AccessDenied", "This account does not own the bucket");
  return 1;
}

namespace {

bool KnownCanned(const std::string &acl) {
  return acl == "private" || acl == "public-read" || acl == "public-read-write" ||
         acl == "authenticated-read" || acl == "bucket-owner-read" ||
         acl == "bucket-owner-full-control" || acl == "aws-exec-read";
}

const char *kGrantHeaders[] = {"x-amz-grant-read",           "read",
                               "x-amz-grant-write",          "write",
                               "x-amz-grant-read-acp",       "read-acp",
                               "x-amz-grant-write-acp",      "write-acp",
                               "x-amz-grant-full-control",   "full-control",
                               nullptr,                      nullptr};

}

int Ops::RejectAclChange(XrdHttpExtReq &req) {
  const std::string *acl = Hdr(req, "x-amz-acl");
  bool grants = false;
  for (int i = 0; kGrantHeaders[i]; i += 2) {
    if (Hdr(req, kGrantHeaders[i]))
      grants = true;
  }
  if (acl && grants)
    return Fail(req, 400, "InvalidArgument", "A canned ACL and grant headers are mutually exclusive");
  if (acl && !KnownCanned(*acl))
    return Fail(req, 400, "InvalidArgument", "The canned ACL is not supported");
  return 1;
}

void Ops::CollectAcl(const XrdHttpExtReq &req, std::string &canned,
                     std::map<std::string, std::string> &grants) {
  canned = "private";
  grants.clear();
  for (int i = 0; kGrantHeaders[i]; i += 2) {
    const std::string *v = Hdr(req, kGrantHeaders[i]);
    if (v && !v->empty())
      grants[kGrantHeaders[i + 1]] = *v;
  }
  if (!grants.empty()) {
    canned.clear();
    return;
  }
  const std::string *acl = Hdr(req, "x-amz-acl");
  if (acl && KnownCanned(*acl))
    canned = *acl;
}

int Ops::GetAcl(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                const std::string &objKey, bool object) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  if (object) {
    Client id(key.user);
    struct stat st;
    int err = 0;
    if (m_meta.Store().Stat(ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), st, err) != 0 ||
        !S_ISREG(st.st_mode))
      return Fail(req, 404, "NoSuchKey", "The specified key does not exist");
  }
  std::string canned = "private";
  std::map<std::string, std::string> grants;
  if (object) {
    ObjectMeta meta;
    if (m_meta.ReadObjectMeta(key.user, bucket, objKey, meta) == 0) {
      canned = meta.acl;
      grants = meta.grants;
    }
  } else {
    canned = rec.acl.empty() ? "private" : rec.acl;
    grants = rec.grants;
  }
  std::string body = AclXml(key.user, canned, grants);
  std::string hdr = "Content-Type: application/xml";
  if (Reply(req, 200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

bool VhostTarget(const Config &cfg, const XrdHttpExtReq &req, const std::string &path,
                 std::string &bucket, std::string &objKey, bool &onlyBucket) {
  if (cfg.vhost.empty())
    return false;
  const std::string *hostHdr = Hdr(req, "host");
  if (!hostHdr || hostHdr->empty() || (*hostHdr)[0] == '[')
    return false;
  std::string host = *hostHdr;
  size_t colon = host.rfind(':');
  if (colon != std::string::npos && colon + 1 < host.size()) {
    bool digits = true;
    for (size_t i = colon + 1; i < host.size(); i++) {
      if (!std::isdigit(static_cast<unsigned char>(host[i])))
        digits = false;
    }
    if (digits)
      host.resize(colon);
  }
  for (char &c : host)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  std::string suffix = "." + cfg.vhost;
  if (host.size() <= suffix.size() ||
      host.compare(host.size() - suffix.size(), suffix.size(), suffix) != 0)
    return false;
  bucket = host.substr(0, host.size() - suffix.size());
  if (path == "/") {
    onlyBucket = true;
    objKey.clear();
    return true;
  }
  if (path.empty() || path[0] != '/')
    return false;
  objKey = path.substr(1);
  onlyBucket = objKey.empty();
  return true;
}

int Ops::Dispatch(XrdHttpExtReq &req, const KeyRecord &key) {
  if (req.verb != "GET" && req.verb != "HEAD" && req.verb != "PUT" && req.verb != "DELETE" &&
      req.verb != "POST")
    return Fail(req, 501, "NotImplemented", "This S3 operation is not implemented");

  std::string path = req.resource;
  if (path.empty())
    path = "/";

  std::string bucket;
  std::string objKey;
  bool onlyBucket = false;
  bool virtualHost = VhostTarget(m_cfg, req, path, bucket, objKey, onlyBucket);
  if (virtualHost) {
    if (!ValidBucket(bucket))
      return Fail(req, 400, "InvalidBucketName", "The bucket name is not valid");
    if (!onlyBucket && !ValidObjectKey(objKey))
      return Fail(req, 400, "InvalidArgument", "The object key is not valid");
  } else if (path == "/") {
    if (req.verb == "GET")
      return ListBuckets(req, key);
    if (req.verb == "HEAD") {
      if (Reply(req, 200, nullptr, nullptr, nullptr, 0) != 0)
        return -1;
      return 0;
    }
    return Fail(req, 405, "MethodNotAllowed", "This method is not valid on the service");
  }
  if (!virtualHost) {
    if (path[0] != '/')
      return Fail(req, 400, "InvalidArgument", "The request path is not absolute");
    std::string rest = path.substr(1);
    size_t slash = rest.find('/');
    bucket = slash == std::string::npos ? rest : rest.substr(0, slash);
    onlyBucket = slash == std::string::npos || slash + 1 == rest.size();
    objKey = onlyBucket ? std::string() : rest.substr(slash + 1);
    if (!ValidBucket(bucket))
      return Fail(req, 400, "InvalidBucketName", "The bucket name is not valid");
    if (!onlyBucket && !ValidObjectKey(objKey))
      return Fail(req, 400, "InvalidArgument", "The object key is not valid");
  }

  auto query = ParseQuery(QueryOf(req));
  if (onlyBucket && query.count("acl")) {
    if (req.verb == "GET")
      return GetAcl(req, key, bucket, std::string(), false);
    if (req.verb == "PUT")
      return PutAcl(req, key, bucket, std::string(), false);
    return Fail(req, 501, "NotImplemented", "Changing an ACL is not implemented");
  }
  if (onlyBucket && query.count("cors"))
    return BucketDocOp(req, key, bucket, "cors");
  if (onlyBucket && query.count("tagging"))
    return BucketDocOp(req, key, bucket, "tagging");
  if (onlyBucket && query.count("policy"))
    return BucketDocOp(req, key, bucket, "policy");
  if (onlyBucket && query.count("notification"))
    return BucketDocOp(req, key, bucket, "notification");
  if (onlyBucket && query.count("requestPayment"))
    return BucketDocOp(req, key, bucket, "requestPayment");
  if (onlyBucket && req.verb == "GET" && query.count("location"))
    return GetBucketLocation(req, key, bucket);
  if (onlyBucket && req.verb == "GET" && query.count("uploads"))
    return ListMultipartUploads(req, key, bucket);
  if (onlyBucket) {
    for (int i = 0; kSubresources[i]; i++) {
      if (query.count(kSubresources[i]))
        return Fail(req, 501, "NotImplemented", "This bucket subresource is not implemented");
    }
    if (req.verb == "POST" && query.count("delete"))
      return DeleteObjects(req, key, bucket);
    if (req.verb == "PUT")
      return CreateBucket(req, key, bucket);
    if (req.verb == "DELETE")
      return DeleteBucket(req, key, bucket);
    if (req.verb == "HEAD")
      return HeadBucket(req, key, bucket);
    if (req.verb == "GET") {
      if (query.count("list-type") && query["list-type"] != "2" && query["list-type"] != "1")
        return Fail(req, 501, "NotImplemented", "This list-type is not implemented");
      return ListObjects(req, key, bucket);
    }
  }

  if (!onlyBucket && query.count("uploads") && req.verb == "POST")
    return CreateMultipart(req, key, bucket, objKey);
  if (!onlyBucket && query.count("uploadId")) {
    if (query.count("partNumber") && req.verb == "PUT") {
      if (Hdr(req, "x-amz-copy-source"))
        return UploadPartCopy(req, key, bucket, objKey);
      return UploadPart(req, key, bucket, objKey);
    }
    if (query.count("partNumber") && req.verb == "HEAD")
      return HeadPart(req, key, bucket, objKey);
    if (req.verb == "POST")
      return CompleteMultipart(req, key, bucket, objKey);
    if (req.verb == "DELETE")
      return AbortMultipart(req, key, bucket, objKey);
    if (req.verb == "GET")
      return ListParts(req, key, bucket, objKey);
    return Fail(req, 501, "NotImplemented", "This multipart operation is not implemented");
  }

  if (!onlyBucket && query.count("acl")) {
    if (req.verb == "GET")
      return GetAcl(req, key, bucket, objKey, true);
    if (req.verb == "PUT")
      return PutAcl(req, key, bucket, objKey, true);
    return Fail(req, 501, "NotImplemented", "Changing an ACL is not implemented");
  }
  if (!onlyBucket && query.count("tagging"))
    return ObjectTagging(req, key, bucket, objKey);
  {
    const char *blocked[] = {"torrent", "restore", "select", "legal-hold", "retention",
                             "attributes", nullptr};
    for (int i = 0; blocked[i]; i++) {
      if (query.count(blocked[i]))
        return Fail(req, 501, "NotImplemented", "This object subresource is not implemented");
    }
  }

  if (req.verb == "PUT" && Hdr(req, "x-amz-copy-source"))
    return CopyObject(req, key, bucket, objKey);
  if (req.verb == "PUT")
    return PutObject(req, key, bucket, objKey);
  if (req.verb == "GET")
    return GetObject(req, key, bucket, objKey, false);
  if (req.verb == "HEAD")
    return GetObject(req, key, bucket, objKey, true);
  if (req.verb == "DELETE")
    return DeleteObject(req, key, bucket, objKey);
  return Fail(req, 501, "NotImplemented", "This S3 operation is not implemented");
}

int Ops::ListBuckets(XrdHttpExtReq &req, const KeyRecord &key) {
  std::vector<BucketRecord> recs;
  int err = 0;
  if (m_meta.ListUserBuckets(key.user, recs, err) != 0)
    return Fail(req, 500, "InternalError", "Could not list buckets");
  std::vector<ListedBucket> listed;
  for (const auto &rec : recs) {
    ListedBucket b;
    b.name = rec.name;
    b.created = static_cast<time_t>(rec.created);
    listed.push_back(b);
  }
  std::string body = ListBucketsXml(key.user, listed);
  std::string hdr = "Content-Type: application/xml";
  if (Reply(req, 200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::CreateBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  int acl = RejectAclChange(req);
  if (acl <= 0)
    return acl;
  if (Drain(req) != 0)
    return -1;
  BucketRecord existing;
  int err = 0;
  if (m_meta.LoadBucket(bucket, existing, err) == 0) {
    if (existing.owner == key.user)
      return Fail(req, 409, "BucketAlreadyOwnedByYou", "You already own this bucket");
    return Fail(req, 409, "BucketAlreadyExists", "The bucket belongs to another account");
  }
  if (err != ENOENT && err != 0 && err != EINVAL)
    return Fail(req, 500, "InternalError", "Could not read the bucket index");
  BucketRecord rec;
  rec.name = bucket;
  rec.owner = key.user;
  rec.created = static_cast<long long>(time(nullptr));
  CollectAcl(req, rec.acl, rec.grants);
  if (rec.acl.empty())
    rec.acl = "private";
  rec.region = m_cfg.region;
  if (m_meta.StoreBucket(rec, err) != 0)
    return Fail(req, 500, "InternalError", "Could not create the bucket");
  std::string hdr = "Location: /" + bucket;
  if (Reply(req, 200, nullptr, hdr.c_str(), nullptr, 0) != 0)
    return -1;
  return 0;
}

int Ops::DeleteBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  Client id(key.user);
  std::vector<std::string> names;
  int err = 0;
  std::string data = BucketData(m_cfg, key.user, bucket);
  if (m_meta.Store().List(data, id.get(), names, err) == 0 && !names.empty())
    return Fail(req, 409, "BucketNotEmpty", "The bucket still contains objects");
  if (err != 0 && err != ENOENT)
    return Fail(req, 500, "InternalError", "Could not inspect the bucket");
  if (m_meta.Store().Rmdir(data, id.get(), err) != 0)
    m_meta.Store().Rmdir(data, m_meta.MetaWho(), err);
  m_meta.RemoveBucket(bucket, key.user, err);
  if (Reply(req, 204, nullptr, nullptr, nullptr, 0) != 0)
    return -1;
  return 0;
}

int Ops::HeadBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  std::string hdr = "x-amz-bucket-region: " + rec.region;
  if (Reply(req, 200, nullptr, hdr.c_str(), nullptr, 0) != 0)
    return -1;
  return 0;
}

int Ops::ListObjects(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  auto query = ParseQuery(QueryOf(req));
  std::string prefix = query.count("prefix") ? query["prefix"] : std::string();
  std::string delimiter = query.count("delimiter") ? query["delimiter"] : std::string();
  bool v2 = query.count("list-type") && query["list-type"] == "2";
  std::string token;
  if (v2)
    token = query.count("continuation-token") ? query["continuation-token"] : std::string();
  else
    token = query.count("marker") ? query["marker"] : std::string();
  int maxKeys = 1000;
  if (query.count("max-keys")) {
    try {
      maxKeys = std::stoi(query["max-keys"]);
    } catch (...) {
      return Fail(req, 400, "InvalidArgument", "max-keys is not an integer");
    }
    if (maxKeys < 0 || maxKeys > 1000)
      return Fail(req, 400, "InvalidArgument", "max-keys must be between 0 and 1000");
  }

  Client id(key.user);
  std::vector<FileInfo> files;
  bool scanTruncated = false;
  int err = 0;
  Walk(m_meta.Store(), id.get(), BucketData(m_cfg, key.user, bucket), "", files, 100000,
       scanTruncated, err);
  if (err && err != ENOENT)
    return Fail(req, 500, "InternalError", "Could not list the bucket");
  std::sort(files.begin(), files.end(),
            [](const FileInfo &a, const FileInfo &b) { return a.key < b.key; });

  std::set<std::string> seen;
  std::vector<ListedObject> merged;
  for (const auto &file : files) {
    if (prefix.size() > file.key.size() || file.key.compare(0, prefix.size(), prefix) != 0)
      continue;
    ListedObject item;
    if (!delimiter.empty()) {
      size_t pos = file.key.find(delimiter, prefix.size());
      if (pos != std::string::npos) {
        item.common = true;
        item.key = file.key.substr(0, pos + delimiter.size());
        if (!seen.insert(item.key).second)
          continue;
        merged.push_back(item);
        continue;
      }
    }
    item.key = file.key;
    item.size = file.size;
    item.mtime = file.mtime;
    ObjectMeta meta;
    if (m_meta.ReadObjectMeta(key.user, bucket, file.key, meta) == 0)
      item.etag = meta.etag;
    merged.push_back(item);
  }
  std::sort(merged.begin(), merged.end(),
            [](const ListedObject &a, const ListedObject &b) { return a.key < b.key; });

  std::vector<ListedObject> page;
  bool more = false;
  for (const auto &item : merged) {
    if (!token.empty() && item.key <= token)
      continue;
    if (static_cast<int>(page.size()) >= maxKeys) {
      more = true;
      break;
    }
    page.push_back(item);
  }
  if (scanTruncated)
    more = true;
  std::string body = v2 ? ListObjectsXml(bucket, prefix, delimiter, maxKeys, page, more, token)
                       : ListObjectsV1Xml(bucket, prefix, delimiter, token, maxKeys, page, more);
  std::string hdr = "Content-Type: application/xml";
  if (Reply(req, 200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::PutObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                   const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  if (req.length < 0)
    return Fail(req, 411, "MissingContentLength", "Content-Length is required");
  int acl = RejectAclChange(req);
  if (acl <= 0)
    return acl;
  ObjectMeta meta;
  int metaRc = CollectUserMeta(req.headers, meta.user);
  if (metaRc == 1)
    return Fail(req, 400, "InvalidArgument", "A user metadata name or value is not valid");
  if (metaRc == 2)
    return Fail(req, 400, "MetadataTooLarge", "User metadata is larger than 2048 bytes");
  Client id(key.user);
  std::string path = ObjectFile(m_cfg, key.user, bucket, objKey);
  struct stat st;
  int err = 0;
  bool exists = m_meta.Store().Stat(path, id.get(), st, err) == 0 && S_ISREG(st.st_mode);
  if (!exists && err != 0 && err != ENOENT)
    return Fail(req, 500, "InternalError", "Could not stat the object");
  std::string oldEtag;
  if (exists) {
    ObjectMeta old;
    if (m_meta.ReadObjectMeta(key.user, bucket, objKey, old) == 0)
      oldEtag = old.etag;
  }
  int cond = EvalWriteCond(req.headers, exists, oldEtag);
  if (cond == 400)
    return Fail(req, 400, "InvalidArgument", "If-None-Match on PutObject must be *");
  if (cond == 412)
    return Fail(req, 412, "PreconditionFailed", "At least one of the preconditions you specified did not hold");
  const std::string *hash = Hdr(req, "x-amz-content-sha256");
  if (hash && hash->compare(0, 10, "STREAMING-") == 0 &&
      *hash != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" &&
      *hash != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" &&
      *hash != "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
    return Fail(req, 501, "NotImplemented", "This streaming payload encoding is not implemented");

  if (WantsContinue(req)) {
    if (Reply(req, 100, nullptr, nullptr, nullptr, 0) != 0)
      return -1;
  }

  std::string etag;
  int wrote = WriteObjectBytes(req, key, path, id.get(), etag);
  if (wrote <= 0)
    return wrote;

  meta.etag = etag;
  const std::string *ctype = Hdr(req, "content-type");
  meta.contentType = CleanType(ctype ? *ctype : "");
  CollectAcl(req, meta.acl, meta.grants);
  m_meta.WriteObjectMeta(key.user, bucket, objKey, meta, err);

  std::string hdr = "ETag: \"" + etag + "\"";
  if (Reply(req, 200, nullptr, hdr.c_str(), nullptr, 0) != 0)
    return -1;
  if (m_cfg.traceInfo)
    m_log.Say("S3", "PutObject", bucket.c_str(), objKey.c_str());
  return 0;
}

int Ops::GetObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                   const std::string &objKey, bool head) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  Client id(key.user);
  std::string path = ObjectFile(m_cfg, key.user, bucket, objKey);
  struct stat st;
  int err = 0;
  if (m_meta.Store().Stat(path, id.get(), st, err) != 0 || !S_ISREG(st.st_mode))
    return Fail(req, 404, "NoSuchKey", "The specified key does not exist");

  ObjectMeta meta;
  m_meta.ReadObjectMeta(key.user, bucket, objKey, meta);
  ReadCond cond = EvalReadCond(req.headers, meta.etag, st.st_mtime);
  if (cond == ReadCond::Failed)
    return Fail(req, 412, "PreconditionFailed",
                "At least one of the preconditions you specified did not hold");
  if (cond == ReadCond::NotModified) {
    std::string hdr = "ETag: \"" + meta.etag + "\"\r\nLast-Modified: " + HttpDate(st.st_mtime) +
                      UserMetaHeaders(meta.user);
    if (Reply(req, 304, nullptr, hdr.c_str(), nullptr, 0) != 0)
      return -1;
    return 0;
  }
  long long start = 0;
  long long end = st.st_size > 0 ? st.st_size - 1 : 0;
  bool partial = false;
  const std::string *range = Hdr(req, "range");
  if (range && !head) {
    if (!ParseRange(*range, st.st_size, start, end)) {
      std::string hdr = "Content-Range: bytes */" + std::to_string(static_cast<long long>(st.st_size));
      std::string body = ErrorXml("InvalidRange", "The requested range is not satisfiable", req.resource);
      std::string all = hdr + "\r\nContent-Type: application/xml";
      if (Reply(req, 416, nullptr, all.c_str(), body.data(),
                             static_cast<long long>(body.size())) != 0)
        return -1;
      return 0;
    }
    partial = true;
  }
  long long send = partial ? (end - start + 1) : static_cast<long long>(st.st_size);
  auto query = ParseQuery(QueryOf(req));
  std::string contentType = meta.contentType;
  std::string extra;
  const char *overrides[][2] = {
      {"response-content-type", ""},
      {"response-content-disposition", "Content-Disposition"},
      {"response-cache-control", "Cache-Control"},
      {"response-content-encoding", "Content-Encoding"},
      {"response-content-language", "Content-Language"},
      {"response-expires", "Expires"},
      {nullptr, nullptr}};
  for (int i = 0; overrides[i][0]; i++) {
    auto it = query.find(overrides[i][0]);
    if (it == query.end() || it->second.empty())
      continue;
    if (!HeaderValueOk(it->second))
      return Fail(req, 400, "InvalidArgument", "A response header override is not valid");
    if (std::string(overrides[i][0]) == "response-content-type")
      contentType = it->second;
    else
      extra += "\r\n" + std::string(overrides[i][1]) + ": " + it->second;
  }
  std::string hdr = "Accept-Ranges: bytes\r\nContent-Type: " + contentType +
                    "\r\nLast-Modified: " + HttpDate(st.st_mtime) + extra;
  if (!meta.etag.empty())
    hdr += "\r\nETag: \"" + meta.etag + "\"";
  hdr += UserMetaHeaders(meta.user);
  if (partial)
    hdr += "\r\nContent-Range: bytes " + std::to_string(start) + "-" + std::to_string(end) + "/" +
           std::to_string(static_cast<long long>(st.st_size));

  int code = partial ? 206 : 200;
  if (head || send == 0) {
    if (Reply(req, code, nullptr, hdr.c_str(), nullptr, send) != 0)
      return -1;
    return 0;
  }
  OwnedFile file;
  if (m_meta.Store().Open(path, id.get(), SFS_O_RDONLY, 0, file, err) != 0)
    return Fail(req, 500, "InternalError", "Could not open the object");
  std::string sendHdr = WithCors(req, hdr);
  if (req.StartSimpleResp(code, nullptr, sendHdr.c_str(), send, true) != 0)
    return -1;
  long long left = send;
  XrdSfsFileOffset off = start;
  std::vector<char> buf(65536);
  while (left > 0) {
    int want = static_cast<int>(std::min<long long>(left, static_cast<long long>(buf.size())));
    int n = file.fh->read(off, buf.data(), want);
    if (n < 0)
      return -1;
    if (n == 0)
      break;
    if (req.SendData(buf.data(), n) != 0)
      return -1;
    off += n;
    left -= n;
  }
  return 0;
}

int Ops::DeleteObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                      const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  Client id(key.user);
  int err = 0;
  int rc = m_meta.Store().Unlink(ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), err);
  if (rc != 0 && err != ENOENT)
    return Fail(req, 500, "InternalError", "Could not delete the object");
  m_meta.RemoveObjectMeta(key.user, bucket, objKey);
  if (Reply(req, 204, nullptr, nullptr, nullptr, 0) != 0)
    return -1;
  return 0;
}

}
