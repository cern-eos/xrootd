#include "XrdHttpS3Ops.hh"

#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdHttpS3Client.hh"
#include "XrdHttpS3Fs.hh"
#include "XrdHttpS3Paths.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include <algorithm>
#include <ctime>
#include <openssl/evp.h>
#include <sys/stat.h>

namespace XrdHttpS3 {
namespace {

const std::string *Hdr(const XrdHttpExtReq &req, const char *name) {
  auto it = req.headers.find(name);
  if (it == req.headers.end())
    return nullptr;
  return &it->second;
}

std::string B64(const unsigned char *p, unsigned n) {
  static const char t[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  for (unsigned i = 0; i < n; i += 3) {
    unsigned v = static_cast<unsigned>(p[i]) << 16;
    if (i + 1 < n)
      v |= static_cast<unsigned>(p[i + 1]) << 8;
    if (i + 2 < n)
      v |= static_cast<unsigned>(p[i + 2]);
    out.push_back(t[(v >> 18) & 63]);
    out.push_back(t[(v >> 12) & 63]);
    out.push_back(i + 1 < n ? t[(v >> 6) & 63] : '=');
    out.push_back(i + 2 < n ? t[v & 63] : '=');
  }
  return out;
}

std::string ContentMd5(const std::string &body) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int n = 0;
  if (!EVP_Digest(body.data(), body.size(), md, &n, EVP_md5(), nullptr))
    return std::string();
  return B64(md, n);
}

int ReadBody(XrdHttpExtReq &req, std::string &body) {
  body.clear();
  if (req.length < 0 || req.length > 256 * 1024)
    return -2;
  long long left = req.length;
  while (left > 0) {
    char *data = nullptr;
    int want = static_cast<int>(std::min<long long>(left, 65536));
    int n = req.BuffgetData(want, &data, true);
    if (n <= 0)
      return -1;
    body.append(data, static_cast<size_t>(n));
    left -= n;
  }
  return 0;
}

Archive ReadAttrs(Fs &fs, const std::string &path, const XrdSecEntity *who, bool offline) {
  std::string ongoing;
  std::string expiry;
  int err = 0;
  if (fs.GetXattr(path, who, kRestoreOngoing, ongoing, err) != 0)
    ongoing.clear();
  if (fs.GetXattr(path, who, kRestoreExpiry, expiry, err) != 0)
    expiry.clear();
  return ClassifyRestore(offline, ongoing, expiry, time(nullptr));
}

void Settle(Fs &fs, const std::string &path, const XrdSecEntity *who, bool offline, Archive &arc) {
  int err = 0;
  if (arc.kind == ArchiveKind::Expired && offline) {
    fs.DelXattr(path, who, kRestoreOngoing, err);
    fs.DelXattr(path, who, kRestoreExpiry, err);
    arc.kind = ArchiveKind::Archived;
    arc.ongoing = false;
    arc.expiry = 0;
    return;
  }
  if (arc.kind == ArchiveKind::Expired) {
    fs.Prepare(path, who, Prep_EVICT, err);
    return;
  }
  if (arc.kind == ArchiveKind::Restored && arc.ongoing)
    fs.SetXattr(path, who, kRestoreOngoing, "false", err);
}

}

void Ops::ClearRestore(const std::string &path, const XrdSecEntity *who) {
  int err = 0;
  m_meta.Store().DelXattr(path, who, kRestoreOngoing, err);
  m_meta.Store().DelXattr(path, who, kRestoreExpiry, err);
}

Archive Ops::LoadArchive(const std::string &path, const XrdSecEntity *who, bool offline) {
  Archive arc = ReadAttrs(m_meta.Store(), path, who, offline);
  Settle(m_meta.Store(), path, who, offline, arc);
  return arc;
}

int Ops::RestoreObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                       const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  const std::string *exp = Hdr(req, "expect");
  if (exp && exp->find("100-continue") != std::string::npos) {
    if (Reply(req, 100, nullptr, nullptr, nullptr, 0) != 0)
      return -1;
  }
  Client id(key.user);
  std::string path = ObjectFile(m_cfg, key.user, bucket, objKey);
  struct stat st;
  int err = 0;
  if (m_meta.Store().Stat(path, id.get(), st, err) != 0 || !S_ISREG(st.st_mode))
    return Fail(req, 404, "NoSuchKey", "The specified key does not exist");
  std::string body;
  int got = ReadBody(req, body);
  if (got == -2)
    return Fail(req, 400, "InvalidArgument", "The restore document is too large");
  if (got != 0)
    return -1;
  const std::string *md5 = Hdr(req, "content-md5");
  if (md5 && *md5 != ContentMd5(body))
    return Fail(req, 400, "InvalidDigest", "Content-MD5 does not match");
  RestoreRequest parsed;
  RestoreXml xml = ParseRestoreRequest(body, parsed);
  if (xml == RestoreXml::Unsupported)
    return Fail(req, 501, "NotImplemented", "Select restore is not implemented");
  if (xml == RestoreXml::BadTier)
    return Fail(req, 400, "InvalidArgument", "The restore tier is not valid");
  if (xml != RestoreXml::Ok)
    return Fail(req, 400, "MalformedXML", "The restore request must contain Days");

  bool offline = FileOffline(st);
  Archive arc = ReadAttrs(m_meta.Store(), path, id.get(), offline);
  time_t expiry = time(nullptr) + static_cast<time_t>(parsed.days) * 86400;
  std::string expText = RestoreDate(expiry);

  if (arc.kind == ArchiveKind::Standard)
    return Fail(req, 403, "InvalidObjectState",
                "The operation is not valid for the object's storage class");
  if (arc.kind == ArchiveKind::Restoring)
    return Fail(req, 409, "RestoreAlreadyInProgress", "Object restore is already in progress");
  if (arc.kind == ArchiveKind::Expired && offline) {
    ClearRestore(path, id.get());
    arc.kind = ArchiveKind::Archived;
  }
  if (arc.kind == ArchiveKind::Restored || arc.kind == ArchiveKind::Expired) {
    if (m_meta.Store().SetXattr(path, id.get(), kRestoreExpiry, expText, err) != 0 ||
        m_meta.Store().SetXattr(path, id.get(), kRestoreOngoing, "false", err) != 0)
      return Fail(req, 500, "InternalError", "Could not record the restore expiry");
    return Reply(req, 200, nullptr, nullptr, nullptr, 0);
  }

  if (m_meta.Store().SetXattr(path, id.get(), kRestoreOngoing, "true", err) != 0 ||
      m_meta.Store().SetXattr(path, id.get(), kRestoreExpiry, expText, err) != 0)
    return Fail(req, 500, "InternalError", "Could not record the restore request");
  if (m_meta.Store().Prepare(path, id.get(), Prep_STAGE | parsed.priority, err) != 0) {
    ClearRestore(path, id.get());
    return Fail(req, 500, "InternalError", "Could not prepare the object for recall");
  }
  return Reply(req, 202, nullptr, nullptr, nullptr, 0);
}

}
