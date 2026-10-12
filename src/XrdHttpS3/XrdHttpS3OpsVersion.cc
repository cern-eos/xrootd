#include "XrdHttpS3Ops.hh"

#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdHttpS3Client.hh"
#include "XrdHttpS3Fs.hh"
#include "XrdHttpS3Paths.hh"
#include "XrdHttpS3SigV4.hh"
#include "XrdHttpS3Xml.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include <openssl/evp.h>
#include <openssl/rand.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <set>
#include <sys/stat.h>
#include <vector>

namespace XrdHttpS3 {
namespace {

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

std::string TrimLine(std::string s) {
  while (!s.empty() && (s.back() == '\n' || s.back() == '\r' || s.back() == ' '))
    s.pop_back();
  return s;
}

std::string B64(const unsigned char *p, size_t n) {
  static const char *t =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  for (size_t i = 0; i < n; i += 3) {
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

std::vector<std::string> TagTexts(const std::string &xml, const std::string &tag) {
  std::string open = "<" + tag + ">";
  std::string close = "</" + tag + ">";
  std::vector<std::string> out;
  size_t i = 0;
  while (i < xml.size()) {
    size_t a = xml.find(open, i);
    if (a == std::string::npos)
      break;
    a += open.size();
    size_t b = xml.find(close, a);
    if (b == std::string::npos)
      break;
    out.push_back(xml.substr(a, b - a));
    i = b + close.size();
  }
  return out;
}

std::string Block(const std::string &xml, const std::string &tag) {
  std::string open = "<" + tag;
  size_t a = xml.find(open);
  if (a == std::string::npos)
    return std::string();
  size_t b = xml.find("</" + tag + ">", a);
  if (b == std::string::npos)
    return std::string();
  return xml.substr(a, b - a);
}

bool ValidVersionId(const std::string &id) {
  if (id == "null")
    return true;
  if (id.size() < 8 || id.size() > 64)
    return false;
  for (unsigned char c : id) {
    if (!std::isalnum(c))
      return false;
  }
  return true;
}

std::string NewSeq() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  unsigned long long n = static_cast<unsigned long long>(ts.tv_sec) * 1000000000ull +
                         static_cast<unsigned long long>(ts.tv_nsec);
  char buf[32];
  snprintf(buf, sizeof(buf), "%019llu", n);
  return buf;
}

std::string NewVersionId() {
  unsigned char r[2] = {0, 0};
  RAND_bytes(r, 2);
  char hex[8];
  snprintf(hex, sizeof(hex), "%02x%02x", r[0], r[1]);
  return NewSeq() + hex;
}

std::string DisplayKey(const std::string &rel) {
  if (rel.size() >= 7 && rel.compare(rel.size() - 7, 7, ".s3dir") == 0)
    return rel.substr(0, rel.size() - 7) + "/";
  return rel;
}

int CopyBytes(Fs &fs, const std::string &src, const XrdSecEntity *srcWho, const std::string &dst,
              const XrdSecEntity *dstWho, int &err) {
  struct stat st;
  if (fs.Stat(src, srcWho, st, err) != 0)
    return -1;
  OwnedFile in;
  OwnedFile out;
  if (fs.Open(src, srcWho, SFS_O_RDONLY, 0, in, err) != 0)
    return -1;
  int flags = SFS_O_TRUNC | SFS_O_WRONLY | SFS_O_MKPTH;
  if (fs.Open(dst, dstWho, flags, 0644, out, err) != 0)
    return -1;
  std::vector<char> buf(65536);
  XrdSfsFileOffset off = 0;
  while (off < static_cast<XrdSfsFileOffset>(st.st_size)) {
    int want = static_cast<int>(std::min<long long>(st.st_size - off, 65536));
    int n = in.fh->read(off, buf.data(), want);
    if (n <= 0) {
      err = EIO;
      return -1;
    }
    int wrote = 0;
    while (wrote < n) {
      int w = out.fh->write(off + wrote, buf.data() + wrote, n - wrote);
      if (w <= 0) {
        err = EIO;
        return -1;
      }
      wrote += w;
    }
    off += n;
  }
  err = 0;
  return 0;
}

int StreamFile(XrdHttpExtReq &req, Fs &fs, const std::string &path, const XrdSecEntity *who,
               int code, const std::string &hdr, long long start, long long send, bool head) {
  if (head || send == 0) {
    if (req.SendSimpleResp(code, nullptr, hdr.c_str(), nullptr, send) != 0)
      return -1;
    return 0;
  }
  OwnedFile file;
  int err = 0;
  if (fs.Open(path, who, SFS_O_RDONLY, 0, file, err) != 0)
    return -1;
  if (req.StartSimpleResp(code, nullptr, hdr.c_str(), send, true) != 0)
    return -1;
  std::vector<char> buf(65536);
  long long left = send;
  XrdSfsFileOffset off = start;
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

std::string TypeFor(const std::string &key) {
  size_t dot = key.rfind('.');
  std::string ext = dot == std::string::npos ? std::string() : key.substr(dot + 1);
  for (char &c : ext)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (ext == "html" || ext == "htm")
    return "text/html";
  if (ext == "css")
    return "text/css";
  if (ext == "js")
    return "application/javascript";
  if (ext == "png")
    return "image/png";
  if (ext == "jpg" || ext == "jpeg")
    return "image/jpeg";
  if (ext == "gif")
    return "image/gif";
  if (ext == "svg")
    return "image/svg+xml";
  if (ext == "txt")
    return "text/plain";
  return "application/octet-stream";
}

struct Route {
  std::string prefix;
  std::string errorCode;
  std::string replaceKey;
  std::string replacePrefix;
  std::string host;
  std::string protocol;
  int status{301};
};

std::vector<Route> RoutesOf(const std::string &xml) {
  std::vector<Route> out;
  size_t i = 0;
  while (i < xml.size()) {
    size_t a = xml.find("<RoutingRule", i);
    if (a == std::string::npos)
      break;
    size_t b = xml.find("</RoutingRule>", a);
    if (b == std::string::npos)
      break;
    std::string block = xml.substr(a, b - a);
    Route rule;
    auto prefix = TagTexts(block, "KeyPrefixEquals");
    auto code = TagTexts(block, "HttpErrorCodeReturnedEquals");
    auto replace = TagTexts(block, "ReplaceKeyWith");
    auto replacePre = TagTexts(block, "ReplaceKeyPrefixWith");
    auto host = TagTexts(block, "HostName");
    auto proto = TagTexts(block, "Protocol");
    auto status = TagTexts(block, "HttpRedirectCode");
    if (!prefix.empty())
      rule.prefix = prefix[0];
    if (!code.empty())
      rule.errorCode = code[0];
    if (!replace.empty())
      rule.replaceKey = replace[0];
    if (!replacePre.empty())
      rule.replacePrefix = replacePre[0];
    if (!host.empty())
      rule.host = host[0];
    if (!proto.empty())
      rule.protocol = proto[0];
    if (!status.empty())
      rule.status = std::atoi(status[0].c_str());
    if (rule.status != 301 && rule.status != 302 && rule.status != 307 && rule.status != 308)
      rule.status = 301;
    out.push_back(rule);
    i = b + 14;
  }
  return out;
}

bool PrefixMatch(const Route &rule, const std::string &key) {
  return rule.prefix.empty() || key.compare(0, rule.prefix.size(), rule.prefix) == 0;
}

std::string LocationFor(const Route &rule, const std::string &key) {
  std::string next = key;
  if (!rule.replaceKey.empty())
    next = rule.replaceKey;
  else if (!rule.replacePrefix.empty() && key.compare(0, rule.prefix.size(), rule.prefix) == 0)
    next = rule.replacePrefix + key.substr(rule.prefix.size());
  if (!rule.host.empty()) {
    std::string proto = rule.protocol.empty() ? "http" : rule.protocol;
    return proto + "://" + rule.host + "/" + next;
  }
  return "/" + next;
}

int ReadLimited(XrdHttpExtReq &req, std::string &body) {
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

}

std::string Ops::CurrentVersion(const std::string &user, const std::string &bucket,
                                const std::string &objKey) {
  std::string body;
  int err = 0;
  if (m_meta.Store().ReadAll(JoinPath(VersionKeyDir(m_cfg, user, bucket, objKey), "latest"),
                             m_meta.MetaWho(), body, err) != 0)
    return std::string();
  return TrimLine(body);
}

int Ops::RecordVersion(const std::string &user, const std::string &bucket,
                       const std::string &objKey, const ObjectMeta &meta,
                       const std::string &livePath, std::string &versionId) {
  versionId.clear();
  BucketRecord rec;
  int err = 0;
  if (m_meta.LoadBucket(bucket, rec, err) != 0)
    return -1;
  if (rec.versioning != "Enabled" && rec.versioning != "Suspended")
    return 0;
  versionId = rec.versioning == "Suspended" ? "null" : NewVersionId();
  std::string dir = VersionKeyDir(m_cfg, user, bucket, objKey);
  std::string bytes = JoinPath(dir, versionId);
  if (CopyBytes(m_meta.Store(), livePath, Client(user).get(), bytes, m_meta.MetaWho(), err) != 0)
    return -1;
  struct stat st;
  m_meta.Store().Stat(bytes, m_meta.MetaWho(), st, err);
  std::map<std::string, std::string> props;
  props["etag"] = meta.etag;
  props["content-type"] = meta.contentType;
  props["acl"] = meta.acl.empty() ? "private" : meta.acl;
  props["delete-marker"] = "0";
  props["seq"] = versionId == "null" ? NewSeq() : versionId.substr(0, 19);
  props["size"] = std::to_string(static_cast<long long>(st.st_size));
  props["mtime"] = std::to_string(static_cast<long long>(time(nullptr)));
  for (const auto &kv : meta.user)
    props["user." + kv.first] = kv.second;
  for (const auto &kv : meta.grants)
    props["grant-" + kv.first] = kv.second;
  if (m_meta.Store().WriteAll(bytes + ".props", m_meta.MetaWho(), FormatProps(props), 0644, err) !=
      0)
    return -1;
  if (m_meta.Store().WriteAll(JoinPath(dir, "latest"), m_meta.MetaWho(), versionId + "\n", 0644,
                             err) != 0)
    return -1;
  return 0;
}

int Ops::MakeDeleteMarker(const std::string &user, const std::string &bucket,
                          const std::string &objKey, const std::string &mode,
                          std::string &versionId) {
  versionId = mode == "Suspended" ? "null" : NewVersionId();
  std::string dir = VersionKeyDir(m_cfg, user, bucket, objKey);
  std::string bytes = JoinPath(dir, versionId);
  std::map<std::string, std::string> props;
  props["delete-marker"] = "1";
  props["seq"] = versionId == "null" ? NewSeq() : versionId.substr(0, 19);
  props["mtime"] = std::to_string(static_cast<long long>(time(nullptr)));
  props["size"] = "0";
  int err = 0;
  if (m_meta.Store().WriteAll(bytes + ".props", m_meta.MetaWho(), FormatProps(props), 0644, err) !=
      0)
    return -1;
  m_meta.Store().Unlink(bytes, m_meta.MetaWho(), err);
  if (m_meta.Store().WriteAll(JoinPath(dir, "latest"), m_meta.MetaWho(), versionId + "\n", 0644,
                             err) != 0)
    return -1;
  Client id(user);
  m_meta.Store().Unlink(ObjectFile(m_cfg, user, bucket, objKey), id.get(), err);
  m_meta.RemoveObjectMeta(user, bucket, objKey);
  return 0;
}

int Ops::BucketVersioning(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  if (req.verb == "GET") {
    std::string body = VersioningXml(rec.versioning);
    std::string hdr = "Content-Type: application/xml";
    return Reply(req, 200, nullptr, hdr.c_str(), body.data(), static_cast<long long>(body.size()));
  }
  if (req.verb != "PUT")
    return Fail(req, 405, "MethodNotAllowed", "This method is not valid for versioning");
  std::string body;
  int got = ReadLimited(req, body);
  if (got == -2)
    return Fail(req, 400, "InvalidArgument", "The versioning document is too large");
  if (got != 0)
    return -1;
  const std::string *md5 = Hdr(req, "content-md5");
  if (!md5 || *md5 != ContentMd5(body))
    return Fail(req, 400, "InvalidDigest", "Content-MD5 is missing or does not match");
  auto status = TagTexts(body, "Status");
  if (status.size() != 1 || (status[0] != "Enabled" && status[0] != "Suspended"))
    return Fail(req, 400, "MalformedXML", "Versioning status must be Enabled or Suspended");
  rec.versioning = status[0];
  int err = 0;
  if (m_meta.StoreBucket(rec, err) != 0)
    return Fail(req, 500, "InternalError", "Could not store the versioning configuration");
  return Reply(req, 200, nullptr, nullptr, nullptr, 0);
}

int Ops::ReadVersion(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                     const std::string &objKey, const std::string &versionId, bool head) {
  if (!ValidVersionId(versionId))
    return Fail(req, 400, "InvalidArgument", "The version id is not valid");
  std::string propsBody;
  int err = 0;
  std::string bytes = JoinPath(VersionKeyDir(m_cfg, key.user, bucket, objKey), versionId);
  if (m_meta.Store().ReadAll(bytes + ".props", m_meta.MetaWho(), propsBody, err) != 0)
    return Fail(req, 404, "NoSuchVersion", "The specified version does not exist");
  auto props = ParseProps(propsBody);
  std::string hdr = "x-amz-version-id: " + versionId;
  if (props["delete-marker"] == "1") {
    hdr += "\r\nx-amz-delete-marker: true\r\nContent-Type: application/xml";
    std::string body = ErrorXml("MethodNotAllowed", "The specified version is a delete marker",
                                req.resource);
    return Reply(req, 405, nullptr, hdr.c_str(), body.data(), static_cast<long long>(body.size()));
  }
  struct stat st;
  if (m_meta.Store().Stat(bytes, m_meta.MetaWho(), st, err) != 0)
    return Fail(req, 404, "NoSuchVersion", "The specified version does not exist");
  std::string etag = props["etag"];
  time_t mtime = props.count("mtime") ? static_cast<time_t>(std::atoll(props["mtime"].c_str()))
                                      : st.st_mtime;
  ReadCond cond = EvalReadCond(req.headers, etag, mtime);
  hdr += "\r\nAccept-Ranges: bytes\r\nLast-Modified: " + HttpDate(mtime);
  if (!etag.empty())
    hdr += "\r\nETag: \"" + etag + "\"";
  std::string ctype = props["content-type"].empty() ? "application/octet-stream" : props["content-type"];
  hdr += "\r\nContent-Type: " + ctype;
  if (cond == ReadCond::Failed)
    return Fail(req, 412, "PreconditionFailed",
                "At least one of the preconditions you specified did not hold");
  if (cond == ReadCond::NotModified)
    return Reply(req, 304, nullptr, hdr.c_str(), nullptr, 0);
  long long start = 0;
  long long send = static_cast<long long>(st.st_size);
  return StreamFile(req, m_meta.Store(), bytes, m_meta.MetaWho(), 200, hdr, start, send, head);
}

int Ops::DeleteOneVersion(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                          const std::string &objKey, const std::string &versionId) {
  if (!ValidVersionId(versionId))
    return Fail(req, 400, "InvalidArgument", "The version id is not valid");
  std::string dir = VersionKeyDir(m_cfg, key.user, bucket, objKey);
  std::string bytes = JoinPath(dir, versionId);
  std::string propsBody;
  int err = 0;
  if (m_meta.Store().ReadAll(bytes + ".props", m_meta.MetaWho(), propsBody, err) != 0)
    return Fail(req, 404, "NoSuchVersion", "The specified version does not exist");
  bool marker = ParseProps(propsBody)["delete-marker"] == "1";
  std::string latest = CurrentVersion(key.user, bucket, objKey);
  m_meta.Store().Unlink(bytes, m_meta.MetaWho(), err);
  m_meta.Store().Unlink(bytes + ".props", m_meta.MetaWho(), err);
  if (latest == versionId) {
    std::vector<std::string> names;
    std::string bestId;
    std::string bestSeq;
    if (m_meta.Store().List(dir, m_meta.MetaWho(), names, err) == 0) {
      for (const std::string &name : names) {
        if (name.size() <= 6 || name.compare(name.size() - 6, 6, ".props") != 0)
          continue;
        std::string id = name.substr(0, name.size() - 6);
        if (id == versionId)
          continue;
        std::string one;
        if (m_meta.Store().ReadAll(JoinPath(dir, name), m_meta.MetaWho(), one, err) != 0)
          continue;
        std::string seq = ParseProps(one)["seq"];
        if (bestId.empty() || seq > bestSeq) {
          bestId = id;
          bestSeq = seq;
        }
      }
    }
    Client id(key.user);
    if (bestId.empty()) {
      m_meta.Store().Unlink(JoinPath(dir, "latest"), m_meta.MetaWho(), err);
      m_meta.Store().Unlink(ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), err);
      m_meta.RemoveObjectMeta(key.user, bucket, objKey);
    } else {
      std::string prevBody;
      m_meta.Store().ReadAll(JoinPath(dir, bestId + ".props"), m_meta.MetaWho(), prevBody, err);
      auto prev = ParseProps(prevBody);
      m_meta.Store().WriteAll(JoinPath(dir, "latest"), m_meta.MetaWho(), bestId + "\n", 0644, err);
      if (prev["delete-marker"] == "1") {
        m_meta.Store().Unlink(ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), err);
        m_meta.RemoveObjectMeta(key.user, bucket, objKey);
      } else if (CopyBytes(m_meta.Store(), JoinPath(dir, bestId), m_meta.MetaWho(),
                           ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), err) == 0) {
        ObjectMeta meta;
        meta.etag = prev["etag"];
        meta.contentType = prev["content-type"].empty() ? "application/octet-stream"
                                                         : prev["content-type"];
        meta.acl = prev["acl"].empty() ? "private" : prev["acl"];
        for (const auto &kv : prev) {
          if (kv.first.compare(0, 5, "user.") == 0 && kv.first.size() > 5)
            meta.user[kv.first.substr(5)] = kv.second;
          if (kv.first.compare(0, 6, "grant-") == 0 && kv.first.size() > 6)
            meta.grants[kv.first.substr(6)] = kv.second;
        }
        m_meta.WriteObjectMeta(key.user, bucket, objKey, meta, err);
      }
    }
  }
  std::string hdr = "x-amz-version-id: " + versionId;
  if (marker)
    hdr += "\r\nx-amz-delete-marker: true";
  return Reply(req, 204, nullptr, hdr.c_str(), nullptr, 0);
}

int Ops::ListObjectVersions(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  auto query = ParseQuery(QueryOf(req));
  std::string prefix = query.count("prefix") ? query["prefix"] : std::string();
  std::string delimiter = query.count("delimiter") ? query["delimiter"] : std::string();
  std::string keyMarker = query.count("key-marker") ? query["key-marker"] : std::string();
  std::string versionMarker =
      query.count("version-id-marker") ? query["version-id-marker"] : std::string();
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

  struct Item {
    std::string key;
    std::string id;
    std::string seq;
    bool marker{false};
    long long size{0};
    time_t mtime{0};
    std::string etag;
    std::string latest;
  };
  std::vector<Item> items;
  std::string root = VersionRoot(m_cfg, key.user, bucket);
  std::vector<std::pair<std::string, std::string>> stack;
  stack.push_back({root, ""});
  int guard = 0;
  while (!stack.empty() && guard < 100000) {
    guard++;
    std::string dir = stack.back().first;
    std::string rel = stack.back().second;
    stack.pop_back();
    std::vector<std::string> names;
    int err = 0;
    if (m_meta.Store().List(dir, m_meta.MetaWho(), names, err) != 0)
      continue;
    std::string latest;
    for (const std::string &name : names) {
      if (name == "latest") {
        std::string body;
        if (m_meta.Store().ReadAll(JoinPath(dir, name), m_meta.MetaWho(), body, err) == 0)
          latest = TrimLine(body);
      }
    }
    for (const std::string &name : names) {
      if (name.size() > 6 && name.compare(name.size() - 6, 6, ".props") == 0 && !rel.empty()) {
        std::string body;
        if (m_meta.Store().ReadAll(JoinPath(dir, name), m_meta.MetaWho(), body, err) != 0)
          continue;
        auto props = ParseProps(body);
        Item item;
        item.key = DisplayKey(rel);
        item.id = name.substr(0, name.size() - 6);
        item.seq = props["seq"];
        item.marker = props["delete-marker"] == "1";
        item.size = std::atoll(props["size"].c_str());
        item.mtime = static_cast<time_t>(std::atoll(props["mtime"].c_str()));
        item.etag = props["etag"];
        item.latest = latest;
        items.push_back(item);
        continue;
      }
      if (name == "latest" || (name.size() > 6 && name.compare(name.size() - 6, 6, ".props") == 0))
        continue;
      struct stat st;
      std::string child = JoinPath(dir, name);
      if (m_meta.Store().Stat(child, m_meta.MetaWho(), st, err) == 0 && S_ISDIR(st.st_mode)) {
        std::string next = rel.empty() ? name : rel + "/" + name;
        stack.push_back({child, next});
      }
    }
  }
  std::set<std::string> have;
  for (const auto &item : items)
    have.insert(item.key);
  std::vector<std::pair<std::string, std::string>> live;
  live.push_back({BucketData(m_cfg, key.user, bucket), ""});
  while (!live.empty() && guard < 100000) {
    guard++;
    std::string dir = live.back().first;
    std::string rel = live.back().second;
    live.pop_back();
    std::vector<std::string> names;
    int err = 0;
    if (m_meta.Store().List(dir, Client(key.user).get(), names, err) != 0)
      continue;
    Client owner(key.user);
    for (const std::string &name : names) {
      if (name == ".s3" || name == "." || name == "..")
        continue;
      std::string child = JoinPath(dir, name);
      struct stat st;
      if (m_meta.Store().Stat(child, owner.get(), st, err) != 0)
        continue;
      if (S_ISDIR(st.st_mode)) {
        live.push_back({child, rel.empty() ? name : rel + "/" + name});
        continue;
      }
      if (!S_ISREG(st.st_mode))
        continue;
      std::string objKey = name == ".s3folder" ? (rel.empty() ? std::string() : rel + "/")
                                               : (rel.empty() ? name : rel + "/" + name);
      if (objKey.empty() || have.count(objKey))
        continue;
      Item item;
      item.key = objKey;
      item.id = "null";
      item.seq = "0";
      item.latest = "null";
      item.size = static_cast<long long>(st.st_size);
      item.mtime = st.st_mtime;
      ObjectMeta meta;
      if (m_meta.ReadObjectMeta(key.user, bucket, objKey, meta) == 0)
        item.etag = meta.etag;
      items.push_back(item);
      have.insert(objKey);
    }
  }
  std::sort(items.begin(), items.end(), [](const Item &a, const Item &b) {
    if (a.key != b.key)
      return a.key < b.key;
    return a.seq > b.seq;
  });

  std::vector<ListedVersion> page;
  std::string nextKey;
  std::string nextVersion;
  bool truncated = false;
  bool skipping = !keyMarker.empty();
  std::map<std::string, bool> seenPrefix;
  for (size_t i = 0; i < items.size(); i++) {
    const Item &item = items[i];
    if (skipping) {
      if (item.key < keyMarker)
        continue;
      if (item.key == keyMarker && item.id != versionMarker)
        continue;
      if (item.key == keyMarker && item.id == versionMarker) {
        skipping = false;
        continue;
      }
      skipping = false;
    }
    if (prefix.size() > item.key.size() || item.key.compare(0, prefix.size(), prefix) != 0)
      continue;
    std::string shown = item.key;
    bool common = false;
    if (!delimiter.empty()) {
      size_t pos = item.key.find(delimiter, prefix.size());
      if (pos != std::string::npos) {
        shown = item.key.substr(0, pos + delimiter.size());
        common = true;
        if (seenPrefix[shown])
          continue;
        seenPrefix[shown] = true;
      }
    }
    if (static_cast<int>(page.size()) >= maxKeys) {
      truncated = true;
      break;
    }
    ListedVersion row;
    row.key = shown;
    row.common = common;
    if (!common) {
      row.id = item.id;
      row.latest = item.id == item.latest;
      row.marker = item.marker;
      row.size = item.size;
      row.mtime = item.mtime;
      row.etag = item.etag;
    }
    page.push_back(row);
    nextKey = shown;
    nextVersion = common ? std::string() : item.id;
  }
  std::string body = ListVersionsXml(bucket, prefix, keyMarker, versionMarker, maxKeys, page,
                                     truncated, nextKey, nextVersion);
  std::string hdr = "Content-Type: application/xml";
  return Reply(req, 200, nullptr, hdr.c_str(), body.data(), static_cast<long long>(body.size()));
}

int Ops::Website(XrdHttpExtReq &req) {
  if (req.verb != "GET" && req.verb != "HEAD")
    return Fail(req, 405, "MethodNotAllowed", "The website endpoint accepts GET and HEAD");
  std::string bucket;
  const std::string *host = Hdr(req, "host");
  if (!host || !HostLabel(*host, m_cfg.website, bucket) || !ValidBucket(bucket))
    return Fail(req, 400, "InvalidBucketName", "The bucket name is not valid");
  BucketRecord rec;
  int err = 0;
  if (m_meta.LoadBucket(bucket, rec, err) != 0)
    return Fail(req, 404, "NoSuchBucket", "The specified bucket does not exist");
  std::string xml;
  if (m_meta.Store().ReadAll(BucketDoc(m_cfg, rec.owner, bucket, "website"), m_meta.MetaWho(), xml,
                             err) != 0 ||
      xml.empty())
    return Fail(req, 404, "NoSuchWebsiteConfiguration", "The bucket has no website configuration");

  std::string index = "index.html";
  auto suffixes = TagTexts(Block(xml, "IndexDocument"), "Suffix");
  if (!suffixes.empty() && !suffixes[0].empty())
    index = suffixes[0];
  std::string errorKey;
  auto errors = TagTexts(Block(xml, "ErrorDocument"), "Key");
  if (!errors.empty())
    errorKey = errors[0];
  std::vector<Route> routes = RoutesOf(xml);

  std::string key = req.resource.empty() ? std::string() : req.resource;
  if (!key.empty() && key[0] == '/')
    key.erase(key.begin());
  key = PercentDecode(key);
  for (const auto &rule : routes) {
    if (!rule.errorCode.empty() || !PrefixMatch(rule, key))
      continue;
    std::string hdr = "Location: " + LocationFor(rule, key);
    return Reply(req, rule.status, nullptr, hdr.c_str(), nullptr, 0);
  }
  Client owner(rec.owner);
  if (!key.empty() && key.back() != '/') {
    struct stat st;
    std::string folder = ObjectFile(m_cfg, rec.owner, bucket, key + "/");
    bool dir = m_meta.Store().Stat(ObjectFile(m_cfg, rec.owner, bucket, key), owner.get(), st,
                                   err) == 0 &&
               S_ISDIR(st.st_mode);
    bool marker = m_meta.Store().Stat(folder, owner.get(), st, err) == 0;
    if (dir || marker) {
      std::string hdr = "Location: /" + key + "/";
      return Reply(req, 302, nullptr, hdr.c_str(), nullptr, 0);
    }
  }
  if (key.empty() || key.back() == '/')
    key += index;

  auto sendObject = [&](const std::string &objKey, int status) -> int {
    std::string path = ObjectFile(m_cfg, rec.owner, bucket, objKey);
    struct stat st;
    if (m_meta.Store().Stat(path, owner.get(), st, err) != 0 || !S_ISREG(st.st_mode))
      return 0;
    ObjectMeta meta;
    m_meta.ReadObjectMeta(rec.owner, bucket, objKey, meta);
    std::string ctype = meta.contentType;
    if (ctype.empty() || ctype == "application/octet-stream")
      ctype = TypeFor(objKey);
    std::string hdr = "Content-Type: " + ctype;
    long long send = static_cast<long long>(st.st_size);
    return StreamFile(req, m_meta.Store(), path, owner.get(), status, hdr, 0, send,
                      req.verb == "HEAD") == 0
               ? 1
               : -1;
  };
  int sent = sendObject(key, 200);
  if (sent != 0)
    return sent < 0 ? -1 : 0;
  for (const auto &rule : routes) {
    if (rule.errorCode != "404" || !PrefixMatch(rule, key))
      continue;
    std::string hdr = "Location: " + LocationFor(rule, key);
    return Reply(req, rule.status, nullptr, hdr.c_str(), nullptr, 0);
  }
  if (!errorKey.empty()) {
    int errSent = sendObject(errorKey, 404);
    if (errSent != 0)
      return errSent < 0 ? -1 : 0;
  }
  return Fail(req, 404, "NoSuchKey", "The specified key does not exist");
}

}
