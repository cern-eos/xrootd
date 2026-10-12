#include "XrdHttpS3Ops.hh"

#include "XrdHttp/XrdHttpExtHandler.hh"
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

const char *kSubresources[] = {
    "acl", "cors", "lifecycle", "location", "logging", "notification", "policy",
    "requestPayment", "tagging", "uploads", "versioning", "website", nullptr};

struct Identity {
  std::string name;
  XrdSecEntity ent;
  explicit Identity(const std::string &user) : name(user), ent("s3") {
    if (!name.empty())
      ent.name = &name[0];
  }
  const XrdSecEntity *get() const { return &ent; }
};

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
  if (req.SendSimpleResp(http, nullptr, hdr.c_str(), body.data(),
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

  const std::string *te = Hdr(req, "transfer-encoding");
  if (te && te->find("chunked") != std::string::npos)
    return Fail(req, 501, "NotImplemented", "Chunked uploads are not implemented");

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
      if (req.SendSimpleResp(200, nullptr, nullptr, nullptr, 0) != 0)
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
      if (query.count("list-type") && query["list-type"] != "2")
        return Fail(req, 501, "NotImplemented", "ListObjects version 1 is not implemented");
      return ListObjects(req, key, bucket);
    }
  }

  if (!onlyBucket && query.count("uploads") && req.verb == "POST")
    return CreateMultipart(req, key, bucket, objKey);
  if (!onlyBucket && query.count("uploadId")) {
    if (query.count("partNumber") && req.verb == "PUT")
      return UploadPart(req, key, bucket, objKey);
    if (req.verb == "POST")
      return CompleteMultipart(req, key, bucket, objKey);
    if (req.verb == "DELETE")
      return AbortMultipart(req, key, bucket, objKey);
    return Fail(req, 501, "NotImplemented", "This multipart operation is not implemented");
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
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::CreateBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
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
  rec.acl = "private";
  rec.region = m_cfg.region;
  if (m_meta.StoreBucket(rec, err) != 0)
    return Fail(req, 500, "InternalError", "Could not create the bucket");
  std::string hdr = "Location: /" + bucket;
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), nullptr, 0) != 0)
    return -1;
  return 0;
}

int Ops::DeleteBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  Identity id(key.user);
  std::vector<std::string> names;
  int err = 0;
  std::string data = BucketData(m_cfg, key.user, bucket);
  if (m_meta.Store().List(data, id.get(), names, err) == 0 && !names.empty())
    return Fail(req, 409, "BucketNotEmpty", "The bucket still contains objects");
  if (err != 0 && err != ENOENT)
    return Fail(req, 500, "InternalError", "Could not inspect the bucket");
  m_meta.Store().Rmdir(data, id.get(), err);
  m_meta.RemoveBucket(bucket, key.user, err);
  if (req.SendSimpleResp(204, nullptr, nullptr, nullptr, 0) != 0)
    return -1;
  return 0;
}

int Ops::HeadBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  std::string hdr = "x-amz-bucket-region: " + rec.region;
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), nullptr, 0) != 0)
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
  std::string token = query.count("continuation-token") ? query["continuation-token"] : std::string();
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

  Identity id(key.user);
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
  std::string body = ListObjectsXml(bucket, prefix, delimiter, maxKeys, page, more, token);
  std::string hdr = "Content-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
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
  const std::string *hash = Hdr(req, "x-amz-content-sha256");
  if (hash && hash->compare(0, 10, "STREAMING-") == 0 &&
      *hash != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
    return Fail(req, 501, "NotImplemented", "This streaming payload encoding is not implemented");

  if (WantsContinue(req)) {
    if (req.SendSimpleResp(100, nullptr, nullptr, nullptr, 0) != 0)
      return -1;
  }

  Identity id(key.user);
  std::string path = ObjectFile(m_cfg, key.user, bucket, objKey);
  std::string etag;
  int wrote = WriteObjectBytes(req, key, path, id.get(), etag);
  if (wrote <= 0)
    return wrote;

  ObjectMeta meta;
  meta.etag = etag;
  const std::string *ctype = Hdr(req, "content-type");
  meta.contentType = CleanType(ctype ? *ctype : "");
  int err = 0;
  m_meta.WriteObjectMeta(key.user, bucket, objKey, meta, err);

  std::string hdr = "ETag: \"" + etag + "\"";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), nullptr, 0) != 0)
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
  Identity id(key.user);
  std::string path = ObjectFile(m_cfg, key.user, bucket, objKey);
  struct stat st;
  int err = 0;
  if (m_meta.Store().Stat(path, id.get(), st, err) != 0 || !S_ISREG(st.st_mode))
    return Fail(req, 404, "NoSuchKey", "The specified key does not exist");

  ObjectMeta meta;
  m_meta.ReadObjectMeta(key.user, bucket, objKey, meta);
  long long start = 0;
  long long end = st.st_size > 0 ? st.st_size - 1 : 0;
  bool partial = false;
  const std::string *range = Hdr(req, "range");
  if (range && !head) {
    if (!ParseRange(*range, st.st_size, start, end)) {
      std::string hdr = "Content-Range: bytes */" + std::to_string(static_cast<long long>(st.st_size));
      std::string body = ErrorXml("InvalidRange", "The requested range is not satisfiable", req.resource);
      std::string all = hdr + "\r\nContent-Type: application/xml";
      if (req.SendSimpleResp(416, nullptr, all.c_str(), body.data(),
                             static_cast<long long>(body.size())) != 0)
        return -1;
      return 0;
    }
    partial = true;
  }
  long long send = partial ? (end - start + 1) : static_cast<long long>(st.st_size);
  std::string hdr = "Accept-Ranges: bytes\r\nContent-Type: " + meta.contentType +
                    "\r\nLast-Modified: " + HttpDate(st.st_mtime);
  if (!meta.etag.empty())
    hdr += "\r\nETag: \"" + meta.etag + "\"";
  if (partial)
    hdr += "\r\nContent-Range: bytes " + std::to_string(start) + "-" + std::to_string(end) + "/" +
           std::to_string(static_cast<long long>(st.st_size));

  int code = partial ? 206 : 200;
  if (head || send == 0) {
    if (req.SendSimpleResp(code, nullptr, hdr.c_str(), nullptr, send) != 0)
      return -1;
    return 0;
  }
  OwnedFile file;
  if (m_meta.Store().Open(path, id.get(), SFS_O_RDONLY, 0, file, err) != 0)
    return Fail(req, 500, "InternalError", "Could not open the object");
  if (req.StartSimpleResp(code, nullptr, hdr.c_str(), send, true) != 0)
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
  Identity id(key.user);
  int err = 0;
  int rc = m_meta.Store().Unlink(ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), err);
  if (rc != 0 && err != ENOENT)
    return Fail(req, 500, "InternalError", "Could not delete the object");
  m_meta.RemoveObjectMeta(key.user, bucket, objKey);
  if (req.SendSimpleResp(204, nullptr, nullptr, nullptr, 0) != 0)
    return -1;
  return 0;
}

}
