#include "XrdHttpS3Ops.hh"

#include "XrdHttp/XrdHttpExtHandler.hh"
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
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <sys/stat.h>
#include <vector>

namespace XrdHttpS3 {
namespace {

struct Who {
  std::string name;
  XrdSecEntity ent;
  explicit Who(const std::string &user) : name(user), ent("s3") {
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

std::string HexOf(const unsigned char *p, unsigned n) {
  static const char *kHex = "0123456789abcdef";
  std::string out(n * 2, '0');
  for (unsigned i = 0; i < n; i++) {
    out[i * 2] = kHex[p[i] >> 4];
    out[i * 2 + 1] = kHex[p[i] & 0x0f];
  }
  return out;
}

std::string Md5Hex(const std::string &data) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int n = 0;
  if (!EVP_Digest(data.data(), data.size(), md, &n, EVP_md5(), nullptr))
    return std::string();
  return HexOf(md, n);
}

std::string Sha256Hex(const std::string &data) {
  return HexSha256(data);
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

int HexNibble(char c) {
  if (c >= '0' && c <= '9')
    return c - '0';
  if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 10;
  return -1;
}

std::string HexToRaw(const std::string &hex) {
  if (hex.size() % 2)
    return std::string();
  std::string out(hex.size() / 2, '\0');
  for (size_t i = 0; i < hex.size(); i += 2) {
    int hi = HexNibble(hex[i]);
    int lo = HexNibble(hex[i + 1]);
    if (hi < 0 || lo < 0)
      return std::string();
    out[i / 2] = static_cast<char>((hi << 4) | lo);
  }
  return out;
}

std::string StripQuotes(std::string s) {
  while (!s.empty() && (s.front() == '"' || s.front() == ' ' || s.front() == '\r'))
    s.erase(s.begin());
  while (!s.empty() &&
         (s.back() == '"' || s.back() == ' ' || s.back() == '\r' || s.back() == '\n'))
    s.pop_back();
  return s;
}

std::string XmlUnescape(const std::string &s) {
  std::string out;
  for (size_t i = 0; i < s.size(); i++) {
    if (s[i] != '&') {
      out.push_back(s[i]);
      continue;
    }
    if (s.compare(i, 5, "&amp;") == 0) {
      out.push_back('&');
      i += 4;
    } else if (s.compare(i, 4, "&lt;") == 0) {
      out.push_back('<');
      i += 3;
    } else if (s.compare(i, 4, "&gt;") == 0) {
      out.push_back('>');
      i += 3;
    } else if (s.compare(i, 6, "&quot;") == 0) {
      out.push_back('"');
      i += 5;
    } else if (s.compare(i, 6, "&apos;") == 0) {
      out.push_back('\'');
      i += 5;
    } else {
      out.push_back(s[i]);
    }
  }
  return out;
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
    out.push_back(XmlUnescape(xml.substr(a, b - a)));
    i = b + close.size();
  }
  return out;
}

bool ValidUploadId(const std::string &id) {
  if (id.size() < 16 || id.size() > 64)
    return false;
  for (unsigned char c : id) {
    if (!std::isalnum(c))
      return false;
  }
  return true;
}

std::string NewUploadId() {
  unsigned char b[16];
  if (RAND_bytes(b, sizeof(b)) != 1) {
    unsigned long t = static_cast<unsigned long>(time(nullptr));
    for (size_t i = 0; i < sizeof(b); i++)
      b[i] = static_cast<unsigned char>(t >> ((i % 4) * 8));
  }
  return HexOf(b, sizeof(b));
}

struct Wire {
  XrdHttpExtReq &req;
  long long left;
  std::string buf;
  explicit Wire(XrdHttpExtReq &r) : req(r), left(r.length) {}
  bool Pull() {
    if (left <= 0)
      return false;
    char *data = nullptr;
    int want = static_cast<int>(std::min<long long>(left, 65536));
    int n = req.BuffgetData(want, &data, true);
    if (n <= 0)
      return false;
    buf.append(data, static_cast<size_t>(n));
    left -= n;
    return true;
  }
  bool Take(size_t n, std::string &out) {
    while (buf.size() < n) {
      if (!Pull())
        return false;
    }
    out.assign(buf, 0, n);
    buf.erase(0, n);
    return true;
  }
  bool Line(std::string &line) {
    for (;;) {
      size_t p = buf.find('\n');
      if (p != std::string::npos) {
        line = buf.substr(0, p);
        if (!line.empty() && line.back() == '\r')
          line.pop_back();
        buf.erase(0, p + 1);
        return true;
      }
      if (!Pull())
        return false;
    }
  }
};

int WriteAt(XrdSfsFile *fh, XrdSfsFileOffset &off, const char *data, int n) {
  int wrote = 0;
  while (wrote < n) {
    int w = fh->write(off, data + wrote, n - wrote);
    if (w <= 0)
      return -1;
    wrote += w;
    off += w;
  }
  return 0;
}

}

int Ops::WriteObjectBytes(XrdHttpExtReq &req, const KeyRecord &key, const std::string &path,
                          const XrdSecEntity *who, std::string &etag) {
  const std::string *hash = Hdr(req, "x-amz-content-sha256");
  bool streaming = hash && *hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
  OwnedFile file;
  int err = 0;
  int flags = SFS_O_CREAT | SFS_O_TRUNC | SFS_O_WRONLY | SFS_O_MKPTH;
  if (m_meta.Store().Open(path, who, flags, 0644, file, err) != 0)
    return Fail(req, 500, "InternalError", "Could not create the object");

  EVP_MD_CTX *sha = EVP_MD_CTX_new();
  EVP_MD_CTX *md5 = EVP_MD_CTX_new();
  EVP_DigestInit_ex(sha, EVP_sha256(), nullptr);
  EVP_DigestInit_ex(md5, EVP_md5(), nullptr);
  auto finish = [](EVP_MD_CTX *ctx) {
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int n = 0;
    EVP_DigestFinal_ex(ctx, md, &n);
    return HexOf(md, n);
  };
  XrdSfsFileOffset off = 0;
  long long decoded = 0;
  bool bad = false;
  int http = 400;
  const char *code = "IncompleteBody";
  std::string message = "The request body ended early";
  bool hangup = true;

  if (!streaming) {
    long long left = req.length;
    while (left > 0) {
      char *data = nullptr;
      int want = static_cast<int>(std::min<long long>(left, 65536));
      int n = req.BuffgetData(want, &data, true);
      if (n <= 0) {
        bad = true;
        break;
      }
      if (WriteAt(file.fh, off, data, n) != 0) {
        bad = true;
        http = 500;
        code = "InternalError";
        message = "Could not write the object";
        break;
      }
      EVP_DigestUpdate(sha, data, static_cast<size_t>(n));
      EVP_DigestUpdate(md5, data, static_cast<size_t>(n));
      left -= n;
      decoded += n;
    }
  } else {
    const std::string *amz = Hdr(req, "x-amz-date");
    const std::string *authz = Hdr(req, "authorization");
    const std::string *declared = Hdr(req, "x-amz-decoded-content-length");
    ParsedAuth auth = authz ? ParseAuthorization(*authz) : ParsedAuth();
    if (!amz || !auth.ok || !declared) {
      bad = true;
      code = "InvalidArgument";
      message = "aws-chunked uploads require x-amz-date, Authorization, and x-amz-decoded-content-length";
      hangup = false;
    } else {
      long long expect = std::atoll(declared->c_str());
      std::string previous = auth.signature;
      Wire wire(req);
      while (!bad) {
        std::string line;
        if (!wire.Line(line)) {
          bad = true;
          break;
        }
        size_t semi = line.find(';');
        std::string sizeText = semi == std::string::npos ? line : line.substr(0, semi);
        std::string sig;
        std::string marker = "chunk-signature=";
        size_t sp = line.find(marker);
        if (sp == std::string::npos) {
          bad = true;
          http = 403;
          code = "SignatureDoesNotMatch";
          message = "aws-chunked body is missing a chunk signature";
          break;
        }
        sig = line.substr(sp + marker.size());
        char *end = nullptr;
        unsigned long sz = std::strtoul(sizeText.c_str(), &end, 16);
        if (!end || *end || sz > 16 * 1024 * 1024) {
          bad = true;
          code = "InvalidArgument";
          message = "aws-chunked chunk size is not valid";
          hangup = false;
          break;
        }
        std::string data;
        if (sz && !wire.Take(static_cast<size_t>(sz), data)) {
          bad = true;
          break;
        }
        std::string crlf;
        if (!wire.Take(2, crlf) || crlf != "\r\n") {
          bad = true;
          break;
        }
        std::string expectSig =
            ChunkSignature(key.secret, *amz, m_cfg.region, previous, data);
        for (char &c : sig)
          c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (!ConstantTimeEqual(expectSig, sig)) {
          bad = true;
          http = 403;
          code = "SignatureDoesNotMatch";
          message = "aws-chunked chunk signature does not match";
          break;
        }
        previous = sig;
        if (sz == 0)
          break;
        if (WriteAt(file.fh, off, data.data(), static_cast<int>(data.size())) != 0) {
          bad = true;
          http = 500;
          code = "InternalError";
          message = "Could not write the object";
          break;
        }
        EVP_DigestUpdate(sha, data.data(), data.size());
        EVP_DigestUpdate(md5, data.data(), data.size());
        decoded += static_cast<long long>(data.size());
      }
      if (!bad && decoded != expect) {
        bad = true;
        code = "InvalidArgument";
        message = "x-amz-decoded-content-length does not match the chunked body";
        hangup = false;
      }
    }
  }

  std::string shaHex = finish(sha);
  std::string md5Hex = finish(md5);
  EVP_MD_CTX_free(sha);
  EVP_MD_CTX_free(md5);
  file.fh->sync();
  file.fh->close();
  delete file.fh;
  file.fh = nullptr;

  if (!bad && !streaming && hash && *hash != "UNSIGNED-PAYLOAD" && *hash != shaHex) {
    bad = true;
    code = "BadDigest";
    message = "The request body does not match x-amz-content-sha256";
    hangup = false;
  }
  if (bad) {
    m_meta.Store().Unlink(path, who, err);
    Fail(req, http, code, message);
    return hangup ? -1 : 0;
  }
  etag = md5Hex;
  (void)Sha256Hex;
  return 1;
}

int Ops::CopyObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                    const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  const std::string *srcHdr = Hdr(req, "x-amz-copy-source");
  if (!srcHdr)
    return Fail(req, 400, "InvalidArgument", "x-amz-copy-source is required");
  std::string src = PercentDecode(*srcHdr);
  size_t q = src.find('?');
  if (q != std::string::npos) {
    if (src.find("versionId=") != std::string::npos)
      return Fail(req, 501, "NotImplemented", "CopyObject version id is not implemented");
    src.resize(q);
  }
  if (!src.empty() && src[0] == '/')
    src.erase(src.begin());
  size_t slash = src.find('/');
  if (slash == std::string::npos || slash == 0 || slash + 1 >= src.size())
    return Fail(req, 400, "InvalidArgument", "x-amz-copy-source is not bucket/key");
  std::string srcBucket = src.substr(0, slash);
  std::string srcKey = src.substr(slash + 1);
  if (!ValidBucket(srcBucket) || !ValidObjectKey(srcKey))
    return Fail(req, 400, "InvalidArgument", "x-amz-copy-source is not a valid object");
  BucketRecord srcRec;
  int srcOwned = LoadOwned(req, key, srcBucket, srcRec);
  if (srcOwned <= 0)
    return srcOwned;

  if (Drain(req) != 0)
    return -1;

  Who id(key.user);
  std::string srcPath = ObjectFile(m_cfg, key.user, srcBucket, srcKey);
  struct stat st;
  int err = 0;
  if (m_meta.Store().Stat(srcPath, id.get(), st, err) != 0 || !S_ISREG(st.st_mode))
    return Fail(req, 404, "NoSuchKey", "The source key does not exist");
  ObjectMeta srcMeta;
  m_meta.ReadObjectMeta(key.user, srcBucket, srcKey, srcMeta);
  if (EvalReadCond(req.headers, srcMeta.etag, st.st_mtime, "x-amz-copy-source-if-match",
                   "x-amz-copy-source-if-none-match", "x-amz-copy-source-if-unmodified-since",
                   "x-amz-copy-source-if-modified-since") != ReadCond::Ok)
    return Fail(req, 412, "PreconditionFailed",
                "At least one of the preconditions you specified did not hold");
  const std::string *dir = Hdr(req, "x-amz-metadata-directive");
  bool replace = dir && *dir == "REPLACE";
  std::map<std::string, std::string> replaceUser;
  if (replace) {
    int metaRc = CollectUserMeta(req.headers, replaceUser);
    if (metaRc != 0)
      return Fail(req, 400, metaRc == 2 ? "MetadataTooLarge" : "InvalidArgument",
                  "Replacement user metadata is not valid");
  }

  OwnedFile in;
  OwnedFile out;
  if (m_meta.Store().Open(srcPath, id.get(), SFS_O_RDONLY, 0, in, err) != 0)
    return Fail(req, 500, "InternalError", "Could not open the source object");
  std::string dstPath = ObjectFile(m_cfg, key.user, bucket, objKey);
  int flags = SFS_O_CREAT | SFS_O_TRUNC | SFS_O_WRONLY | SFS_O_MKPTH;
  if (m_meta.Store().Open(dstPath, id.get(), flags, 0644, out, err) != 0)
    return Fail(req, 500, "InternalError", "Could not create the destination object");

  EVP_MD_CTX *md5 = EVP_MD_CTX_new();
  EVP_DigestInit_ex(md5, EVP_md5(), nullptr);
  std::vector<char> buf(65536);
  XrdSfsFileOffset roff = 0;
  XrdSfsFileOffset woff = 0;
  bool ok = true;
  while (roff < st.st_size) {
    int want = static_cast<int>(std::min<long long>(st.st_size - roff, 65536));
    int n = in.fh->read(roff, buf.data(), want);
    if (n <= 0) {
      ok = false;
      break;
    }
    if (WriteAt(out.fh, woff, buf.data(), n) != 0) {
      ok = false;
      break;
    }
    EVP_DigestUpdate(md5, buf.data(), static_cast<size_t>(n));
    roff += n;
  }
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int nmd = 0;
  EVP_DigestFinal_ex(md5, md, &nmd);
  EVP_MD_CTX_free(md5);
  out.fh->sync();
  out.fh->close();
  delete out.fh;
  out.fh = nullptr;
  if (!ok) {
    m_meta.Store().Unlink(dstPath, id.get(), err);
    return Fail(req, 500, "InternalError", "Could not copy the object");
  }
  std::string etag = HexOf(md, nmd);
  ObjectMeta meta;
  if (replace) {
    const std::string *ctype = Hdr(req, "content-type");
    meta.contentType = CleanType(ctype ? *ctype : "");
    meta.user = replaceUser;
  } else {
    meta.contentType = srcMeta.contentType;
    meta.user = srcMeta.user;
  }
  meta.etag = etag;
  m_meta.WriteObjectMeta(key.user, bucket, objKey, meta, err);
  std::string body = CopyObjectXml(etag, time(nullptr));
  std::string hdr = "Content-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::DeleteObjects(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  if (req.length < 0 || req.length > 1024 * 1024)
    return Fail(req, 400, "InvalidArgument", "DeleteObjects body is missing or too large");
  std::string body;
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
  const std::string *md5 = Hdr(req, "content-md5");
  if (!md5)
    return Fail(req, 400, "InvalidRequest", "Missing required header for this request: Content-MD5");
  if (*md5 != ContentMd5(body))
    return Fail(req, 400, "BadDigest", "The Content-MD5 you specified was invalid");

  bool quiet = body.find("<Quiet>true</Quiet>") != std::string::npos ||
               body.find("<Quiet>True</Quiet>") != std::string::npos;
  auto keys = TagTexts(body, "Key");
  if (keys.size() > 1000)
    return Fail(req, 400, "InvalidArgument", "DeleteObjects accepts at most 1000 keys");

  Who id(key.user);
  std::vector<std::string> deleted, errors, codes, messages;
  for (const std::string &objKey : keys) {
    if (!ValidObjectKey(objKey)) {
      errors.push_back(objKey);
      codes.push_back("InvalidArgument");
      messages.push_back("The object key is not valid");
      continue;
    }
    int err = 0;
    int rc = m_meta.Store().Unlink(ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), err);
    if (rc != 0 && err != ENOENT) {
      errors.push_back(objKey);
      codes.push_back("InternalError");
      messages.push_back("Could not delete the object");
      continue;
    }
    m_meta.RemoveObjectMeta(key.user, bucket, objKey);
    if (!quiet)
      deleted.push_back(objKey);
  }
  std::string xml = DeleteObjectsXml(deleted, errors, codes, messages);
  std::string hdr = "Content-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), xml.data(),
                         static_cast<long long>(xml.size())) != 0)
    return -1;
  return 0;
}

int Ops::CreateMultipart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                         const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  std::map<std::string, std::string> userMeta;
  int metaRc = CollectUserMeta(req.headers, userMeta);
  if (metaRc == 1)
    return Fail(req, 400, "InvalidArgument", "A user metadata name or value is not valid");
  if (metaRc == 2)
    return Fail(req, 400, "MetadataTooLarge", "User metadata is larger than 2048 bytes");
  if (Drain(req) != 0)
    return -1;
  std::string id = NewUploadId();
  const std::string *ctype = Hdr(req, "content-type");
  std::map<std::string, std::string> props;
  props["bucket"] = bucket;
  props["key"] = objKey;
  props["content-type"] = CleanType(ctype ? *ctype : "");
  for (const auto &kv : userMeta)
    props["user." + kv.first] = kv.second;
  props["created"] = std::to_string(static_cast<long long>(time(nullptr)));
  int err = 0;
  if (m_meta.Store().WriteAll(UploadMeta(m_cfg, key.user, id), m_meta.MetaWho(),
                              FormatProps(props), 0644, err) != 0)
    return Fail(req, 500, "InternalError", "Could not create the multipart upload");
  std::string body = CreateMultipartXml(bucket, objKey, id);
  std::string hdr = "Content-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::UploadPart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                    const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  auto query = ParseQuery(QueryOf(req));
  std::string upload = query["uploadId"];
  if (!ValidUploadId(upload))
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  int part = 0;
  try {
    part = std::stoi(query["partNumber"]);
  } catch (...) {
    return Fail(req, 400, "InvalidArgument", "partNumber is not an integer");
  }
  if (part < 1 || part > 10000)
    return Fail(req, 400, "InvalidArgument", "partNumber must be between 1 and 10000");
  std::string metaBody;
  int err = 0;
  if (m_meta.Store().ReadAll(UploadMeta(m_cfg, key.user, upload), m_meta.MetaWho(), metaBody,
                             err) != 0)
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  auto props = ParseProps(metaBody);
  if (props["bucket"] != bucket || props["key"] != objKey)
    return Fail(req, 404, "NoSuchUpload", "The upload does not belong to this object");
  if (req.length < 0)
    return Fail(req, 411, "MissingContentLength", "Content-Length is required");
  if (WantsContinue(req)) {
    if (req.SendSimpleResp(100, nullptr, nullptr, nullptr, 0) != 0)
      return -1;
  }
  std::string partName = std::to_string(part);
  std::string path = UploadPartFile(m_cfg, key.user, upload, partName);
  std::string etag;
  int wrote = WriteObjectBytes(req, key, path, m_meta.MetaWho(), etag);
  if (wrote <= 0)
    return wrote;
  if (m_meta.Store().WriteAll(path + ".etag", m_meta.MetaWho(), etag + "\n", 0644, err) != 0) {
    m_meta.Store().Unlink(path, m_meta.MetaWho(), err);
    return Fail(req, 500, "InternalError", "Could not record the part etag");
  }
  std::string hdr = "ETag: \"" + etag + "\"";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), nullptr, 0) != 0)
    return -1;
  return 0;
}

int Ops::UploadPartCopy(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                        const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  auto query = ParseQuery(QueryOf(req));
  std::string upload = query["uploadId"];
  if (!ValidUploadId(upload))
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  int part = 0;
  try {
    part = std::stoi(query["partNumber"]);
  } catch (...) {
    return Fail(req, 400, "InvalidArgument", "partNumber is not an integer");
  }
  if (part < 1 || part > 10000)
    return Fail(req, 400, "InvalidArgument", "partNumber must be between 1 and 10000");
  std::string metaBody;
  int err = 0;
  if (m_meta.Store().ReadAll(UploadMeta(m_cfg, key.user, upload), m_meta.MetaWho(), metaBody,
                             err) != 0)
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  auto props = ParseProps(metaBody);
  if (props["bucket"] != bucket || props["key"] != objKey)
    return Fail(req, 404, "NoSuchUpload", "The upload does not belong to this object");

  const std::string *srcHdr = Hdr(req, "x-amz-copy-source");
  if (!srcHdr)
    return Fail(req, 400, "InvalidArgument", "x-amz-copy-source is required");
  std::string src = PercentDecode(*srcHdr);
  size_t qpos = src.find('?');
  if (qpos != std::string::npos)
    src.resize(qpos);
  if (!src.empty() && src[0] == '/')
    src.erase(src.begin());
  size_t slash = src.find('/');
  if (slash == std::string::npos || slash == 0 || slash + 1 >= src.size())
    return Fail(req, 400, "InvalidArgument", "x-amz-copy-source is not bucket/key");
  std::string srcBucket = src.substr(0, slash);
  std::string srcKey = src.substr(slash + 1);
  if (!ValidBucket(srcBucket) || !ValidObjectKey(srcKey))
    return Fail(req, 400, "InvalidArgument", "x-amz-copy-source is not a valid object");
  BucketRecord srcRec;
  int srcOwned = LoadOwned(req, key, srcBucket, srcRec);
  if (srcOwned <= 0)
    return srcOwned;
  if (Drain(req) != 0)
    return -1;

  Who id(key.user);
  std::string srcPath = ObjectFile(m_cfg, key.user, srcBucket, srcKey);
  struct stat st;
  if (m_meta.Store().Stat(srcPath, id.get(), st, err) != 0 || !S_ISREG(st.st_mode))
    return Fail(req, 404, "NoSuchKey", "The source key does not exist");
  ObjectMeta srcMeta;
  m_meta.ReadObjectMeta(key.user, srcBucket, srcKey, srcMeta);
  if (EvalReadCond(req.headers, srcMeta.etag, st.st_mtime, "x-amz-copy-source-if-match",
                   "x-amz-copy-source-if-none-match", "x-amz-copy-source-if-unmodified-since",
                   "x-amz-copy-source-if-modified-since") != ReadCond::Ok)
    return Fail(req, 412, "PreconditionFailed",
                "At least one of the preconditions you specified did not hold");
  long long start = 0;
  long long end = st.st_size > 0 ? st.st_size - 1 : -1;
  const std::string *range = Hdr(req, "x-amz-copy-source-range");
  if (range) {
    if (range->compare(0, 6, "bytes=") != 0 || range->find(',') != std::string::npos)
      return Fail(req, 400, "InvalidArgument", "x-amz-copy-source-range is not a single byte range");
    size_t dash = range->find('-', 6);
    if (dash == std::string::npos)
      return Fail(req, 400, "InvalidArgument", "x-amz-copy-source-range is not a single byte range");
    try {
      start = std::stoll(range->substr(6, dash - 6));
      end = std::stoll(range->substr(dash + 1));
    } catch (...) {
      return Fail(req, 400, "InvalidArgument", "x-amz-copy-source-range is not a single byte range");
    }
    if (start < 0 || end < start || end >= st.st_size)
      return Fail(req, 416, "InvalidRange", "The requested range is not satisfiable");
  }
  if (end < start)
    return Fail(req, 400, "InvalidArgument", "The source object is empty");

  OwnedFile in;
  if (m_meta.Store().Open(srcPath, id.get(), SFS_O_RDONLY, 0, in, err) != 0)
    return Fail(req, 500, "InternalError", "Could not open the source object");
  std::string path = UploadPartFile(m_cfg, key.user, upload, std::to_string(part));
  OwnedFile out;
  int flags = SFS_O_CREAT | SFS_O_TRUNC | SFS_O_WRONLY | SFS_O_MKPTH;
  if (m_meta.Store().Open(path, m_meta.MetaWho(), flags, 0644, out, err) != 0)
    return Fail(req, 500, "InternalError", "Could not create the part");

  EVP_MD_CTX *md5 = EVP_MD_CTX_new();
  EVP_DigestInit_ex(md5, EVP_md5(), nullptr);
  std::vector<char> buf(65536);
  XrdSfsFileOffset roff = start;
  XrdSfsFileOffset woff = 0;
  bool ok = true;
  long long left = end - start + 1;
  while (left > 0) {
    int want = static_cast<int>(std::min<long long>(left, 65536));
    int n = in.fh->read(roff, buf.data(), want);
    if (n <= 0) {
      ok = false;
      break;
    }
    if (WriteAt(out.fh, woff, buf.data(), n) != 0) {
      ok = false;
      break;
    }
    EVP_DigestUpdate(md5, buf.data(), static_cast<size_t>(n));
    roff += n;
    left -= n;
  }
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int nmd = 0;
  EVP_DigestFinal_ex(md5, md, &nmd);
  EVP_MD_CTX_free(md5);
  out.fh->sync();
  out.fh->close();
  delete out.fh;
  out.fh = nullptr;
  if (!ok) {
    m_meta.Store().Unlink(path, m_meta.MetaWho(), err);
    return Fail(req, 500, "InternalError", "Could not copy the part");
  }
  std::string etag = HexOf(md, nmd);
  if (m_meta.Store().WriteAll(path + ".etag", m_meta.MetaWho(), etag + "\n", 0644, err) != 0) {
    m_meta.Store().Unlink(path, m_meta.MetaWho(), err);
    return Fail(req, 500, "InternalError", "Could not record the part etag");
  }
  std::string body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                     "<CopyPartResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                     "<LastModified>" + IsoDate(time(nullptr)) + "</LastModified><ETag>&quot;" +
                     XmlEscape(etag) + "&quot;</ETag></CopyPartResult>\n";
  std::string hdr = "ETag: \"" + etag + "\"\r\nContent-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::CompleteMultipart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                           const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  auto query = ParseQuery(QueryOf(req));
  std::string upload = query["uploadId"];
  if (!ValidUploadId(upload))
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  std::string metaBody;
  int err = 0;
  if (m_meta.Store().ReadAll(UploadMeta(m_cfg, key.user, upload), m_meta.MetaWho(), metaBody,
                             err) != 0)
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  auto props = ParseProps(metaBody);
  if (props["bucket"] != bucket || props["key"] != objKey)
    return Fail(req, 404, "NoSuchUpload", "The upload does not belong to this object");
  if (req.length < 0 || req.length > 1024 * 1024)
    return Fail(req, 400, "InvalidArgument", "CompleteMultipartUpload body is missing or too large");
  std::string xmlBody;
  long long left = req.length;
  while (left > 0) {
    char *data = nullptr;
    int want = static_cast<int>(std::min<long long>(left, 65536));
    int n = req.BuffgetData(want, &data, true);
    if (n <= 0)
      return -1;
    xmlBody.append(data, static_cast<size_t>(n));
    left -= n;
  }
  auto numbers = TagTexts(xmlBody, "PartNumber");
  auto etags = TagTexts(xmlBody, "ETag");
  if (numbers.empty() || numbers.size() != etags.size())
    return Fail(req, 400, "MalformedXML", "CompleteMultipartUpload parts are not well formed");

  Who id(key.user);
  std::string dst = ObjectFile(m_cfg, key.user, bucket, objKey);
  OwnedFile out;
  int flags = SFS_O_CREAT | SFS_O_TRUNC | SFS_O_WRONLY | SFS_O_MKPTH;
  if (m_meta.Store().Open(dst, id.get(), flags, 0644, out, err) != 0)
    return Fail(req, 500, "InternalError", "Could not create the object");

  std::string rawMd5s;
  int prev = 0;
  XrdSfsFileOffset woff = 0;
  bool ok = true;
  std::string failCode = "InvalidPart";
  std::string failMsg = "One or more of the specified parts could not be found";
  for (size_t i = 0; i < numbers.size(); i++) {
    int part = 0;
    try {
      part = std::stoi(numbers[i]);
    } catch (...) {
      ok = false;
      failCode = "InvalidArgument";
      failMsg = "PartNumber is not an integer";
      break;
    }
    if (part <= prev || part > 10000) {
      ok = false;
      failCode = "InvalidPartOrder";
      failMsg = "The list of parts was not in ascending order";
      break;
    }
    prev = part;
    std::string partName = std::to_string(part);
    std::string partPath = UploadPartFile(m_cfg, key.user, upload, partName);
    std::string etagFile;
    if (m_meta.Store().ReadAll(partPath + ".etag", m_meta.MetaWho(), etagFile, err) != 0) {
      ok = false;
      break;
    }
    while (!etagFile.empty() && (etagFile.back() == '\n' || etagFile.back() == '\r'))
      etagFile.pop_back();
    std::string want = StripQuotes(etags[i]);
    for (char &c : want)
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (want != etagFile) {
      ok = false;
      failMsg = "The etag of a part does not match";
      break;
    }
    std::string raw = HexToRaw(etagFile);
    if (raw.size() != 16) {
      ok = false;
      break;
    }
    rawMd5s += raw;
    struct stat st;
    if (m_meta.Store().Stat(partPath, m_meta.MetaWho(), st, err) != 0) {
      ok = false;
      break;
    }
    OwnedFile in;
    if (m_meta.Store().Open(partPath, m_meta.MetaWho(), SFS_O_RDONLY, 0, in, err) != 0) {
      ok = false;
      break;
    }
    std::vector<char> buf(65536);
    XrdSfsFileOffset roff = 0;
    while (roff < st.st_size) {
      int wantN = static_cast<int>(std::min<long long>(st.st_size - roff, 65536));
      int n = in.fh->read(roff, buf.data(), wantN);
      if (n <= 0) {
        ok = false;
        break;
      }
      if (WriteAt(out.fh, woff, buf.data(), n) != 0) {
        ok = false;
        failCode = "InternalError";
        failMsg = "Could not write the completed object";
        break;
      }
      roff += n;
    }
    if (!ok)
      break;
  }
  out.fh->sync();
  out.fh->close();
  delete out.fh;
  out.fh = nullptr;
  if (!ok) {
    m_meta.Store().Unlink(dst, id.get(), err);
    return Fail(req, 400, failCode.c_str(), failMsg);
  }
  std::string etag = Md5Hex(rawMd5s) + "-" + std::to_string(numbers.size());
  ObjectMeta meta;
  meta.etag = etag;
  meta.contentType = props["content-type"].empty() ? "application/octet-stream"
                                                    : props["content-type"];
  for (const auto &kv : props) {
    if (kv.first.compare(0, 5, "user.") == 0 && kv.first.size() > 5)
      meta.user[kv.first.substr(5)] = kv.second;
  }
  m_meta.WriteObjectMeta(key.user, bucket, objKey, meta, err);
  AbortMultipart(req, key, bucket, objKey);
  std::string body = CompleteMultipartXml(bucket, objKey, etag);
  std::string hdr = "Content-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::AbortMultipart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                        const std::string &objKey) {
  auto query = ParseQuery(QueryOf(req));
  std::string upload = query.count("uploadId") ? query["uploadId"] : std::string();
  if (!ValidUploadId(upload)) {
    if (req.verb == "DELETE")
      return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
    return 0;
  }
  std::string metaBody;
  int err = 0;
  bool found = m_meta.Store().ReadAll(UploadMeta(m_cfg, key.user, upload), m_meta.MetaWho(),
                                      metaBody, err) == 0;
  if (found) {
    auto props = ParseProps(metaBody);
    if (props["bucket"] != bucket || props["key"] != objKey)
      found = false;
  }
  std::string dir = UploadDir(m_cfg, key.user, upload);
  std::vector<std::string> names;
  m_meta.Store().List(JoinPath(dir, "parts"), m_meta.MetaWho(), names, err);
  for (const std::string &name : names)
    m_meta.Store().Unlink(JoinPath(JoinPath(dir, "parts"), name), m_meta.MetaWho(), err);
  m_meta.Store().Rmdir(JoinPath(dir, "parts"), m_meta.MetaWho(), err);
  m_meta.Store().Unlink(UploadMeta(m_cfg, key.user, upload), m_meta.MetaWho(), err);
  m_meta.Store().Rmdir(dir, m_meta.MetaWho(), err);
  if (req.verb != "DELETE")
    return found ? 0 : 0;
  if (!found)
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  if (req.SendSimpleResp(204, nullptr, nullptr, nullptr, 0) != 0)
    return -1;
  return 0;
}

int Ops::ListParts(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                   const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  auto query = ParseQuery(QueryOf(req));
  std::string upload = query.count("uploadId") ? query["uploadId"] : std::string();
  if (!ValidUploadId(upload))
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  std::string metaBody;
  int err = 0;
  if (m_meta.Store().ReadAll(UploadMeta(m_cfg, key.user, upload), m_meta.MetaWho(), metaBody,
                             err) != 0)
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  auto props = ParseProps(metaBody);
  if (props["bucket"] != bucket || props["key"] != objKey)
    return Fail(req, 404, "NoSuchUpload", "The upload does not belong to this object");

  int marker = 0;
  int maxParts = 1000;
  if (query.count("part-number-marker") && !query["part-number-marker"].empty()) {
    try {
      marker = std::stoi(query["part-number-marker"]);
    } catch (...) {
      return Fail(req, 400, "InvalidArgument", "part-number-marker is not an integer");
    }
  }
  if (query.count("max-parts") && !query["max-parts"].empty()) {
    try {
      maxParts = std::stoi(query["max-parts"]);
    } catch (...) {
      return Fail(req, 400, "InvalidArgument", "max-parts is not an integer");
    }
    if (maxParts < 0 || maxParts > 1000)
      return Fail(req, 400, "InvalidArgument", "max-parts must be between 0 and 1000");
  }

  std::vector<std::string> names;
  m_meta.Store().List(JoinPath(UploadDir(m_cfg, key.user, upload), "parts"), m_meta.MetaWho(),
                      names, err);
  std::vector<ListedPart> all;
  for (const std::string &name : names) {
    if (name.size() >= 5 && name.compare(name.size() - 5, 5, ".etag") == 0)
      continue;
    int number = 0;
    try {
      number = std::stoi(name);
    } catch (...) {
      continue;
    }
    if (number <= marker)
      continue;
    ListedPart part;
    part.number = number;
    struct stat st;
    std::string path = UploadPartFile(m_cfg, key.user, upload, name);
    if (m_meta.Store().Stat(path, m_meta.MetaWho(), st, err) == 0) {
      part.size = st.st_size;
      part.mtime = st.st_mtime;
    }
    std::string etag;
    if (m_meta.Store().ReadAll(path + ".etag", m_meta.MetaWho(), etag, err) == 0) {
      while (!etag.empty() && (etag.back() == '\n' || etag.back() == '\r'))
        etag.pop_back();
      part.etag = etag;
    }
    all.push_back(part);
  }
  std::sort(all.begin(), all.end(),
            [](const ListedPart &a, const ListedPart &b) { return a.number < b.number; });
  bool truncated = static_cast<int>(all.size()) > maxParts;
  if (truncated)
    all.resize(static_cast<size_t>(maxParts));
  std::string body = ListPartsXml(bucket, objKey, upload, maxParts, marker, all, truncated);
  std::string hdr = "Content-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::ListMultipartUploads(XrdHttpExtReq &req, const KeyRecord &key,
                              const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  auto query = ParseQuery(QueryOf(req));
  std::string prefix = query.count("prefix") ? query["prefix"] : std::string();
  int maxUploads = 1000;
  if (query.count("max-uploads") && !query["max-uploads"].empty()) {
    try {
      maxUploads = std::stoi(query["max-uploads"]);
    } catch (...) {
      return Fail(req, 400, "InvalidArgument", "max-uploads is not an integer");
    }
    if (maxUploads < 0 || maxUploads > 1000)
      return Fail(req, 400, "InvalidArgument", "max-uploads must be between 0 and 1000");
  }
  std::vector<std::string> ids;
  int err = 0;
  m_meta.Store().List(JoinPath(UserMeta(m_cfg, key.user), "uploads"), m_meta.MetaWho(), ids, err);
  std::vector<ListedUpload> all;
  for (const std::string &id : ids) {
    if (!ValidUploadId(id))
      continue;
    std::string metaBody;
    if (m_meta.Store().ReadAll(UploadMeta(m_cfg, key.user, id), m_meta.MetaWho(), metaBody,
                               err) != 0)
      continue;
    auto props = ParseProps(metaBody);
    if (props["bucket"] != bucket)
      continue;
    if (prefix.size() > props["key"].size() ||
        props["key"].compare(0, prefix.size(), prefix) != 0)
      continue;
    ListedUpload up;
    up.key = props["key"];
    up.uploadId = id;
    up.initiated = static_cast<time_t>(std::atoll(props["created"].c_str()));
    all.push_back(up);
  }
  std::sort(all.begin(), all.end(), [](const ListedUpload &a, const ListedUpload &b) {
    if (a.key != b.key)
      return a.key < b.key;
    return a.uploadId < b.uploadId;
  });
  bool truncated = static_cast<int>(all.size()) > maxUploads;
  if (truncated)
    all.resize(static_cast<size_t>(maxUploads));
  std::string body = ListUploadsXml(bucket, prefix, maxUploads, all, truncated);
  std::string hdr = "Content-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

int Ops::GetBucketLocation(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  std::string region = rec.region.empty() ? m_cfg.region : rec.region;
  std::string body = LocationXml(region);
  std::string hdr = "Content-Type: application/xml";
  if (req.SendSimpleResp(200, nullptr, hdr.c_str(), body.data(),
                         static_cast<long long>(body.size())) != 0)
    return -1;
  return 0;
}

}
