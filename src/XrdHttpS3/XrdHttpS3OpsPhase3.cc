#include "XrdHttpS3Ops.hh"

#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdHttpS3Client.hh"
#include "XrdHttpS3Fs.hh"
#include "XrdHttpS3Paths.hh"
#include "XrdHttpS3SigV4.hh"
#include "XrdHttpS3Xml.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include <openssl/evp.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <map>
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

bool Chunked(const XrdHttpExtReq &req) {
  const std::string *te = Hdr(req, "transfer-encoding");
  return te && te->find("chunked") != std::string::npos;
}

std::string Lower(std::string s) {
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

std::string XmlUnescape(const std::string &s) {
  std::string out;
  for (size_t i = 0; i < s.size(); i++) {
    if (s.compare(i, 4, "&lt;") == 0) {
      out.push_back('<');
      i += 3;
    } else if (s.compare(i, 4, "&gt;") == 0) {
      out.push_back('>');
      i += 3;
    } else if (s.compare(i, 5, "&amp;") == 0) {
      out.push_back('&');
      i += 4;
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
    size_t gt = xml.find('>', a);
    if (gt == std::string::npos || gt > xml.find(close, a))
      a += open.size() - 1;
    else
      a = gt + 1;
    size_t b = xml.find(close, a);
    if (b == std::string::npos)
      break;
    out.push_back(XmlUnescape(xml.substr(a, b - a)));
    i = b + close.size();
  }
  return out;
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

int B64Val(char c) {
  if (c >= 'A' && c <= 'Z')
    return c - 'A';
  if (c >= 'a' && c <= 'z')
    return c - 'a' + 26;
  if (c >= '0' && c <= '9')
    return c - '0' + 52;
  if (c == '+')
    return 62;
  if (c == '/')
    return 63;
  return -1;
}

bool B64Decode(const std::string &in, std::string &out) {
  out.clear();
  unsigned val = 0;
  int bits = -8;
  for (char c : in) {
    if (c == '=' || c == '\n' || c == '\r')
      continue;
    int d = B64Val(c);
    if (d < 0)
      return false;
    val = (val << 6) | static_cast<unsigned>(d);
    bits += 6;
    if (bits >= 0) {
      out.push_back(static_cast<char>((val >> bits) & 0xff));
      bits -= 8;
    }
  }
  return true;
}

struct Pull {
  XrdHttpExtReq &req;
  std::string buf;
  bool eof{false};
  explicit Pull(XrdHttpExtReq &r) : req(r) {}
  bool More() {
    if (eof)
      return false;
    char *data = nullptr;
    int n = req.BuffgetData(65536, &data, false);
    if (n <= 0)
      n = req.BuffgetData(65536, &data, true);
    if (n <= 0) {
      eof = true;
      return false;
    }
    buf.append(data, static_cast<size_t>(n));
    return true;
  }
  bool Take(size_t n, std::string &out) {
    while (buf.size() < n) {
      if (!More())
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
      if (!More())
        return false;
    }
  }
};

int ReadBody(XrdHttpExtReq &req, long long maxBytes, std::string &out) {
  out.clear();
  if (Chunked(req)) {
    Pull wire(req);
    for (;;) {
      std::string line;
      if (!wire.Line(line))
        return -1;
      size_t semi = line.find(';');
      std::string sizeText = semi == std::string::npos ? line : line.substr(0, semi);
      char *end = nullptr;
      unsigned long sz = std::strtoul(sizeText.c_str(), &end, 16);
      if (!end || *end)
        return -1;
      if (static_cast<long long>(out.size()) + static_cast<long long>(sz) > maxBytes)
        return -2;
      std::string data;
      if (sz && !wire.Take(sz, data))
        return -1;
      std::string crlf;
      if (!wire.Take(2, crlf) || crlf != "\r\n")
        return -1;
      if (sz == 0)
        return 0;
      out += data;
    }
  }
  if (req.length < 0 || req.length > maxBytes)
    return -2;
  long long left = req.length;
  while (left > 0) {
    char *data = nullptr;
    int want = static_cast<int>(std::min<long long>(left, 65536));
    int n = req.BuffgetData(want, &data, true);
    if (n <= 0)
      return -1;
    out.append(data, static_cast<size_t>(n));
    left -= n;
  }
  return 0;
}

std::string BucketOf(const Config &cfg, const XrdHttpExtReq &req) {
  const std::string *hostHdr = Hdr(req, "host");
  if (hostHdr && !cfg.vhost.empty() && !hostHdr->empty() && (*hostHdr)[0] != '[') {
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
    host = Lower(host);
    std::string suffix = "." + cfg.vhost;
    if (host.size() > suffix.size() &&
        host.compare(host.size() - suffix.size(), suffix.size(), suffix) == 0)
      return host.substr(0, host.size() - suffix.size());
  }
  std::string path = req.resource.empty() ? "/" : req.resource;
  if (path.empty() || path[0] != '/')
    return std::string();
  path.erase(path.begin());
  size_t slash = path.find('/');
  if (slash != std::string::npos)
    path.resize(slash);
  return path;
}

struct CorsRule {
  std::vector<std::string> origins;
  std::vector<std::string> methods;
  std::vector<std::string> headers;
  std::vector<std::string> expose;
  std::string maxAge;
};

std::vector<CorsRule> ParseCors(const std::string &xml) {
  std::vector<CorsRule> rules;
  size_t i = 0;
  while (i < xml.size()) {
    size_t a = xml.find("<CORSRule", i);
    if (a == std::string::npos)
      break;
    size_t b = xml.find("</CORSRule>", a);
    if (b == std::string::npos)
      break;
    std::string block = xml.substr(a, b - a);
    CorsRule rule;
    rule.origins = TagTexts(block, "AllowedOrigin");
    rule.methods = TagTexts(block, "AllowedMethod");
    rule.headers = TagTexts(block, "AllowedHeader");
    rule.expose = TagTexts(block, "ExposeHeader");
    auto ages = TagTexts(block, "MaxAgeSeconds");
    if (!ages.empty())
      rule.maxAge = ages[0];
    rules.push_back(rule);
    i = b + 11;
  }
  return rules;
}

bool Listed(const std::vector<std::string> &items, const std::string &want) {
  for (const auto &item : items) {
    if (item == "*" || Lower(item) == Lower(want))
      return true;
  }
  return false;
}

std::string JoinComma(const std::vector<std::string> &items) {
  std::string out;
  for (const auto &item : items) {
    if (!out.empty())
      out += ", ";
    out += item;
  }
  return out;
}

bool TagOk(const std::string &key, const std::string &value) {
  if (key.empty() || key.size() > 128 || value.size() > 256)
    return false;
  for (unsigned char c : key + value) {
    if (c < 0x20 || c == 0x7f)
      return false;
  }
  return true;
}

std::map<std::string, std::string> ParseTagFile(const std::string &body) {
  std::map<std::string, std::string> tags;
  size_t i = 0;
  while (i < body.size()) {
    size_t nl = body.find('\n', i);
    if (nl == std::string::npos)
      nl = body.size();
    std::string line = body.substr(i, nl - i);
    i = nl + 1;
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    size_t tab = line.find('\t');
    if (tab == std::string::npos)
      continue;
    tags[line.substr(0, tab)] = line.substr(tab + 1);
  }
  return tags;
}

std::string FormatTagFile(const std::map<std::string, std::string> &tags) {
  std::string out;
  for (const auto &kv : tags)
    out += kv.first + "\t" + kv.second + "\n";
  return out;
}

std::string TagXml(const std::map<std::string, std::string> &tags) {
  std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging><TagSet>";
  for (const auto &kv : tags) {
    xml += "<Tag><Key>" + XmlEscape(kv.first) + "</Key><Value>" + XmlEscape(kv.second) +
           "</Value></Tag>";
  }
  xml += "</TagSet></Tagging>\n";
  return xml;
}

std::string PermKey(const std::string &permission) {
  std::string p = permission;
  for (char &c : p)
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  if (p == "READ")
    return "read";
  if (p == "WRITE")
    return "write";
  if (p == "READ_ACP")
    return "read-acp";
  if (p == "WRITE_ACP")
    return "write-acp";
  if (p == "FULL_CONTROL")
    return "full-control";
  return std::string();
}

bool ParseAclXml(const std::string &xml, std::map<std::string, std::string> &grants) {
  grants.clear();
  size_t i = 0;
  bool any = false;
  while (i < xml.size()) {
    size_t a = xml.find("<Grant", i);
    if (a == std::string::npos)
      break;
    size_t b = xml.find("</Grant>", a);
    if (b == std::string::npos)
      return false;
    std::string block = xml.substr(a, b - a);
    auto perms = TagTexts(block, "Permission");
    auto ids = TagTexts(block, "ID");
    auto uris = TagTexts(block, "URI");
    if (perms.empty())
      return false;
    std::string key = PermKey(perms[0]);
    if (key.empty())
      return false;
    std::string who;
    if (!uris.empty())
      who = "uri=" + uris[0];
    else if (!ids.empty())
      who = "id=" + ids[0];
    else
      return false;
    if (!grants[key].empty())
      grants[key] += ",";
    grants[key] += who;
    any = true;
    i = b + 8;
  }
  return any;
}

struct FormPart {
  std::string name;
  std::string filename;
  std::string body;
};

bool ParseForm(const std::string &contentType, const std::string &body,
               std::vector<FormPart> &parts) {
  parts.clear();
  std::string ct = Lower(contentType);
  size_t bpos = ct.find("boundary=");
  if (bpos == std::string::npos)
    return false;
  std::string boundary = contentType.substr(bpos + 9);
  if (!boundary.empty() && boundary.front() == '"') {
    size_t end = boundary.find('"', 1);
    boundary = end == std::string::npos ? boundary.substr(1) : boundary.substr(1, end - 1);
  } else {
    size_t semi = boundary.find(';');
    if (semi != std::string::npos)
      boundary.resize(semi);
  }
  while (!boundary.empty() && (boundary.back() == ' ' || boundary.back() == '\r'))
    boundary.pop_back();
  std::string sep = "--" + boundary;
  size_t pos = body.find(sep);
  if (pos == std::string::npos)
    return false;
  pos += sep.size();
  while (pos < body.size()) {
    if (body.compare(pos, 2, "--") == 0)
      break;
    if (body.compare(pos, 2, "\r\n") == 0)
      pos += 2;
    size_t next = body.find("\r\n" + sep, pos);
    if (next == std::string::npos)
      return false;
    std::string part = body.substr(pos, next - pos);
    size_t blank = part.find("\r\n\r\n");
    if (blank == std::string::npos)
      return false;
    std::string headers = Lower(part.substr(0, blank));
    FormPart item;
    item.body = part.substr(blank + 4);
    size_t name = headers.find("name=\"");
    if (name != std::string::npos) {
      name += 6;
      size_t end = headers.find('"', name);
      if (end != std::string::npos)
        item.name = part.substr(name, end - name);
    }
    size_t file = headers.find("filename=\"");
    if (file != std::string::npos) {
      file += 10;
      size_t end = part.find('"', file);
      if (end != std::string::npos)
        item.filename = part.substr(file, end - file);
    }
    parts.push_back(item);
    pos = next + 2 + sep.size();
  }
  return !parts.empty();
}

const FormPart *Field(const std::vector<FormPart> &parts, const std::string &name) {
  for (const auto &part : parts) {
    if (part.name == name)
      return &part;
  }
  return nullptr;
}

std::string JsonString(const std::string &json, const std::string &key) {
  std::string pat = "\"" + key + "\"";
  size_t p = json.find(pat);
  if (p == std::string::npos)
    return std::string();
  p = json.find(':', p + pat.size());
  if (p == std::string::npos)
    return std::string();
  p = json.find('"', p);
  if (p == std::string::npos)
    return std::string();
  size_t e = json.find('"', p + 1);
  if (e == std::string::npos)
    return std::string();
  return json.substr(p + 1, e - p - 1);
}

bool PolicyAllows(const std::string &json, const std::string &bucket, const std::string &key,
                  long long size) {
  std::string exp = JsonString(json, "expiration");
  if (exp.size() < 19)
    return false;
  std::tm tm{};
  if (!strptime(exp.c_str(), "%Y-%m-%dT%H:%M:%S", &tm))
    return false;
  time_t when = timegm(&tm);
  if (when == static_cast<time_t>(-1) || time(nullptr) > when)
    return false;
  if (JsonString(json, "bucket") != bucket)
    return false;
  bool keyOk = false;
  std::string exact = JsonString(json, "key");
  if (!exact.empty()) {
    keyOk = key == exact;
  }
  std::string marker = "\"starts-with\"";
  size_t s = 0;
  while ((s = json.find(marker, s)) != std::string::npos) {
    std::string rest = json.substr(s, 160);
    if (rest.find("$key") != std::string::npos) {
      size_t q = rest.rfind('"');
      size_t q2 = rest.rfind('"', q == std::string::npos ? 0 : q - 1);
      if (q != std::string::npos && q2 != std::string::npos && q2 < q) {
        std::string prefix = rest.substr(q2 + 1, q - q2 - 1);
        if (prefix != "$key" && key.compare(0, prefix.size(), prefix) == 0)
          keyOk = true;
      }
    }
    s += marker.size();
  }
  if (!keyOk)
    return false;
  size_t range = json.find("content-length-range");
  if (range != std::string::npos) {
    long long lo = 0;
    long long hi = 0;
    if (sscanf(json.c_str() + range, "content-length-range\"%*[^0-9]%lld%*[^0-9]%lld", &lo, &hi) ==
            2 ||
        sscanf(json.c_str() + range, "content-length-range\", %lld, %lld", &lo, &hi) == 2) {
      if (size < lo || size > hi)
        return false;
    }
  }
  return true;
}

std::string Md5Hex(const std::string &data) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int n = 0;
  EVP_Digest(data.data(), data.size(), md, &n, EVP_md5(), nullptr);
  static const char *hex = "0123456789abcdef";
  std::string out;
  out.resize(n * 2);
  for (unsigned int i = 0; i < n; i++) {
    out[i * 2] = hex[md[i] >> 4];
    out[i * 2 + 1] = hex[md[i] & 0xf];
  }
  return out;
}

}

std::string Ops::WithCors(const XrdHttpExtReq &req, const std::string &hdr) {
  const std::string *origin = Hdr(req, "origin");
  if (!origin || origin->empty())
    return hdr;
  std::string bucket = BucketOf(m_cfg, req);
  if (!ValidBucket(bucket))
    return hdr;
  BucketRecord rec;
  int err = 0;
  if (m_meta.LoadBucket(bucket, rec, err) != 0)
    return hdr;
  std::string xml;
  if (m_meta.Store().ReadAll(BucketDoc(m_cfg, rec.owner, bucket, "cors"), m_meta.MetaWho(), xml,
                             err) != 0)
    return hdr;
  std::string method = req.verb;
  if (req.verb == "OPTIONS") {
    const std::string *asked = Hdr(req, "access-control-request-method");
    if (asked && !asked->empty())
      method = *asked;
  }
  std::string extra;
  for (const auto &rule : ParseCors(xml)) {
    if (!Listed(rule.origins, *origin) || !Listed(rule.methods, method))
      continue;
    std::string allow = Listed(rule.origins, "*") && origin->find('*') == std::string::npos
                            ? std::string("*")
                            : *origin;
    bool starOnly = false;
    for (const auto &item : rule.origins) {
      if (item == "*")
        starOnly = true;
    }
    if (starOnly)
      allow = "*";
    extra = "Access-Control-Allow-Origin: " + allow + "\r\nVary: Origin";
    if (!rule.expose.empty())
      extra += "\r\nAccess-Control-Expose-Headers: " + JoinComma(rule.expose);
    if (req.verb == "OPTIONS") {
      extra += "\r\nAccess-Control-Allow-Methods: " + JoinComma(rule.methods);
      const std::string *asked = Hdr(req, "access-control-request-headers");
      if (asked && !asked->empty() && (rule.headers.empty() || Listed(rule.headers, "*") ||
                                       Listed(rule.headers, *asked)))
        extra += "\r\nAccess-Control-Allow-Headers: " + *asked;
      if (!rule.maxAge.empty())
        extra += "\r\nAccess-Control-Max-Age: " + rule.maxAge;
    }
    break;
  }
  if (extra.empty())
    return hdr;
  if (hdr.empty())
    return extra;
  return hdr + "\r\n" + extra;
}

int Ops::Reply(XrdHttpExtReq &req, int code, const char *text, const char *hdr, const char *body,
               long long len) {
  const char *use = hdr;
  std::string merged;
  if (code != 100) {
    merged = WithCors(req, hdr ? hdr : "");
    use = merged.empty() ? nullptr : merged.c_str();
  }
  if (req.SendSimpleResp(code, text, use, body, len) != 0)
    return -1;
  return 0;
}

int Ops::Options(XrdHttpExtReq &req) {
  std::string bucket = BucketOf(m_cfg, req);
  if (!ValidBucket(bucket))
    return Fail(req, 400, "InvalidBucketName", "The bucket name is not valid");
  BucketRecord rec;
  int err = 0;
  if (m_meta.LoadBucket(bucket, rec, err) != 0)
    return Fail(req, 404, "NoSuchBucket", "The specified bucket does not exist");
  std::string xml;
  if (m_meta.Store().ReadAll(BucketDoc(m_cfg, rec.owner, bucket, "cors"), m_meta.MetaWho(), xml,
                             err) != 0)
    return Fail(req, 403, "AccessDenied", "CORS is not configured for this bucket");
  std::string hdr = WithCors(req, "");
  if (hdr.find("Access-Control-Allow-Origin") == std::string::npos)
    return Fail(req, 403, "AccessDenied", "This origin is not allowed");
  return Reply(req, 200, nullptr, nullptr, nullptr, 0);
}

int Ops::BucketDocOp(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                     const std::string &kind) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  std::string path = BucketDoc(m_cfg, key.user, bucket, kind);
  int err = 0;
  if (req.verb == "GET") {
    std::string body;
    if (m_meta.Store().ReadAll(path, m_meta.MetaWho(), body, err) != 0 || body.empty()) {
      if (kind == "notification") {
        body = "<NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>";
      } else if (kind == "requestPayment") {
        body = "<RequestPaymentConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
               "<Payer>BucketOwner</Payer></RequestPaymentConfiguration>";
      } else if (kind == "tagging") {
        return Fail(req, 404, "NoSuchTagSet", "The bucket has no tags");
      } else if (kind == "policy") {
        return Fail(req, 404, "NoSuchBucketPolicy", "The bucket has no policy");
      } else if (kind == "website") {
        return Fail(req, 404, "NoSuchWebsiteConfiguration", "The bucket has no website configuration");
      } else {
        return Fail(req, 404, "NoSuchCORSConfiguration", "The bucket has no CORS configuration");
      }
    }
    std::string hdr = "Content-Type: application/xml";
    return Reply(req, 200, nullptr, hdr.c_str(), body.data(), static_cast<long long>(body.size()));
  }
  if (req.verb == "DELETE") {
    if (kind == "requestPayment")
      return Fail(req, 405, "MethodNotAllowed", "RequestPayment cannot be deleted");
    m_meta.Store().Unlink(path, m_meta.MetaWho(), err);
    return Reply(req, 204, nullptr, nullptr, nullptr, 0);
  }
  if (req.verb != "PUT")
    return Fail(req, 405, "MethodNotAllowed", "This method is not valid for this subresource");
  std::string body;
  int got = ReadBody(req, 256 * 1024, body);
  if (got == -2)
    return Fail(req, 400, "InvalidArgument", "The document is too large");
  if (got != 0)
    return -1;
  const std::string *md5 = Hdr(req, "content-md5");
  if (!md5 || *md5 != ContentMd5(body))
    return Fail(req, 400, "InvalidDigest", "Content-MD5 is missing or does not match");
  if (kind == "website" && body.find("WebsiteConfiguration") == std::string::npos)
    return Fail(req, 400, "MalformedXML", "The website document is not well formed");
  if (kind == "tagging") {
    auto keys = TagTexts(body, "Key");
    auto values = TagTexts(body, "Value");
    if (keys.size() != values.size() || keys.size() > 50)
      return Fail(req, 400, "InvalidArgument", "Bucket tags are not well formed");
    for (size_t i = 0; i < keys.size(); i++) {
      if (!TagOk(keys[i], values[i]))
        return Fail(req, 400, "InvalidTag", "A tag key or value is not valid");
    }
  }
  if (m_meta.Store().WriteAll(path, m_meta.MetaWho(), body, 0644, err) != 0)
    return Fail(req, 500, "InternalError", "Could not store the bucket document");
  int status = kind == "tagging" ? 204 : 200;
  return Reply(req, status, nullptr, nullptr, nullptr, 0);
}

int Ops::ObjectTagging(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                       const std::string &objKey) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  Client id(key.user);
  struct stat st;
  int err = 0;
  if (m_meta.Store().Stat(ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), st, err) != 0 ||
      !S_ISREG(st.st_mode))
    return Fail(req, 404, "NoSuchKey", "The specified key does not exist");
  std::string path = ObjectTags(m_cfg, key.user, bucket, objKey);
  if (req.verb == "GET") {
    std::string raw;
    m_meta.Store().ReadAll(path, m_meta.MetaWho(), raw, err);
    std::string body = TagXml(ParseTagFile(raw));
    std::string hdr = "Content-Type: application/xml";
    return Reply(req, 200, nullptr, hdr.c_str(), body.data(), static_cast<long long>(body.size()));
  }
  if (req.verb == "DELETE") {
    m_meta.Store().Unlink(path, m_meta.MetaWho(), err);
    return Reply(req, 204, nullptr, nullptr, nullptr, 0);
  }
  if (req.verb != "PUT")
    return Fail(req, 405, "MethodNotAllowed", "This method is not valid for tagging");
  std::string body;
  int got = ReadBody(req, 64 * 1024, body);
  if (got != 0)
    return got == -2 ? Fail(req, 400, "InvalidArgument", "The tagging document is too large") : -1;
  const std::string *md5 = Hdr(req, "content-md5");
  if (!md5 || *md5 != ContentMd5(body))
    return Fail(req, 400, "InvalidDigest", "Content-MD5 is missing or does not match");
  auto keys = TagTexts(body, "Key");
  auto values = TagTexts(body, "Value");
  if (keys.size() != values.size() || keys.size() > 10)
    return Fail(req, 400, "InvalidArgument", "Object tags are not well formed");
  std::map<std::string, std::string> tags;
  for (size_t i = 0; i < keys.size(); i++) {
    if (!TagOk(keys[i], values[i]))
      return Fail(req, 400, "InvalidTag", "A tag key or value is not valid");
    tags[keys[i]] = values[i];
  }
  if (m_meta.Store().WriteAll(path, m_meta.MetaWho(), FormatTagFile(tags), 0644, err) != 0)
    return Fail(req, 500, "InternalError", "Could not store the object tags");
  return Reply(req, 200, nullptr, nullptr, nullptr, 0);
}

int Ops::HeadPart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                  const std::string &) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  auto query = ParseQuery(QueryOf(req));
  std::string upload = query["uploadId"];
  if (upload.size() < 16 || upload.size() > 64)
    return Fail(req, 404, "NoSuchUpload", "The specified upload does not exist");
  int part = 0;
  try {
    part = std::stoi(query["partNumber"]);
  } catch (...) {
    return Fail(req, 400, "InvalidArgument", "partNumber is not an integer");
  }
  std::string path = UploadPartFile(m_cfg, key.user, upload, std::to_string(part));
  struct stat st;
  int err = 0;
  if (m_meta.Store().Stat(path, m_meta.MetaWho(), st, err) != 0)
    return Fail(req, 404, "NoSuchUpload", "The specified part does not exist");
  std::string etag;
  m_meta.Store().ReadAll(path + ".etag", m_meta.MetaWho(), etag, err);
  while (!etag.empty() && (etag.back() == '\n' || etag.back() == '\r'))
    etag.pop_back();
  std::string hdr = "ETag: \"" + etag + "\"";
  return Reply(req, 200, nullptr, hdr.c_str(), nullptr, static_cast<long long>(st.st_size));
}

int Ops::PutAcl(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                const std::string &objKey, bool object) {
  BucketRecord rec;
  int owned = LoadOwned(req, key, bucket, rec);
  if (owned <= 0)
    return owned;
  std::string canned = "private";
  std::map<std::string, std::string> grants;
  if (req.length > 0 || Chunked(req)) {
    std::string body;
    int got = ReadBody(req, 64 * 1024, body);
    if (got != 0)
      return got == -2 ? Fail(req, 400, "InvalidArgument", "The ACL document is too large") : -1;
    const std::string *md5 = Hdr(req, "content-md5");
    if (!md5 || *md5 != ContentMd5(body))
      return Fail(req, 400, "InvalidDigest", "Content-MD5 is missing or does not match");
    if (!ParseAclXml(body, grants))
      return Fail(req, 400, "MalformedACLError", "The ACL document is not well formed");
    canned.clear();
  } else {
    int acl = RejectAclChange(req);
    if (acl <= 0)
      return acl;
    CollectAcl(req, canned, grants);
  }
  if (object) {
    Client id(key.user);
    struct stat st;
    int err = 0;
    if (m_meta.Store().Stat(ObjectFile(m_cfg, key.user, bucket, objKey), id.get(), st, err) != 0 ||
        !S_ISREG(st.st_mode))
      return Fail(req, 404, "NoSuchKey", "The specified key does not exist");
    ObjectMeta meta;
    m_meta.ReadObjectMeta(key.user, bucket, objKey, meta);
    meta.acl = canned.empty() ? "private" : canned;
    meta.grants = grants;
    if (m_meta.WriteObjectMeta(key.user, bucket, objKey, meta, err) != 0)
      return Fail(req, 500, "InternalError", "Could not store the object ACL");
  } else {
    rec.acl = canned.empty() ? "private" : canned;
    rec.grants = grants;
    int err = 0;
    if (m_meta.StoreBucket(rec, err) != 0)
      return Fail(req, 500, "InternalError", "Could not store the bucket ACL");
  }
  return Reply(req, 200, nullptr, nullptr, nullptr, 0);
}

int Ops::PostObject(XrdHttpExtReq &req) {
  std::string bucket = BucketOf(m_cfg, req);
  if (!ValidBucket(bucket))
    return Fail(req, 400, "InvalidBucketName", "The bucket name is not valid");
  std::string body;
  int got = ReadBody(req, 32 * 1024 * 1024, body);
  if (got == -2)
    return Fail(req, 400, "EntityTooLarge", "The form upload is too large");
  if (got != 0)
    return -1;
  const std::string *ct = Hdr(req, "content-type");
  std::vector<FormPart> parts;
  if (!ct || !ParseForm(*ct, body, parts))
    return Fail(req, 400, "MalformedPOSTRequest", "The form upload is not valid");
  const FormPart *policy = Field(parts, "policy");
  const FormPart *alg = Field(parts, "x-amz-algorithm");
  const FormPart *cred = Field(parts, "x-amz-credential");
  const FormPart *date = Field(parts, "x-amz-date");
  const FormPart *sig = Field(parts, "x-amz-signature");
  const FormPart *keyField = Field(parts, "key");
  const FormPart *file = Field(parts, "file");
  if (!policy || !alg || !cred || !date || !sig || !keyField || !file)
    return Fail(req, 400, "InvalidArgument", "The form upload is missing a required field");
  if (alg->body != "AWS4-HMAC-SHA256")
    return Fail(req, 400, "InvalidArgument", "The form algorithm is not SigV4");
  size_t slash = cred->body.find('/');
  if (slash == std::string::npos || slash == 0)
    return Fail(req, 403, "InvalidAccessKeyId", "The access key is not valid");
  std::string akid = cred->body.substr(0, slash);
  if (!ValidAccessKey(akid))
    return Fail(req, 403, "InvalidAccessKeyId", "The access key is not valid");
  KeyRecord key;
  int err = 0;
  if (m_meta.LoadKey(akid, key, err) != 0)
    return Fail(req, 403, "InvalidAccessKeyId", "The access key ID does not exist");
  if (key.status != "active")
    return Fail(req, 403, "InvalidAccessKeyId", "The access key is not active");
  std::string expect = PolicySignature(key.secret, date->body, m_cfg.region, policy->body);
  std::string gotSig = Lower(sig->body);
  if (!ConstantTimeEqual(expect, gotSig))
    return Fail(req, 403, "SignatureDoesNotMatch", "The form signature does not match");
  if (date->body.size() < 8 ||
      cred->body.find("/" + date->body.substr(0, 8) + "/" + m_cfg.region + "/s3/aws4_request") ==
          std::string::npos)
    return Fail(req, 400, "AuthorizationHeaderMalformed", "The credential scope is not valid");
  if (key.expires > 0 && static_cast<long long>(time(nullptr)) > key.expires)
    return Fail(req, 403, "InvalidAccessKeyId", "The access key has expired");
  std::string policyJson;
  if (!B64Decode(policy->body, policyJson))
    return Fail(req, 400, "InvalidArgument", "The form policy is not base64");
  if (!ValidObjectKey(keyField->body) ||
      !PolicyAllows(policyJson, bucket, keyField->body, static_cast<long long>(file->body.size())))
    return Fail(req, 403, "AccessDenied", "The form policy does not allow this upload");
  BucketRecord rec;
  if (m_meta.LoadBucket(bucket, rec, err) != 0 || rec.owner != key.user)
    return Fail(req, 404, "NoSuchBucket", "The specified bucket does not exist");
  Client id(key.user);
  std::string path = ObjectFile(m_cfg, key.user, bucket, keyField->body);
  if (m_meta.Store().WriteAll(path, id.get(), file->body, 0644, err) != 0)
    return Fail(req, 500, "InternalError", "Could not store the uploaded object");
  ObjectMeta meta;
  meta.etag = Md5Hex(file->body);
  const FormPart *fct = Field(parts, "content-type");
  meta.contentType = fct && !fct->body.empty() ? fct->body : "application/octet-stream";
  if (meta.contentType.size() > 256)
    meta.contentType.resize(256);
  m_meta.WriteObjectMeta(key.user, bucket, keyField->body, meta, err);
  std::string vid;
  if (RecordVersion(key.user, bucket, keyField->body, meta, path, vid) != 0)
    return Fail(req, 500, "InternalError", "Could not record the object version");
  int status = 204;
  const FormPart *want = Field(parts, "success_action_status");
  if (want && (want->body == "200" || want->body == "201"))
    status = std::atoi(want->body.c_str());
  if (status == 201) {
    std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PostResponse><Bucket>" +
                      XmlEscape(bucket) + "</Bucket><Key>" + XmlEscape(keyField->body) +
                      "</Key><ETag>&quot;" + meta.etag + "&quot;</ETag></PostResponse>\n";
    std::string hdr = "Content-Type: application/xml";
    return Reply(req, 201, nullptr, hdr.c_str(), xml.data(), static_cast<long long>(xml.size()));
  }
  return Reply(req, status, nullptr, nullptr, nullptr, 0);
}

}
