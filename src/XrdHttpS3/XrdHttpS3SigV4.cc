#include "XrdHttpS3SigV4.hh"

#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <ctime>

namespace XrdHttpS3 {
namespace {

std::string HexLower(const unsigned char *p, unsigned int n) {
  static const char *kHex = "0123456789abcdef";
  std::string out;
  out.resize(n * 2);
  for (unsigned int i = 0; i < n; i++) {
    out[i * 2] = kHex[p[i] >> 4];
    out[i * 2 + 1] = kHex[p[i] & 0x0f];
  }
  return out;
}

std::string EvpHex(const EVP_MD *type, const void *data, size_t len) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int mdlen = 0;
  if (!EVP_Digest(data, len, md, &mdlen, type, nullptr))
    return std::string();
  return HexLower(md, mdlen);
}

std::string HmacRaw(const std::string &key, const std::string &data) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int mdlen = 0;
  if (!HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
            reinterpret_cast<const unsigned char *>(data.data()), data.size(),
            md, &mdlen))
    return std::string();
  return std::string(reinterpret_cast<char *>(md), mdlen);
}

std::string TrimAws(const std::string &s) {
  size_t b = 0;
  size_t e = s.size();
  while (b < e && (s[b] == ' ' || s[b] == '\t'))
    b++;
  while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\r' ||
                   s[e - 1] == '\n'))
    e--;
  std::string out;
  out.reserve(e - b);
  bool sp = false;
  for (size_t i = b; i < e; i++) {
    if (s[i] == ' ' || s[i] == '\t') {
      if (!sp) {
        out.push_back(' ');
        sp = true;
      }
    } else {
      sp = false;
      out.push_back(s[i]);
    }
  }
  return out;
}

std::string Lower(std::string s) {
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

bool ParseAmzDate(const std::string &s, time_t &out) {
  if (s.size() != 16 || s[8] != 'T' || s[15] != 'Z')
    return false;
  auto dig = [&](int i, int n) -> int {
    int v = 0;
    for (int k = 0; k < n; k++) {
      unsigned char c = static_cast<unsigned char>(s[i + k]);
      if (!std::isdigit(c))
        return -1;
      v = v * 10 + (c - '0');
    }
    return v;
  };
  int year = dig(0, 4);
  int mon = dig(4, 2);
  int day = dig(6, 2);
  int hour = dig(9, 2);
  int min = dig(11, 2);
  int sec = dig(13, 2);
  if (year < 1970 || mon < 1 || mon > 12 || day < 1 || day > 31 || hour > 23 ||
      min > 59 || sec > 60)
    return false;
  std::tm tm{};
  tm.tm_year = year - 1900;
  tm.tm_mon = mon - 1;
  tm.tm_mday = day;
  tm.tm_hour = hour;
  tm.tm_min = min;
  tm.tm_sec = sec;
  out = timegm(&tm);
  return out != static_cast<time_t>(-1);
}

const std::string *Header(const std::map<std::string, std::string> &headers,
                          const std::string &name) {
  auto it = headers.find(name);
  if (it == headers.end())
    return nullptr;
  return &it->second;
}

}

std::string AwsUriEncode(const std::string &in, bool encodeSlash) {
  static const char *kHex = "0123456789ABCDEF";
  std::string out;
  out.reserve(in.size());
  for (unsigned char c : in) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~' ||
        (c == '/' && !encodeSlash))
      out.push_back(static_cast<char>(c));
    else {
      out.push_back('%');
      out.push_back(kHex[c >> 4]);
      out.push_back(kHex[c & 0x0f]);
    }
  }
  return out;
}

std::string PercentDecode(const std::string &in) {
  std::string out;
  out.reserve(in.size());
  for (size_t i = 0; i < in.size(); i++) {
    if (in[i] == '%' && i + 2 < in.size() &&
        std::isxdigit(static_cast<unsigned char>(in[i + 1])) &&
        std::isxdigit(static_cast<unsigned char>(in[i + 2]))) {
      auto hex = [](char c) -> int {
        if (c >= '0' && c <= '9')
          return c - '0';
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return c - 'a' + 10;
      };
      out.push_back(static_cast<char>((hex(in[i + 1]) << 4) | hex(in[i + 2])));
      i += 2;
    } else {
      out.push_back(in[i]);
    }
  }
  return out;
}

std::string HexSha256(const std::string &data) {
  return EvpHex(EVP_sha256(), data.data(), data.size());
}

std::string HexMd5(const std::string &data) {
  return EvpHex(EVP_md5(), data.data(), data.size());
}

std::map<std::string, std::string> ParseQuery(const std::string &raw) {
  std::map<std::string, std::string> out;
  std::string q = raw;
  while (!q.empty() && (q[0] == '&' || q[0] == '?'))
    q.erase(q.begin());
  size_t i = 0;
  while (i < q.size()) {
    size_t amp = q.find('&', i);
    if (amp == std::string::npos)
      amp = q.size();
    std::string piece = q.substr(i, amp - i);
    i = amp + 1;
    if (piece.empty())
      continue;
    size_t eq = piece.find('=');
    std::string name = PercentDecode(eq == std::string::npos ? piece : piece.substr(0, eq));
    std::string value = eq == std::string::npos ? std::string()
                                                : PercentDecode(piece.substr(eq + 1));
    out[name] = value;
  }
  return out;
}

std::string CanonicalQuery(const std::string &raw) {
  std::string q = raw;
  while (!q.empty() && (q[0] == '&' || q[0] == '?'))
    q.erase(q.begin());
  if (q.empty())
    return std::string();
  std::vector<std::pair<std::string, std::string>> params;
  size_t i = 0;
  while (i < q.size()) {
    size_t amp = q.find('&', i);
    if (amp == std::string::npos)
      amp = q.size();
    std::string piece = q.substr(i, amp - i);
    i = amp + 1;
    if (piece.empty())
      continue;
    size_t eq = piece.find('=');
    std::string name = PercentDecode(eq == std::string::npos ? piece : piece.substr(0, eq));
    std::string value = eq == std::string::npos ? std::string()
                                                : PercentDecode(piece.substr(eq + 1));
    params.emplace_back(AwsUriEncode(name, true), AwsUriEncode(value, true));
  }
  std::sort(params.begin(), params.end());
  std::string out;
  for (size_t n = 0; n < params.size(); n++) {
    if (n)
      out.push_back('&');
    out += params[n].first;
    out.push_back('=');
    out += params[n].second;
  }
  return out;
}

std::string CanonicalUri(const std::string &resource) {
  if (resource.empty())
    return "/";
  std::string out;
  size_t i = 0;
  while (i < resource.size()) {
    if (resource[i] == '/') {
      out.push_back('/');
      i++;
      continue;
    }
    size_t j = resource.find('/', i);
    if (j == std::string::npos)
      j = resource.size();
    out += AwsUriEncode(resource.substr(i, j - i), true);
    i = j;
  }
  if (out.empty() || out[0] != '/')
    out.insert(out.begin(), '/');
  return out;
}

ParsedAuth ParseAuthorization(const std::string &header) {
  ParsedAuth auth;
  const char *p = header.c_str();
  while (*p == ' ')
    p++;
  const char *scheme = "AWS4-HMAC-SHA256";
  size_t n = std::strlen(scheme);
  if (std::strncmp(p, scheme, n) != 0)
    return auth;
  p += n;
  if (*p != ' ' && *p != '\0')
    return auth;
  while (*p == ' ')
    p++;

  std::map<std::string, std::string> fields;
  std::string rest(p);
  size_t i = 0;
  while (i < rest.size()) {
    size_t comma = rest.find(',', i);
    if (comma == std::string::npos)
      comma = rest.size();
    std::string piece = TrimAws(rest.substr(i, comma - i));
    i = comma + 1;
    size_t eq = piece.find('=');
    if (eq == std::string::npos || eq == 0)
      continue;
    fields[piece.substr(0, eq)] = piece.substr(eq + 1);
  }
  auto cred = fields.find("Credential");
  auto signedh = fields.find("SignedHeaders");
  auto sig = fields.find("Signature");
  if (cred == fields.end() || signedh == fields.end() || sig == fields.end())
    return auth;

  std::vector<std::string> parts;
  std::string cur;
  for (size_t k = 0; k <= cred->second.size(); k++) {
    if (k == cred->second.size() || cred->second[k] == '/') {
      parts.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(cred->second[k]);
    }
  }
  if (parts.size() != 5 || parts[4] != "aws4_request" || parts[0].empty() ||
      parts[1].size() != 8 || parts[2].empty() || parts[3].empty())
    return auth;

  auth.ok = true;
  auth.accessKey = parts[0];
  auth.date = parts[1];
  auth.region = parts[2];
  auth.service = parts[3];
  auth.signedHeaders = Lower(signedh->second);
  auth.signature = Lower(sig->second);
  return auth;
}

std::string CanonicalRequest(const std::string &method, const std::string &uri,
                             const std::string &query,
                             const std::vector<std::pair<std::string, std::string>> &headers,
                             const std::string &signedHeaders,
                             const std::string &payloadHash) {
  std::string out;
  out += method;
  out.push_back('\n');
  out += uri;
  out.push_back('\n');
  out += query;
  out.push_back('\n');
  for (const auto &h : headers) {
    out += h.first;
    out.push_back(':');
    out += h.second;
    out.push_back('\n');
  }
  out.push_back('\n');
  out += signedHeaders;
  out.push_back('\n');
  out += payloadHash;
  return out;
}

std::string SignatureHex(const std::string &secret, const std::string &amzDate,
                         const std::string &region, const std::string &service,
                         const std::string &canonical) {
  std::string date = amzDate.size() >= 8 ? amzDate.substr(0, 8) : amzDate;
  std::string kSecret = "AWS4" + secret;
  std::string kDate = HmacRaw(kSecret, date);
  std::string kRegion = HmacRaw(kDate, region);
  std::string kService = HmacRaw(kRegion, service);
  std::string kSigning = HmacRaw(kService, "aws4_request");
  std::string scope = date + "/" + region + "/" + service + "/aws4_request";
  std::string sts = std::string("AWS4-HMAC-SHA256\n") + amzDate + "\n" + scope +
                    "\n" + HexSha256(canonical);
  std::string raw = HmacRaw(kSigning, sts);
  return HexLower(reinterpret_cast<const unsigned char *>(raw.data()),
                   static_cast<unsigned int>(raw.size()));
}

AuthFail VerifySigV4(const VerifyInput &in, const std::string &secret,
                     std::string &detail) {
  ParsedAuth auth = ParseAuthorization(in.authorization);
  if (!auth.ok) {
    detail = "Authorization header is not AWS SigV4";
    return AuthFail::Malformed;
  }
  if (auth.service != "s3") {
    detail = "credential scope service is not s3";
    return AuthFail::Malformed;
  }
  if (auth.region != in.region) {
    detail = "credential region does not match s3.region";
    return AuthFail::Malformed;
  }

  const std::string *amz = Header(in.headers, "x-amz-date");
  const std::string *hash = Header(in.headers, "x-amz-content-sha256");
  if (!amz || amz->empty()) {
    detail = "x-amz-date is required";
    return AuthFail::Malformed;
  }
  if (!hash || hash->empty()) {
    detail = "x-amz-content-sha256 is required";
    return AuthFail::MissingHash;
  }
  if (amz->size() < 8 || amz->compare(0, 8, auth.date) != 0) {
    detail = "x-amz-date does not match the credential scope";
    return AuthFail::Malformed;
  }
  time_t when = 0;
  if (!ParseAmzDate(*amz, when)) {
    detail = "x-amz-date is not a basic ISO-8601 timestamp";
    return AuthFail::Malformed;
  }
  long long now = in.now ? in.now : static_cast<long long>(time(nullptr));
  long long delta = now - static_cast<long long>(when);
  if (delta < 0)
    delta = -delta;
  if (delta > in.skew) {
    detail = "request time is outside s3.skew";
    return AuthFail::Skew;
  }

  std::vector<std::string> names;
  std::string cur;
  for (size_t i = 0; i <= auth.signedHeaders.size(); i++) {
    if (i == auth.signedHeaders.size() || auth.signedHeaders[i] == ';') {
      if (!cur.empty())
        names.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(auth.signedHeaders[i]);
    }
  }
  if (names.empty()) {
    detail = "SignedHeaders is empty";
    return AuthFail::Malformed;
  }
  bool sawHost = false;
  std::vector<std::pair<std::string, std::string>> hdrs;
  for (const std::string &name : names) {
    if (name == "host")
      sawHost = true;
    const std::string *val = Header(in.headers, name);
    if (!val) {
      detail = "signed header is missing: " + name;
      return AuthFail::Signature;
    }
    hdrs.emplace_back(name, TrimAws(*val));
  }
  if (!sawHost) {
    detail = "host is not a signed header";
    return AuthFail::Signature;
  }

  std::string canonical = CanonicalRequest(in.method, CanonicalUri(in.resource),
                                           CanonicalQuery(in.query), hdrs,
                                           auth.signedHeaders, *hash);
  std::string expect = SignatureHex(secret, *amz, auth.region, auth.service, canonical);
  if (!ConstantTimeEqual(expect, auth.signature)) {
    detail = "signature does not match";
    return AuthFail::Signature;
  }
  return AuthFail::Ok;
}

SignOutput SignHeader(const SignInput &in) {
  std::vector<std::pair<std::string, std::string>> hdrs;
  hdrs.emplace_back("host", TrimAws(in.host));
  hdrs.emplace_back("x-amz-content-sha256", in.payloadHash);
  hdrs.emplace_back("x-amz-date", in.amzDate);
  for (const auto &extra : in.extra)
    hdrs.emplace_back(Lower(extra.first), TrimAws(extra.second));
  std::sort(hdrs.begin(), hdrs.end(),
            [](const auto &a, const auto &b) { return a.first < b.first; });
  std::string signedHeaders;
  for (size_t i = 0; i < hdrs.size(); i++) {
    if (i)
      signedHeaders.push_back(';');
    signedHeaders += hdrs[i].first;
  }
  SignOutput out;
  out.signedHeaders = signedHeaders;
  out.canonical = CanonicalRequest(in.method, CanonicalUri(in.uri),
                                   CanonicalQuery(in.query), hdrs, signedHeaders,
                                   in.payloadHash);
  out.signature = SignatureHex(in.secret, in.amzDate, in.region, "s3", out.canonical);
  std::string date = in.amzDate.size() >= 8 ? in.amzDate.substr(0, 8) : in.amzDate;
  out.authorization = "AWS4-HMAC-SHA256 Credential=" + in.accessKey + "/" + date +
                      "/" + in.region + "/s3/aws4_request, SignedHeaders=" +
                      signedHeaders + ", Signature=" + out.signature;
  return out;
}

bool ConstantTimeEqual(const std::string &a, const std::string &b) {
  if (a.size() != b.size())
    return false;
  unsigned char diff = 0;
  for (size_t i = 0; i < a.size(); i++)
    diff |= static_cast<unsigned char>(a[i] ^ b[i]);
  return diff == 0;
}

}
