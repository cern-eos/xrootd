//------------------------------------------------------------------------------
// SPNEGO (HTTP Negotiate) for xiofsagent. GSS stays in userspace.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioUrl.hh"

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#ifdef HAVE_KRB5
extern "C" {
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
}
#endif

namespace {

constexpr char kB64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string b64enc(const void *data, size_t len)
{
  const auto *p = static_cast<const unsigned char *>(data);
  std::string out;
  out.reserve(((len + 2) / 3) * 4);
  for (size_t i = 0; i < len; i += 3) {
    unsigned n = p[i] << 16;
    if (i + 1 < len)
      n |= p[i + 1] << 8;
    if (i + 2 < len)
      n |= p[i + 2];
    out.push_back(kB64[(n >> 18) & 63]);
    out.push_back(kB64[(n >> 12) & 63]);
    out.push_back(i + 1 < len ? kB64[(n >> 6) & 63] : '=');
    out.push_back(i + 2 < len ? kB64[n & 63] : '=');
  }
  return out;
}

int b64val(char c)
{
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

bool b64dec(const std::string &in, std::vector<unsigned char> &out)
{
  out.clear();
  int val = 0, bits = 0;
  for (char c : in) {
    if (c == '=' || c == '\r' || c == '\n' || c == ' ' || c == '\t')
      continue;
    int d = b64val(c);
    if (d < 0)
      return false;
    val = (val << 6) | d;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push_back(static_cast<unsigned char>((val >> bits) & 0xff));
    }
  }
  return true;
}

int writeAll(int fd, const void *buf, size_t n)
{
  const auto *p = static_cast<const char *>(buf);
  while (n) {
    ssize_t w = ::send(fd, p, n, MSG_NOSIGNAL);
    if (w < 0) {
      if (errno == EINTR)
        continue;
      return -1;
    }
    if (w == 0) {
      errno = EPIPE;
      return -1;
    }
    p += w;
    n -= static_cast<size_t>(w);
  }
  return 0;
}

int readHeaders(int fd, std::string &hdrs, std::string &err)
{
  hdrs.clear();
  hdrs.reserve(1024);
  char c;
  while (hdrs.size() < 8192) {
    ssize_t n = ::recv(fd, &c, 1, 0);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      err = std::string("recv HTTP headers: ") + strerror(errno);
      return -1;
    }
    if (n == 0) {
      err = "connection closed during HTTP headers";
      errno = ECONNRESET;
      return -1;
    }
    hdrs.push_back(c);
    if (hdrs.size() >= 4 &&
        hdrs.compare(hdrs.size() - 4, 4, "\r\n\r\n") == 0)
      return 0;
  }
  err = "HTTP headers too large";
  errno = EMSGSIZE;
  return -1;
}

bool ieqPrefix(const char *s, size_t n, const char *want)
{
  size_t i = 0;
  for (; want[i]; ++i) {
    if (i >= n)
      return false;
    char a = s[i];
    char b = want[i];
    if (a >= 'A' && a <= 'Z')
      a = static_cast<char>(a - 'A' + 'a');
    if (b >= 'A' && b <= 'Z')
      b = static_cast<char>(b - 'A' + 'a');
    if (a != b)
      return false;
  }
  return true;
}

int httpStatus(const std::string &hdrs)
{
  if (hdrs.size() < 12 || !ieqPrefix(hdrs.c_str(), 5, "HTTP/"))
    return -1;
  auto sp = hdrs.find(' ');
  if (sp == std::string::npos)
    return -1;
  return std::atoi(hdrs.c_str() + sp + 1);
}

bool parseNegotiate(const std::string &hdrs, std::string &tok)
{
  tok.clear();
  size_t i = 0;
  while (i < hdrs.size()) {
    size_t e = hdrs.find("\r\n", i);
    if (e == std::string::npos)
      break;
    if (ieqPrefix(hdrs.c_str() + i, e - i, "WWW-Authenticate") &&
        i + 16 < e && hdrs[i + 16] == ':') {
      size_t v = i + 17;
      while (v < e && (hdrs[v] == ' ' || hdrs[v] == '\t'))
        ++v;
      if (ieqPrefix(hdrs.c_str() + v, e - v, "Negotiate") && e - v >= 9) {
        v += 9;
        while (v < e && (hdrs[v] == ' ' || hdrs[v] == '\t'))
          ++v;
        tok.assign(hdrs, v, e - v);
        while (!tok.empty() &&
               (tok.back() == ' ' || tok.back() == '\t' || tok.back() == ','))
          tok.pop_back();
        return true;
      }
    }
    i = e + 2;
  }
  return false;
}

std::string hostHeader(const XioFS::Url &url)
{
  std::string host = url.host;
  if (host.find(':') != std::string::npos)
    host = "[" + host + "]";
  if (!((url.tls && url.port == 443) || (!url.tls && url.port == 80)))
    host += ":" + std::to_string(url.port);
  return host;
}

int httpHead(int fd, const XioFS::Url &url, const std::string *authB64,
             std::string &hdrs, std::string &err)
{
  const char *path = url.path.empty() ? "/" : url.path.c_str();
  std::ostringstream req;
  req << "HEAD " << path << " HTTP/1.1\r\n"
      << "Host: " << hostHeader(url) << "\r\n"
      << "Connection: keep-alive\r\n";
  if (authB64 && !authB64->empty())
    req << "Authorization: Negotiate " << *authB64 << "\r\n";
  req << "\r\n";
  const std::string s = req.str();
  if (writeAll(fd, s.data(), s.size())) {
    err = std::string("send HTTP HEAD: ") + strerror(errno);
    return -1;
  }
  return readHeaders(fd, hdrs, err);
}

#ifdef HAVE_KRB5

std::string gssErr(OM_uint32 maj, OM_uint32 min)
{
  OM_uint32 ctx = 0, lmin;
  gss_buffer_desc buf = GSS_C_EMPTY_BUFFER;
  std::string out;
  gss_display_status(&lmin, maj, GSS_C_GSS_CODE, GSS_C_NO_OID, &ctx, &buf);
  if (buf.value && buf.length)
    out.assign(static_cast<const char *>(buf.value), buf.length);
  gss_release_buffer(&lmin, &buf);
  ctx = 0;
  gss_display_status(&lmin, min, GSS_C_MECH_CODE, GSS_C_NO_OID, &ctx, &buf);
  if (buf.value && buf.length) {
    if (!out.empty())
      out += "; ";
    out.append(static_cast<const char *>(buf.value), buf.length);
  }
  gss_release_buffer(&lmin, &buf);
  return out.empty() ? "GSS error" : out;
}

int gssStep(uid_t uid, gss_ctx_id_t *ctx, gss_name_t name,
            const std::vector<unsigned char> *inTok, std::string &outB64,
            std::string &err)
{
  const uid_t old = geteuid();
  if (seteuid(uid) != 0) {
    err = std::string("seteuid: ") + strerror(errno);
    errno = EPERM;
    return -1;
  }

  const char *oldcc = getenv("KRB5CCNAME");
  const std::string saved = oldcc ? oldcc : "";
  const bool had = oldcc != nullptr;
  unsetenv("KRB5CCNAME");

  static char spnego_oid_bytes[] = "\x2b\x06\x01\x05\x05\x02";
  static gss_OID_desc spnego_oid_desc = {6, spnego_oid_bytes};
  gss_OID mech = &spnego_oid_desc;

  gss_buffer_desc inBuf = GSS_C_EMPTY_BUFFER;
  if (inTok && !inTok->empty()) {
    inBuf.value = const_cast<unsigned char *>(inTok->data());
    inBuf.length = inTok->size();
  }

  gss_buffer_desc outBuf = GSS_C_EMPTY_BUFFER;
  OM_uint32 maj = 0, min = 0, flags = 0;
  maj = gss_init_sec_context(&min, GSS_C_NO_CREDENTIAL, ctx, name, mech,
                             GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG,
                             GSS_C_INDEFINITE, GSS_C_NO_CHANNEL_BINDINGS,
                             inBuf.length ? &inBuf : GSS_C_NO_BUFFER, nullptr,
                             &outBuf, &flags, nullptr);
  if (maj != GSS_S_COMPLETE && maj != GSS_S_CONTINUE_NEEDED &&
      *ctx == GSS_C_NO_CONTEXT) {
    if (outBuf.length)
      gss_release_buffer(&min, &outBuf);
    outBuf = GSS_C_EMPTY_BUFFER;
    maj = gss_init_sec_context(&min, GSS_C_NO_CREDENTIAL, ctx, name,
                               GSS_C_NO_OID,
                               GSS_C_MUTUAL_FLAG | GSS_C_REPLAY_FLAG,
                               GSS_C_INDEFINITE, GSS_C_NO_CHANNEL_BINDINGS,
                               inBuf.length ? &inBuf : GSS_C_NO_BUFFER, nullptr,
                               &outBuf, &flags, nullptr);
  }

  if (outBuf.length) {
    outB64 = b64enc(outBuf.value, outBuf.length);
    gss_release_buffer(&min, &outBuf);
  } else {
    outB64.clear();
  }

  if (had)
    setenv("KRB5CCNAME", saved.c_str(), 1);

  if (seteuid(old) != 0) {
    err = "seteuid restore failed";
    errno = EPERM;
    return -1;
  }

  if (maj != GSS_S_COMPLETE && maj != GSS_S_CONTINUE_NEEDED) {
    err = std::string("gss_init_sec_context: ") + gssErr(maj, min);
    errno = EACCES;
    return -1;
  }
  return 0;
}

#endif

} // namespace

int xiofsagentSpnego(int fd, const XioFS::Url &url, uid_t uid,
                     std::string &err)
{
#ifndef HAVE_KRB5
  (void)fd;
  (void)url;
  (void)uid;
  err = "xiofsagent was built without Kerberos support";
  errno = ENOSYS;
  return -1;
#else
  std::string hdrs;
  if (httpHead(fd, url, nullptr, hdrs, err))
    return -1;

  int st = httpStatus(hdrs);
  if (st >= 200 && st < 300)
    return 0;

  std::string inB64;
  if (st != 401 || !parseNegotiate(hdrs, inB64)) {
    err = "unexpected HTTP status " + std::to_string(st) +
          " (wanted 2xx or 401 Negotiate)";
    errno = EACCES;
    return -1;
  }

  gss_ctx_id_t ctx = GSS_C_NO_CONTEXT;
  gss_name_t name = GSS_C_NO_NAME;
  const std::string target = std::string("HTTP@") + url.host;
  gss_buffer_desc nameBuf;
  nameBuf.value = const_cast<char *>(target.c_str());
  nameBuf.length = target.size();
  OM_uint32 maj, min;
  maj = gss_import_name(&min, &nameBuf, GSS_C_NT_HOSTBASED_SERVICE, &name);
  if (maj != GSS_S_COMPLETE) {
    err = std::string("gss_import_name: ") + gssErr(maj, min);
    errno = EACCES;
    return -1;
  }

  std::vector<unsigned char> inTok;
  const std::vector<unsigned char> *inPtr = nullptr;
  if (!inB64.empty()) {
    if (!b64dec(inB64, inTok)) {
      err = "invalid Negotiate token";
      errno = EACCES;
      gss_release_name(&min, &name);
      return -1;
    }
    inPtr = &inTok;
  }

  int rc = -1;
  for (int round = 0; round < 16; ++round) {
    std::string outB64;
    if (gssStep(uid, &ctx, name, inPtr, outB64, err))
      break;
    if (outB64.empty()) {
      err = "GSS produced an empty Negotiate token";
      errno = EACCES;
      break;
    }
    if (httpHead(fd, url, &outB64, hdrs, err))
      break;
    st = httpStatus(hdrs);
    if (st >= 200 && st < 300) {
      rc = 0;
      break;
    }
    if (st != 401 || !parseNegotiate(hdrs, inB64)) {
      err = "SPNEGO failed with HTTP status " + std::to_string(st);
      errno = EACCES;
      break;
    }
    inTok.clear();
    inPtr = nullptr;
    if (!inB64.empty()) {
      if (!b64dec(inB64, inTok)) {
        err = "invalid Negotiate token";
        errno = EACCES;
        break;
      }
      inPtr = &inTok;
    }
  }
  if (rc != 0 && err.empty()) {
    err = "SPNEGO did not complete";
    errno = EACCES;
  }
  if (ctx != GSS_C_NO_CONTEXT)
    gss_delete_sec_context(&min, &ctx, GSS_C_NO_BUFFER);
  gss_release_name(&min, &name);
  return rc;
#endif
}
