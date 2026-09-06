//------------------------------------------------------------------------------
// xiofsagent — userspace TLS handshake / kTLS install for xiofs.ko.
//
// Completes the OpenSSL handshake, lets the kernel take over the TLS
// record layer (kTLS), and imports the socket into a mounted XIOFS.
// Steady-state HTTP/1.1 then stays in the module.
//
//   xiofsagent [--cacert FILE] [--insecure] [--token TOK | --tokenfile F]
//              [--import-only] URL [MOUNTPOINT]
//
// Also works as /sbin/mount.xiofs (util-linux helper):
//   mount -t xiofs -o host=...,port=...,path=...,cacert=... none /mnt
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioUrl.hh"
#include "xiofs_uapi.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#ifndef SSL_OP_ENABLE_KTLS
#define SSL_OP_ENABLE_KTLS 0
#endif

static_assert(sizeof(xiofs_import_sock) == 1036,
              "xiofs_import_sock layout must match xiofs.ko");

namespace {

constexpr const char *kCtlDev = "/dev/xiofsctl";

struct Options {
  std::string cacert;
  std::string bearer;
  std::string url;
  std::string mountpoint;
  bool verify_peer{true};
  bool import_only{false};
  bool verbose{false};
  bool fake{false};
};

void usage(const char *argv0)
{
  std::cerr
      << "Usage: " << argv0 << " [--cacert FILE] [--insecure]\n"
      << "          [--token TOK | --tokenfile FILE] [--import-only]\n"
      << "          URL [MOUNTPOINT]\n"
      << "\n"
      << "  Handshake to URL, install kTLS, import the socket into xiofs.ko.\n"
      << "  With MOUNTPOINT (default), mount -t xiofs first then import.\n"
      << "  --import-only assumes the filesystem is already mounted.\n"
      << "\n"
      << "  As mount.xiofs: mount -t xiofs -o host=H,port=P,path=/export none DIR\n"
      << "  Extra -o keys (stripped before the kernel): cacert, token, tokenfile,\n"
      << "  insecure, url.\n";
}

int fail(const std::string &msg, int rc)
{
  std::cerr << "xiofsagent: " << msg << "\n";
  return rc;
}

std::string opensslErr()
{
  char buf[256];
  ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
  return buf;
}

std::string readTokenFile(const std::string &path, std::string &err)
{
  std::ifstream in(path);
  if (!in) {
    err = "cannot read token file: " + path;
    return {};
  }
  std::string tok;
  std::getline(in, tok);
  while (!tok.empty() && (tok.back() == '\r' || tok.back() == '\n' ||
                          tok.back() == ' ' || tok.back() == '\t'))
    tok.pop_back();
  return tok;
}

std::string basenameOf(const char *argv0)
{
  const char *slash = std::strrchr(argv0, '/');
  return slash ? slash + 1 : argv0;
}

bool ktlsSend(SSL *ssl)
{
#ifdef BIO_get_ktls_send
  BIO *b = SSL_get_wbio(ssl);
  return b && BIO_get_ktls_send(b) > 0;
#else
  (void)ssl;
  return false;
#endif
}

bool ktlsRecv(SSL *ssl)
{
#ifdef BIO_get_ktls_recv
  BIO *b = SSL_get_rbio(ssl);
  return b && BIO_get_ktls_recv(b) > 0;
#else
  (void)ssl;
  return false;
#endif
}

int tcpConnect(const XioFS::Url &url, std::string &err)
{
  addrinfo hints{};
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family = AF_UNSPEC;
  addrinfo *res = nullptr;
  int rc = getaddrinfo(url.host.c_str(), std::to_string(url.port).c_str(),
                       &hints, &res);
  if (rc != 0) {
    err = std::string("getaddrinfo: ") + gai_strerror(rc);
    return -1;
  }
  int last = 0;
  for (addrinfo *ai = res; ai; ai = ai->ai_next) {
    int s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (s < 0) {
      last = errno;
      continue;
    }
    int one = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    if (::connect(s, ai->ai_addr, ai->ai_addrlen) == 0) {
      freeaddrinfo(res);
      return s;
    }
    last = errno;
    ::close(s);
  }
  freeaddrinfo(res);
  err = std::string("connect: ") + strerror(last);
  return -1;
}

int tlsHandshake(int fd, const XioFS::Url &url, const Options &opt,
                 bool force_tls12, SSL_CTX **out_ctx, SSL **out_ssl,
                 std::string &err)
{
  if (!SSL_OP_ENABLE_KTLS) {
    err = "OpenSSL was built without enable-ktls (SSL_OP_ENABLE_KTLS)";
    return -1;
  }

  SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
  if (!ctx) {
    err = "SSL_CTX_new failed";
    return -1;
  }
  SSL_CTX_set_options(ctx, SSL_OP_ENABLE_KTLS);
  SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  if (force_tls12)
    SSL_CTX_set_max_proto_version(ctx, TLS1_2_VERSION);

  // kTLS on Linux 5.14 (Alma 9) is AES-GCM / ChaCha20, not CBC.
  SSL_CTX_set_cipher_list(ctx,
                          "ECDHE-ECDSA-AES128-GCM-SHA256:"
                          "ECDHE-RSA-AES128-GCM-SHA256:"
                          "ECDHE-ECDSA-AES256-GCM-SHA384:"
                          "ECDHE-RSA-AES256-GCM-SHA384:"
                          "ECDHE-RSA-CHACHA20-POLY1305:"
                          "ECDHE-ECDSA-CHACHA20-POLY1305");
#ifdef TLS1_3_VERSION
  SSL_CTX_set_ciphersuites(ctx, "TLS_AES_128_GCM_SHA256:"
                                "TLS_AES_256_GCM_SHA384:"
                                "TLS_CHACHA20_POLY1305_SHA256");
#endif

  if (opt.verify_peer) {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    if (!opt.cacert.empty()) {
      if (SSL_CTX_load_verify_locations(ctx, opt.cacert.c_str(), nullptr) !=
          1) {
        SSL_CTX_free(ctx);
        err = "failed to load CA file: " + opt.cacert;
        return -1;
      }
    } else {
      SSL_CTX_set_default_verify_paths(ctx);
    }
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
  }

  SSL *ssl = SSL_new(ctx);
  if (!ssl) {
    SSL_CTX_free(ctx);
    err = "SSL_new failed";
    return -1;
  }
  SSL_set_fd(ssl, fd);
  SSL_set_tlsext_host_name(ssl, url.host.c_str());
#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
  SSL_set1_host(ssl, url.host.c_str());
#endif

  int rc = SSL_connect(ssl);
  if (rc != 1) {
    err = std::string("TLS handshake failed: ") + opensslErr();
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    return -1;
  }

  *out_ctx = ctx;
  *out_ssl = ssl;
  return 0;
}

int handshakeAndKtls(int *fd, const XioFS::Url &url, const Options &opt,
                     SSL_CTX **out_ctx, SSL **out_ssl, std::string &err)
{
  SSL_CTX *ctx = nullptr;
  SSL *ssl = nullptr;
  if (tlsHandshake(*fd, url, opt, false, &ctx, &ssl, err))
    return -1;

  if (ktlsSend(ssl) && ktlsRecv(ssl)) {
    *out_ctx = ctx;
    *out_ssl = ssl;
    return 0;
  }

  // OpenSSL 3.0 (Alma 9) often enables kTLS TX but not RX on TLS 1.3.
  // The kernel HTTP client needs both directions; reconnect at TLS 1.2.
  if (opt.verbose)
    std::cerr << "xiofsagent: kTLS incomplete on "
              << SSL_get_version(ssl) << " (tx=" << ktlsSend(ssl)
              << " rx=" << ktlsRecv(ssl) << "), retrying TLS 1.2\n";
  SSL_set_quiet_shutdown(ssl, 1);
  SSL_free(ssl);
  SSL_CTX_free(ctx);
  ctx = nullptr;
  ssl = nullptr;
  ::close(*fd);
  *fd = tcpConnect(url, err);
  if (*fd < 0)
    return -1;

  if (tlsHandshake(*fd, url, opt, true, &ctx, &ssl, err))
    return -1;
  if (!ktlsSend(ssl) || !ktlsRecv(ssl)) {
    err = "kTLS TX/RX was not installed (modprobe tls; OpenSSL enable-ktls; "
          "AES-GCM or ChaCha20). tx=" +
          std::to_string(ktlsSend(ssl)) +
          " rx=" + std::to_string(ktlsRecv(ssl));
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    return -1;
  }
  *out_ctx = ctx;
  *out_ssl = ssl;
  return 0;
}

int importSock(int fd, const XioFS::Url &url, const Options &opt,
               std::string &err)
{
  int ctl = open(kCtlDev, O_RDWR);
  if (ctl < 0) {
    err = std::string("open ") + kCtlDev + ": " + strerror(errno) +
          " (is xiofs.ko loaded?)";
    return -1;
  }

  xiofs_import_sock im{};
  im.sockfd = fd;
  im.flags = 0;
  if (url.tls)
    im.flags |= XIOFS_IMPORT_TLS;
  if (!opt.bearer.empty()) {
    im.flags |= XIOFS_IMPORT_BEARER;
    std::snprintf(im.bearer, sizeof(im.bearer), "%s", opt.bearer.c_str());
  }
  im.port = static_cast<__u16>(url.port);
  std::snprintf(im.host, sizeof(im.host), "%s", url.host.c_str());
  std::snprintf(im.export_path, sizeof(im.export_path), "%s",
                url.path.empty() ? "/" : url.path.c_str());

  int rc = ioctl(ctl, XIOFS_IOC_IMPORT_SOCK, &im);
  int saved = errno;
  close(ctl);
  if (rc < 0) {
    err = std::string("XIOFS_IOC_IMPORT_SOCK: ") + strerror(saved);
    if (saved == ENOENT)
      err += " (no mount matching host/port/path)";
    else if (saved == EPROTO)
      err += " (socket is not a kTLS TCP socket)";
    return -1;
  }
  return 0;
}

std::string kernelMountData(const XioFS::Url &url)
{
  std::ostringstream os;
  os << "host=" << url.host << ",port=" << url.port << ",path="
     << (url.path.empty() ? "/" : url.path);
  return os.str();
}

int doMount(const XioFS::Url &url, const std::string &dir, std::string &err)
{
  std::string data = kernelMountData(url);
  if (mount("none", dir.c_str(), "xiofs", MS_NOSUID | MS_NODEV, data.c_str()) <
      0) {
    err = std::string("mount: ") + strerror(errno) + " (data=" + data + ")";
    return -1;
  }
  return 0;
}

void splitCsv(const std::string &in, std::vector<std::string> &out)
{
  std::string cur;
  for (char c : in) {
    if (c == ',') {
      if (!cur.empty())
        out.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  if (!cur.empty())
    out.push_back(cur);
}

bool applyMountOpt(const std::string &kv, Options &opt, XioFS::Url &url,
                   std::string &err)
{
  auto eq = kv.find('=');
  std::string key = eq == std::string::npos ? kv : kv.substr(0, eq);
  std::string val = eq == std::string::npos ? std::string() : kv.substr(eq + 1);
  if (key == "host")
    url.host = val;
  else if (key == "port")
    url.port = std::atoi(val.c_str());
  else if (key == "path")
    url.path = val.empty() ? "/" : val;
  else if (key == "cacert")
    opt.cacert = val;
  else if (key == "token")
    opt.bearer = val;
  else if (key == "tokenfile") {
    opt.bearer = readTokenFile(val, err);
    if (!err.empty())
      return false;
  } else if (key == "insecure")
    opt.verify_peer = false;
  else if (key == "url")
    opt.url = val;
  else {
    err = "unknown mount option: " + key;
    return false;
  }
  return true;
}

int parseHelperArgs(int argc, char **argv, Options &opt, std::string &err)
{
  std::string device;
  std::string oopts;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "-o" && i + 1 < argc)
      oopts = argv[++i];
    else if (a == "-v")
      opt.verbose = true;
    else if (a == "-f")
      opt.fake = true;
    else if (a == "-s" || a == "-n" || a == "-r" || a == "-w")
      continue;
    else if (a.size() && a[0] == '-') {
      err = "unknown helper option " + a;
      return -1;
    } else if (device.empty())
      device = a;
    else if (opt.mountpoint.empty())
      opt.mountpoint = a;
    else {
      err = "too many arguments";
      return -1;
    }
  }
  if (opt.mountpoint.empty()) {
    err = "mount.xiofs needs DEVICE DIR";
    return -1;
  }

  XioFS::Url parsed;
  if (device.find("://") != std::string::npos) {
    if (!XioFS::parseUrl(device, parsed, err))
      return -1;
    opt.url = device;
  } else if (device != "none" && !device.empty()) {
    err = "device must be none or an http(s) URL";
    return -1;
  }

  std::vector<std::string> parts;
  splitCsv(oopts, parts);
  XioFS::Url fromopts;
  fromopts.tls = true;
  fromopts.port = 443;
  fromopts.path = "/";
  for (const auto &p : parts) {
    if (!applyMountOpt(p, opt, fromopts, err))
      return -1;
  }
  if (!opt.url.empty()) {
    if (!XioFS::parseUrl(opt.url, parsed, err))
      return -1;
  } else if (!parsed.host.empty()) {
    // device was a URL
  } else {
    parsed = fromopts;
    parsed.tls = true;
    if (parsed.path.empty())
      parsed.path = "/";
    if (parsed.path.front() != '/')
      parsed.path.insert(parsed.path.begin(), '/');
    if (parsed.host.empty()) {
      err = "mount.xiofs needs host= or url=";
      return -1;
    }
    opt.url = std::string(parsed.tls ? "https://" : "http://") + parsed.host +
              ":" + std::to_string(parsed.port) + parsed.path;
    return 0;
  }
  if (!fromopts.host.empty())
    parsed.host = fromopts.host;
  if (fromopts.port)
    parsed.port = fromopts.port;
  if (!fromopts.path.empty() && fromopts.path != "/")
    parsed.path = fromopts.path;
  opt.url = std::string(parsed.tls ? "https://" : "http://") + parsed.host;
  if (!((parsed.tls && parsed.port == 443) ||
        (!parsed.tls && parsed.port == 80)))
    opt.url += ":" + std::to_string(parsed.port);
  opt.url += parsed.path.empty() ? "/" : parsed.path;
  return 0;
}

int parseAgentArgs(int argc, char **argv, Options &opt, std::string &err)
{
  std::vector<std::string> pos;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--cacert" && i + 1 < argc)
      opt.cacert = argv[++i];
    else if (a == "--insecure")
      opt.verify_peer = false;
    else if (a == "--token" && i + 1 < argc)
      opt.bearer = argv[++i];
    else if (a == "--tokenfile" && i + 1 < argc) {
      opt.bearer = readTokenFile(argv[++i], err);
      if (!err.empty())
        return -1;
    } else if (a == "--import-only")
      opt.import_only = true;
    else if (a == "--verbose" || a == "-v")
      opt.verbose = true;
    else if (a == "-h" || a == "--help") {
      usage(argv[0]);
      return 1;
    } else if (a.size() && a[0] == '-') {
      err = "unknown option " + a;
      return -1;
    } else
      pos.push_back(a);
  }
  if (pos.empty()) {
    usage(argv[0]);
    return 1;
  }
  opt.url = pos[0];
  if (pos.size() >= 2)
    opt.mountpoint = pos[1];
  if (pos.size() > 2) {
    err = "too many arguments";
    return -1;
  }
  if (opt.mountpoint.empty() && !opt.import_only) {
    err = "MOUNTPOINT is required unless --import-only";
    return -1;
  }
  return 0;
}

int run(const Options &opt)
{
  std::string err;
  XioFS::Url url;
  if (!XioFS::parseUrl(opt.url, url, err))
    return fail(err, 2);

  if (opt.fake) {
    std::cout << "xiofsagent: would mount " << kernelMountData(url) << " on "
              << opt.mountpoint << "\n";
    return 0;
  }

  bool did_mount = false;
  if (!opt.import_only) {
    if (doMount(url, opt.mountpoint, err))
      return fail(err, 1);
    did_mount = true;
  }

  int fd = tcpConnect(url, err);
  if (fd < 0) {
    if (did_mount)
      umount(opt.mountpoint.c_str());
    return fail(err, 1);
  }

  SSL_CTX *ctx = nullptr;
  SSL *ssl = nullptr;
  if (url.tls) {
    if (handshakeAndKtls(&fd, url, opt, &ctx, &ssl, err)) {
      if (fd >= 0)
        close(fd);
      if (did_mount)
        umount(opt.mountpoint.c_str());
      return fail(err, 1);
    }
    SSL_set_quiet_shutdown(ssl, 1);
  }

  if (importSock(fd, url, opt, err)) {
    if (ssl) {
      SSL_free(ssl);
      SSL_CTX_free(ctx);
    }
    close(fd);
    if (did_mount)
      umount(opt.mountpoint.c_str());
    return fail(err, 1);
  }

  if (ssl) {
    SSL_free(ssl);
    SSL_CTX_free(ctx);
  }
  close(fd);

  if (opt.verbose)
    std::cerr << "xiofsagent: imported "
              << (url.tls ? "kTLS" : "plaintext") << " socket for " << opt.url
              << "\n";
  return 0;
}

} // namespace

int main(int argc, char **argv)
{
  Options opt;
  std::string err;
  int prc;
  if (basenameOf(argv[0]) == "mount.xiofs")
    prc = parseHelperArgs(argc, argv, opt, err);
  else
    prc = parseAgentArgs(argc, argv, opt, err);
  if (prc > 0)
    return 0;
  if (prc < 0)
    return fail(err, 2);
  return run(opt);
}
