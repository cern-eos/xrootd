//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioHttp2.hh"
#include "XioKrb5.hh"

#ifdef HAVE_PARSOCKET
#include "parsock.h"
#endif

#ifdef HAVE_ZSTD
#include <zstd.h>
#endif

#include <nghttp2/nghttp2.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <strings.h>
#include <algorithm>
#include <cctype>
#include <vector>

namespace XioFS {

namespace {

bool ieq(const std::string &a, const char *b)
{
  return strcasecmp(a.c_str(), b) == 0;
}

bool isRedirectStatus(int status)
{
  return status == 301 || status == 302 || status == 303 ||
         status == 307 || status == 308;
}

std::string lowerCopy(std::string s)
{
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return s;
}

void refreshAuthority(Url &u)
{
  const bool v6 = u.host.find(':') != std::string::npos;
  const std::string h = v6 ? "[" + u.host + "]" : u.host;
  if ((u.tls && !u.parsock && u.port == 443) ||
      (!u.tls && !u.parsock && u.port == 80))
    u.authority = h;
  else
    u.authority = h + ":" + std::to_string(u.port);
  u.hops.clear();
  Hop hop;
  hop.host = u.host;
  hop.port = u.port;
  u.hops.push_back(hop);
}

bool sameHost(const std::string &a, const std::string &b)
{
  return lowerCopy(a) == lowerCopy(b);
}

bool headerFlag(const std::string &params, const char *key, std::string &value)
{
  size_t start = 0;
  const std::string want = lowerCopy(key);
  while (start < params.size()) {
    while (start < params.size() &&
           (params[start] == ';' || params[start] == ' ' || params[start] == '\t'))
      start++;
    if (start >= params.size())
      break;
    size_t end = params.find(';', start);
    if (end == std::string::npos)
      end = params.size();
    std::string tok = params.substr(start, end - start);
    start = end + 1;
    while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t'))
      tok.erase(tok.begin());
    while (!tok.empty() && (tok.back() == ' ' || tok.back() == '\t'))
      tok.pop_back();
    auto eq = tok.find('=');
    if (eq == std::string::npos)
      continue;
    if (lowerCopy(tok.substr(0, eq)) != want)
      continue;
    value = tok.substr(eq + 1);
    return true;
  }
  return false;
}

bool encodingHasZstd(const std::string &value)
{
  std::string enc = value;
  std::transform(enc.begin(), enc.end(), enc.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  size_t start = 0;
  while (start < enc.size()) {
    while (start < enc.size() &&
           (enc[start] == ' ' || enc[start] == ',' || enc[start] == '\t'))
      start++;
    if (start >= enc.size())
      break;
    size_t end = enc.find(',', start);
    if (end == std::string::npos)
      end = enc.size();
    std::string tok = enc.substr(start, end - start);
    start = end + 1;
    auto semi = tok.find(';');
    if (semi != std::string::npos)
      tok.resize(semi);
    while (!tok.empty() && (tok.back() == ' ' || tok.back() == '\t'))
      tok.pop_back();
    if (tok == "zstd")
      return true;
  }
  return false;
}

nghttp2_nv makeNv(const char *name, const std::string &value)
{
  return nghttp2_nv{
      const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(name)),
      const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(value.data())),
      strlen(name), value.size(), NGHTTP2_NV_FLAG_NONE};
}

int pollFd(int fd, short events, int timeout_ms)
{
  pollfd pfd{};
  pfd.fd = fd;
  pfd.events = events;
  int rc = poll(&pfd, 1, timeout_ms);
  if (rc == 0)
    return ETIMEDOUT;
  if (rc < 0)
    return errno;
  return 0;
}

} // namespace

std::string HttpResponse::header(const char *name) const
{
  for (const auto &h : headers) {
    if (ieq(h.first, name))
      return h.second;
  }
  return {};
}

void Http2Session::Inflight::cleanupZstd()
{
#ifdef HAVE_ZSTD
  if (dstream) {
    ZSTD_freeDStream(static_cast<ZSTD_DStream *>(dstream));
    dstream = nullptr;
  }
  is_zstd = false;
#endif
}

int Http2Session::Inflight::deliverBody(const uint8_t *data, size_t len,
                                        Http2Session *self)
{
  if (on_chunk) {
    if (self && self->opt_.timeout_ms > 0)
      deadline = Clock::now() + std::chrono::milliseconds(self->opt_.timeout_ms);
    int crc = on_chunk(data, len);
    if (crc < 0) {
      if (err && err->empty())
        *err = "Stream callback aborted";
      rc = crc;
      return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    }
    return 0;
  }
  if (!resp)
    return 0;
  if (resp->body.size() + len > self->opt_.max_body) {
    if (err)
      *err = "HTTP/2 response exceeded max_body";
    rc = -EFBIG;
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
  }
  resp->body.append(reinterpret_cast<const char *>(data), len);
  return 0;
}

#ifdef HAVE_ZSTD
int Http2Session::Inflight::feedZstd(const uint8_t *data, size_t len,
                                     Http2Session *self)
{
  if (!dstream) {
    if (err && err->empty())
      *err = "zstd decoder not initialized";
    rc = -EPROTO;
    return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
  }
  auto *zds = static_cast<ZSTD_DStream *>(dstream);
  ZSTD_inBuffer in = {data, len, 0};
  char out_buf[32768];
  while (in.pos < in.size) {
    ZSTD_outBuffer out = {out_buf, sizeof(out_buf), 0};
    size_t ret = ZSTD_decompressStream(zds, &out, &in);
    if (ZSTD_isError(ret)) {
      if (err && err->empty())
        *err = std::string("zstd decompress: ") + ZSTD_getErrorName(ret);
      rc = -EPROTO;
      return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    }
    if (out.pos > 0) {
      int drc = deliverBody(reinterpret_cast<const uint8_t *>(out_buf),
                            out.pos, self);
      if (drc != 0)
        return drc;
    }
  }
  return 0;
}

void Http2Session::Inflight::noteDecoded()
{
  if (!is_zstd || !resp)
    return;
  auto &hdrs = resp->headers;
  hdrs.erase(std::remove_if(hdrs.begin(), hdrs.end(),
                            [](const std::pair<std::string, std::string> &h) {
                              return ieq(h.first, "content-encoding") ||
                                     ieq(h.first, "content-length");
                            }),
             hdrs.end());
  if (!on_chunk)
    hdrs.emplace_back("content-length", std::to_string(resp->body.size()));
}
#endif

Http2Session::Http2Session() = default;

Http2Session::~Http2Session()
{
  close();
}

int Http2Session::setNonBlocking(int fd)
{
  int fl = fcntl(fd, F_GETFL, 0);
  if (fl < 0)
    return -errno;
  if (fcntl(fd, F_SETFL, fl | O_NONBLOCK) < 0)
    return -errno;
  return 0;
}

bool Http2Session::connected() const
{
  std::lock_guard<std::mutex> lock(mu_);
  return (fd_ >= 0
#ifdef HAVE_PARSOCKET
          || parsock_ != nullptr
#endif
         ) && session_ != nullptr && io_.joinable() && !stop_;
}

bool Http2Session::isParsock() const
{
#ifdef HAVE_PARSOCKET
  std::lock_guard<std::mutex> lock(mu_);
  return parsock_ != nullptr;
#else
  return false;
#endif
}

int Http2Session::registerDeviceMem(void *dev_ptr, size_t len, int dmabuf_fd,
                                    void **mem_handle, std::string &err)
{
  if (!mem_handle) {
    err = "null mem_handle pointer";
    return -EINVAL;
  }
  *mem_handle = nullptr;

#ifdef HAVE_PARSOCKET
  std::lock_guard<std::mutex> lock(mu_);
  if (parsock_) {
    auto *dm = new ps_device_mem{};
    dm->type = dmabuf_fd >= 0 ? PS_MEM_DMABUF : PS_MEM_CUDA;
    dm->ptr = dev_ptr;
    dm->length = len;
    dm->dmabuf_fd = dmabuf_fd;
    dm->offset = 0;
    dm->internal_mr = nullptr;

    int rc = ps_rdma_reg_device_mem(static_cast<ps_connection_t *>(parsock_), dm);
    if (rc != PS_OK) {
      err = "ps_rdma_reg_device_mem failed: " + std::string(ps_strerror(rc < 0 ? -rc : rc));
      delete dm;
      return -EIO;
    }
    *mem_handle = dm;
    return 0;
  }
#endif

  auto *dummy = new size_t(len);
  *mem_handle = dummy;
  return 0;
}

int Http2Session::deregisterDeviceMem(void *mem_handle, std::string &err)
{
  if (!mem_handle)
    return 0;

#ifdef HAVE_PARSOCKET
  std::lock_guard<std::mutex> lock(mu_);
  if (parsock_) {
    auto *dm = static_cast<ps_device_mem *>(mem_handle);
    int rc = ps_rdma_dereg_device_mem(static_cast<ps_connection_t *>(parsock_), dm);
    delete dm;
    if (rc != PS_OK) {
      err = "ps_rdma_dereg_device_mem failed: " + std::string(ps_strerror(rc < 0 ? -rc : rc));
      return -EIO;
    }
    return 0;
  }
#endif

  delete static_cast<size_t *>(mem_handle);
  return 0;
}

void Http2Session::wakeIo()
{
  if (wake_wr_ < 0)
    return;
  char c = 1;
  ssize_t n = ::write(wake_wr_, &c, 1);
  (void)n;
}

void Http2Session::drainWake()
{
  if (wake_rd_ < 0)
    return;
  char buf[64];
  while (::read(wake_rd_, buf, sizeof(buf)) > 0) {
  }
}

void Http2Session::failInflight(int rc, const std::string &msg)
{
  for (auto &p : inflight_) {
    Inflight *st = p.second;
    if (!st || st->done)
      continue;
    st->rc = rc;
    if (st->err && st->err->empty())
      *st->err = msg;
    st->done = true;
    st->cv.notify_one();
  }
  inflight_.clear();
  slot_cv_.notify_all();
}

void Http2Session::close()
{
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
    io_cv_.notify_all();
    slot_cv_.notify_all();
  }
  wakeIo();
  if (io_.joinable())
    io_.join();

  std::lock_guard<std::mutex> lock(mu_);
  failInflight(-ENOTCONN, "connection closed");

#ifdef HAVE_PARSOCKET
  if (parsock_) {
    ps_close(static_cast<ps_connection_t *>(parsock_));
    parsock_ = nullptr;
  }
#endif

  if (session_) {
    nghttp2_session_del(static_cast<nghttp2_session *>(session_));
    session_ = nullptr;
  }
  if (ssl_) {
    SSL_shutdown(static_cast<SSL *>(ssl_));
    SSL_free(static_cast<SSL *>(ssl_));
    ssl_ = nullptr;
  }
  if (ssl_ctx_) {
    SSL_CTX_free(static_cast<SSL_CTX *>(ssl_ctx_));
    ssl_ctx_ = nullptr;
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  if (wake_rd_ >= 0) {
    ::close(wake_rd_);
    wake_rd_ = -1;
  }
  if (wake_wr_ >= 0) {
    ::close(wake_wr_);
    wake_wr_ = -1;
  }
  stop_ = false;
}

int Http2Session::tcpConnect(std::string &err)
{
  addrinfo hints{};
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_family = AF_UNSPEC;
  addrinfo *res = nullptr;
  int rc = getaddrinfo(url_.host.c_str(), std::to_string(url_.port).c_str(),
                       &hints, &res);
  if (rc != 0) {
    err = std::string("getaddrinfo: ") + gai_strerror(rc);
    return -EHOSTUNREACH;
  }

  int last_errno = 0;
  for (addrinfo *ai = res; ai; ai = ai->ai_next) {
    int s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (s < 0) {
      last_errno = errno;
      continue;
    }
    int one = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    if (::connect(s, ai->ai_addr, ai->ai_addrlen) == 0) {
      fd_ = s;
      freeaddrinfo(res);
      return 0;
    }
    last_errno = errno;
    ::close(s);
  }
  freeaddrinfo(res);
  err = std::string("connect: ") + strerror(last_errno);
  return last_errno ? -last_errno : -ECONNREFUSED;
}

int Http2Session::tlsHandshake(std::string &err)
{
  SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
  if (!ctx) {
    err = "SSL_CTX_new failed";
    return -EIO;
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  if (opt_.verify_peer) {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    const char *file = opt_.cacert.empty() ? nullptr : opt_.cacert.c_str();
    const char *dir = opt_.cadir.empty() ? nullptr : opt_.cadir.c_str();
    if (file || dir) {
      if (SSL_CTX_load_verify_locations(ctx, file, dir) != 1) {
        SSL_CTX_free(ctx);
        err = "failed to load CA";
        if (file)
          err += std::string(" file: ") + file;
        if (dir)
          err += std::string(" dir: ") + dir;
        return -EINVAL;
      }
    } else {
      SSL_CTX_set_default_verify_paths(ctx);
    }
  } else {
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
  }

  static const unsigned char alpn[] = {2, 'h', '2'};
  if (SSL_CTX_set_alpn_protos(ctx, alpn, sizeof(alpn)) != 0) {
    SSL_CTX_free(ctx);
    err = "SSL_CTX_set_alpn_protos failed";
    return -EIO;
  }

  SSL *ssl = SSL_new(ctx);
  if (!ssl) {
    SSL_CTX_free(ctx);
    err = "SSL_new failed";
    return -EIO;
  }
  SSL_set_fd(ssl, fd_);
  SSL_set_tlsext_host_name(ssl, url_.host.c_str());
  SSL_set_alpn_protos(ssl, alpn, sizeof(alpn));
#ifdef SSL_CTRL_SET_TLSEXT_HOSTNAME
  SSL_set1_host(ssl, url_.host.c_str());
#endif

  int rc;
  while ((rc = SSL_connect(ssl)) != 1) {
    int serr = SSL_get_error(ssl, rc);
    if (serr == SSL_ERROR_WANT_READ) {
      if (int e = pollFd(fd_, POLLIN, opt_.timeout_ms)) {
        SSL_free(ssl);
        SSL_CTX_free(ctx);
        err = "TLS handshake timed out waiting to read";
        return -e;
      }
      continue;
    }
    if (serr == SSL_ERROR_WANT_WRITE) {
      if (int e = pollFd(fd_, POLLOUT, opt_.timeout_ms)) {
        SSL_free(ssl);
        SSL_CTX_free(ctx);
        err = "TLS handshake timed out waiting to write";
        return -e;
      }
      continue;
    }
    char buf[256];
    ERR_error_string_n(ERR_get_error(), buf, sizeof(buf));
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    err = std::string("TLS handshake failed: ") + buf;
    return -EPROTO;
  }

  const unsigned char *proto = nullptr;
  unsigned int plen = 0;
  SSL_get0_alpn_selected(ssl, &proto, &plen);
  if (plen == 2 && memcmp(proto, "h2", 2) == 0) {
    // negotiated via ALPN
  } else if (plen == 0) {
    // xrd.tls currently does not advertise ALPN. XrdHttp still accepts the
    // HTTP/2 connection preface on a TLS socket (detectWireMode).
  } else {
    SSL_free(ssl);
    SSL_CTX_free(ctx);
    err = "server negotiated ALPN " +
          std::string(reinterpret_cast<const char *>(proto), plen) +
          ", not h2";
    return -EPROTO;
  }

  ssl_ctx_ = ctx;
  ssl_ = ssl;
  return 0;
}

int Http2Session::sslWrite(const uint8_t *buf, size_t len, std::string &err)
{
#ifdef HAVE_PARSOCKET
  if (parsock_) {
    auto *conn = static_cast<ps_connection_t *>(parsock_);
    const bool allow_auth = (opt_.parsock_protection == "authenticated" ||
                             opt_.parsock_protection == "auth");

    size_t off = 0;
    static const char kPreface[] = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
    const size_t kPrefaceLen = sizeof(kPreface) - 1;
    if (off + kPrefaceLen <= len && memcmp(buf + off, kPreface, kPrefaceLen) == 0) {
      ssize_t sent = ps_send_protected(conn, buf + off, kPrefaceLen, PS_PROTECTION_CONFIDENTIAL);
      if (sent <= 0) {
        err = "parsock send failed: " + std::string(ps_strerror(ps_connection_error(conn)));
        return -EIO;
      }
      off += kPrefaceLen;
    }

    while (off < len) {
      if (len - off < 9) {
        ssize_t sent = ps_send_protected(conn, buf + off, len - off, PS_PROTECTION_CONFIDENTIAL);
        if (sent <= 0) {
          err = "parsock send failed: " + std::string(ps_strerror(ps_connection_error(conn)));
          return -EIO;
        }
        break;
      }

      size_t payload_len = (static_cast<size_t>(buf[off]) << 16) |
                           (static_cast<size_t>(buf[off + 1]) << 8) |
                           static_cast<size_t>(buf[off + 2]);
      uint8_t type = buf[off + 3];
      size_t frame_total = 9 + payload_len;
      if (off + frame_total > len)
        frame_total = len - off;

      ps_protection_t prot_mode = PS_PROTECTION_CONFIDENTIAL;
      if (type == 0x00 && allow_auth) {
        prot_mode = PS_PROTECTION_AUTHENTICATED;
      }

      size_t frame_sent = 0;
      while (frame_sent < frame_total) {
        ssize_t sent = ps_send_protected(conn, buf + off + frame_sent,
                                         frame_total - frame_sent, prot_mode);
        if (sent <= 0) {
          err = "parsock send failed: " + std::string(ps_strerror(ps_connection_error(conn)));
          return -EIO;
        }
        frame_sent += static_cast<size_t>(sent);
      }
      off += frame_total;
    }
    return 0;
  }
#endif
  size_t off = 0;
  while (off < len) {
    if (ssl_) {
      int n = SSL_write(static_cast<SSL *>(ssl_), buf + off,
                        static_cast<int>(len - off));
      if (n > 0) {
        off += static_cast<size_t>(n);
        continue;
      }
      int serr = SSL_get_error(static_cast<SSL *>(ssl_), n);
      if (serr == SSL_ERROR_WANT_READ) {
        if (int e = pollFd(fd_, POLLIN, opt_.timeout_ms)) {
          err = "timeout during TLS write";
          return -e;
        }
        continue;
      }
      if (serr == SSL_ERROR_WANT_WRITE) {
        if (int e = pollFd(fd_, POLLOUT, opt_.timeout_ms)) {
          err = "timeout during TLS write";
          return -e;
        }
        continue;
      }
      err = "SSL_write failed";
      return -EIO;
    }
    ssize_t n = ::send(fd_, buf + off, len - off, 0);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
      if (int e = pollFd(fd_, POLLOUT, opt_.timeout_ms)) {
        err = "timeout during TCP write";
        return -e;
      }
      continue;
    }
    err = std::string("send: ") + strerror(errno);
    return -errno;
  }
  return 0;
}

int Http2Session::sslRead(uint8_t *buf, size_t len, std::string &err)
{
#ifdef HAVE_PARSOCKET
  if (parsock_) {
    auto *conn = static_cast<ps_connection_t *>(parsock_);
    struct ps_stats stats;
    if (ps_connection_stats(conn, &stats) != 0) {
      err = "parsock stats failed";
      return -EIO;
    }
    if (stats.reassembly_bytes == 0) {
      if (ps_connection_state(conn) != PS_STATE_OPEN) {
        err = "parsock connection closed";
        return -ECONNRESET;
      }
      return 0;
    }
    ssize_t n = ps_recv(conn, buf, len);
    if (n > 0)
      return static_cast<int>(n);
    if (n == 0) {
      err = "parsock connection closed";
      return -ECONNRESET;
    }
    err = "parsock recv failed: " +
          std::string(ps_strerror(ps_connection_error(conn)));
    return -EIO;
  }
#endif
  if (ssl_) {
    int n = SSL_read(static_cast<SSL *>(ssl_), buf, static_cast<int>(len));
    if (n > 0)
      return n;
    int serr = SSL_get_error(static_cast<SSL *>(ssl_), n);
    if (serr == SSL_ERROR_WANT_READ || serr == SSL_ERROR_WANT_WRITE)
      return 0;
    if (serr == SSL_ERROR_ZERO_RETURN) {
      err = "TLS connection closed";
      return -ECONNRESET;
    }
    err = "SSL_read failed";
    return -EIO;
  }
  ssize_t n = ::recv(fd_, buf, len, 0);
  if (n > 0)
    return static_cast<int>(n);
  if (n == 0) {
    err = "connection closed";
    return -ECONNRESET;
  }
  if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
    return 0;
  err = std::string("recv: ") + strerror(errno);
  return -errno;
}

int Http2Session::connect(const Url &url, const Options &opt, std::string &err)
{
  close();
  std::unique_lock<std::mutex> lock(mu_);
  url_ = url;
  opt_ = opt;
  max_concurrent_ = opt.max_streams ? opt.max_streams : 100;
  stop_ = false;

  if (opt_.krb5 && opt_.negotiate.empty()) {
    if (!getSpnegoToken(url_.host, opt_.principal, opt_.negotiate, err)) {
      return -EACCES;
    }
  }

#ifdef HAVE_PARSOCKET
  if (url_.parsock || opt_.parsock) {
    struct ps_options popt;
    ps_options_init(&popt);
    popt.initial_sockets =
        opt_.parsock_sockets > 0 ? static_cast<unsigned>(opt_.parsock_sockets) : 4;
    popt.min_sockets = 1;
    popt.max_sockets = popt.initial_sockets > 4 ? popt.initial_sockets : 4;
    if (opt_.parsock_transport == "rdma")
      popt.transport = PS_TRANSPORT_RDMA_RC;
    else if (opt_.parsock_transport == "quic")
      popt.transport = PS_TRANSPORT_QUIC;
    else
      popt.transport = PS_TRANSPORT_TCP;

    if (opt_.parsock_protection == "authenticated" ||
        opt_.parsock_protection == "auth")
      popt.min_protection = PS_PROTECTION_AUTHENTICATED;
    else
      popt.min_protection = PS_PROTECTION_CONFIDENTIAL;

    popt.connect_timeout_ms =
        opt_.timeout_ms > 0 ? static_cast<uint32_t>(opt_.timeout_ms) : 10000;
    popt.io_timeout_ms =
        opt_.timeout_ms > 0 ? static_cast<uint32_t>(opt_.timeout_ms) : 30000;

    struct ps_tls_config ptls;
    memset(&ptls, 0, sizeof(ptls));
    ptls.server_name = url_.host.c_str();
    if (!opt_.cacert.empty())
      ptls.ca_file = opt_.cacert.c_str();
    ptls.verify_peer = opt_.verify_peer ? 1 : 0;
    ptls.allow_insecure = opt_.verify_peer ? 0 : 1;
    popt.tls = &ptls;

    ps_connection_t *pconn =
        ps_connect(url_.host.c_str(), static_cast<uint16_t>(url_.port), &popt);
    if (!pconn) {
      err = std::string("ps_connect failed: ") + ps_strerror(ps_recent_error());
      return -ECONNREFUSED;
    }
    parsock_ = pconn;
    fd_ = 0;
  } else
#endif
  {
    int rc = tcpConnect(err);
    if (rc)
      return rc;
    if (url_.tls) {
      rc = tlsHandshake(err);
      if (rc) {
        if (fd_ >= 0) {
          ::close(fd_);
          fd_ = -1;
        }
        return rc;
      }
    }
    if (int e = setNonBlocking(fd_)) {
      err = "fcntl O_NONBLOCK failed";
      return e;
    }
  }

  int p[2];
  if (::pipe(p) != 0) {
    err = std::string("pipe: ") + strerror(errno);
    return -errno;
  }
  wake_rd_ = p[0];
  wake_wr_ = p[1];
  setNonBlocking(wake_rd_);
  setNonBlocking(wake_wr_);

  nghttp2_session_callbacks *cb = nullptr;
  nghttp2_session_callbacks_new(&cb);
  nghttp2_session_callbacks_set_on_header_callback(
      cb,
      [](nghttp2_session *sess, const nghttp2_frame *frame, const uint8_t *name,
         size_t namelen, const uint8_t *value, size_t valuelen, uint8_t,
         void *) -> int {
        auto *st = static_cast<Inflight *>(
            nghttp2_session_get_stream_user_data(sess, frame->hd.stream_id));
        if (!st || !st->resp)
          return 0;
        std::string n(reinterpret_cast<const char *>(name), namelen);
        std::string v(reinterpret_cast<const char *>(value), valuelen);
        if (n == ":status")
          st->resp->status = std::atoi(v.c_str());
        else
          st->resp->headers.emplace_back(n, v);
        if (ieq(n, "content-encoding") && encodingHasZstd(v)) {
#ifdef HAVE_ZSTD
          st->is_zstd = true;
          if (!st->dstream)
            st->dstream = ZSTD_createDStream();
          if (st->dstream)
            ZSTD_initDStream(static_cast<ZSTD_DStream *>(st->dstream));
          else {
            if (st->err && st->err->empty())
              *st->err = "ZSTD_createDStream failed";
            st->rc = -ENOMEM;
          }
#else
          if (st->err && st->err->empty())
            *st->err = "content-encoding: zstd is not supported";
          st->rc = -EPROTO;
#endif
        }
        return 0;
      });
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
      cb,
      [](nghttp2_session *sess, uint8_t, int32_t stream_id, const uint8_t *data,
         size_t len, void *user) -> int {
        auto *self = static_cast<Http2Session *>(user);
        auto *st = static_cast<Inflight *>(
            nghttp2_session_get_stream_user_data(sess, stream_id));
        if (!st || !st->resp)
          return 0;
        if (isRedirectStatus(st->resp->status))
          return 0;
#ifdef HAVE_ZSTD
        if (st->is_zstd)
          return st->feedZstd(data, len, self);
#endif
        return st->deliverBody(data, len, self);
      });
  nghttp2_session_callbacks_set_on_stream_close_callback(
      cb,
      [](nghttp2_session *sess, int32_t stream_id, uint32_t error_code,
         void *user) -> int {
        auto *self = static_cast<Http2Session *>(user);
        auto *st = static_cast<Inflight *>(
            nghttp2_session_get_stream_user_data(sess, stream_id));
        if (!st)
          return 0;
        if (error_code != 0 && st->rc == 0) {
          st->rc = -ECONNRESET;
          if (st->err && st->err->empty())
            *st->err = "HTTP/2 stream closed";
        }
#ifdef HAVE_ZSTD
        if (st->rc == 0)
          st->noteDecoded();
#endif
        self->inflight_.erase(stream_id);
        st->done = true;
        st->cv.notify_one();
        self->slot_cv_.notify_all();
        return 0;
      });
  nghttp2_session_callbacks_set_on_frame_recv_callback(
      cb,
      [](nghttp2_session *sess, const nghttp2_frame *frame, void *user) -> int {
        if (frame->hd.type != NGHTTP2_SETTINGS ||
            (frame->hd.flags & NGHTTP2_FLAG_ACK))
          return 0;
        auto *self = static_cast<Http2Session *>(user);
        uint32_t remote = nghttp2_session_get_remote_settings(
            sess, NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS);
        if (remote > 0 && remote < self->max_concurrent_)
          self->max_concurrent_ = remote;
        return 0;
      });

  nghttp2_session *sess = nullptr;
  nghttp2_session_client_new(&sess, cb, this);
  nghttp2_session_callbacks_del(cb);
  session_ = sess;

  nghttp2_settings_entry iv[2];
  iv[0].settings_id = NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS;
  iv[0].value = max_concurrent_;
  iv[1].settings_id = NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE;
  iv[1].value = 16 * 1024 * 1024;
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, iv, 2);
  lock.unlock();
  io_ = std::thread(&Http2Session::ioLoop, this);
  return 0;
}

int Http2Session::flushSend(std::unique_lock<std::mutex> &lock, std::string &err)
{
  auto *sess = static_cast<nghttp2_session *>(session_);
  for (;;) {
    const uint8_t *data = nullptr;
    ssize_t n = nghttp2_session_mem_send(sess, &data);
    if (n < 0) {
      err = nghttp2_strerror(static_cast<int>(n));
      return -EIO;
    }
    if (n == 0)
      return 0;
    std::vector<uint8_t> tmp(data, data + n);
    lock.unlock();
    int wrc = sslWrite(tmp.data(), tmp.size(), err);
    lock.lock();
    if (wrc)
      return wrc;
  }
}

int Http2Session::nextPollTimeoutMsLocked() const
{
  if (inflight_.empty())
    return 250;
  auto now = Clock::now();
  int ms = 1000;
  for (const auto &p : inflight_) {
    int left = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            p.second->deadline - now)
            .count());
    ms = std::min(ms, left);
  }
  return ms < 0 ? 0 : ms;
}

void Http2Session::ioLoop()
{
  std::string err;
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_) {
    if (session_) {
      int wrc = flushSend(lock, err);
      if (wrc) {
        failInflight(wrc, err);
        break;
      }
    }

    auto now = Clock::now();
    auto *sess = static_cast<nghttp2_session *>(session_);
    std::vector<int32_t> timedout;
    for (const auto &p : inflight_) {
      if (now >= p.second->deadline)
        timedout.push_back(p.first);
    }
    for (int32_t sid : timedout) {
      nghttp2_submit_rst_stream(sess, NGHTTP2_FLAG_NONE, sid, NGHTTP2_CANCEL);
      nghttp2_session_set_stream_user_data(sess, sid, nullptr);
      auto it = inflight_.find(sid);
      if (it == inflight_.end())
        continue;
      Inflight *st = it->second;
      inflight_.erase(it);
      st->rc = -ETIMEDOUT;
      if (st->err && st->err->empty())
        *st->err = "HTTP/2 request timed out";
      st->done = true;
      st->cv.notify_one();
      slot_cv_.notify_all();
    }
    if (!timedout.empty())
      continue;

    const bool want_write = sess && nghttp2_session_want_write(sess);
    if (inflight_.empty() && !want_write) {
      io_cv_.wait(lock, [&] { return stop_ || !inflight_.empty(); });
      continue;
    }

    int timeout = nextPollTimeoutMsLocked();
    const bool pending_ssl =
        ssl_ && SSL_pending(static_cast<SSL *>(ssl_)) > 0;
#ifdef HAVE_PARSOCKET
    const bool pending_parsock = parsock_ && ([&]() {
      struct ps_stats st;
      return ps_connection_stats(static_cast<ps_connection_t *>(parsock_), &st) == 0 &&
             st.reassembly_bytes > 0;
    })();
#else
    const bool pending_parsock = false;
#endif
    lock.unlock();

    if (!pending_ssl && !pending_parsock) {
      pollfd pfds[2]{};
      nfds_t np = 0;
#ifdef HAVE_PARSOCKET
      if (parsock_) {
        if (wake_rd_ >= 0) {
          pfds[np].fd = wake_rd_;
          pfds[np].events = POLLIN;
          np++;
        }
        int wait_ms = inflight_.empty() ? timeout : std::min(timeout, 2);
        poll(pfds, np, wait_ms);
        drainWake();
      } else
#endif
      {
        pfds[np].fd = fd_;
        pfds[np].events = POLLIN;
        if (want_write)
          pfds[np].events = static_cast<short>(pfds[np].events | POLLOUT);
        np++;
        if (wake_rd_ >= 0) {
          pfds[np].fd = wake_rd_;
          pfds[np].events = POLLIN;
          np++;
        }
        poll(pfds, np, timeout);
        drainWake();
      }
    }

    uint8_t buf[16384];
    int n = sslRead(buf, sizeof(buf), err);
    lock.lock();
    if (stop_)
      break;
    if (n < 0) {
      failInflight(n, err);
      break;
    }
    if (n == 0)
      continue;
    ssize_t rv = nghttp2_session_mem_recv(
        static_cast<nghttp2_session *>(session_), buf, static_cast<size_t>(n));
    if (rv < 0) {
      failInflight(-EPROTO, nghttp2_strerror(static_cast<int>(rv)));
      break;
    }
  }
  if (!inflight_.empty())
    failInflight(-ENOTCONN, "HTTP/2 I/O thread exiting");
}

int planRedirect(const Url &current, const Http2Session::Options &opt,
                 const HttpRequest &req, const HttpResponse &resp,
                 RedirectPlan &plan, std::string &err)
{
  plan = RedirectPlan{};
  if (!isRedirectStatus(resp.status))
    return 0;

  const std::string location = resp.header("location");
  if (location.empty()) {
    err = "redirect missing Location";
    return -EPROTO;
  }

  std::string absolute = location;
  if (location.front() == '/') {
    absolute = (current.tls ? "https://" : "http://");
    if (current.host.find(':') != std::string::npos)
      absolute += "[" + current.host + "]";
    else
      absolute += current.host;
    if (!((current.tls && current.port == 443) ||
          (!current.tls && current.port == 80)))
      absolute += ":" + std::to_string(current.port);
    absolute += location;
  } else if (location.find("://") == std::string::npos) {
    err = "redirect Location is not an absolute URL";
    return -EPROTO;
  }

  Url next;
  if (!parseUrl(absolute, next, err))
    return -EPROTO;

  std::string transport = lowerCopy(resp.header("x-xrd-transport"));
  Http2Session::Options next_opt = opt;
  if (transport.empty()) {
    next_opt.parsock = next.parsock;
  } else if (transport == "standard") {
    next.parsock = false;
    if (next.scheme == "spt" || next.scheme == "parsock") {
      next.scheme = "https";
      next.tls = true;
    }
    next_opt.parsock = false;
    refreshAuthority(next);
  } else if (transport == "parsock") {
    const std::string hint = resp.header("x-xrd-parsock");
    std::string port_s;
    if (!headerFlag(hint, "port", port_s)) {
      err = "parsock redirect missing X-Xrd-Parsock port";
      return -EPROTO;
    }
    int port = std::atoi(port_s.c_str());
    if (port <= 0 || port > 65535) {
      err = "parsock redirect has an invalid port";
      return -EPROTO;
    }
    next.parsock = true;
    next.tls = true;
    next.scheme = "spt";
    next.port = port;
    refreshAuthority(next);
    next_opt.parsock = true;
    std::string sockets;
    if (headerFlag(hint, "sockets", sockets)) {
      int n = std::atoi(sockets.c_str());
      if (n > 0)
        next_opt.parsock_sockets = n;
    }
    std::string tr;
    if (headerFlag(hint, "transport", tr) && !tr.empty())
      next_opt.parsock_transport = tr;
    std::string prot;
    if (headerFlag(hint, "protection", prot) && !prot.empty())
      next_opt.parsock_protection = prot;
  } else {
    err = "unsupported X-Xrd-Transport value: " + transport;
    return -EPROTO;
  }

  HttpRequest next_req = req;
  const bool rewrite_get =
      resp.status == 303 ||
      ((resp.status == 301 || resp.status == 302) &&
       req.method != "GET" && req.method != "HEAD");
  if (rewrite_get) {
    next_req.method = "GET";
    next_req.body.clear();
  }
  next_req.path = next.path.empty() ? "/" : next.path;
  next_req.headers.erase(
      std::remove_if(next_req.headers.begin(), next_req.headers.end(),
                     [&](const std::pair<std::string, std::string> &h) {
                       if (ieq(h.first, "content-length") || ieq(h.first, "host"))
                         return true;
                       return rewrite_get && ieq(h.first, "content-type");
                     }),
      next_req.headers.end());

  const bool same_host = sameHost(current.host, next.host);
  if (!same_host) {
    next_req.headers.erase(
        std::remove_if(next_req.headers.begin(), next_req.headers.end(),
                       [](const std::pair<std::string, std::string> &h) {
                         return ieq(h.first, "authorization");
                       }),
        next_req.headers.end());
  }

  plan.request = std::move(next_req);
  plan.url = next;
  plan.opt = next_opt;
  plan.send_credentials = same_host;
  plan.reconnect = !sameHost(current.host, next.host) || current.port != next.port ||
                   current.parsock != next.parsock || current.tls != next.tls;
  const std::string sticky = lowerCopy(resp.header("x-xrd-sticky"));
  plan.sticky = sticky == "1" || sticky == "true" || sticky == "yes" || sticky == "on";
  return 1;
}

int Http2Session::exchange(const HttpRequest &req, HttpResponse &resp,
                           std::string &err, bool send_credentials)
{
  return requestOnce(req, resp, err, send_credentials);
}

int Http2Session::request(const HttpRequest &req, HttpResponse &resp,
                          std::string &err)
{
  HttpRequest cur = req;
  bool send_credentials = true;
  for (int followed = 0;; ++followed) {
    int rc = requestOnce(cur, resp, err, send_credentials);
    if (rc)
      return rc;
    RedirectPlan plan;
    int prc = planRedirect(url_, opt_, cur, resp, plan, err);
    if (prc <= 0)
      return prc;
    if (followed >= 8) {
      err = "too many HTTP redirects";
      return -ELOOP;
    }
    cur = std::move(plan.request);
    send_credentials = plan.send_credentials;
    if (!plan.reconnect) {
      url_.path = plan.url.path;
      continue;
    }
    rc = connect(plan.url, plan.opt, err);
    if (rc)
      return rc;
  }
}

int Http2Session::requestOnce(const HttpRequest &req, HttpResponse &resp,
                              std::string &err, bool send_credentials)
{
  Inflight st;
  st.resp = &resp;
  st.err = &err;
  st.body = req.body;
  st.deadline = Clock::now() + std::chrono::milliseconds(opt_.timeout_ms);
  st.on_chunk = req.on_chunk;

  std::unique_lock<std::mutex> lock(mu_);
  if ((fd_ < 0
#ifdef HAVE_PARSOCKET
       && !parsock_
#endif
      ) || !session_ || !io_.joinable() || stop_) {
    err = "not connected";
    return -ENOTCONN;
  }

  while (inflight_.size() >= max_concurrent_ && !stop_)
    slot_cv_.wait(lock);
  if (stop_ || !session_) {
    err = "not connected";
    return -ENOTCONN;
  }

  resp = HttpResponse{};
  err.clear();

  // nghttp2_nv stores pointers; every name/value must outlive submit_request().
  std::vector<std::string> hdrstore;
  auto addHdr = [&](const char *name, const std::string &value) {
    hdrstore.emplace_back(name);
    hdrstore.push_back(value);
  };
  addHdr(":method", req.method);
  addHdr(":path", req.path.empty() ? "/" : req.path);
  addHdr(":scheme", url_.tls ? "https" : "http");
  addHdr(":authority", url_.authority);
  addHdr("user-agent", "XIOFS/0.1");
#ifdef HAVE_ZSTD
  bool has_ae = false;
  for (const auto &h : req.headers) {
    if (strcasecmp(h.first.c_str(), "accept-encoding") == 0) {
      has_ae = true;
      break;
    }
  }
  if (!has_ae)
    addHdr("accept-encoding", "zstd");
#endif
  std::string auth;
  if (send_credentials) {
    if (!opt_.bearer.empty()) {
      auth = "Bearer " + opt_.bearer;
      addHdr("authorization", auth);
    } else if (!opt_.negotiate.empty()) {
      auth = "Negotiate " + opt_.negotiate;
      addHdr("authorization", auth);
    }
  }
  for (const auto &h : req.headers) {
    std::string name = h.first;
    for (char &c : name)
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    if (!send_credentials && name == "authorization")
      continue;
    addHdr(name.c_str(), h.second);
  }

  nghttp2_data_provider prd{};
  nghttp2_data_provider *prdptr = nullptr;
  if (!st.body.empty()) {
    prd.source.ptr = &st;
    prd.read_callback = [](nghttp2_session *, int32_t, uint8_t *buf,
                           size_t length, uint32_t *data_flags,
                           nghttp2_data_source *source, void *) -> ssize_t {
      auto *b = static_cast<Inflight *>(source->ptr);
      size_t left = b->body.size() - b->body_off;
      size_t n = left < length ? left : length;
      memcpy(buf, b->body.data() + b->body_off, n);
      b->body_off += n;
      if (b->body_off >= b->body.size())
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
      return static_cast<ssize_t>(n);
    };
    prdptr = &prd;
    addHdr("content-length", std::to_string(st.body.size()));
  }

  std::vector<nghttp2_nv> nva;
  nva.reserve(hdrstore.size() / 2);
  for (size_t i = 0; i + 1 < hdrstore.size(); i += 2)
    nva.push_back(makeNv(hdrstore[i].c_str(), hdrstore[i + 1]));

  auto *sess = static_cast<nghttp2_session *>(session_);
  int32_t sid = nghttp2_submit_request(sess, nullptr, nva.data(), nva.size(),
                                       prdptr, &st);
  if (sid < 0) {
    err = nghttp2_strerror(sid);
    return -EIO;
  }
  inflight_[sid] = &st;
  io_cv_.notify_one();
  wakeIo();

  while (!st.done) {
    if (!opt_.on_wait) {
      st.cv.wait(lock);
      continue;
    }
    st.cv.wait_for(lock, std::chrono::milliseconds(200));
    if (st.done)
      break;
    auto cb = opt_.on_wait;
    lock.unlock();
    cb();
    lock.lock();
  }

  if (st.rc)
    return st.rc;
  if (resp.status == 0) {
    err = "HTTP/2 response missing :status";
    return -EPROTO;
  }
  return 0;
}

} // namespace XioFS
