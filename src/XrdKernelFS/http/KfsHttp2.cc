//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "KfsHttp2.hh"

#include <nghttp2/nghttp2.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <arpa/inet.h>
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
#include <chrono>
#include <cctype>
#include <vector>

namespace Kfs {

namespace {

using Clock = std::chrono::steady_clock;

bool ieq(const std::string &a, const char *b)
{
  return strcasecmp(a.c_str(), b) == 0;
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

Http2Session::Http2Session() = default;

Http2Session::~Http2Session()
{
  close();
}

void Http2Session::close()
{
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
    if (!opt_.cacert.empty()) {
      if (SSL_CTX_load_verify_locations(ctx, opt_.cacert.c_str(), nullptr) != 1) {
        SSL_CTX_free(ctx);
        err = "failed to load CA file: " + opt_.cacert;
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

int Http2Session::waitReadable(std::string &err)
{
  int e = pollFd(fd_, POLLIN, opt_.timeout_ms);
  if (e) {
    err = (e == ETIMEDOUT) ? "timed out waiting for HTTP/2 data"
                           : strerror(e);
    return -e;
  }
  return 0;
}

int Http2Session::connect(const Url &url, const Options &opt, std::string &err)
{
  std::lock_guard<std::mutex> lock(mu_);
  close();
  url_ = url;
  opt_ = opt;
  int rc = tcpConnect(err);
  if (rc)
    return rc;
  if (url_.tls) {
    rc = tlsHandshake(err);
    if (rc) {
      close();
      return rc;
    }
  }

  nghttp2_session_callbacks *cb = nullptr;
  nghttp2_session_callbacks_new(&cb);
  nghttp2_session_callbacks_set_on_header_callback(
      cb,
      [](nghttp2_session *, const nghttp2_frame *frame, const uint8_t *name,
         size_t namelen, const uint8_t *value, size_t valuelen, uint8_t,
         void *user) -> int {
        auto *self = static_cast<Http2Session *>(user);
        if (!self->cur_ || frame->hd.stream_id != self->stream_id_)
          return 0;
        std::string n(reinterpret_cast<const char *>(name), namelen);
        std::string v(reinterpret_cast<const char *>(value), valuelen);
        if (n == ":status")
          self->cur_->status = std::atoi(v.c_str());
        else
          self->cur_->headers.emplace_back(std::move(n), std::move(v));
        return 0;
      });
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
      cb,
      [](nghttp2_session *, uint8_t, int32_t stream_id, const uint8_t *data,
         size_t len, void *user) -> int {
        auto *self = static_cast<Http2Session *>(user);
        if (!self->cur_ || stream_id != self->stream_id_)
          return 0;
        if (self->cur_->body.size() + len > self->opt_.max_body) {
          if (self->err_)
            *self->err_ = "HTTP/2 response exceeded max_body";
          return NGHTTP2_ERR_CALLBACK_FAILURE;
        }
        self->cur_->body.append(reinterpret_cast<const char *>(data), len);
        return 0;
      });
  nghttp2_session_callbacks_set_on_stream_close_callback(
      cb,
      [](nghttp2_session *, int32_t stream_id, uint32_t, void *user) -> int {
        auto *self = static_cast<Http2Session *>(user);
        if (stream_id == self->stream_id_)
          self->stream_closed_ = true;
        return 0;
      });

  nghttp2_session *sess = nullptr;
  nghttp2_session_client_new(&sess, cb, this);
  nghttp2_session_callbacks_del(cb);
  session_ = sess;

  nghttp2_settings_entry iv[2];
  iv[0].settings_id = NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS;
  iv[0].value = 100;
  iv[1].settings_id = NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE;
  iv[1].value = 16 * 1024 * 1024;
  nghttp2_submit_settings(sess, NGHTTP2_FLAG_NONE, iv, 2);
  return 0;
}

int Http2Session::drive(std::string &err)
{
  auto *sess = static_cast<nghttp2_session *>(session_);
  auto deadline = Clock::now() + std::chrono::milliseconds(opt_.timeout_ms);
  while (!stream_closed_) {
    if (Clock::now() > deadline) {
      err = "HTTP/2 request timed out";
      return -ETIMEDOUT;
    }
    for (;;) {
      const uint8_t *data = nullptr;
      ssize_t n = nghttp2_session_mem_send(sess, &data);
      if (n < 0) {
        err = nghttp2_strerror(static_cast<int>(n));
        return -EIO;
      }
      if (n == 0)
        break;
      int wrc = sslWrite(data, static_cast<size_t>(n), err);
      if (wrc)
        return wrc;
    }
    if (stream_closed_)
      break;
    if (!nghttp2_session_want_read(sess) && !nghttp2_session_want_write(sess)) {
      err = "HTTP/2 session closed unexpectedly";
      return -ECONNRESET;
    }
    int prc = waitReadable(err);
    if (prc)
      return prc;
    uint8_t buf[16384];
    int n = sslRead(buf, sizeof(buf), err);
    if (n < 0)
      return n;
    if (n == 0)
      continue;
    ssize_t rv = nghttp2_session_mem_recv(sess, buf, static_cast<size_t>(n));
    if (rv < 0) {
      err = nghttp2_strerror(static_cast<int>(rv));
      return -EPROTO;
    }
  }
  return 0;
}

int Http2Session::request(const HttpRequest &req, HttpResponse &resp,
                          std::string &err)
{
  std::lock_guard<std::mutex> lock(mu_);
  if (!connected()) {
    err = "not connected";
    return -ENOTCONN;
  }

  resp = HttpResponse{};
  cur_ = &resp;
  err_ = &err;
  stream_closed_ = false;

  std::vector<nghttp2_nv> nva;
  std::string method = req.method;
  std::string path = req.path.empty() ? "/" : req.path;
  std::string scheme = url_.tls ? "https" : "http";
  nva.push_back(makeNv(":method", method));
  nva.push_back(makeNv(":path", path));
  nva.push_back(makeNv(":scheme", scheme));
  nva.push_back(makeNv(":authority", url_.authority));
  nva.push_back(makeNv("user-agent", "XrdKernelFS/0.1"));
  std::string auth;
  if (!opt_.bearer.empty()) {
    auth = "Bearer " + opt_.bearer;
    nva.push_back(makeNv("authorization", auth));
  }
  std::vector<std::string> hdrstore;
  hdrstore.reserve(req.headers.size() * 2);
  for (const auto &h : req.headers) {
    std::string name = h.first;
    for (char &c : name)
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    hdrstore.push_back(std::move(name));
    hdrstore.push_back(h.second);
  }
  for (size_t i = 0; i + 1 < hdrstore.size(); i += 2)
    nva.push_back(makeNv(hdrstore[i].c_str(), hdrstore[i + 1]));

  struct BodySrc {
    const std::string *body;
    size_t off{0};
  } src{&req.body, 0};

  nghttp2_data_provider prd{};
  nghttp2_data_provider *prdptr = nullptr;
  if (!req.body.empty()) {
    prd.source.ptr = &src;
    prd.read_callback = [](nghttp2_session *, int32_t, uint8_t *buf,
                           size_t length, uint32_t *data_flags,
                           nghttp2_data_source *source, void *) -> ssize_t {
      auto *b = static_cast<BodySrc *>(source->ptr);
      size_t left = b->body->size() - b->off;
      size_t n = left < length ? left : length;
      memcpy(buf, b->body->data() + b->off, n);
      b->off += n;
      if (b->off >= b->body->size())
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
      return static_cast<ssize_t>(n);
    };
    prdptr = &prd;
    nva.push_back(makeNv("content-length", std::to_string(req.body.size())));
  }

  auto *sess = static_cast<nghttp2_session *>(session_);
  int32_t sid = nghttp2_submit_request(sess, nullptr, nva.data(), nva.size(),
                                       prdptr, this);
  if (sid < 0) {
    cur_ = nullptr;
    err = nghttp2_strerror(sid);
    return -EIO;
  }
  stream_id_ = sid;
  int rc = drive(err);
  cur_ = nullptr;
  err_ = nullptr;
  if (rc)
    return rc;
  if (resp.status == 0) {
    err = "HTTP/2 response missing :status";
    return -EPROTO;
  }
  return 0;
}

} // namespace Kfs
