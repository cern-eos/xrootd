//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioServer.hh"

#include "XioLanes.hh"

#include <nghttp2/nghttp2.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <deque>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

#if OPENSSL_VERSION_NUMBER < 0x30000000L
// OpenSSL 3 renamed this. 1.1.1 returns the same owned reference.
static X509 *SSL_get1_peer_certificate(const SSL *ssl)
{
  return SSL_get_peer_certificate(ssl);
}
#endif

namespace XioFS {

namespace {

std::string lower(std::string s)
{
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

bool needsBody(const std::string &method)
{
  const std::string m = lower(method);
  return m == "put" || m == "post" || m == "patch" || m == "proppatch";
}

int setCommonSock(int fd)
{
  int one = 1;
#ifdef SO_NOSIGPIPE
  setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#else
  (void)one;
#endif
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0)
    return -errno;
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    return -errno;
  return 0;
}

struct Pending {
  int32_t stream_id{0};
  int status{200};
  std::string body;
  size_t off{0};
  std::vector<std::pair<std::string, std::string>> headers;
  bool head{false};
};

struct StreamState {
  int32_t id{0};
  std::string method;
  std::string path;
  std::string query;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
  bool headers_done{false};
  bool end{false};
  bool dispatched{false};
};

class Conn : public std::enable_shared_from_this<Conn>
{
public:
  Conn(int fd, SSL *ssl, std::string peer, std::shared_ptr<Fs> fs, Lanes &lanes,
       Server::AuthHook auth)
    : fd_(fd), ssl_(ssl), peer_(std::move(peer)), fs_(std::move(fs)),
      lanes_(lanes), auth_(std::move(auth))
  {}

  ~Conn() { close(); }

  void run()
  {
    int p[2];
    if (::pipe(p) != 0)
      return;
    wake_rd_ = p[0];
    wake_wr_ = p[1];
    setCommonSock(wake_rd_);
    setCommonSock(wake_wr_);

    nghttp2_session_callbacks *cb = nullptr;
    nghttp2_session_callbacks_new(&cb);
    nghttp2_session_callbacks_set_on_begin_headers_callback(cb, onBegin);
    nghttp2_session_callbacks_set_on_header_callback(cb, onHeader);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cb, onData);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cb, onFrame);
    nghttp2_session_callbacks_set_on_stream_close_callback(cb, onClose);
    nghttp2_session_server_new(&session_, cb, this);
    nghttp2_session_callbacks_del(cb);

    nghttp2_settings_entry iv[2];
    iv[0].settings_id = NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS;
    iv[0].value = 100;
    iv[1].settings_id = NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE;
    iv[1].value = 16 * 1024 * 1024;
    nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE, iv, 2);
    loop();
  }

  void close()
  {
    alive_.store(false);
    if (session_) {
      nghttp2_session_del(session_);
      session_ = nullptr;
    }
    if (ssl_) {
      SSL_free(ssl_);
      ssl_ = nullptr;
    }
    if (fd_ >= 0) {
      ::shutdown(fd_, SHUT_RDWR);
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
  }

  void poke()
  {
    if (!alive_.load())
      return;
    char c = 1;
    // EAGAIN means the pipe already holds a wakeup.
    if (wake_wr_ >= 0 && ::write(wake_wr_, &c, 1) < 0)
      return;
  }

private:
  static int onBegin(nghttp2_session *, const nghttp2_frame *frame, void *ud)
  {
    if (frame->hd.type != NGHTTP2_HEADERS ||
        frame->headers.cat != NGHTTP2_HCAT_REQUEST)
      return 0;
    auto *self = static_cast<Conn *>(ud);
    StreamState st;
    st.id = frame->hd.stream_id;
    self->streams_[st.id] = std::move(st);
    return 0;
  }

  static int onHeader(nghttp2_session *, const nghttp2_frame *frame,
                      const uint8_t *name, size_t namelen, const uint8_t *value,
                      size_t valuelen, uint8_t, void *ud)
  {
    if (frame->hd.type != NGHTTP2_HEADERS)
      return 0;
    auto *self = static_cast<Conn *>(ud);
    auto it = self->streams_.find(frame->hd.stream_id);
    if (it == self->streams_.end())
      return 0;
    std::string n(reinterpret_cast<const char *>(name), namelen);
    std::string v(reinterpret_cast<const char *>(value), valuelen);
    if (n == ":method")
      it->second.method = std::move(v);
    else if (n == ":path") {
      auto q = v.find('?');
      if (q == std::string::npos)
        it->second.path = std::move(v);
      else {
        it->second.path = v.substr(0, q);
        it->second.query = v.substr(q + 1);
      }
    } else if (n != ":scheme" && n != ":authority") {
      it->second.headers.emplace_back(std::move(n), std::move(v));
    }
    return 0;
  }

  static int onData(nghttp2_session *sess, uint8_t, int32_t stream_id,
                    const uint8_t *data, size_t len, void *ud)
  {
    auto *self = static_cast<Conn *>(ud);
    auto it = self->streams_.find(stream_id);
    if (it == self->streams_.end())
      return 0;
    it->second.body.append(reinterpret_cast<const char *>(data), len);
    nghttp2_session_consume(sess, stream_id, len);
    return 0;
  }

  static int onFrame(nghttp2_session *, const nghttp2_frame *frame, void *ud)
  {
    if (frame->hd.type != NGHTTP2_HEADERS && frame->hd.type != NGHTTP2_DATA)
      return 0;
    auto *self = static_cast<Conn *>(ud);
    auto it = self->streams_.find(frame->hd.stream_id);
    if (it == self->streams_.end())
      return 0;
    if (frame->hd.type == NGHTTP2_HEADERS)
      it->second.headers_done = true;
    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)
      it->second.end = true;
    self->maybeDispatch(it->second);
    return 0;
  }

  static int onClose(nghttp2_session *, int32_t stream_id, uint32_t, void *ud)
  {
    auto *self = static_cast<Conn *>(ud);
    self->streams_.erase(stream_id);
    self->pending_.erase(stream_id);
    return 0;
  }

  static ssize_t readBody(nghttp2_session *, int32_t stream_id, uint8_t *buf,
                          size_t length, uint32_t *flags, nghttp2_data_source *,
                          void *ud)
  {
    auto *self = static_cast<Conn *>(ud);
    auto it = self->pending_.find(stream_id);
    if (it == self->pending_.end()) {
      *flags |= NGHTTP2_DATA_FLAG_EOF;
      return 0;
    }
    Pending &p = it->second;
    size_t left = p.body.size() - p.off;
    if (left == 0) {
      *flags |= NGHTTP2_DATA_FLAG_EOF;
      return 0;
    }
    size_t n = left < length ? left : length;
    memcpy(buf, p.body.data() + p.off, n);
    p.off += n;
    if (p.off == p.body.size())
      *flags |= NGHTTP2_DATA_FLAG_EOF;
    return static_cast<ssize_t>(n);
  }

  void maybeDispatch(StreamState &st)
  {
    if (st.dispatched || !st.headers_done)
      return;
    if (!st.end && needsBody(st.method))
      return;
    st.dispatched = true;
    FsRequest req;
    req.method = st.method;
    req.path = st.path.empty() ? "/" : st.path;
    // root:// URLs are written with an empty authority slash: host:port//path.
    while (req.path.size() > 1 && req.path[0] == '/' && req.path[1] == '/')
      req.path.erase(0, 1);
    req.query = st.query;
    req.headers = st.headers;
    req.body = std::move(st.body);
    req.peer = peer_;
    req.tls = ssl_ != nullptr;
    const int32_t id = st.id;
    if (auth_) {
      int arc = auth_(req, req.cred);
      if (arc) {
        FsResult denied;
        denied.err = EACCES;
        denied.http_status = 401;
        denied.body = "unauthorized";
        enqueue(id, req.method, denied);
        poke();
        return;
      }
    }
    std::weak_ptr<Conn> weak = shared_from_this();
    const bool queued = lanes_.submit(req.path, [weak, id, req](int) {
      std::shared_ptr<Conn> self = weak.lock();
      if (!self || !self->alive_.load())
        return;
      FsResult result;
      try {
        self->fs_->handle(req, result);
      } catch (const std::exception &ex) {
        result.err = EIO;
        result.http_status = 500;
        result.body = ex.what();
      }
      {
        std::lock_guard<std::mutex> lock(self->mu_);
        self->done_.push_back(Done{id, req.method, std::move(result)});
      }
      self->poke();
    });
    if (!queued) {
      FsResult busy;
      busy.http_status = 503;
      busy.stall_sec = 1;
      busy.body = "server stopping";
      enqueue(id, req.method, busy);
    }
  }

  struct Done {
    int32_t id;
    std::string method;
    FsResult result;
  };

  void enqueue(int32_t id, const std::string &method, FsResult result)
  {
    std::lock_guard<std::mutex> lock(mu_);
    done_.push_back(Done{id, method, std::move(result)});
  }

  void drainDone()
  {
    std::deque<Done> batch;
    {
      std::lock_guard<std::mutex> lock(mu_);
      batch.swap(done_);
    }
    for (Done &d : batch)
      submitResponse(d);
  }

  void submitResponse(Done &d)
  {
    if (!session_ || streams_.find(d.id) == streams_.end())
      return;
    FsResult &r = d.result;
    int status = r.http_status;
    if (!r.redirect.empty())
      status = 307;
    else if (r.stall_sec > 0 && status == 0)
      status = 503;
    else if (status == 0)
      status = errnoToHttp(r.err);
    const bool head = lower(d.method) == "head";
    const bool nobody = head || status == 204 || status == 205 || status == 304;

    Pending p;
    p.stream_id = d.id;
    p.status = status;
    p.head = head;
    p.headers = std::move(r.headers);
    if (!r.redirect.empty())
      p.headers.emplace_back("location", r.redirect);
    if (r.stall_sec > 0)
      p.headers.emplace_back("retry-after", std::to_string(r.stall_sec));
    if (!nobody)
      p.body = std::move(r.body);
    if (!r.content_type.empty() && !nobody && !p.body.empty())
      p.headers.emplace_back("content-type", r.content_type);
    if (!nobody)
      p.headers.emplace_back("content-length", std::to_string(p.body.size()));

    std::vector<std::pair<std::string, std::string>> owned;
    owned.emplace_back(":status", std::to_string(status));
    for (const auto &h : p.headers)
      owned.push_back(h);
    pending_[d.id] = std::move(p);

    std::vector<nghttp2_nv> nva(owned.size());
    for (size_t i = 0; i < owned.size(); ++i) {
      nva[i].name = reinterpret_cast<uint8_t *>(&owned[i].first[0]);
      nva[i].namelen = owned[i].first.size();
      nva[i].value = reinterpret_cast<uint8_t *>(&owned[i].second[0]);
      nva[i].valuelen = owned[i].second.size();
      nva[i].flags = NGHTTP2_NV_FLAG_NONE;
    }
    nghttp2_data_provider prd;
    prd.source.ptr = this;
    prd.read_callback = readBody;
    nghttp2_submit_response(session_, d.id, nva.data(), nva.size(),
                            nobody ? nullptr : &prd);
  }

  int rawRead(uint8_t *buf, size_t len)
  {
    if (ssl_) {
      int n = SSL_read(ssl_, buf, static_cast<int>(len));
      if (n > 0)
        return n;
      int e = SSL_get_error(ssl_, n);
      if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE)
        return 0;
      return -1;
    }
    ssize_t n = ::recv(fd_, buf, len, 0);
    if (n > 0)
      return static_cast<int>(n);
    if (n == 0)
      return -1;
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
      return 0;
    return -1;
  }

  int rawWrite(const uint8_t *buf, size_t len)
  {
    size_t off = 0;
    while (off < len) {
      int n;
      if (ssl_) {
        n = SSL_write(ssl_, buf + off, static_cast<int>(len - off));
        if (n <= 0) {
          int e = SSL_get_error(ssl_, n);
          if (e == SSL_ERROR_WANT_READ || e == SSL_ERROR_WANT_WRITE)
            return off ? static_cast<int>(off) : 0;
          return -1;
        }
      } else {
#ifdef MSG_NOSIGNAL
        ssize_t w = ::send(fd_, buf + off, len - off, MSG_NOSIGNAL);
#else
        ssize_t w = ::send(fd_, buf + off, len - off, 0);
#endif
        if (w < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            return off ? static_cast<int>(off) : 0;
          return -1;
        }
        n = static_cast<int>(w);
      }
      off += static_cast<size_t>(n);
    }
    return static_cast<int>(off);
  }

  int flushSend()
  {
    for (;;) {
      const uint8_t *data = nullptr;
      ssize_t n = nghttp2_session_mem_send(session_, &data);
      if (n < 0)
        return -1;
      if (n == 0)
        return 0;
      size_t off = 0;
      while (off < static_cast<size_t>(n)) {
        int w = rawWrite(data + off, static_cast<size_t>(n) - off);
        if (w < 0)
          return -1;
        if (w == 0) {
          struct pollfd p;
          p.fd = fd_;
          p.events = POLLOUT;
          if (::poll(&p, 1, 10000) <= 0)
            return -1;
          continue;
        }
        off += static_cast<size_t>(w);
      }
    }
  }

  void loop()
  {
    uint8_t buf[16384];
    while (alive_.load()) {
      drainDone();
      if (flushSend() < 0)
        break;
      if (nghttp2_session_want_read(session_) == 0 &&
          nghttp2_session_want_write(session_) == 0)
        break;
      struct pollfd pf[2];
      pf[0].fd = fd_;
      pf[0].events = POLLIN;
      pf[1].fd = wake_rd_;
      pf[1].events = POLLIN;
      int pr = ::poll(pf, 2, 1000);
      if (pr < 0) {
        if (errno == EINTR)
          continue;
        break;
      }
      if (pf[1].revents & POLLIN) {
        char tmp[64];
        while (::read(wake_rd_, tmp, sizeof(tmp)) > 0) {
        }
      }
      if (pf[0].revents & (POLLERR | POLLHUP | POLLNVAL))
        break;
      if (pf[0].revents & POLLIN) {
        int n = rawRead(buf, sizeof(buf));
        if (n < 0)
          break;
        if (n > 0) {
          ssize_t rc = nghttp2_session_mem_recv(session_, buf,
                                                static_cast<size_t>(n));
          if (rc < 0)
            break;
        }
      }
    }
    alive_.store(false);
  }

  int fd_{-1};
  int wake_rd_{-1};
  int wake_wr_{-1};
  SSL *ssl_{nullptr};
  std::string peer_;
  nghttp2_session *session_{nullptr};
  std::shared_ptr<Fs> fs_;
  Lanes &lanes_;
  Server::AuthHook auth_;
  std::atomic<bool> alive_{true};
  std::mutex mu_;
  std::deque<Done> done_;
  std::map<int32_t, StreamState> streams_;
  std::map<int32_t, Pending> pending_;
};

int alpnSelect(SSL *, const unsigned char **out, unsigned char *outlen,
               const unsigned char *in, unsigned int inlen, void *)
{
  static const unsigned char h2[] = {2, 'h', '2'};
  unsigned char *sel = nullptr;
  unsigned char slen = 0;
  if (SSL_select_next_proto(&sel, &slen, h2, sizeof(h2), in, inlen) !=
      OPENSSL_NPN_NEGOTIATED)
    return SSL_TLSEXT_ERR_ALERT_FATAL;
  *out = sel;
  *outlen = slen;
  return SSL_TLSEXT_ERR_OK;
}

} // namespace

struct Server::Impl {
  std::shared_ptr<Fs> fs;
  Options opt;
  Server::AuthHook auth;
  Lanes lanes;
  std::atomic<bool> stop{false};
  int listen_fd{-1};
  SSL_CTX *ctx{nullptr};
  std::thread accept_thr;
  std::mutex conn_mu;
  std::vector<std::shared_ptr<Conn>> conns;
  std::vector<std::thread> conn_thr;

  explicit Impl(std::shared_ptr<Fs> f, Options o)
    : fs(std::move(f)), opt(std::move(o)), lanes(opt.workers < 1 ? 1 : opt.workers)
  {}

  ~Impl() { shutdown(); }

  void shutdown()
  {
    stop.store(true);
    if (listen_fd >= 0) {
      ::shutdown(listen_fd, SHUT_RDWR);
      ::close(listen_fd);
      listen_fd = -1;
    }
    if (accept_thr.joinable())
      accept_thr.join();
    std::vector<std::shared_ptr<Conn>> local;
    std::vector<std::thread> threads;
    {
      std::lock_guard<std::mutex> lock(conn_mu);
      local.swap(conns);
      threads.swap(conn_thr);
    }
    for (auto &c : local)
      c->close();
    for (auto &t : threads)
      if (t.joinable())
        t.join();
    lanes.shutdown();
    if (ctx) {
      SSL_CTX_free(ctx);
      ctx = nullptr;
    }
  }
};

Server::Server(std::shared_ptr<Fs> fs, Options opt)
  : fs_(std::move(fs)), opt_(std::move(opt)),
    impl_(new Impl(fs_, opt_))
{}

Server::~Server()
{
  stop();
}

void Server::setAuthHook(AuthHook hook)
{
  auth_ = std::move(hook);
  if (impl_)
    impl_->auth = auth_;
}

int Server::start(std::string &err)
{
  if (!fs_) {
    err = "no filesystem";
    return -EINVAL;
  }
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    err = strerror(errno);
    return -errno;
  }
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(opt_.port < 0 ? 0 : opt_.port));
  if (opt_.bind_host.empty() || opt_.bind_host == "0.0.0.0")
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
  else if (inet_pton(AF_INET, opt_.bind_host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    err = "bad bind address";
    return -EINVAL;
  }
  if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    int e = errno;
    ::close(fd);
    err = strerror(e);
    return -e;
  }
  if (listen(fd, 128) != 0) {
    int e = errno;
    ::close(fd);
    err = strerror(e);
    return -e;
  }
  socklen_t alen = sizeof(addr);
  if (getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &alen) == 0)
    port_ = ntohs(addr.sin_port);
  else
    port_ = opt_.port;

  if (!opt_.tls_cert.empty()) {
    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) {
      ::close(fd);
      err = "SSL_CTX_new failed";
      return -EIO;
    }
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    SSL_CTX_set_alpn_select_cb(ctx, alpnSelect, nullptr);
    if (SSL_CTX_use_certificate_file(ctx, opt_.tls_cert.c_str(),
                                     SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_use_PrivateKey_file(ctx, opt_.tls_key.c_str(),
                                    SSL_FILETYPE_PEM) != 1) {
      SSL_CTX_free(ctx);
      ::close(fd);
      err = "failed to load TLS certificate or key";
      return -EINVAL;
    }
    if (!opt_.tls_client_ca.empty()) {
      const char *cafile = nullptr;
      const char *cadir = nullptr;
      struct stat st;
      if (::stat(opt_.tls_client_ca.c_str(), &st) == 0 && S_ISDIR(st.st_mode))
        cadir = opt_.tls_client_ca.c_str();
      else
        cafile = opt_.tls_client_ca.c_str();
      if (SSL_CTX_load_verify_locations(ctx, cafile, cadir) != 1) {
        SSL_CTX_free(ctx);
        ::close(fd);
        err = "failed to load client CA";
        return -EINVAL;
      }
      SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
    }
    impl_->ctx = ctx;
    tls_ = true;
  }

  impl_->listen_fd = fd;
  impl_->stop.store(false);
  impl_->auth = auth_;
  impl_->accept_thr = std::thread([this] {
    while (!impl_->stop.load()) {
      sockaddr_in peer;
      socklen_t plen = sizeof(peer);
      int cfd = ::accept(impl_->listen_fd, reinterpret_cast<sockaddr *>(&peer),
                         &plen);
      if (cfd < 0) {
        if (impl_->stop.load())
          break;
        if (errno == EINTR)
          continue;
        break;
      }
      setCommonSock(cfd);
      SSL *ssl = nullptr;
      if (impl_->ctx) {
        ssl = SSL_new(impl_->ctx);
        SSL_set_fd(ssl, cfd);
        int rc;
        while ((rc = SSL_accept(ssl)) != 1) {
          int e = SSL_get_error(ssl, rc);
          if (e != SSL_ERROR_WANT_READ && e != SSL_ERROR_WANT_WRITE) {
            SSL_free(ssl);
            ssl = nullptr;
            ::close(cfd);
            cfd = -1;
            break;
          }
          struct pollfd p;
          p.fd = cfd;
          p.events = (e == SSL_ERROR_WANT_READ) ? POLLIN : POLLOUT;
          ::poll(&p, 1, 10000);
        }
        if (cfd < 0)
          continue;
      }
      std::string peer_dn;
      if (ssl && SSL_get_verify_result(ssl) == X509_V_OK) {
        X509 *cert = SSL_get1_peer_certificate(ssl);
        if (cert) {
          BIO *bio = BIO_new(BIO_s_mem());
          if (bio) {
            X509_NAME_print_ex(bio, X509_get_subject_name(cert), 0,
                               XN_FLAG_RFC2253);
            char *data = nullptr;
            long n = BIO_get_mem_data(bio, &data);
            if (n > 0 && data)
              peer_dn.assign(data, static_cast<size_t>(n));
            BIO_free(bio);
          }
          X509_free(cert);
        }
      }
      auto conn = std::make_shared<Conn>(cfd, ssl, std::move(peer_dn), fs_,
                                        impl_->lanes, impl_->auth);
      std::thread thr([conn] { conn->run(); });
      std::lock_guard<std::mutex> lock(impl_->conn_mu);
      impl_->conns.push_back(conn);
      impl_->conn_thr.push_back(std::move(thr));
    }
  });
  return 0;
}

void Server::stop()
{
  if (impl_)
    impl_->shutdown();
}

} // namespace XioFS
