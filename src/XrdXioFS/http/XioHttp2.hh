//------------------------------------------------------------------------------
// HTTP/2 client session (nghttp2 + OpenSSL) for XIOFS.
//
// Speaks ALPN "h2" over TLS, or h2c prior knowledge on cleartext. One
// connection is shared: an I/O thread drives nghttp2 so FUSE threads can
// have several streams in flight (Range GET / PATCH) without serializing
// on a mutex for the whole request.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_HTTP2_HH
#define XIOFS_HTTP2_HH

#include "XioUrl.hh"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace XioFS {

struct HttpRequest {
  std::string method{"GET"};
  std::string path{"/"};
  std::vector<std::pair<std::string, std::string>> headers;
  /// Small generated payloads (PROPFIND XML and the like). Prefer `pull`
  /// when the bytes already live in the caller's buffer.
  std::string body;
  /// Read `n` bytes of the request body at `off` into `dst`. Return the
  /// count copied, 0 at the end, or a negative errno-style code on error.
  /// Called on the session I/O thread; it must not block or re-enter the
  /// session. Each attempt starts at offset 0, so a redirect can resend.
  std::function<ssize_t(uint64_t off, uint8_t *dst, size_t n)> pull;
  /// Known upload length. -1 uses `body.size()` when `body` is set, and
  /// otherwise omits Content-Length (pull is read until it returns 0).
  int64_t content_length{-1};
  /// Per-request deadline in milliseconds. 0 uses the session timeout.
  int timeout_ms{0};
  std::function<int(const uint8_t *data, size_t len)> on_chunk;
};

struct HttpResponse {
  int status{0};
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;

  std::string header(const char *name) const;
};

/// Completion for start(). `rc` is 0 when an HTTP response was received.
using HttpCallback =
    std::function<void(int rc, HttpResponse resp, std::string err)>;

class Http2Session {
public:
  struct Options {
    std::string cacert;
    /// OpenSSL CA directory (SSL_CTX_load_verify_locations path argument).
    std::string cadir;
    std::string bearer;
    std::string negotiate;
    bool krb5{false};
    std::string principal;
    /// Idle timeout when the request streams through on_chunk (reset as body
    /// bytes arrive). Total timeout for a response that is buffered instead.
    int timeout_ms{30000};
    bool verify_peer{true};
    size_t max_body{64ull * 1024ull * 1024ull};
    uint32_t max_streams{100};
    bool parsock{false};
    int parsock_sockets{4};
    std::string parsock_transport{"tcp"}; // tcp, rdma, quic
    std::string parsock_protection{"confidential"}; // confidential, authenticated
    /// Invoked on the request() caller thread, without the session lock,
    /// about every 200 ms while a request is in flight. Must not re-enter
    /// this session.
    std::function<void()> on_wait;
  };

  const Options &options() const { return opt_; }

  Http2Session();
  ~Http2Session();
  Http2Session(const Http2Session &) = delete;
  Http2Session &operator=(const Http2Session &) = delete;

  int connect(const Url &url, const Options &opt, std::string &err);
  void close();
  bool connected() const;
  bool isParsock() const;

  int registerDeviceMem(void *dev_ptr, size_t len, int dmabuf_fd, void **mem_handle, std::string &err);
  int deregisterDeviceMem(void *mem_handle, std::string &err);

  int request(const HttpRequest &req, HttpResponse &resp, std::string &err);

  /// Queue a request and return. Follows redirects the same way request()
  /// does, including reconnecting this session. The callback runs on a
  /// library thread without the session lock. It may call start() again.
  /// It must not call request() or exchange(): those wait for a completion
  /// that is delivered on this same thread.
  /// Returns 0 when the first hop is queued. The callback is not invoked
  /// when the return value is non-zero.
  int start(HttpRequest req, HttpCallback cb);

  /// One HTTP/2 exchange on this connection. Does not follow redirects.
  int exchange(const HttpRequest &req, HttpResponse &resp, std::string &err,
               bool send_credentials = true);

  const Url &peer() const { return url_; }

private:
  using Clock = std::chrono::steady_clock;

  int requestOnce(const HttpRequest &req, HttpResponse &resp, std::string &err,
                  bool send_credentials);
  struct Follow;
  int follow(const std::shared_ptr<Follow> &f, bool report);

  struct Inflight {
    HttpResponse owned;
    std::string owned_err;
    HttpResponse *resp{nullptr};
    std::string *err{nullptr};
    HttpRequest req;
    std::function<void()> on_done;
    std::condition_variable cv;
    bool done{false};
    int rc{0};
    uint64_t body_off{0};
    Clock::time_point deadline{};
    std::function<int(const uint8_t *data, size_t len)> on_chunk;
    ~Inflight() { cleanupZstd(); }
    void cleanupZstd();
    int deliverBody(const uint8_t *data, size_t len, Http2Session *self);
#ifdef HAVE_ZSTD
    void *dstream{nullptr};
    bool is_zstd{false};
    int feedZstd(const uint8_t *data, size_t len, Http2Session *self);
    void noteDecoded();
#endif
  };

  int submit(std::unique_lock<std::mutex> &lock,
             const std::shared_ptr<Inflight> &st, bool send_credentials);
  void finish(const std::shared_ptr<Inflight> &st);

  Url url_;
  Options opt_;
  int fd_{-1};
  int wake_rd_{-1};
  int wake_wr_{-1};
  void *ssl_ctx_{nullptr};
  void *ssl_{nullptr};
  void *session_{nullptr};
#ifdef HAVE_PARSOCKET
  void *parsock_{nullptr};
#endif
  uint32_t max_concurrent_{100};
  bool stop_{false};
  std::thread io_;
  std::unordered_map<int32_t, std::shared_ptr<Inflight>> inflight_;
  mutable std::mutex mu_;
  std::condition_variable io_cv_;
  std::condition_variable slot_cv_;

  int tlsHandshake(std::string &err);
  int tcpConnect(std::string &err);
  int sslRead(uint8_t *buf, size_t len, std::string &err);
  int sslWrite(const uint8_t *buf, size_t len, std::string &err);
  void ioLoop();
  void wakeIo();
  void drainWake();
  void failInflight(int rc, const std::string &msg);
  int flushSend(std::unique_lock<std::mutex> &lock, std::string &err);
  int nextPollTimeoutMsLocked() const;
  static int setNonBlocking(int fd);
};

struct RedirectPlan {
  HttpRequest request;
  Url url;
  Http2Session::Options opt;
  bool reconnect{false};
  bool send_credentials{true};
  /// X-Xrd-Sticky was set. File I/O for this path stays on url.
  bool sticky{false};
};

/// 1 when resp should be followed, 0 when it is not a redirect, <0 on error.
int planRedirect(const Url &current, const Http2Session::Options &opt,
                 const HttpRequest &req, const HttpResponse &resp,
                 RedirectPlan &plan, std::string &err);

} // namespace XioFS

#endif
