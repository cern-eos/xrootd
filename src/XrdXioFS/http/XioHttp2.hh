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
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace XioFS {

struct HttpRequest {
  std::string method{"GET"};
  std::string path{"/"};
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
  std::function<int(const uint8_t *data, size_t len)> on_chunk;
};

struct HttpResponse {
  int status{0};
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;

  std::string header(const char *name) const;
};

class Http2Session {
public:
  struct Options {
    std::string cacert;
    std::string bearer;
    std::string negotiate;
    bool krb5{false};
    std::string principal;
    int timeout_ms{30000};
    bool verify_peer{true};
    size_t max_body{64ull * 1024ull * 1024ull};
    uint32_t max_streams{100};
    bool parsock{false};
    int parsock_sockets{4};
    std::string parsock_transport{"tcp"}; // tcp, rdma, quic
    std::string parsock_protection{"confidential"}; // confidential, authenticated
  };

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

  const Url &peer() const { return url_; }

private:
  using Clock = std::chrono::steady_clock;

  struct Inflight {
    HttpResponse *resp{nullptr};
    std::string *err{nullptr};
    std::condition_variable cv;
    bool done{false};
    int rc{0};
    std::string body;
    size_t body_off{0};
    Clock::time_point deadline{};
    std::function<int(const uint8_t *data, size_t len)> on_chunk;
  };

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
  std::unordered_map<int32_t, Inflight *> inflight_;
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

} // namespace XioFS

#endif
