//------------------------------------------------------------------------------
// HTTP/2 client session (nghttp2 + OpenSSL) for KernelFS.
//
// Speaks ALPN "h2" over TLS, or h2c prior knowledge on cleartext. After
// connect, requests are serialized on one connection (mutex). Multiplexing
// is a later increment; large Range GETs already amortize FUSE 4 KiB reads.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef KFS_HTTP2_HH
#define KFS_HTTP2_HH

#include "KfsUrl.hh"

#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace Kfs {

struct HttpRequest {
  std::string method{"GET"};
  std::string path{"/"};
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
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
    int timeout_ms{30000};
    bool verify_peer{true};
    size_t max_body{64ull * 1024ull * 1024ull};
  };

  Http2Session();
  ~Http2Session();
  Http2Session(const Http2Session &) = delete;
  Http2Session &operator=(const Http2Session &) = delete;

  int connect(const Url &url, const Options &opt, std::string &err);
  void close();
  bool connected() const { return fd_ >= 0 && session_ != nullptr; }

  int request(const HttpRequest &req, HttpResponse &resp, std::string &err);

  const Url &peer() const { return url_; }

private:
  Url url_;
  Options opt_;
  int fd_{-1};
  void *ssl_ctx_{nullptr};
  void *ssl_{nullptr};
  void *session_{nullptr};
  bool stream_closed_{false};
  int32_t stream_id_{0};
  HttpResponse *cur_{nullptr};
  std::string *err_{nullptr};
  mutable std::mutex mu_;

  int tlsHandshake(std::string &err);
  int tcpConnect(std::string &err);
  int drive(std::string &err);
  int sslRead(uint8_t *buf, size_t len, std::string &err);
  int sslWrite(const uint8_t *buf, size_t len, std::string &err);
  int waitReadable(std::string &err);
};

} // namespace Kfs

#endif
