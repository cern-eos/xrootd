//------------------------------------------------------------------------------
// Standalone HTTP/2 server.
//
// The accept loop and nghttp2 session run on the connection. Filesystem work
// runs on Lanes. There is no XrdProtocol, XrdLink, XrdScheduler, or
// XrdXrootdTransit.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_SERVER_HH
#define XIOFS_SERVER_HH

#include "XioFs.hh"

#include <functional>
#include <memory>
#include <string>

namespace XioFS {

class Server
{
public:
  struct Options {
    std::string bind_host{"127.0.0.1"};
    // 0 asks the kernel for a free port.
    int port{0};
    int workers{8};
    std::string tls_cert;
    std::string tls_key;
    /// CA file or directory. When set, a presented client certificate is
    /// verified. Connections without a certificate are still accepted.
    std::string tls_client_ca;
  };

  // Optional. Turns an Authorization header into a Credential before the
  // request is queued. A non-zero return becomes an HTTP error.
  using AuthHook = std::function<int(const FsRequest &req, Credential &cred)>;

  Server(std::shared_ptr<Fs> fs, Options opt);
  ~Server();

  Server(const Server &) = delete;
  Server &operator=(const Server &) = delete;

  void setAuthHook(AuthHook hook);

  // Bind and accept until stop().
  int start(std::string &err);
  void stop();

  int port() const { return port_; }
  bool tls() const { return tls_; }

private:
  struct Impl;
  std::shared_ptr<Fs> fs_;
  Options opt_;
  AuthHook auth_;
  int port_{0};
  bool tls_{false};
  std::unique_ptr<Impl> impl_;
};

} // namespace XioFS

#endif
