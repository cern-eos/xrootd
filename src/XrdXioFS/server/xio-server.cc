//------------------------------------------------------------------------------
// xio-server — HTTP/2 filesystem server with no XRootD process.
//
//   xio-server --root DIR [--bind ADDR] [--port N] [--workers N]
//               [--cert FILE --key FILE]
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioPosixFs.hh"
#include "XioServer.hh"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>

namespace {

void usage(const char *argv0)
{
  std::cerr
      << "Usage: " << argv0
      << " --root DIR [--bind ADDR] [--port N] [--workers N]\n"
      << "            [--cert FILE --key FILE]\n"
      << "\n"
      << "  Serve DIR over HTTP/2. Cleartext speaks h2c prior knowledge.\n"
      << "  With --cert and --key the listener is TLS and offers ALPN h2.\n";
}

} // namespace

int main(int argc, char **argv)
{
  std::string root;
  XioFS::Server::Options opt;
  for (int i = 1; i < argc; ++i) {
    auto need = [&](const char *flag) -> const char * {
      if (i + 1 >= argc) {
        std::cerr << "missing value for " << flag << "\n";
        std::exit(2);
      }
      return argv[++i];
    };
    if (!strcmp(argv[i], "--root"))
      root = need("--root");
    else if (!strcmp(argv[i], "--bind"))
      opt.bind_host = need("--bind");
    else if (!strcmp(argv[i], "--port"))
      opt.port = std::atoi(need("--port"));
    else if (!strcmp(argv[i], "--workers"))
      opt.workers = std::atoi(need("--workers"));
    else if (!strcmp(argv[i], "--cert"))
      opt.tls_cert = need("--cert");
    else if (!strcmp(argv[i], "--key"))
      opt.tls_key = need("--key");
    else if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
      usage(argv[0]);
      return 0;
    } else {
      std::cerr << "unknown argument: " << argv[i] << "\n";
      usage(argv[0]);
      return 2;
    }
  }
  if (root.empty()) {
    usage(argv[0]);
    return 2;
  }
  auto fs = std::make_shared<XioFS::PosixFs>(root);
  XioFS::Server server(fs, opt);
  std::string err;
  int rc = server.start(err);
  if (rc) {
    std::cerr << "xio-server: " << err << "\n";
    return 1;
  }
  std::cerr << "xio-server: " << (server.tls() ? "https" : "http")
            << "://" << (opt.bind_host.empty() ? "0.0.0.0" : opt.bind_host)
            << ":" << server.port() << "/ root " << root << "\n";
  for (;;)
    ::pause();
  return 0;
}
