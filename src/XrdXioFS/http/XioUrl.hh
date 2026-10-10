//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_URL_HH
#define XIOFS_URL_HH

#include <string>
#include <vector>

namespace XioFS {

// One hop in the mount authority.
// host:port is a data server; host,port (XRootD manager syntax) is an alias
// that xiofsagent expands to a server list.
struct Hop {
  std::string host;
  int         port{0};
  bool        alias{false};
};

struct Url {
  std::string scheme;     // "https" or "http"
  std::string host;
  int         port{443};
  std::string path{"/"};  // always starts with '/'
  std::string authority;  // host[:port] for :authority
  bool        tls{true};
  std::vector<Hop> hops;
};

bool parseUrl(const std::string &in, Url &out, std::string &err);
bool parseHops(const std::string &authority, int default_port,
               std::vector<Hop> &hops, std::string &err);
std::string formatUrl(const Url &u);

// Join a mount prefix with a FUSE/CLI relative path. Both sides may be "/".
std::string joinPath(const std::string &base, const std::string &rel);

} // namespace XioFS

#endif
