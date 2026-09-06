//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef KFS_URL_HH
#define KFS_URL_HH

#include <string>

namespace Kfs {

struct Url {
  std::string scheme;     // "https" or "http"
  std::string host;
  int         port{443};
  std::string path{"/"};  // always starts with '/'
  std::string authority;  // host[:port] for :authority
  bool        tls{true};
};

bool parseUrl(const std::string &in, Url &out, std::string &err);

// Join a mount prefix with a FUSE/CLI relative path. Both sides may be "/".
std::string joinPath(const std::string &base, const std::string &rel);

} // namespace Kfs

#endif
