//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioUrl.hh"

#include <cctype>
#include <cstdlib>

namespace XioFS {

bool parseUrl(const std::string &in, Url &out, std::string &err)
{
  out = Url{};
  auto sep = in.find("://");
  if (sep == std::string::npos) {
    err = "URL must include a scheme (https:// or http://)";
    return false;
  }

  out.scheme = in.substr(0, sep);
  for (char &c : out.scheme)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));

  if (out.scheme == "https") {
    out.tls = true;
    out.port = 443;
  } else if (out.scheme == "http") {
    out.tls = false;
    out.port = 80;
  } else {
    err = "unsupported URL scheme (use http or https)";
    return false;
  }

  auto rest = in.substr(sep + 3);
  if (rest.empty()) {
    err = "URL is missing a host";
    return false;
  }

  std::string hostport;
  auto slash = rest.find('/');
  if (slash == std::string::npos) {
    hostport = rest;
    out.path = "/";
  } else {
    hostport = rest.substr(0, slash);
    out.path = rest.substr(slash);
    if (out.path.empty())
      out.path = "/";
  }

  if (hostport.empty()) {
    err = "URL is missing a host";
    return false;
  }

  if (hostport.front() == '[') {
    auto rb = hostport.find(']');
    if (rb == std::string::npos) {
      err = "unterminated IPv6 literal in URL";
      return false;
    }
    out.host = hostport.substr(1, rb - 1);
    if (rb + 1 < hostport.size()) {
      if (hostport[rb + 1] != ':') {
        err = "invalid host:port in URL";
        return false;
      }
      out.port = std::atoi(hostport.c_str() + rb + 2);
    }
  } else {
    auto colon = hostport.rfind(':');
    if (colon != std::string::npos && hostport.find(':') == colon) {
      out.host = hostport.substr(0, colon);
      out.port = std::atoi(hostport.c_str() + colon + 1);
    } else {
      out.host = hostport;
    }
  }

  if (out.host.empty() || out.port <= 0 || out.port > 65535) {
    err = "invalid host or port in URL";
    return false;
  }

  if ((out.tls && out.port == 443) || (!out.tls && out.port == 80))
    out.authority = out.host;
  else
    out.authority = out.host + ":" + std::to_string(out.port);

  return true;
}

std::string joinPath(const std::string &base, const std::string &rel)
{
  std::string b = base.empty() ? "/" : base;
  std::string r = rel.empty() ? "/" : rel;
  if (r == "/")
    return b.empty() ? "/" : b;

  if (r.front() != '/')
    r.insert(r.begin(), '/');

  if (b == "/")
    return r;
  if (b.back() == '/')
    b.pop_back();
  return b + r;
}

} // namespace XioFS
