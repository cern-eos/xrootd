//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioUrl.hh"

#include <cctype>
#include <cstdlib>
#include <vector>

namespace XioFS {
namespace {

bool allDigits(const std::string &s)
{
  if (s.empty())
    return false;
  for (char c : s) {
    if (!std::isdigit(static_cast<unsigned char>(c)))
      return false;
  }
  return true;
}

void splitAuthority(const std::string &in, std::vector<std::string> &tok)
{
  std::string cur;
  int brack = 0;
  for (char c : in) {
    if (c == '[')
      brack++;
    else if (c == ']')
      brack--;
    if (c == ',' && brack == 0) {
      if (!cur.empty())
        tok.push_back(cur);
      cur.clear();
    } else {
      cur.push_back(c);
    }
  }
  if (!cur.empty())
    tok.push_back(cur);
}

bool parseOneServer(const std::string &tok, int default_port, Hop &h,
                    std::string &err)
{
  h = Hop{};
  if (tok.empty()) {
    err = "empty host in URL";
    return false;
  }
  if (tok.front() == '[') {
    auto rb = tok.find(']');
    if (rb == std::string::npos) {
      err = "unterminated IPv6 literal in URL";
      return false;
    }
    h.host = tok.substr(1, rb - 1);
    if (rb + 1 < tok.size()) {
      if (tok[rb + 1] != ':') {
        err = "invalid host:port in URL";
        return false;
      }
      h.port = std::atoi(tok.c_str() + rb + 2);
    } else {
      h.port = default_port;
    }
  } else {
    auto colon = tok.rfind(':');
    if (colon != std::string::npos && tok.find(':') == colon) {
      h.host = tok.substr(0, colon);
      h.port = std::atoi(tok.c_str() + colon + 1);
    } else {
      h.host = tok;
      h.port = default_port;
    }
  }
  if (h.host.empty() || h.port <= 0 || h.port > 65535) {
    err = "invalid host or port in URL";
    return false;
  }
  return true;
}

} // namespace

bool parseHops(const std::string &authority, int default_port,
               std::vector<Hop> &hops, std::string &err)
{
  hops.clear();
  if (authority.empty()) {
    err = "URL is missing a host";
    return false;
  }
  std::vector<std::string> tok;
  splitAuthority(authority, tok);
  for (size_t i = 0; i < tok.size();) {
    Hop h;
    if (i + 1 < tok.size() && tok[i].find(':') == std::string::npos &&
        tok[i].front() != '[' && allDigits(tok[i + 1])) {
      // XRootD manager/alias syntax: host,port
      h.host = tok[i];
      h.port = std::atoi(tok[i + 1].c_str());
      h.alias = true;
      if (h.host.empty() || h.port <= 0 || h.port > 65535) {
        err = "invalid alias host,port in URL";
        return false;
      }
      hops.push_back(h);
      i += 2;
      continue;
    }
    if (!parseOneServer(tok[i], default_port, h, err))
      return false;
    hops.push_back(h);
    i++;
  }
  if (hops.empty()) {
    err = "URL is missing a host";
    return false;
  }
  return true;
}

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
  } else if (out.scheme == "spt" || out.scheme == "parsock") {
    out.tls = true;
    out.port = 8443;
    out.parsock = true;
  } else {
    err = "unsupported URL scheme (use http, https, spt, or parsock)";
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

  if (!parseHops(hostport, out.port, out.hops, err))
    return false;
  out.host = out.hops[0].host;
  out.port = out.hops[0].port;

  if ((out.tls && !out.parsock && out.port == 443) || (!out.tls && out.port == 80))
    out.authority = out.host;
  else
    out.authority = out.host + ":" + std::to_string(out.port);

  return true;
}

std::string formatUrl(const Url &u)
{
  std::string s = u.parsock ? (u.scheme.empty() ? "spt://" : u.scheme + "://")
                            : (u.tls ? "https://" : "http://");
  auto hopStr = [](const Hop &h) {
    bool v6 = h.host.find(':') != std::string::npos;
    if (h.alias)
      return h.host + "," + std::to_string(h.port);
    if (v6)
      return "[" + h.host + "]:" + std::to_string(h.port);
    return h.host + ":" + std::to_string(h.port);
  };
  if (!u.hops.empty()) {
    for (size_t i = 0; i < u.hops.size(); ++i) {
      if (i)
        s += ',';
      s += hopStr(u.hops[i]);
    }
  } else if (!u.host.empty()) {
    Hop h;
    h.host = u.host;
    h.port = u.port;
    s += hopStr(h);
  }
  s += u.path.empty() ? "/" : u.path;
  return s;
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
