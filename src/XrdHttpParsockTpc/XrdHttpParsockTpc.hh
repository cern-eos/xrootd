//------------------------------------------------------------------------------
// Parsock pull third-party copy helpers.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDHTTP_PARSOCK_TPC_HH
#define XRDHTTP_PARSOCK_TPC_HH

#include "XioHttp2.hh"

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <sstream>
#include <string>
#include <strings.h>

namespace XrdPsTpc {

inline std::string asciiLower(std::string s)
{
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

inline std::string trimCopy(std::string s)
{
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t'))
    s.erase(s.begin());
  while (!s.empty() &&
         (s.back() == ' ' || s.back() == '\t' || s.back() == '\r'))
    s.pop_back();
  return s;
}

inline std::string unwrapHttpUrl(std::string s)
{
  s = trimCopy(std::move(s));
  if (s.size() >= 2 && s.front() == '<' && s.back() == '>')
    s = trimCopy(s.substr(1, s.size() - 2));
  return s;
}

inline std::string headerValue(const std::map<std::string, std::string> &headers,
                               const char *name)
{
  for (const auto &kv : headers) {
    if (strcasecmp(kv.first.c_str(), name) == 0)
      return kv.second;
  }
  return {};
}

inline bool sourceSchemeParsock(const std::string &url)
{
  const std::string l = asciiLower(url);
  return l.compare(0, 6, "spt://") == 0 || l.compare(0, 10, "parsock://") == 0;
}

/// True when this handler, not XrdHttpTPC, owns the COPY.
inline bool claimsParsockCopy(const std::string &verb,
                              const std::map<std::string, std::string> &headers)
{
  if (asciiLower(verb) != "copy")
    return false;
  if (asciiLower(trimCopy(headerValue(headers, "x-xrd-transport"))) == "parsock")
    return true;
  return sourceSchemeParsock(unwrapHttpUrl(headerValue(headers, "source")));
}

enum class TransportKind { Default, Parsock, Standard };

inline TransportKind transportKind(const std::string &value)
{
  const std::string v = asciiLower(trimCopy(value));
  if (v == "parsock")
    return TransportKind::Parsock;
  if (v == "standard")
    return TransportKind::Standard;
  return TransportKind::Default;
}

inline bool paramValue(const std::string &params, const char *key,
                       std::string &value)
{
  const std::string want = asciiLower(key);
  size_t start = 0;
  while (start < params.size()) {
    while (start < params.size() &&
           (params[start] == ';' || params[start] == ' ' || params[start] == '\t'))
      start++;
    if (start >= params.size())
      break;
    size_t end = params.find(';', start);
    if (end == std::string::npos)
      end = params.size();
    std::string tok = trimCopy(params.substr(start, end - start));
    start = end + 1;
    auto eq = tok.find('=');
    if (eq == std::string::npos)
      continue;
    if (asciiLower(tok.substr(0, eq)) != want)
      continue;
    value = tok.substr(eq + 1);
    return true;
  }
  return false;
}

inline void setAuthority(XioFS::Url &url)
{
  const bool v6 = url.host.find(':') != std::string::npos;
  const std::string host = v6 ? "[" + url.host + "]" : url.host;
  if ((url.tls && !url.parsock && url.port == 443) ||
      (!url.tls && !url.parsock && url.port == 80))
    url.authority = host;
  else
    url.authority = host + ":" + std::to_string(url.port);
  url.hops.clear();
  XioFS::Hop hop;
  hop.host = url.host;
  hop.port = url.port;
  url.hops.push_back(hop);
}

/// Apply a COPY X-Xrd-Transport / X-Xrd-Parsock hint onto the source URL.
inline bool applyTransport(XioFS::Url &url, XioFS::Http2Session::Options &opt,
                           TransportKind kind, const std::string &params,
                           std::string &err)
{
  auto applyParams = [&](bool require_port) -> bool {
    std::string port_s;
    if (paramValue(params, "port", port_s)) {
      char *end = nullptr;
      long port = strtol(port_s.c_str(), &end, 10);
      if (end == port_s.c_str() || (end && *end) || port <= 0 || port > 65535) {
        err = "X-Xrd-Parsock has an invalid port";
        return false;
      }
      url.port = static_cast<int>(port);
    } else if (require_port) {
      err = "X-Xrd-Parsock is missing port";
      return false;
    }
    std::string sockets;
    if (paramValue(params, "sockets", sockets)) {
      int n = std::atoi(sockets.c_str());
      if (n > 0 && n <= 64)
        opt.parsock_sockets = n;
    }
    std::string transport;
    if (paramValue(params, "transport", transport) && !transport.empty())
      opt.parsock_transport = transport;
    std::string protection;
    if (paramValue(params, "protection", protection) && !protection.empty())
      opt.parsock_protection = protection;
    return true;
  };

  if (kind == TransportKind::Standard) {
    url.parsock = false;
    opt.parsock = false;
    if (url.scheme == "spt" || url.scheme == "parsock")
      url.scheme = url.tls ? "https" : "http";
    setAuthority(url);
    return true;
  }

  if (kind == TransportKind::Parsock || url.parsock) {
    if (!applyParams(kind == TransportKind::Parsock))
      return false;
    url.parsock = true;
    url.tls = true;
    url.scheme = "spt";
    opt.parsock = true;
    setAuthority(url);
    return true;
  }

  opt.parsock = false;
  setAuthority(url);
  return true;
}

enum class ResumeAction { Truncate, Resume, Complete };

struct ResumeInput {
  bool overwrite{false};
  bool have_marker{false};
  std::string marker_url;
  std::string marker_etag;
  int64_t marker_size{-1};
  int64_t local_size{0};
  std::string remote_etag;
  int64_t remote_size{0};
  std::string source_url;
};

struct ResumeDecision {
  ResumeAction action{ResumeAction::Truncate};
  int64_t cursor{0};
};

/// Resume from the bytes already in the local file unless Overwrite is T,
/// the marker is missing, or the source URL / ETag / size do not match.
/// A local file larger than the source is truncated and started over.
inline ResumeDecision decideResume(const ResumeInput &in)
{
  ResumeDecision d;
  const bool same = in.have_marker && in.marker_url == in.source_url &&
                    in.marker_etag == in.remote_etag &&
                    in.marker_size == in.remote_size;
  if (in.overwrite || !same || in.local_size < 0 ||
      in.local_size > in.remote_size) {
    d.action = ResumeAction::Truncate;
    d.cursor = 0;
    return d;
  }
  if (in.local_size == in.remote_size) {
    d.action = ResumeAction::Complete;
    d.cursor = in.local_size;
    return d;
  }
  d.action = ResumeAction::Resume;
  d.cursor = in.local_size;
  return d;
}

inline std::string scrubLine(std::string s)
{
  for (char &c : s) {
    if (c == '\n' || c == '\r')
      c = ' ';
  }
  return s;
}

inline std::string formatMarker(const std::string &url, const std::string &etag,
                                int64_t size)
{
  std::ostringstream ss;
  ss << "url " << scrubLine(url) << "\n"
     << "etag " << scrubLine(etag) << "\n"
     << "size " << size << "\n";
  return ss.str();
}

inline bool parseMarker(const std::string &text, std::string &url,
                        std::string &etag, int64_t &size)
{
  bool have_url = false, have_etag = false, have_size = false;
  size_t i = 0;
  while (i < text.size()) {
    size_t nl = text.find('\n', i);
    std::string line = text.substr(i, nl == std::string::npos ? std::string::npos
                                                               : nl - i);
    i = nl == std::string::npos ? text.size() : nl + 1;
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    if (line.compare(0, 4, "url ") == 0) {
      url = line.substr(4);
      have_url = true;
    } else if (line.compare(0, 5, "etag ") == 0) {
      etag = line.substr(5);
      have_etag = true;
    } else if (line.compare(0, 5, "size ") == 0) {
      char *end = nullptr;
      long long n = std::strtoll(line.c_str() + 5, &end, 10);
      if (end == line.c_str() + 5 || (end && *end) || n < 0)
        return false;
      size = static_cast<int64_t>(n);
      have_size = true;
    }
  }
  return have_url && have_etag && have_size;
}

/// True when a Content-Range start offset is present (`bytes START-...`).
inline bool contentRangeStart(const std::string &value, int64_t &start)
{
  std::string v = trimCopy(value);
  if (v.size() < 6 || asciiLower(v.substr(0, 5)) != "bytes")
    return false;
  size_t i = 5;
  while (i < v.size() && (v[i] == ' ' || v[i] == '\t'))
    i++;
  if (i >= v.size() || v[i] == '*')
    return false;
  char *end = nullptr;
  long long n = std::strtoll(v.c_str() + static_cast<std::ptrdiff_t>(i), &end, 10);
  if (end == v.c_str() + static_cast<std::ptrdiff_t>(i) || n < 0)
    return false;
  start = static_cast<int64_t>(n);
  return true;
}

} // namespace XrdPsTpc

#endif
