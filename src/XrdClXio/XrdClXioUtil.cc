//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XrdClXioUtil.hh"

#include "XioUrl.hh"

#include <XrdCl/XrdClURL.hh>

#include <cerrno>
#include <cstdlib>

namespace XrdClXio {

bool parseTarget(const std::string &url, XioFS::Url &out, std::string &path,
                 std::string &err)
{
  XrdCl::URL u(url);
  if (!u.IsValid() || u.GetHostName().empty()) {
    err = "invalid URL";
    return false;
  }
  const std::string proto = u.GetProtocol();
  out = XioFS::Url{};
  out.host = u.GetHostName();
  out.port = u.GetPort();
  if (proto == "xios" || proto == "https" || proto == "davs") {
    out.tls = true;
    out.scheme = "https";
    if (out.port <= 0)
      out.port = 443;
  } else if (proto == "xio" || proto == "http" || proto == "dav") {
    out.tls = false;
    out.scheme = "http";
    if (out.port <= 0)
      out.port = 80;
  } else {
    err = "unsupported URL scheme";
    return false;
  }
  const bool defport = (out.tls && out.port == 443) || (!out.tls && out.port == 80);
  out.authority = out.host;
  if (!defport)
    out.authority += ":" + std::to_string(out.port);
  path = u.GetPathWithParams();
  if (path.empty())
    path = "/";
  out.path = path;
  return true;
}

XioFS::Http2Session::Options sessionOptions(time_t timeout)
{
  XioFS::Http2Session::Options opt;
  opt.timeout_ms = timeout > 0 ? static_cast<int>(timeout) * 1000 : 30000;
  if (const char *c = std::getenv("SSL_CERT_FILE"))
    opt.cacert = c;
  if (const char *c = std::getenv("SSL_CERT_DIR"))
    opt.cadir = c;
  else if (const char *c = std::getenv("X509_CERT_DIR"))
    opt.cadir = c;
  if (std::getenv("XRD_XIO_INSECURE"))
    opt.verify_peer = false;
  return opt;
}

void applyTimeout(XioFS::HttpRequest &req, time_t timeout)
{
  if (timeout > 0)
    req.timeout_ms = static_cast<int>(timeout) * 1000;
}

std::shared_ptr<XioFS::Http2Session> connectTarget(const XioFS::Url &url,
                                                   time_t timeout,
                                                   std::string &err)
{
  auto sess = std::make_shared<XioFS::Http2Session>();
  if (sess->connect(url, sessionOptions(timeout), err))
    return {};
  return sess;
}

XrdCl::XRootDStatus statusFrom(int rc, int http, const std::string &err)
{
  if (rc == -ETIMEDOUT)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errSocketTimeout, 0, err);
  if (rc == -ECONNREFUSED || rc == -ENOTCONN || rc == -ECONNRESET)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errConnectionError,
                               rc < 0 ? -rc : 0, err);
  if (rc == -ELOOP)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errRedirectLimit, 0, err);
  if (rc != 0)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInternal,
                               rc < 0 ? -rc : rc, err);
  if (http == 404)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errNotFound, 404,
                               err.empty() ? "not found" : err);
  if (http < 200 || http >= 300)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errErrorResponse, http,
                               err.empty() ? "HTTP error" : err);
  return XrdCl::XRootDStatus();
}

} // namespace XrdClXio
