//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDCLXIO_UTIL_HH
#define XRDCLXIO_UTIL_HH

#include "XioHttp2.hh"

#include <XrdCl/XrdClXRootDResponses.hh>

#include <memory>
#include <string>

namespace XrdClXio {

bool parseTarget(const std::string &url, XioFS::Url &out, std::string &path,
                 std::string &err);

// Fills CA, bearer, and Kerberos options. Returns false when a bearer file
// exists but cannot be used; err explains why. A missing token file is not
// an error.
bool sessionOptions(time_t timeout, XioFS::Http2Session::Options &opt,
                    std::string &err);

void applyTimeout(XioFS::HttpRequest &req, time_t timeout);

std::shared_ptr<XioFS::Http2Session> connectTarget(const XioFS::Url &url,
                                                   time_t timeout,
                                                   std::string &err);

// Translate a finished exchange. rc != 0 is a transport error. An HTTP
// status outside 2xx is an error response. 404 is errNotFound.
XrdCl::XRootDStatus statusFrom(int rc, int http, const std::string &err);

inline void deliver(XrdCl::ResponseHandler *handler, const XrdCl::XRootDStatus &st,
                    XrdCl::AnyObject *obj)
{
  if (!handler) {
    delete obj;
    return;
  }
  handler->HandleResponse(new XrdCl::XRootDStatus(st), obj);
}

} // namespace XrdClXio

#endif
