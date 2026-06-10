//------------------------------------------------------------------------------
// llhttp-backed HTTP/1.1 request header parser for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDHTTP1SESSION_HH
#define XRDHTTP1SESSION_HH

#include "XrdHttpReq.hh"

extern "C" {
#include "vendor/llhttp/llhttp.h"
}

class XrdHttpProtocol;

class XrdHttp1Session
{
public:

  XrdHttp1Session();
  ~XrdHttp1Session();
  XrdHttp1Session(const XrdHttp1Session &) = delete;
  XrdHttp1Session &operator=(const XrdHttp1Session &) = delete;
  XrdHttp1Session(XrdHttp1Session &&) = delete;
  XrdHttp1Session &operator=(XrdHttp1Session &&) = delete;

  void reset();

  /// Feed bytes from the protocol read buffer. Returns:
  ///   0  headers parsed and applied to req
  ///   1  need more bytes
  ///  -1  parse or apply error
  int parseHeaders(XrdHttpProtocol &prot, XrdHttpReq &req);

  bool headersReady() const { return headersReady_; }

  void resetMessage();
  void appendMethod(const char *at, size_t length);
  void appendUrl(const char *at, size_t length);
  void appendVersion(const char *at, size_t length);
  void appendHeaderField(const char *at, size_t length);
  void appendHeaderValue(const char *at, size_t length);
  void completeHeaderValue();
  void markHeadersReady();

private:

  llhttp_t  parser_;
  void     *sessionCtx_;
  bool      headersReady_;
  bool      haveRequestLine_;

  std::string method_;
  std::string url_;
  int         httpMajor_;
  int         httpMinor_;

  std::string headerField_;
  std::string headerValue_;
  std::vector<std::pair<std::string, std::string>> headers_;

  int applyTo(XrdHttpReq &req);
};

#endif
