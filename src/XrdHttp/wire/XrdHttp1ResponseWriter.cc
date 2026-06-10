//------------------------------------------------------------------------------
// HTTP/1.1 response framing for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------

#include "wire/XrdHttp1ResponseWriter.hh"

#include "XrdHttpCors/XrdHttpCors.hh"
#include "XrdHttpMon.hh"
#include "XrdHttpProtocol.hh"
#include "XrdHttpTrace.hh"
#include "XrdHttpUtils.hh"

#include <sstream>

namespace
{
const char *TraceID = "Http1Resp";
}

int XrdHttp1ResponseWriter::startSimple(XrdHttpProtocol &prot, int code,
                                        const char *desc,
                                        const char *header_to_add,
                                        long long bodylen, bool keepalive)
{
  std::stringstream ss;
  const std::string crlf = "\r\n";

  ss << "HTTP/1.1 " << code << " ";

  if (desc) {
    ss << desc;
  } else {
    ss << httpStatusToString(code);
  }
  ss << crlf;

  if (keepalive && (code != 100))
    ss << "Connection: Keep-Alive" << crlf;
  else
    ss << "Connection: Close" << crlf;

  ss << "Server: XRootD" << crlf;

  const auto iter = prot.m_staticheaders.find(prot.CurrentReq.requestverb);
  if (iter != prot.m_staticheaders.end()) {
    ss << iter->second;
  } else {
    ss << prot.m_staticheaders[""];
  }

  if (prot.xrdcors) {
    auto corsAllowOrigin =
        prot.xrdcors->getCORSAllowOriginHeader(prot.CurrentReq.m_origin);
    if (corsAllowOrigin) {
      ss << *corsAllowOrigin << crlf;
    }
  }

  if ((bodylen >= 0) && (code != 100))
    ss << "Content-Length: " << bodylen << crlf;

  if (header_to_add && (header_to_add[0] != '\0'))
    ss << header_to_add << crlf;

  ss << crlf;

  const std::string &outhdr = ss.str();
  TRACE(ALL, "Sending resp: " << code << " header len:" << outhdr.size());
  if (prot.SendData(outhdr.c_str(), outhdr.size()))
    return -1;

  return 0;
}

int XrdHttp1ResponseWriter::sendSimple(XrdHttpProtocol &prot, int code,
                                       const char *desc,
                                       const char *header_to_add,
                                       const char *body, long long bodylen,
                                       bool keepalive)
{
  prot.CurrentReq.setHttpStatusCode(code);
  XrdHttpMon::Record(prot.CurrentReq, code);

  long long content_length = bodylen;
  if (bodylen <= 0)
    content_length = body ? strlen(body) : 0;

  if (startSimple(prot, code, desc, header_to_add, content_length, keepalive) < 0) {
    XrdHttpMon::Record(prot.CurrentReq, code);
    return -1;
  }

  int r = 0;
  if (body)
    r = prot.SendData(body, content_length);

  XrdHttpMon::Record(prot.CurrentReq, code);
  return r <= 0 ? -1 : 0;
}

int XrdHttp1ResponseWriter::startChunked(XrdHttpProtocol &prot, int code,
                                           const char *desc,
                                           const char *header_to_add,
                                           long long bodylen, bool keepalive)
{
  std::stringstream ss;
  const std::string crlf = "\r\n";
  prot.CurrentReq.setHttpStatusCode(code);
  XrdHttpMon::Record(prot.CurrentReq, code);

  if (header_to_add && (header_to_add[0] != '\0'))
    ss << header_to_add << crlf;

  ss << "Transfer-Encoding: chunked";
  TRACE(ALL, "Starting chunked response");

  int r = startSimple(prot, code, desc, ss.str().c_str(), bodylen, keepalive);
  if (r < 0)
    XrdHttpMon::Record(prot.CurrentReq, code);
  return r;
}
