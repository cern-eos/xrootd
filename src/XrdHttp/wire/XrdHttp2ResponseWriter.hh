//------------------------------------------------------------------------------
// HTTP/2 response framing for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDHTTP2RESPONSEWRITER_HH
#define XRDHTTP2RESPONSEWRITER_HH

#include <string>

class XrdHttpProtocol;

class XrdHttp2ResponseWriter
{
public:

  static int startSimple(XrdHttpProtocol &prot, int code, const char *desc,
                         const char *header_to_add, long long bodylen,
                         bool keepalive);

  static int sendSimple(XrdHttpProtocol &prot, int code, const char *desc,
                        const char *header_to_add, const char *body,
                        long long bodylen, bool keepalive);

  static int sendStreamData(XrdHttpProtocol &prot, const char *body,
                            int bodylen);

  static int finishStream(XrdHttpProtocol &prot);

private:

  static std::string mergeHeaders(XrdHttpProtocol &prot,
                                  const char *header_to_add);
};

#endif
