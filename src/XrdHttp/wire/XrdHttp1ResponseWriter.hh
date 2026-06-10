//------------------------------------------------------------------------------
// HTTP/1.1 response framing for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDHTTP1RESPONSEWRITER_HH
#define XRDHTTP1RESPONSEWRITER_HH

class XrdHttpProtocol;

class XrdHttp1ResponseWriter
{
public:

  static int startSimple(XrdHttpProtocol &prot, int code, const char *desc,
                         const char *header_to_add, long long bodylen,
                         bool keepalive);

  static int sendSimple(XrdHttpProtocol &prot, int code, const char *desc,
                        const char *header_to_add, const char *body,
                        long long bodylen, bool keepalive);

  static int startChunked(XrdHttpProtocol &prot, int code, const char *desc,
                          const char *header_to_add, long long bodylen,
                          bool keepalive);
};

#endif
