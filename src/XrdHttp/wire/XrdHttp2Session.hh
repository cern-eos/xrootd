//------------------------------------------------------------------------------
// nghttp2-backed HTTP/2 session for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDHTTP2SESSION_HH
#define XRDHTTP2SESSION_HH

#include <cstdint>
#include <string>
#include <vector>

class XrdHttpProtocol;
class XrdHttpReq;
class XrdLink;

struct XrdHttp2PendingResponse
{
  int32_t     stream_id{0};
  int         status_code{200};
  std::string body;
  std::size_t body_offset{0};
  bool        active{false};
  bool        streaming{false};
  long long   content_length{0};
  long long   bytes_sent{0};
};

struct XrdHttp2StreamState
{
  int32_t stream_id{0};
  std::string method;
  std::string path;
  std::string scheme;
  std::string authority;
  std::vector<std::pair<std::string, std::string>> headers;
  std::vector<char> body;
  bool end_stream{false};
};

class XrdHttp2Session
{
public:

  XrdHttp2Session();
  ~XrdHttp2Session();
  XrdHttp2Session(const XrdHttp2Session &) = delete;
  XrdHttp2Session &operator=(const XrdHttp2Session &) = delete;

  void reset();

  /// Drive the HTTP/2 session. Returns Process()-compatible rc.
  int drive(XrdHttpProtocol &prot, XrdLink *lp);

  int32_t activeStreamId() const { return activeStreamId_; }

  XrdHttp2PendingResponse &pendingResponse() { return pendingResponse_; }
  const XrdHttp2PendingResponse &pendingResponse() const { return pendingResponse_; }

  int flushSend(XrdHttpProtocol &prot);

  bool hasPendingSend() const;

  void *nghttp2SessionHandle() const { return session_; }

  void beginStream(int32_t stream_id);
  void addHeader(const std::string &name, const std::string &value);
  void appendBody(const uint8_t *data, size_t len);
  void markEndStream();
  bool requestReady() const { return requestReady_; }
  void consumeRequestReady() { requestReady_ = false; }

  bool needsReschedule(XrdHttpProtocol &prot) const;

private:

  int applyTo(XrdHttpReq &req);
  int dispatchReadyRequest(XrdHttpProtocol &prot, XrdLink *lp);
  void clearStream();

  void *session_;
  void *sessionCtx_;
  int32_t activeStreamId_;
  bool requestReady_;
  XrdHttp2StreamState *stream_;
  XrdHttp2PendingResponse pendingResponse_;
};

#endif
