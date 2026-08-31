//------------------------------------------------------------------------------
// nghttp2-backed HTTP/2 session for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XRDHTTP2SESSION_HH
#define XRDHTTP2SESSION_HH

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
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
  std::size_t body_offset{0};
  bool headers_done{false};
  bool end_stream{false};
  bool dispatched{false};
  bool queued{false};
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

  XrdHttp2PendingResponse &pendingResponse();
  XrdHttp2PendingResponse *pendingFor(int32_t stream_id);
  XrdHttp2PendingResponse &ensurePending(int32_t stream_id);
  bool hasOutboundPending() const;

  int flushSend(XrdHttpProtocol &prot);

  bool hasPendingSend() const;

  void *nghttp2SessionHandle() const { return session_; }

  int acceptH2cUpgrade(XrdHttpProtocol &prot, const uint8_t *settings,
                       size_t settings_len, bool head_request);
  void attachUpgradedRequest();
  void maybePush(XrdHttpProtocol &prot, const std::string &scheme,
                 const std::string &authority, const std::string &current_path,
                 const std::vector<std::string> &paths);

  void beginStream(int32_t stream_id);
  void addHeader(int32_t stream_id, const std::string &name,
                 const std::string &value);
  void appendBody(int32_t stream_id, const uint8_t *data, size_t len);
  void markHeadersComplete(int32_t stream_id, bool end_stream);
  void markEndStream(int32_t stream_id);
  void onStreamClosed(int32_t stream_id, uint32_t error_code);
  void onGoaway();

private:

  static const uint32_t kMaxConcurrentStreams = 100;
  static const size_t kRecvBufSize = 16384;

  XrdHttp2StreamState *findStream(int32_t stream_id);
  bool appInFlight(XrdHttpProtocol &prot) const;
  bool readyToDispatch(const XrdHttp2StreamState *st) const;
  void enqueueIfReady(XrdHttp2StreamState *st);
  void dropStream(int32_t stream_id);
  void synthesizeContentLength(XrdHttp2StreamState *st);

  int applyTo(XrdHttp2StreamState *st, XrdHttpReq &req);
  int injectPendingBody(XrdHttpProtocol &prot);
  int recvFrames(XrdHttpProtocol &prot, XrdLink *lp);
  int feedRecv(XrdHttpProtocol &prot, const uint8_t *data, size_t len);
  int ensureSession(XrdHttpProtocol &prot, bool flush = true);
  int dispatchStream(int32_t stream_id, XrdHttpProtocol &prot, XrdLink *lp);
  int dispatchNext(XrdHttpProtocol &prot, XrdLink *lp);
  bool finishActiveIfIdle(XrdHttpProtocol &prot);

  void *session_;
  void *sessionCtx_;
  int32_t activeStreamId_;
  bool goaway_;
  bool wire_drained_;
  std::map<int32_t, std::unique_ptr<XrdHttp2StreamState>> streams_;
  std::deque<int32_t> ready_queue_;
  std::map<int32_t, XrdHttp2PendingResponse> pendingResponses_;
  XrdHttp2PendingResponse emptyPending_;
  uint8_t recvbuf_[kRecvBufSize];
};

#endif
