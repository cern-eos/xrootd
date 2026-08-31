//------------------------------------------------------------------------------
// nghttp2-backed HTTP/2 session for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------

#include "wire/XrdHttp2Session.hh"
#include "XrdHttpProtocol.hh"
#include "XrdHttpTrace.hh"

#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <utility>
#include <vector>

namespace
{
const char *TraceID = "Http2Session";

struct SessionCtx
{
  XrdHttp2Session *self;
  XrdHttpProtocol   *prot;
};

bool iequals(const std::string &a, const char *b)
{
  if (!b)
    return false;
  size_t i = 0;
  for (; a[i] && b[i]; ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i])))
      return false;
  }
  return a[i] == '\0' && b[i] == '\0';
}

bool isBodyMethod(const std::string &method)
{
  return method == "PUT" || method == "POST" || method == "PATCH";
}

bool hasContentLength(const XrdHttp2StreamState *st)
{
  if (!st)
    return false;
  for (const auto &hdr : st->headers) {
    if (iequals(hdr.first, "content-length"))
      return true;
  }
  return false;
}

ssize_t sendCallback(nghttp2_session * /*session*/, const uint8_t *data,
                     size_t length, int /*flags*/, void *user_data)
{
  auto *ctx = static_cast<SessionCtx *>(user_data);
  if (!ctx || !ctx->prot)
    return NGHTTP2_ERR_CALLBACK_FAILURE;

  const int sent = ctx->prot->SendWireData(reinterpret_cast<const char *>(data),
                                           static_cast<int>(length));
  if (sent < 0)
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  if (sent == 0)
    return NGHTTP2_ERR_WOULDBLOCK;
  return sent;
}

int onBeginHeaders(nghttp2_session * /*session*/, const nghttp2_frame *frame,
                   void *user_data)
{
  auto *ctx = static_cast<SessionCtx *>(user_data);
  if (!ctx || !ctx->self)
    return NGHTTP2_ERR_CALLBACK_FAILURE;

  if (frame->hd.type != NGHTTP2_HEADERS ||
      frame->headers.cat != NGHTTP2_HCAT_REQUEST)
    return 0;

  ctx->self->beginStream(frame->hd.stream_id);
  return 0;
}

int onHeader(nghttp2_session * /*session*/, const nghttp2_frame *frame,
             const uint8_t *name, size_t namelen, const uint8_t *value,
             size_t valuelen, uint8_t /*flags*/, void *user_data)
{
  auto *ctx = static_cast<SessionCtx *>(user_data);
  if (!ctx || !ctx->self)
    return 0;

  const std::string n(reinterpret_cast<const char *>(name), namelen);
  const std::string v(reinterpret_cast<const char *>(value), valuelen);
  ctx->self->addHeader(frame->hd.stream_id, n, v);
  return 0;
}

int onDataChunk(nghttp2_session * /*session*/, uint8_t /*flags*/,
                int32_t stream_id, const uint8_t *data, size_t len,
                void *user_data)
{
  auto *ctx = static_cast<SessionCtx *>(user_data);
  if (!ctx || !ctx->self)
    return 0;

  ctx->self->appendBody(stream_id, data, len);
  return 0;
}

int onFrameRecv(nghttp2_session * /*session*/, const nghttp2_frame *frame,
                void *user_data)
{
  auto *ctx = static_cast<SessionCtx *>(user_data);
  if (!ctx || !ctx->self || !frame)
    return 0;

  if (frame->hd.type == NGHTTP2_GOAWAY) {
    ctx->self->onGoaway();
    return 0;
  }

  const int32_t stream_id = frame->hd.stream_id;
  if (stream_id == 0)
    return 0;

  if (frame->hd.type == NGHTTP2_RST_STREAM) {
    ctx->self->onStreamClosed(stream_id, frame->rst_stream.error_code);
    return 0;
  }

  if (frame->hd.type == NGHTTP2_HEADERS &&
      frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
    ctx->self->markHeadersComplete(stream_id,
                                   frame->hd.flags & NGHTTP2_FLAG_END_STREAM);
    return 0;
  }

  if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)
    ctx->self->markEndStream(stream_id);
  return 0;
}

int onStreamClose(nghttp2_session * /*session*/, int32_t stream_id,
                  uint32_t error_code, void *user_data)
{
  auto *ctx = static_cast<SessionCtx *>(user_data);
  if (!ctx || !ctx->self)
    return 0;
  if (error_code != 0)
    ctx->self->onStreamClosed(stream_id, error_code);
  return 0;
}
}

void XrdHttp2Session::beginStream(int32_t stream_id)
{
  if (stream_id <= 0)
    return;

  if (goaway_ || streams_.size() >= kMaxConcurrentStreams) {
    if (session_) {
      nghttp2_submit_rst_stream(static_cast<nghttp2_session *>(session_),
                                NGHTTP2_FLAG_NONE, stream_id,
                                NGHTTP2_REFUSED_STREAM);
    }
    return;
  }

  if (streams_.count(stream_id))
    return;

  auto st = std::make_unique<XrdHttp2StreamState>();
  st->stream_id = stream_id;
  streams_[stream_id] = std::move(st);
}

void XrdHttp2Session::addHeader(int32_t stream_id, const std::string &name,
                                const std::string &value)
{
  XrdHttp2StreamState *st = findStream(stream_id);
  if (!st)
    return;

  if (name == ":method")
    st->method = value;
  else if (name == ":path")
    st->path = value;
  else if (name == ":scheme")
    st->scheme = value;
  else if (name == ":authority")
    st->authority = value;
  else
    st->headers.emplace_back(name, value);
}

void XrdHttp2Session::appendBody(int32_t stream_id, const uint8_t *data,
                                 size_t len)
{
  XrdHttp2StreamState *st = findStream(stream_id);
  if (!st || !data || !len)
    return;
  st->body.insert(st->body.end(), data, data + len);
}

void XrdHttp2Session::markHeadersComplete(int32_t stream_id, bool end_stream)
{
  XrdHttp2StreamState *st = findStream(stream_id);
  if (!st)
    return;
  st->headers_done = true;
  if (end_stream)
    st->end_stream = true;
  enqueueIfReady(st);
}

void XrdHttp2Session::markEndStream(int32_t stream_id)
{
  XrdHttp2StreamState *st = findStream(stream_id);
  if (!st)
    return;
  st->end_stream = true;
  enqueueIfReady(st);
}

void XrdHttp2Session::onStreamClosed(int32_t stream_id, uint32_t error_code)
{
  if (error_code == 0)
    return;

  TRACE(ALL, " HTTP/2 stream reset stream=" << stream_id
        << " error=" << error_code);

  if (stream_id == activeStreamId_)
    activeStreamId_ = -1;
  pendingResponses_.erase(stream_id);
  dropStream(stream_id);
}

void XrdHttp2Session::onGoaway()
{
  TRACE(ALL, " HTTP/2 GOAWAY received");
  goaway_ = true;

  for (auto it = ready_queue_.begin(); it != ready_queue_.end(); ) {
    if (*it != activeStreamId_)
      it = ready_queue_.erase(it);
    else
      ++it;
  }

  for (auto it = streams_.begin(); it != streams_.end(); ) {
    if (it->first != activeStreamId_)
      it = streams_.erase(it);
    else
      ++it;
  }
}

XrdHttp2Session::XrdHttp2Session()
: session_(nullptr),
  sessionCtx_(nullptr),
  activeStreamId_(-1),
  goaway_(false),
  wire_drained_(false)
{
}

XrdHttp2Session::~XrdHttp2Session()
{
  reset();
}

XrdHttp2StreamState *XrdHttp2Session::findStream(int32_t stream_id)
{
  auto it = streams_.find(stream_id);
  if (it == streams_.end())
    return nullptr;
  return it->second.get();
}

void XrdHttp2Session::dropStream(int32_t stream_id)
{
  ready_queue_.erase(std::remove(ready_queue_.begin(), ready_queue_.end(),
                                 stream_id),
                     ready_queue_.end());
  streams_.erase(stream_id);
}

bool XrdHttp2Session::readyToDispatch(const XrdHttp2StreamState *st) const
{
  if (!st || !st->headers_done || st->dispatched)
    return false;
  if (st->method.empty() || st->path.empty())
    return false;
  if (st->end_stream)
    return true;
  if (!isBodyMethod(st->method))
    return true;
  return hasContentLength(st);
}

void XrdHttp2Session::enqueueIfReady(XrdHttp2StreamState *st)
{
  if (!readyToDispatch(st) || st->queued)
    return;
  st->queued = true;
  ready_queue_.push_back(st->stream_id);
}

void XrdHttp2Session::synthesizeContentLength(XrdHttp2StreamState *st)
{
  if (!st || !st->end_stream || hasContentLength(st))
    return;
  const size_t remain = st->body.size() - st->body_offset;
  if (remain == 0)
    return;
  st->headers.emplace_back("content-length", std::to_string(remain));
}

void XrdHttp2Session::reset()
{
  pendingResponses_.clear();
  emptyPending_ = {};
  streams_.clear();
  ready_queue_.clear();
  activeStreamId_ = -1;
  goaway_ = false;
  wire_drained_ = false;

  if (session_) {
    nghttp2_session_del(static_cast<nghttp2_session *>(session_));
    session_ = nullptr;
  }

  delete static_cast<SessionCtx *>(sessionCtx_);
  sessionCtx_ = nullptr;
}

int XrdHttp2Session::flushSend(XrdHttpProtocol &prot)
{
  if (!session_)
    return 0;

  auto *session = static_cast<nghttp2_session *>(session_);
  auto *ctx = static_cast<SessionCtx *>(sessionCtx_);
  if (ctx)
    ctx->prot = &prot;

  const uint8_t *data = nullptr;
  ssize_t datalen = 0;
  size_t send_offset = 0;

  for (;;) {
    if (send_offset >= static_cast<size_t>(datalen)) {
      datalen = nghttp2_session_mem_send(session, &data);
      send_offset = 0;
      if (datalen < 0)
        return -1;
      if (datalen == 0)
        return 0;
    }

    const int to_send = static_cast<int>(datalen - send_offset);
    const int sent =
        prot.SendWireData(reinterpret_cast<const char *>(data + send_offset),
                          to_send);
    if (sent < 0)
      return -1;
    if (sent == 0)
      return 0;
    send_offset += static_cast<size_t>(sent);
  }
}

bool XrdHttp2Session::hasPendingSend() const
{
  if (!session_)
    return false;
  return nghttp2_session_want_write(
             static_cast<nghttp2_session *>(session_)) != 0;
}

XrdHttp2PendingResponse &XrdHttp2Session::pendingResponse()
{
  if (activeStreamId_ < 0)
    return emptyPending_;
  return pendingResponses_[activeStreamId_];
}

XrdHttp2PendingResponse *XrdHttp2Session::pendingFor(int32_t stream_id)
{
  auto it = pendingResponses_.find(stream_id);
  if (it == pendingResponses_.end())
    return nullptr;
  return &it->second;
}

XrdHttp2PendingResponse &XrdHttp2Session::ensurePending(int32_t stream_id)
{
  return pendingResponses_[stream_id];
}

bool XrdHttp2Session::hasOutboundPending() const
{
  for (const auto &kv : pendingResponses_) {
    const XrdHttp2PendingResponse &p = kv.second;
    if (!p.active)
      continue;
    if (!p.body.empty() && p.body_offset < p.body.size())
      return true;
    if (p.streaming && p.bytes_sent < p.content_length)
      return true;
  }
  return false;
}

static int applyLine(XrdHttpReq &req, bool firstLine, std::string &line)
{
  line.push_back('\0');
  if (firstLine)
    return req.parseFirstLine(line.data(), static_cast<int>(line.size()));
  return req.parseLine(line.data(), static_cast<int>(line.size()));
}

int XrdHttp2Session::applyTo(XrdHttp2StreamState *st, XrdHttpReq &req)
{
  if (!st || st->method.empty() || st->path.empty())
    return -1;

  std::string requestLine = st->method + " " + st->path + " HTTP/2\r\n";
  if (applyLine(req, true, requestLine) < 0)
    return -1;

  if (!st->authority.empty()) {
    std::string hostLine = "Host: " + st->authority + "\r\n";
    if (applyLine(req, false, hostLine) < 0)
      return -1;
  }

  for (const auto &hdr : st->headers) {
    std::string line = hdr.first + ": " + hdr.second + "\r\n";
    if (applyLine(req, false, line) < 0)
      return -1;
  }

  req.headerok = true;
  return 0;
}

int XrdHttp2Session::injectPendingBody(XrdHttpProtocol &prot)
{
  if (activeStreamId_ < 0)
    return 0;

  XrdHttp2StreamState *st = findStream(activeStreamId_);
  if (!st || st->body_offset >= st->body.size())
    return 0;

  const char *data = st->body.data() + st->body_offset;
  const int remain = static_cast<int>(st->body.size() - st->body_offset);
  const int n = prot.BuffInject(data, remain);
  if (n < 0)
    return -1;
  st->body_offset += static_cast<size_t>(n);
  if (st->body_offset >= 4096 && st->body_offset >= st->body.size() / 2) {
    st->body.erase(st->body.begin(), st->body.begin() +
                   static_cast<std::ptrdiff_t>(st->body_offset));
    st->body_offset = 0;
  }
  return 0;
}

int XrdHttp2Session::feedRecv(XrdHttpProtocol &prot, const uint8_t *data,
                              size_t len)
{
  if (!session_ || !data || !len)
    return 0;

  auto *session = static_cast<nghttp2_session *>(session_);
  const ssize_t consumed =
      nghttp2_session_mem_recv(session, data, len);
  if (consumed < 0)
    return -1;
  if (consumed > 0 && flushSend(prot) < 0)
    return -1;
  return static_cast<int>(consumed);
}

int XrdHttp2Session::recvFrames(XrdHttpProtocol &prot, XrdLink *lp)
{
  // The protocol buffer is shared with injected request bodies. Drain the
  // ALPN/preface leftovers once, then never treat myBuff as wire data again.
  if (!wire_drained_) {
    for (;;) {
      int avail = 0;
      char *wired = prot.BuffPeek(avail);
      if (avail <= 0 || !wired)
        break;
      std::vector<uint8_t> tmp(reinterpret_cast<uint8_t *>(wired),
                               reinterpret_cast<uint8_t *>(wired) + avail);
      prot.BuffConsume(avail);
      if (feedRecv(prot, tmp.data(), tmp.size()) < 0)
        return -1;
    }
    wire_drained_ = true;
  }

  auto drainPending = [&]() -> int {
    while (prot.SSLPending() > 0) {
      const int n = prot.RecvWireData(reinterpret_cast<char *>(recvbuf_),
                                      static_cast<int>(kRecvBufSize));
      if (n < 0)
        return -1;
      if (n == 0)
        break;
      if (feedRecv(prot, recvbuf_, static_cast<size_t>(n)) < 0)
        return -1;
    }
    return 0;
  };

  if (drainPending() < 0)
    return -1;

  if (lp) {
    const int n = prot.RecvWireData(reinterpret_cast<char *>(recvbuf_),
                                    static_cast<int>(kRecvBufSize));
    if (n < 0)
      return -1;
    if (n > 0 && feedRecv(prot, recvbuf_, static_cast<size_t>(n)) < 0)
      return -1;
    if (drainPending() < 0)
      return -1;
  }

  return 0;
}

int XrdHttp2Session::ensureSession(XrdHttpProtocol &prot, bool flush)
{
  if (session_)
    return 0;

  nghttp2_session_callbacks *callbacks = nullptr;
  nghttp2_session_callbacks_new(&callbacks);
  nghttp2_session_callbacks_set_send_callback(callbacks, sendCallback);
  nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks,
                                                          onBeginHeaders);
  nghttp2_session_callbacks_set_on_header_callback(callbacks, onHeader);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks,
                                                            onDataChunk);
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks,
                                                       onFrameRecv);
  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks,
                                                         onStreamClose);

  auto *ctx = new SessionCtx{this, &prot};
  sessionCtx_ = ctx;
  nghttp2_session *session = nullptr;
  if (nghttp2_session_server_new(&session, callbacks, ctx) != 0) {
    nghttp2_session_callbacks_del(callbacks);
    delete ctx;
    sessionCtx_ = nullptr;
    return -1;
  }
  nghttp2_session_callbacks_del(callbacks);
  session_ = session;

  nghttp2_settings_entry settings[] = {
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, kMaxConcurrentStreams}};
  nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, settings, 1);
  return flush ? flushSend(prot) : 0;
}

int XrdHttp2Session::dispatchStream(int32_t stream_id, XrdHttpProtocol &prot,
                                    XrdLink *lp)
{
  XrdHttp2StreamState *st = findStream(stream_id);
  if (!st)
    return -1;

  synthesizeContentLength(st);
  if (applyTo(st, prot.CurrentReq) < 0)
    return -1;

  activeStreamId_ = stream_id;
  st->dispatched = true;
  st->queued = false;
  if (injectPendingBody(prot) < 0)
    return -1;

  TRACE(ALL, " HTTP/2 request ready stream=" << st->stream_id << " "
        << st->method << " " << st->path);

  int rc = prot.processParsedRequest(lp);
  if (rc == 0 && lp && prot.CurrentReq.fopened) {
    prot.CurrentReq.reqstate++;
    rc = prot.processParsedRequest(nullptr);
  }
  if (flushSend(prot) < 0)
    return -1;
  return rc;
}

int XrdHttp2Session::dispatchNext(XrdHttpProtocol &prot, XrdLink *lp)
{
  while (!ready_queue_.empty()) {
    const int32_t stream_id = ready_queue_.front();
    ready_queue_.pop_front();
    if (!findStream(stream_id))
      continue;
    return dispatchStream(stream_id, prot, lp);
  }
  return 0;
}

bool XrdHttp2Session::finishActiveIfIdle(XrdHttpProtocol &prot)
{
  if (activeStreamId_ < 0)
    return false;
  if (prot.CurrentReq.headerok || prot.CurrentReq.reqstate > 0 ||
      prot.DoingLogin)
    return false;

  // Leave pendingResponses_ in place so DATA can finish flushing while
  // the next stream starts on the Bridge.
  dropStream(activeStreamId_);
  activeStreamId_ = -1;
  return true;
}

bool XrdHttp2Session::appInFlight(XrdHttpProtocol &prot) const
{
  return prot.CurrentReq.headerok || prot.CurrentReq.reqstate > 0 ||
         prot.DoingLogin;
}

int XrdHttp2Session::drive(XrdHttpProtocol &prot, XrdLink *lp)
{
  if (ensureSession(prot) < 0)
    return -1;
  if (flushSend(prot) < 0)
    return -1;

  if (recvFrames(prot, lp) < 0)
    return -1;
  if (injectPendingBody(prot) < 0)
    return -1;

  if (appInFlight(prot)) {
    if (!prot.CurrentReq.headerok && !prot.DoingLogin)
      return 1;
    const int rc = prot.processParsedRequest(lp);
    if (rc < 0)
      return rc;
    if (flushSend(prot) < 0)
      return -1;
    finishActiveIfIdle(prot);
    if (!ready_queue_.empty() && !appInFlight(prot)) {
      const int drc = dispatchNext(prot, lp);
      if (drc != 0)
        return drc;
      if (appInFlight(prot))
        return 0;
    }
    return rc;
  }

  finishActiveIfIdle(prot);

  if (!ready_queue_.empty()) {
    const int rc = dispatchNext(prot, lp);
    if (flushSend(prot) < 0)
      return -1;
    return rc;
  }

  if (flushSend(prot) < 0)
    return -1;

  // Wait for the next poll event. Returning 0 here busy-loops the scheduler
  // on keep-alive connections that have no Bridge work left.
  return 1;
}

int XrdHttp2Session::acceptH2cUpgrade(XrdHttpProtocol &prot,
                                      const uint8_t *settings,
                                      size_t settings_len, bool head_request)
{
  if (ensureSession(prot, false) < 0)
    return -1;

  auto *session = static_cast<nghttp2_session *>(session_);
  if (nghttp2_session_upgrade2(session, settings, settings_len,
                               head_request ? 1 : 0, nullptr) != 0)
    return -1;

  attachUpgradedRequest();
  return 0;
}

void XrdHttp2Session::attachUpgradedRequest()
{
  auto st = std::make_unique<XrdHttp2StreamState>();
  st->stream_id = 1;
  st->headers_done = true;
  st->end_stream = true;
  st->dispatched = true;
  streams_[1] = std::move(st);
  activeStreamId_ = 1;
  wire_drained_ = true;
}

void XrdHttp2Session::maybePush(XrdHttpProtocol &prot, const std::string &scheme,
                               const std::string &authority,
                               const std::string &current_path,
                               const std::vector<std::string> &paths)
{
  (void)prot;
  if (!session_ || paths.empty() || activeStreamId_ <= 0)
    return;
  if ((activeStreamId_ % 2) == 0)
    return;

  auto *session = static_cast<nghttp2_session *>(session_);
  if (nghttp2_session_get_remote_settings(
          session, NGHTTP2_SETTINGS_ENABLE_PUSH) == 0)
    return;

  for (const auto &path : paths) {
    if (path.empty() || path == current_path)
      continue;

    std::string method = "GET";
    std::string sch = scheme.empty() ? "https" : scheme;
    std::string auth = authority;
    std::string pth = path;
    if (auth.empty())
      continue;

    nghttp2_nv nva[4];
    nva[0] = {reinterpret_cast<uint8_t *>(const_cast<char *>(":method")),
              reinterpret_cast<uint8_t *>(method.data()),
              7, method.size(), NGHTTP2_NV_FLAG_NONE};
    nva[1] = {reinterpret_cast<uint8_t *>(const_cast<char *>(":path")),
              reinterpret_cast<uint8_t *>(pth.data()),
              5, pth.size(), NGHTTP2_NV_FLAG_NONE};
    nva[2] = {reinterpret_cast<uint8_t *>(const_cast<char *>(":scheme")),
              reinterpret_cast<uint8_t *>(sch.data()),
              7, sch.size(), NGHTTP2_NV_FLAG_NONE};
    nva[3] = {reinterpret_cast<uint8_t *>(const_cast<char *>(":authority")),
              reinterpret_cast<uint8_t *>(auth.data()),
              10, auth.size(), NGHTTP2_NV_FLAG_NONE};

    const int32_t promised = nghttp2_submit_push_promise(
        session, NGHTTP2_FLAG_NONE, activeStreamId_, nva, 4, nullptr);
    if (promised <= 0)
      continue;

    auto st = std::make_unique<XrdHttp2StreamState>();
    st->stream_id = promised;
    st->method = method;
    st->path = pth;
    st->scheme = sch;
    st->authority = auth;
    st->headers_done = true;
    st->end_stream = true;
    XrdHttp2StreamState *ready = st.get();
    streams_[promised] = std::move(st);
    enqueueIfReady(ready);

    TRACE(ALL, " HTTP/2 PUSH_PROMISE parent=" << activeStreamId_
          << " promised=" << promised << " GET " << pth);
  }
}
