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
#include <cstring>
#include <utility>

namespace
{
const char *TraceID = "Http2Session";

struct SessionCtx
{
  XrdHttp2Session *self;
  XrdHttpProtocol   *prot;
};

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

  if (frame->hd.stream_id != ctx->self->activeStreamId())
    return 0;

  const std::string n(reinterpret_cast<const char *>(name), namelen);
  const std::string v(reinterpret_cast<const char *>(value), valuelen);
  ctx->self->addHeader(n, v);
  return 0;
}

int onDataChunk(nghttp2_session * /*session*/, uint8_t /*flags*/,
                int32_t stream_id, const uint8_t *data, size_t len,
                void *user_data)
{
  auto *ctx = static_cast<SessionCtx *>(user_data);
  if (!ctx || !ctx->self || stream_id != ctx->self->activeStreamId())
    return 0;

  ctx->self->appendBody(data, len);
  return 0;
}

int onFrameRecv(nghttp2_session * /*session*/, const nghttp2_frame *frame,
                void *user_data)
{
  auto *ctx = static_cast<SessionCtx *>(user_data);
  if (!ctx || !ctx->self || frame->hd.stream_id != ctx->self->activeStreamId())
    return 0;

  if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)
    ctx->self->markEndStream();
  return 0;
}
}

void XrdHttp2Session::beginStream(int32_t stream_id)
{
  clearStream();
  stream_ = new XrdHttp2StreamState();
  stream_->stream_id = stream_id;
  activeStreamId_ = stream_id;
}

void XrdHttp2Session::addHeader(const std::string &name,
                                const std::string &value)
{
  if (!stream_)
    return;

  if (name == ":method")
    stream_->method = value;
  else if (name == ":path")
    stream_->path = value;
  else if (name == ":scheme")
    stream_->scheme = value;
  else if (name == ":authority")
    stream_->authority = value;
  else
    stream_->headers.emplace_back(name, value);
}

void XrdHttp2Session::appendBody(const uint8_t *data, size_t len)
{
  if (!stream_ || !data || !len)
    return;
  stream_->body.insert(stream_->body.end(), data, data + len);
}

void XrdHttp2Session::markEndStream()
{
  if (!stream_ || stream_->method.empty() || stream_->path.empty())
    return;
  stream_->end_stream = true;
  requestReady_ = true;
}

XrdHttp2Session::XrdHttp2Session()
: session_(nullptr),
  sessionCtx_(nullptr),
  activeStreamId_(-1),
  requestReady_(false),
  stream_(nullptr)
{
}

XrdHttp2Session::~XrdHttp2Session()
{
  reset();
}

void XrdHttp2Session::clearStream()
{
  delete stream_;
  stream_ = nullptr;
  activeStreamId_ = -1;
  requestReady_ = false;
}

void XrdHttp2Session::reset()
{
  pendingResponse_ = {};
  clearStream();

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

  const uint8_t *data = nullptr;
  return nghttp2_session_mem_send(static_cast<nghttp2_session *>(session_),
                                  &data) > 0;
}

static int applyLine(XrdHttpReq &req, bool firstLine, std::string &line)
{
  line.push_back('\0');
  if (firstLine)
    return req.parseFirstLine(line.data(), static_cast<int>(line.size()));
  return req.parseLine(line.data(), static_cast<int>(line.size()));
}

int XrdHttp2Session::applyTo(XrdHttpReq &req)
{
  if (!stream_ || stream_->method.empty() || stream_->path.empty())
    return -1;

  std::string requestLine =
      stream_->method + " " + stream_->path + " HTTP/2\r\n";
  if (applyLine(req, true, requestLine) < 0)
    return -1;

  if (!stream_->authority.empty()) {
    std::string hostLine = "Host: " + stream_->authority + "\r\n";
    if (applyLine(req, false, hostLine) < 0)
      return -1;
  }

  for (const auto &hdr : stream_->headers) {
    std::string line = hdr.first + ": " + hdr.second + "\r\n";
    if (applyLine(req, false, line) < 0)
      return -1;
  }

  req.headerok = true;
  return 0;
}

int XrdHttp2Session::dispatchReadyRequest(XrdHttpProtocol &prot, XrdLink *lp)
{
  if (!stream_ || applyTo(prot.CurrentReq) < 0)
    return -1;

  if (!stream_->body.empty())
    prot.BuffInject(stream_->body.data(),
                    static_cast<int>(stream_->body.size()));

  TRACE(ALL, " HTTP/2 request ready stream=" << stream_->stream_id << " "
        << stream_->method << " " << stream_->path);

  const int32_t stream_id = stream_->stream_id;
  int rc = prot.processParsedRequest(lp);
  if (rc == 0 && lp && prot.CurrentReq.fopened) {
    prot.CurrentReq.reqstate++;
    rc = prot.processParsedRequest(nullptr);
  }
  clearStream();
  activeStreamId_ = stream_id;
  if (flushSend(prot) < 0)
    return -1;
  return rc;
}

bool XrdHttp2Session::needsContinueProcessing(XrdHttpProtocol &prot) const
{
  if (requestReady_ || prot.BuffUsed() > 0 || prot.CurrentReq.headerok ||
      prot.CurrentReq.reqstate > 0 || prot.DoingLogin)
    return true;
  return false;
}

int XrdHttp2Session::drive(XrdHttpProtocol &prot, XrdLink *lp)
{
  if (session_) {
    if (flushSend(prot) < 0)
      return -1;
  }

  if (!session_) {
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
        {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 1}};
    nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, settings, 1);
    if (flushSend(prot) < 0)
      return -1;
  }

  if (!lp) {
    if (!prot.CurrentReq.headerok && !prot.DoingLogin) {
      if (requestReady_)
        return dispatchReadyRequest(prot, nullptr);
      if (flushSend(prot) < 0)
        return -1;
      return 1;
    }
    const int rc = prot.processParsedRequest(nullptr);
    if (rc < 0)
      return rc;
    if (session_ && flushSend(prot) < 0)
      return -1;
    return rc;
  }

  if (prot.CurrentReq.reqstate > 0 || prot.DoingLogin) {
    if (!prot.CurrentReq.headerok && !prot.DoingLogin)
      return 0;
    const int rc = prot.processParsedRequest(lp);
    if (rc < 0)
      return rc;
    if (session_ && flushSend(prot) < 0)
      return -1;
    return rc;
  }

  if (lp && prot.BuffUsed() <= 0) {
    if (prot.getDataOneShot(prot.BuffAvailable()) < 0)
      return -1;
  }

  auto *session = static_cast<nghttp2_session *>(session_);
  int avail = 0;
  char *data = prot.BuffPeek(avail);
  if (avail > 0 && data) {
    const ssize_t consumed = nghttp2_session_mem_recv(
        session, reinterpret_cast<const uint8_t *>(data),
        static_cast<size_t>(avail));
    if (consumed < 0)
      return -1;
    if (consumed > 0) {
      prot.BuffConsume(static_cast<int>(consumed));
      if (flushSend(prot) < 0)
        return -1;
    } else if (lp) {
      // Incomplete frame: keep buffered bytes and read more from the wire.
      if (prot.getDataOneShot(prot.BuffAvailable()) < 0)
        return -1;
    }
  } else if (!requestReady_ && lp && needsContinueProcessing(prot)) {
    return 0;
  }

  if (requestReady_) {
    if (prot.CurrentReq.headerok &&
        (prot.CurrentReq.reqstate > 0 || prot.DoingLogin)) {
      if (!lp)
        return prot.processParsedRequest(nullptr);
      return 0;
    }
    if (prot.CurrentReq.headerok && prot.CurrentReq.reqstate == 0)
      prot.CurrentReq.headerok = false;
    consumeRequestReady();
    return dispatchReadyRequest(prot, lp);
  }

  if (session_ && flushSend(prot) < 0)
    return -1;

  if (!pendingResponse_.active && activeStreamId_ >= 0 &&
      !prot.CurrentReq.headerok && prot.CurrentReq.reqstate == 0 &&
      !requestReady_ && !prot.DoingLogin) {
    activeStreamId_ = -1;
  }

  // rc=0 continues Transit reInvoke / in-request work; rc=1 waits for poll I/O.
  return needsContinueProcessing(prot) ? 0 : 1;
}
