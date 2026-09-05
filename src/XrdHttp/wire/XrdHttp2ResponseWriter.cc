//------------------------------------------------------------------------------
// HTTP/2 response framing for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------

#include "wire/XrdHttp2ResponseWriter.hh"
#include "wire/XrdHttp2Session.hh"
#include "XrdHttpCors/XrdHttpCors.hh"
#include "XrdHttpMon.hh"
#include "XrdHttpProtocol.hh"
#include "XrdHttpReq.hh"
#include "XrdHttpTrace.hh"
#include "XrdHttpUtils.hh"

#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

namespace
{
const char *TraceID = "Http2Resp";

void compactPendingBody(XrdHttp2PendingResponse &resp)
{
  if (resp.body_offset > 0 && resp.body_offset >= resp.body.size()) {
    resp.body.clear();
    resp.body_offset = 0;
  } else if (resp.body_offset > 4096) {
    resp.body.erase(0, resp.body_offset);
    resp.body_offset = 0;
  }
}

bool bodyComplete(const XrdHttp2PendingResponse &resp)
{
  if (!resp.streaming)
    return true;
  if (resp.finished)
    return true;
  return resp.content_length >= 0 && resp.bytes_sent >= resp.content_length;
}

// Emit EOF on the stream; if trailers were queued send them as a trailing
// HEADERS frame instead of END_STREAM on the last DATA frame.
void markEof(nghttp2_session *session, XrdHttp2PendingResponse &resp,
             uint32_t *data_flags)
{
  *data_flags |= NGHTTP2_DATA_FLAG_EOF;
  resp.active = false;
  if (resp.trailers.empty())
    return;

  std::vector<nghttp2_nv> nva;
  nva.reserve(resp.trailers.size());
  for (auto &kv : resp.trailers) {
    nva.push_back({reinterpret_cast<uint8_t *>(const_cast<char *>(kv.first.data())),
                   reinterpret_cast<uint8_t *>(const_cast<char *>(kv.second.data())),
                   kv.first.size(), kv.second.size(), NGHTTP2_NV_FLAG_NONE});
  }
  if (nghttp2_submit_trailer(session, resp.stream_id, nva.data(),
                             nva.size()) == 0)
    *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
}

ssize_t readResponse(nghttp2_session *session, int32_t stream_id,
                     uint8_t *buf, size_t length, uint32_t *data_flags,
                     nghttp2_data_source *source, void * /*user_data*/)
{
  if (!source || !source->ptr)
    return NGHTTP2_ERR_CALLBACK_FAILURE;

  auto *h2 = static_cast<XrdHttp2Session *>(source->ptr);

  XrdHttp2PendingResponse *respP = h2->pendingFor(stream_id);
  if (!respP || !respP->active)
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  XrdHttp2PendingResponse &resp = *respP;

  const size_t remain = resp.unsent();
  if (remain == 0) {
    if (!bodyComplete(resp))
      return NGHTTP2_ERR_DEFERRED;
    markEof(session, resp, data_flags);
    return 0;
  }

  const size_t n = std::min(length, remain);
  memcpy(buf, resp.body.data() + resp.body_offset, n);
  resp.body_offset += n;
  resp.bytes_sent += static_cast<long long>(n);
  compactPendingBody(resp);

  if (resp.unsent() == 0 && bodyComplete(resp))
    markEof(session, resp, data_flags);

  return static_cast<ssize_t>(n);
}

int submitResponse(XrdHttp2Session &http2Session, XrdHttpProtocol &prot,
                   XrdHttp2PendingResponse &pending, int code,
                   const char *header_to_add, long long content_length,
                   bool use_provider)
{
  const int32_t stream_id = pending.stream_id;

  std::vector<std::string> names;
  std::vector<std::string> values;

  names.emplace_back(":status");
  values.push_back(std::to_string(code));

  if (content_length >= 0 && code != 100) {
    names.emplace_back("content-length");
    values.push_back(std::to_string(content_length));
  }

  names.emplace_back("server");
  values.emplace_back("XRootD");

  if (header_to_add && header_to_add[0]) {
    std::istringstream hdrs(header_to_add);
    std::string line;
    while (std::getline(hdrs, line)) {
      if (!line.empty() && line.back() == '\r')
        line.pop_back();
      const auto colon = line.find(':');
      if (colon == std::string::npos)
        continue;

      std::string name = line.substr(0, colon);
      std::string value = line.substr(colon + 1);
      while (!value.empty() && value.front() == ' ')
        value.erase(value.begin());
      std::transform(name.begin(), name.end(), name.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      if (name == "connection" || name == "keep-alive" ||
          name == "proxy-connection" || name == "transfer-encoding" ||
          name == "upgrade")
        continue;

      names.push_back(std::move(name));
      values.push_back(std::move(value));
    }
  }

  std::vector<nghttp2_nv> nva;
  nva.reserve(names.size());
  for (size_t i = 0; i < names.size(); ++i) {
    nva.push_back({reinterpret_cast<uint8_t *>(names[i].data()),
                   reinterpret_cast<uint8_t *>(values[i].data()),
                   names[i].size(), values[i].size(),
                   NGHTTP2_NV_FLAG_NONE});
  }

  nghttp2_data_provider provider;
  provider.source.ptr = &http2Session;
  provider.read_callback = readResponse;

  nghttp2_session *session =
      static_cast<nghttp2_session *>(http2Session.nghttp2SessionHandle());
  if (!session)
    return -1;

  nghttp2_data_provider *provider_ptr = use_provider ? &provider : nullptr;
  if (nghttp2_submit_response(session, stream_id, nva.data(), nva.size(),
                              provider_ptr) != 0) {
    pending.active = false;
    return -1;
  }

  TRACE(ALL, "Submitting HTTP/2 response " << code << " stream=" << stream_id);
  const int flushed = http2Session.flushSend(prot);
  if (flushed < 0) {
    pending.active = false;
    return -1;
  }
  if (!use_provider)
    pending.active = false;

  return 0;
}
}

std::string XrdHttp2ResponseWriter::mergeHeaders(XrdHttpProtocol &prot,
                                                 const char *header_to_add)
{
  std::ostringstream hdr;
  if (header_to_add && header_to_add[0])
    hdr << header_to_add;

  const auto iter = prot.m_staticheaders.find(prot.CurrentReq.requestverb);
  if (iter != prot.m_staticheaders.end())
    hdr << iter->second;
  else
    hdr << prot.m_staticheaders[""];

  if (prot.xrdcors) {
    auto corsAllowOrigin =
        prot.xrdcors->getCORSAllowOriginHeader(prot.CurrentReq.m_origin);
    if (corsAllowOrigin)
      hdr << *corsAllowOrigin << "\r\n";
  }

  return hdr.str();
}

int XrdHttp2ResponseWriter::startSimple(XrdHttpProtocol &prot, int code,
                                      const char *desc,
                                      const char *header_to_add,
                                      long long bodylen, bool /*keepalive*/)
{
  return sendSimple(prot, code, desc, header_to_add, nullptr, bodylen, true);
}

int XrdHttp2ResponseWriter::sendSimple(XrdHttpProtocol &prot, int code,
                                     const char *desc,
                                     const char *header_to_add,
                                     const char *body, long long bodylen,
                                     bool /*keepalive*/)
{
  prot.CurrentReq.setHttpStatusCode(code);
  XrdHttpMon::Record(prot.CurrentReq, code);

  // bodylen < 0 with no inline body means "length unknown, body follows via
  // SendData()/ChunkResp()" (the HTTP/1 chunked case).
  const bool is_head = prot.CurrentReq.request == XrdHttpReq::rtHEAD;
  const bool unknown_length = !body && bodylen < 0 && !is_head;

  long long content_length = bodylen;
  if (unknown_length)
    content_length = -1;
  else if (bodylen <= 0)
    content_length = body ? static_cast<long long>(strlen(body)) : 0;

  const int32_t stream_id = prot.http2Session_.activeStreamId();
  if (stream_id < 0)
    return -1;

  const bool streaming =
      !body && !is_head && (content_length > 0 || unknown_length);

  XrdHttp2PendingResponse &pending = prot.http2Session_.ensurePending(stream_id);
  pending = {};
  pending.stream_id = stream_id;
  pending.status_code = code;
  pending.content_length = content_length;
  pending.streaming = streaming;
  pending.active = true;

  if (body && content_length > 0)
    pending.body.assign(body, static_cast<size_t>(content_length));

  const bool use_provider =
      (content_length > 0 && body != nullptr) || streaming;

  const std::string merged = mergeHeaders(prot, header_to_add);
  if (submitResponse(prot.http2Session_, prot, pending, code,
                     merged.empty() ? nullptr : merged.c_str(),
                     content_length, use_provider) < 0) {
    XrdHttpMon::Record(prot.CurrentReq, code);
    return -1;
  }

  if (code >= 200 && code < 300 &&
      prot.CurrentReq.request == XrdHttpReq::rtGET &&
      !XrdHttpProtocol::h2pushPaths().empty()) {
    const std::string &authority = prot.CurrentReq.host;
    prot.http2Session_.maybePush(prot, prot.isHTTPS() ? "https" : "http",
                                 authority, prot.CurrentReq.resource.c_str(),
                                 XrdHttpProtocol::h2pushPaths());
  }

  XrdHttpMon::Record(prot.CurrentReq, code);
  (void)desc;
  return 0;
}

int XrdHttp2ResponseWriter::sendStreamData(XrdHttpProtocol &prot,
                                           const char *body, int bodylen)
{
  if (!body || bodylen <= 0)
    return 0;

  XrdHttp2PendingResponse *pendingP = prot.http2Session_.pendingFor(
      prot.http2Session_.activeStreamId());
  if (!pendingP)
    pendingP = &prot.http2Session_.pendingResponse();
  XrdHttp2PendingResponse &pending = *pendingP;
  if (!pending.active || !pending.streaming)
    return -1;

  pending.body.append(body, static_cast<size_t>(bodylen));

  nghttp2_session *session =
      static_cast<nghttp2_session *>(prot.http2Session_.nghttp2SessionHandle());
  if (!session)
    return -1;

  if (nghttp2_session_resume_data(session, pending.stream_id) != 0)
    return -1;

  return prot.http2Session_.flushSend(prot) < 0 ? -1 : 0;
}

int XrdHttp2ResponseWriter::finishStream(XrdHttpProtocol &prot)
{
  XrdHttp2PendingResponse *pendingP = prot.http2Session_.pendingFor(
      prot.http2Session_.activeStreamId());
  if (!pendingP)
    pendingP = &prot.http2Session_.pendingResponse();
  XrdHttp2PendingResponse &pending = *pendingP;
  if (!pending.active || !pending.streaming)
    return 0;

  pending.finished = true;

  nghttp2_session *session =
      static_cast<nghttp2_session *>(prot.http2Session_.nghttp2SessionHandle());
  if (!session)
    return -1;

  if (nghttp2_session_resume_data(session, pending.stream_id) != 0)
    return -1;

  return prot.http2Session_.flushSend(prot) < 0 ? -1 : 0;
}

int XrdHttp2ResponseWriter::addTrailers(XrdHttpProtocol &prot,
                                        const char *header_lines)
{
  XrdHttp2PendingResponse *pending = prot.http2Session_.pendingFor(
      prot.http2Session_.activeStreamId());
  if (!pending || !pending->active || !pending->streaming)
    return -1;
  if (!header_lines || !header_lines[0])
    return 0;

  std::istringstream hdrs(header_lines);
  std::string line;
  while (std::getline(hdrs, line)) {
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    const auto colon = line.find(':');
    if (colon == std::string::npos || colon == 0)
      continue;
    std::string name = line.substr(0, colon);
    std::string value = line.substr(colon + 1);
    while (!value.empty() && value.front() == ' ')
      value.erase(value.begin());
    std::transform(name.begin(), name.end(), name.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    pending->trailers.emplace_back(std::move(name), std::move(value));
  }
  return 0;
}
