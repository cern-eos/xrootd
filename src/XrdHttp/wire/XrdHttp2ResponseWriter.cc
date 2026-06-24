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

ssize_t readResponse(nghttp2_session * /*session*/, int32_t stream_id,
                     uint8_t *buf, size_t length, uint32_t *data_flags,
                     nghttp2_data_source *source, void * /*user_data*/)
{
  if (!source || !source->ptr)
    return NGHTTP2_ERR_CALLBACK_FAILURE;

  auto *session = static_cast<XrdHttp2Session *>(source->ptr);

  XrdHttp2PendingResponse &resp = session->pendingResponse();
  if (!resp.active || resp.stream_id != stream_id)
    return NGHTTP2_ERR_CALLBACK_FAILURE;

  const size_t remain = resp.body.size() - resp.body_offset;
  if (remain == 0) {
    if (resp.streaming && resp.bytes_sent < resp.content_length)
      return NGHTTP2_ERR_DEFERRED;
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    if (resp.streaming)
      resp.active = false;
    return 0;
  }

  const size_t n = std::min(length, remain);
  memcpy(buf, resp.body.data() + resp.body_offset, n);
  resp.body_offset += n;
  resp.bytes_sent += static_cast<long long>(n);
  compactPendingBody(resp);

  if (!resp.streaming && resp.body_offset >= resp.body.size())
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
  else if (resp.streaming && resp.bytes_sent >= resp.content_length)
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;

  if (*data_flags & NGHTTP2_DATA_FLAG_EOF)
    resp.active = false;

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

  long long content_length = bodylen;
  if (bodylen <= 0)
    content_length = body ? static_cast<long long>(strlen(body)) : 0;

  const int32_t stream_id = prot.http2Session_.activeStreamId();
  if (stream_id < 0)
    return -1;

  const bool streaming =
      !body && content_length > 0 &&
      prot.CurrentReq.request != XrdHttpReq::rtHEAD;

  XrdHttp2PendingResponse &pending = prot.http2Session_.pendingResponse();
  pending = {};
  pending.stream_id = stream_id;
  pending.status_code = code;
  pending.content_length = content_length;
  pending.streaming = streaming;
  pending.active = true;

  if (body && content_length > 0)
    pending.body.assign(body, static_cast<size_t>(content_length));

  const bool use_provider =
      content_length > 0 && (body != nullptr || streaming);

  const std::string merged = mergeHeaders(prot, header_to_add);
  if (submitResponse(prot.http2Session_, prot, pending, code,
                     merged.empty() ? nullptr : merged.c_str(),
                     content_length, use_provider) < 0) {
    XrdHttpMon::Record(prot.CurrentReq, code);
    return -1;
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

  XrdHttp2PendingResponse &pending = prot.http2Session_.pendingResponse();
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
  XrdHttp2PendingResponse &pending = prot.http2Session_.pendingResponse();
  if (!pending.active || !pending.streaming)
    return 0;

  pending.bytes_sent = pending.content_length;

  nghttp2_session *session =
      static_cast<nghttp2_session *>(prot.http2Session_.nghttp2SessionHandle());
  if (!session)
    return -1;

  if (nghttp2_session_resume_data(session, pending.stream_id) != 0)
    return -1;

  return prot.http2Session_.flushSend(prot) < 0 ? -1 : 0;
}
