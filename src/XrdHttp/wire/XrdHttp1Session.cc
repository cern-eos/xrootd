//------------------------------------------------------------------------------
// llhttp-backed HTTP/1.1 request header parser for XrdHTTP
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------

#include "wire/XrdHttp1Session.hh"
#include "XrdHttpProtocol.hh"
#include "XrdHttpTrace.hh"

extern "C" {
#include "vendor/llhttp/llhttp.h"
}

#include <cstdio>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace
{
const char *TraceID = "Http1Session";

struct SessionCtx
{
  XrdHttp1Session *self;
};

int on_message_begin(llhttp_t *parser)
{
  auto *ctx = static_cast<SessionCtx *>(parser->data);
  ctx->self->resetMessage();
  return HPE_OK;
}

int on_method(llhttp_t *parser, const char *at, size_t length)
{
  auto *ctx = static_cast<SessionCtx *>(parser->data);
  ctx->self->appendMethod(at, length);
  return HPE_OK;
}

int on_url(llhttp_t *parser, const char *at, size_t length)
{
  auto *ctx = static_cast<SessionCtx *>(parser->data);
  ctx->self->appendUrl(at, length);
  return HPE_OK;
}

int on_version(llhttp_t *parser, const char *at, size_t length)
{
  auto *ctx = static_cast<SessionCtx *>(parser->data);
  ctx->self->appendVersion(at, length);
  return HPE_OK;
}

int on_header_field(llhttp_t *parser, const char *at, size_t length)
{
  auto *ctx = static_cast<SessionCtx *>(parser->data);
  ctx->self->appendHeaderField(at, length);
  return HPE_OK;
}

int on_header_value(llhttp_t *parser, const char *at, size_t length)
{
  auto *ctx = static_cast<SessionCtx *>(parser->data);
  ctx->self->appendHeaderValue(at, length);
  return HPE_OK;
}

int on_header_value_complete(llhttp_t *parser)
{
  auto *ctx = static_cast<SessionCtx *>(parser->data);
  ctx->self->completeHeaderValue();
  return HPE_OK;
}

int on_headers_complete(llhttp_t *parser)
{
  auto *ctx = static_cast<SessionCtx *>(parser->data);
  ctx->self->markHeadersReady();
  return HPE_PAUSED;
}

llhttp_settings_t makeSettings()
{
  llhttp_settings_t settings;
  llhttp_settings_init(&settings);
  settings.on_message_begin = on_message_begin;
  settings.on_method = on_method;
  settings.on_url = on_url;
  settings.on_version = on_version;
  settings.on_header_field = on_header_field;
  settings.on_header_value = on_header_value;
  settings.on_header_value_complete = on_header_value_complete;
  settings.on_headers_complete = on_headers_complete;
  return settings;
}

const llhttp_settings_t kSettings = makeSettings();
}

void XrdHttp1Session::resetMessage()
{
  headersReady_ = false;
  haveRequestLine_ = false;
  method_.clear();
  url_.clear();
  httpMajor_ = 1;
  httpMinor_ = 1;
  headerField_.clear();
  headerValue_.clear();
  headers_.clear();
}

void XrdHttp1Session::appendMethod(const char *at, size_t length)
{
  method_.append(at, length);
}

void XrdHttp1Session::appendUrl(const char *at, size_t length)
{
  url_.append(at, length);
}

void XrdHttp1Session::appendVersion(const char *at, size_t length)
{
  if (length >= 8 && !strncmp(at, "HTTP/", 5) && at[5] >= '0' && at[5] <= '9' &&
      at[6] == '.' && at[7] >= '0' && at[7] <= '9') {
    httpMajor_ = at[5] - '0';
    httpMinor_ = at[7] - '0';
  }
  haveRequestLine_ = true;
}

void XrdHttp1Session::appendHeaderField(const char *at, size_t length)
{
  headerField_.append(at, length);
}

void XrdHttp1Session::appendHeaderValue(const char *at, size_t length)
{
  headerValue_.append(at, length);
}

void XrdHttp1Session::completeHeaderValue()
{
  if (!headerField_.empty()) {
    headers_.emplace_back(headerField_, headerValue_);
    headerField_.clear();
    headerValue_.clear();
  }
}

void XrdHttp1Session::markHeadersReady()
{
  headersReady_ = true;
}

XrdHttp1Session::XrdHttp1Session()
: sessionCtx_(new SessionCtx{this}),
  headersReady_(false),
  haveRequestLine_(false),
  httpMajor_(1),
  httpMinor_(1)
{
  llhttp_init(&parser_, HTTP_REQUEST, &kSettings);
  parser_.data = sessionCtx_;
}

XrdHttp1Session::~XrdHttp1Session()
{
  delete static_cast<SessionCtx *>(sessionCtx_);
  sessionCtx_ = nullptr;
}

void XrdHttp1Session::reset()
{
  resetMessage();
  llhttp_reset(&parser_);
  parser_.data = sessionCtx_;
}

static int applyLine(XrdHttpReq &req, bool firstLine, std::string &line)
{
  line.push_back('\0');
  if (firstLine)
    return req.parseFirstLine(line.data(), static_cast<int>(line.size()));
  return req.parseLine(line.data(), static_cast<int>(line.size()));
}

int XrdHttp1Session::applyTo(XrdHttpReq &req)
{
  if (!haveRequestLine_ || method_.empty() || url_.empty())
    return -1;

  std::string requestLine = method_ + " " + url_ + " HTTP/" +
                            std::to_string(httpMajor_) + "." +
                            std::to_string(httpMinor_) + "\r\n";
  if (applyLine(req, true, requestLine) < 0)
    return -1;

  for (const auto &hdr : headers_) {
    std::string line = hdr.first + ": " + hdr.second + "\r\n";
    if (applyLine(req, false, line) < 0)
      return -1;
  }

  req.headerok = true;
  return 0;
}

int XrdHttp1Session::parseHeaders(XrdHttpProtocol &prot, XrdHttpReq &req)
{
  if (prot.BuffUsed() == 0) {
    if (prot.getDataOneShot(prot.BuffAvailable()) < 0)
      return -1;
    if (prot.BuffUsed() == 0)
      return 1;
  }

  char *data = nullptr;
  const int avail = prot.BuffgetData(prot.BuffUsed(), &data, false);
  if (avail <= 0 || !data)
    return 1;

  const llhttp_errno_t err =
      llhttp_execute(&parser_, data, static_cast<size_t>(avail));

  if (err == HPE_PAUSED && headersReady_) {
    const char *pos = llhttp_get_error_pos(&parser_);
    size_t consumed = pos ? static_cast<size_t>(pos - data) : static_cast<size_t>(avail);
    if (consumed > static_cast<size_t>(avail))
      consumed = static_cast<size_t>(avail);
    prot.BuffConsume(static_cast<int>(consumed));
    llhttp_reset(&parser_);
    parser_.data = sessionCtx_;
    resetMessage();
    return applyTo(req);
  }

  if (err == HPE_OK)
    return 1;

  TRACE(ALL, " llhttp error "
        << llhttp_errno_name(err) << " "
        << (llhttp_get_error_reason(&parser_) ? llhttp_get_error_reason(&parser_) : ""));
  return -1;
}
