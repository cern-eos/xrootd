//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XrdClXioFile.hh"
#include "XrdClXioUtil.hh"

#include "XioDav.hh"
#include "XioHttp2.hh"
#include "XioUrl.hh"

#include <XrdCl/XrdClBuffer.hh>

#include <cerrno>
#include <cstring>
#include <ctime>
#include <functional>
#include <vector>

namespace XrdClXio {

namespace {

bool success(int rc, int http)
{
  return rc == 0 && http >= 200 && http < 300;
}

void putEmpty(const std::shared_ptr<XioFS::Http2Session> &sess,
              const std::string &path, time_t timeout,
              XrdCl::ResponseHandler *handler,
              const std::function<void()> &done)
{
  XioFS::HttpRequest put;
  put.method = "PUT";
  put.path = path;
  put.content_length = 0;
  applyTimeout(put, timeout);
  int rc = sess->start(std::move(put), [=](int prc, XioFS::HttpResponse resp,
                                           std::string err) {
    if (!success(prc, resp.status)) {
      deliver(handler, statusFrom(prc, resp.status, err), nullptr);
      return;
    }
    done();
  });
  if (rc)
    deliver(handler, statusFrom(rc, 0, "HTTP/2 request was not queued"), nullptr);
}

} // namespace

File::File() : st_(std::make_shared<State>()) {}

File::~File() = default;

bool File::snapshot(std::shared_ptr<XioFS::Http2Session> &sess,
                    std::string &path) const
{
  std::lock_guard<std::mutex> lock(st_->mu);
  if (!st_->open || !st_->sess)
    return false;
  sess = st_->sess;
  path = st_->path;
  return true;
}

bool File::IsOpen() const
{
  std::lock_guard<std::mutex> lock(st_->mu);
  return st_->open;
}

bool File::SetProperty(const std::string &name, const std::string &value)
{
  std::lock_guard<std::mutex> lock(st_->mu);
  st_->props[name] = value;
  return true;
}

bool File::GetProperty(const std::string &name, std::string &value) const
{
  std::lock_guard<std::mutex> lock(st_->mu);
  auto it = st_->props.find(name);
  if (it == st_->props.end())
    return false;
  value = it->second;
  return true;
}

XrdCl::XRootDStatus File::Open(const std::string &url,
                               XrdCl::OpenFlags::Flags flags,
                               XrdCl::Access::Mode, XrdCl::ResponseHandler *handler,
                               time_t timeout)
{
  XioFS::Url endpoint;
  std::string path;
  std::string err;
  if (!parseTarget(url, endpoint, path, err))
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidArgs, 0, err);

  auto sess = connectTarget(endpoint, timeout, err);
  if (!sess)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errConnectionError, 0, err);

  {
    std::lock_guard<std::mutex> lock(st_->mu);
    st_->open = false;
    st_->url = endpoint;
    st_->path = path;
    st_->sess = sess;
  }

  const bool create = (flags & XrdCl::OpenFlags::New) ||
                      (flags & XrdCl::OpenFlags::Delete);
  const bool truncate = (flags & XrdCl::OpenFlags::Delete) != 0;
  auto state = st_;

  auto opened = [=] {
    {
      std::lock_guard<std::mutex> lock(state->mu);
      state->open = true;
      state->props["LastURL"] = XioFS::formatUrl(sess->peer());
    }
    deliver(handler, XrdCl::XRootDStatus(), nullptr);
  };

  XioFS::HttpRequest req;
  req.method = "PROPFIND";
  req.path = path;
  req.headers.push_back({"depth", "0"});
  applyTimeout(req, timeout);

  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string e) {
    const bool missing = prc == 0 && resp.status == 404;
    if (prc == 0 && (resp.status == 207 || resp.status == 200)) {
      std::vector<XioFS::DavEntry> ents;
      std::string perr;
      if (!XioFS::parseMultistatus(resp.body, ents, perr) || ents.empty()) {
        deliver(handler,
                XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidResponse, 0,
                                    perr.empty() ? "PROPFIND parse failed" : perr),
                nullptr);
        return;
      }
      if (ents[0].is_dir) {
        deliver(handler,
                XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errErrorResponse, 0,
                                    "Cannot open a directory"),
                nullptr);
        return;
      }
      if (truncate) {
        putEmpty(sess, path, timeout, handler, opened);
        return;
      }
      opened();
      return;
    }
    if (missing) {
      if (!create) {
        deliver(handler, statusFrom(0, 404, "not found"), nullptr);
        return;
      }
      putEmpty(sess, path, timeout, handler, opened);
      return;
    }
    deliver(handler, statusFrom(prc, resp.status, e), nullptr);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus File::Close(XrdCl::ResponseHandler *handler, time_t)
{
  {
    std::lock_guard<std::mutex> lock(st_->mu);
    if (!st_->open)
      return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidOp, 0,
                                 "file is not open");
    st_->open = false;
    st_->sess.reset();
  }
  deliver(handler, XrdCl::XRootDStatus(), nullptr);
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus File::Stat(bool, XrdCl::ResponseHandler *handler,
                               time_t timeout)
{
  std::shared_ptr<XioFS::Http2Session> sess;
  std::string path;
  if (!snapshot(sess, path))
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidOp, 0,
                               "file is not open");

  XioFS::HttpRequest req;
  req.method = "PROPFIND";
  req.path = path;
  req.headers.push_back({"depth", "0"});
  applyTimeout(req, timeout);
  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string err) {
    if (!success(prc, resp.status)) {
      deliver(handler, statusFrom(prc, resp.status, err), nullptr);
      return;
    }
    std::vector<XioFS::DavEntry> ents;
    std::string perr;
    if (!XioFS::parseMultistatus(resp.body, ents, perr) || ents.empty()) {
      deliver(handler,
              XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidResponse, 0,
                                  perr.empty() ? "PROPFIND parse failed" : perr),
              nullptr);
      return;
    }
    const XioFS::DavEntry &e = ents[0];
    uint32_t flags = XrdCl::StatInfo::Flags::IsReadable;
    if (e.is_dir)
      flags |= XrdCl::StatInfo::Flags::IsDir;
    uint64_t sz = e.size < 0 ? 0 : static_cast<uint64_t>(e.size);
    auto *info = new XrdCl::StatInfo(
        "nobody", sz, flags, e.mtime ? e.mtime : std::time(nullptr));
    auto *obj = new XrdCl::AnyObject();
    obj->Set(info);
    deliver(handler, XrdCl::XRootDStatus(), obj);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus File::Read(uint64_t offset, uint32_t size, void *buffer,
                               XrdCl::ResponseHandler *handler, time_t timeout)
{
  std::shared_ptr<XioFS::Http2Session> sess;
  std::string path;
  if (!snapshot(sess, path))
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidOp, 0,
                               "file is not open");
  if (!buffer && size)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidArgs, 0,
                               "null read buffer");

  auto sink = std::make_shared<size_t>(0);
  XioFS::HttpRequest req;
  req.method = "GET";
  req.path = path;
  if (size > 0) {
    req.headers.push_back(
        {"range", "bytes=" + std::to_string(offset) + "-" +
                      std::to_string(offset + size - 1)});
  }
  applyTimeout(req, timeout);
  req.on_chunk = [buffer, size, sink](const uint8_t *data, size_t len) -> int {
    if (*sink + len > size)
      return -EPROTO;
    if (len && buffer)
      memcpy(static_cast<uint8_t *>(buffer) + *sink, data, len);
    *sink += len;
    return 0;
  };

  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string err) {
    if (prc || (resp.status != 200 && resp.status != 206 && resp.status != 204)) {
      deliver(handler, statusFrom(prc, resp.status, err), nullptr);
      return;
    }
    auto *info = new XrdCl::ChunkInfo(offset, static_cast<uint32_t>(*sink), buffer);
    auto *obj = new XrdCl::AnyObject();
    obj->Set(info);
    deliver(handler, XrdCl::XRootDStatus(), obj);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus File::VectorRead(const XrdCl::ChunkList &chunks, void *buffer,
                                     XrdCl::ResponseHandler *handler,
                                     time_t timeout)
{
  std::shared_ptr<XioFS::Http2Session> sess;
  std::string path;
  if (!snapshot(sess, path))
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidOp, 0,
                               "file is not open");

  struct Part {
    uint64_t offset{0};
    uint32_t length{0};
    uint8_t *dest{nullptr};
    size_t got{0};
  };
  auto parts = std::make_shared<std::vector<Part>>();
  uint64_t cursor = 0;
  for (const auto &c : chunks) {
    if (!c.GetLength())
      continue;
    Part p;
    p.offset = c.GetOffset();
    p.length = c.GetLength();
    if (c.buffer)
      p.dest = static_cast<uint8_t *>(c.buffer);
    else if (buffer)
      p.dest = static_cast<uint8_t *>(buffer) + cursor;
    else
      return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidArgs, 0,
                                 "vector read is missing a buffer");
    cursor += c.GetLength();
    parts->push_back(p);
  }

  if (parts->empty()) {
    auto *info = new XrdCl::VectorReadInfo();
    auto *obj = new XrdCl::AnyObject();
    obj->Set(info);
    deliver(handler, XrdCl::XRootDStatus(), obj);
    return XrdCl::XRootDStatus();
  }

  struct Gate {
    std::mutex mu;
    size_t left{0};
    bool failed{false};
    XrdCl::XRootDStatus status;
  };
  auto gate = std::make_shared<Gate>();
  gate->left = parts->size();

  auto finish = [=] {
    std::lock_guard<std::mutex> lock(gate->mu);
    if (gate->failed) {
      deliver(handler, gate->status, nullptr);
      return;
    }
    auto *info = new XrdCl::VectorReadInfo();
    uint32_t total = 0;
    for (const auto &p : *parts) {
      info->GetChunks().emplace_back(p.offset, static_cast<uint32_t>(p.got),
                                     p.dest);
      total += static_cast<uint32_t>(p.got);
    }
    info->SetSize(total);
    auto *obj = new XrdCl::AnyObject();
    obj->Set(info);
    deliver(handler, XrdCl::XRootDStatus(), obj);
  };

  for (size_t i = 0; i < parts->size(); ++i) {
    XioFS::HttpRequest req;
    req.method = "GET";
    req.path = path;
    req.headers.push_back(
        {"range", "bytes=" + std::to_string((*parts)[i].offset) + "-" +
                      std::to_string((*parts)[i].offset + (*parts)[i].length - 1)});
    applyTimeout(req, timeout);
    req.on_chunk = [parts, i](const uint8_t *data, size_t len) -> int {
      Part &p = (*parts)[i];
      if (p.got + len > p.length)
        return -EPROTO;
      if (len && p.dest)
        memcpy(p.dest + p.got, data, len);
      p.got += len;
      return 0;
    };
    int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                             std::string err) {
      bool last = false;
      {
        std::lock_guard<std::mutex> lock(gate->mu);
        if (!gate->failed &&
            (prc || (resp.status != 200 && resp.status != 206 && resp.status != 204))) {
          gate->failed = true;
          gate->status = statusFrom(prc, resp.status, err);
        }
        last = --gate->left == 0;
      }
      if (last)
        finish();
    });
    if (rc) {
      bool last = false;
      {
        std::lock_guard<std::mutex> lock(gate->mu);
        if (!gate->failed) {
          gate->failed = true;
          gate->status = statusFrom(rc, 0, "HTTP/2 request was not queued");
        }
        last = --gate->left == 0;
      }
      if (last)
        finish();
    }
  }
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus File::Write(uint64_t offset, uint32_t size, const void *buffer,
                                XrdCl::ResponseHandler *handler, time_t timeout)
{
  std::shared_ptr<XioFS::Http2Session> sess;
  std::string path;
  if (!snapshot(sess, path))
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidOp, 0,
                               "file is not open");
  if (!buffer && size)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidArgs, 0,
                               "null write buffer");
  if (size == 0) {
    deliver(handler, XrdCl::XRootDStatus(), nullptr);
    return XrdCl::XRootDStatus();
  }

  XioFS::HttpRequest req;
  req.method = "PATCH";
  req.path = path;
  req.content_length = size;
  req.headers.push_back({"content-type", "application/octet-stream"});
  req.headers.push_back(
      {"content-range", "bytes " + std::to_string(offset) + "-" +
                            std::to_string(offset + size - 1) + "/*"});
  applyTimeout(req, timeout);
  req.pull = [buffer, size](uint64_t off, uint8_t *dst, size_t n) -> ssize_t {
    if (off >= size)
      return 0;
    size_t c = n;
    if (c > size - static_cast<size_t>(off))
      c = size - static_cast<size_t>(off);
    memcpy(dst, static_cast<const uint8_t *>(buffer) + off, c);
    return static_cast<ssize_t>(c);
  };

  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string err) {
    deliver(handler, statusFrom(prc, resp.status, err), nullptr);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus File::Write(uint64_t offset, XrdCl::Buffer &&buffer,
                                XrdCl::ResponseHandler *handler, time_t timeout)
{
  auto owned = std::make_shared<XrdCl::Buffer>(std::move(buffer));
  std::shared_ptr<XioFS::Http2Session> sess;
  std::string path;
  if (!snapshot(sess, path))
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidOp, 0,
                               "file is not open");
  const uint32_t size = owned->GetSize();
  if (size == 0) {
    deliver(handler, XrdCl::XRootDStatus(), nullptr);
    return XrdCl::XRootDStatus();
  }
  XioFS::HttpRequest req;
  req.method = "PATCH";
  req.path = path;
  req.content_length = size;
  req.headers.push_back({"content-type", "application/octet-stream"});
  req.headers.push_back(
      {"content-range", "bytes " + std::to_string(offset) + "-" +
                            std::to_string(offset + size - 1) + "/*"});
  applyTimeout(req, timeout);
  req.pull = [owned](uint64_t off, uint8_t *dst, size_t n) -> ssize_t {
    const uint32_t size = owned->GetSize();
    if (off >= size)
      return 0;
    size_t c = n;
    if (c > size - static_cast<size_t>(off))
      c = size - static_cast<size_t>(off);
    memcpy(dst, owned->GetBuffer() + off, c);
    return static_cast<ssize_t>(c);
  };
  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string err) {
    deliver(handler, statusFrom(prc, resp.status, err), nullptr);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

} // namespace XrdClXio
