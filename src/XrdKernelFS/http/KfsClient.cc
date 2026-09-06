//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "KfsClient.hh"

#include <cerrno>
#include <functional>

namespace Kfs {

int httpToErrno(int status)
{
  switch (status) {
    case 200:
    case 201:
    case 204:
    case 206:
    case 207:
      return 0;
    case 401:
    case 403:
      return EACCES;
    case 404:
      return ENOENT;
    case 405:
      return EPERM;
    case 409:
      return EEXIST;
    case 412:
      return ESTALE;
    case 416:
      return 0;
    case 507:
      return ENOSPC;
    default:
      if (status >= 500)
        return EIO;
      return EPROTO;
  }
}

namespace {

uint64_t makeIno(const std::string &path, const std::string &etag)
{
  return std::hash<std::string>{}(path + "\n" + etag);
}

} // namespace

int Client::open(const std::string &url, Http2Session::Options opt, std::string &err)
{
  std::lock_guard<std::mutex> lock(mu_);
  if (!parseUrl(url, base_, err))
    return -EINVAL;
  opt_ = std::move(opt);
  return sess_.connect(base_, opt_, err);
}

void Client::close()
{
  std::lock_guard<std::mutex> lock(mu_);
  sess_.close();
}

std::string Client::absPath(const std::string &rel) const
{
  return joinPath(base_.path, rel);
}

int Client::ensure(std::string &err)
{
  if (sess_.connected())
    return 0;
  return sess_.connect(base_, opt_, err);
}

int Client::doReq(const char *method, const std::string &rel,
                  const std::vector<std::pair<std::string, std::string>> &hdrs,
                  const std::string &body, HttpResponse &resp, std::string &err)
{
  {
    std::lock_guard<std::mutex> lock(mu_);
    int rc = ensure(err);
    if (rc)
      return rc;
  }
  HttpRequest req;
  req.method = method;
  req.path = absPath(rel);
  req.headers = hdrs;
  req.body = body;
  int rc = sess_.request(req, resp, err);
  if (rc) {
    std::lock_guard<std::mutex> lock(mu_);
    sess_.close();
    int rc2 = sess_.connect(base_, opt_, err);
    if (rc2)
      return rc2;
  } else {
    return 0;
  }
  return sess_.request(req, resp, err);
}

int Client::getattr(const std::string &relpath, Attr &out, std::string &err)
{
  HttpResponse resp;
  int rc = doReq("PROPFIND", relpath, {{"depth", "0"}}, {}, resp, err);
  if (rc)
    return rc;
  std::vector<DavEntry> ents;
  if (resp.status != 207 && resp.status != 200) {
    ents.clear();
  } else if (!parseMultistatus(resp.body, ents, err)) {
    ents.clear();
  }
  if (ents.empty()) {
    // Some origins answer HEAD more reliably than PROPFIND for files.
    HttpResponse head;
    rc = doReq("HEAD", relpath, {}, {}, head, err);
    if (rc)
      return rc;
    if (head.status != 200) {
      err = "HEAD status " + std::to_string(head.status);
      return -httpToErrno(head.status);
    }
    out = Attr{};
    out.path = absPath(relpath);
    auto cl = head.header("content-length");
    out.size = cl.empty() ? 0 : std::stoll(cl);
    out.etag = head.header("etag");
    out.ino = makeIno(out.path, out.etag);
    auto lm = head.header("last-modified");
    if (!lm.empty())
      parseHttpDate(lm, out.mtime);
    return 0;
  }

  const DavEntry &e = ents.front();
  out = Attr{};
  out.path = absPath(relpath);
  out.size = e.size < 0 ? 0 : e.size;
  out.mtime = e.mtime;
  out.is_dir = e.is_dir;
  out.etag = resp.header("etag");
  if (out.etag.empty())
    out.etag = e.href;
  out.ino = makeIno(out.path, out.etag);
  return 0;
}

int Client::readdir(const std::string &relpath, std::vector<DavEntry> &out,
                    std::string &err)
{
  HttpResponse resp;
  int rc = doReq("PROPFIND", relpath, {{"depth", "1"}}, {}, resp, err);
  if (rc)
    return rc;
  if (resp.status != 207 && resp.status != 200) {
    err = "PROPFIND status " + std::to_string(resp.status);
    return -httpToErrno(resp.status);
  }
  if (!parseMultistatus(resp.body, out, err))
    return -EIO;

  // Drop the collection itself (first href matching the request path).
  std::string self = hrefBasename(absPath(relpath));
  auto it = out.begin();
  while (it != out.end()) {
    if (it->href.empty()) {
      ++it;
      continue;
    }
    std::string h = it->href;
    while (!h.empty() && h.back() == '/')
      h.pop_back();
    std::string req = absPath(relpath);
    while (!req.empty() && req.back() == '/')
      req.pop_back();
    if (h == req || (it == out.begin() && it->is_dir && it->name == self))
      it = out.erase(it);
    else
      ++it;
  }
  return 0;
}

int Client::read(const std::string &relpath, uint64_t offset, uint64_t length,
                 std::string &out, std::string &err)
{
  if (length == 0) {
    out.clear();
    return 0;
  }
  HttpResponse resp;
  std::string range = "bytes=" + std::to_string(offset) + "-" +
                      std::to_string(offset + length - 1);
  int rc = doReq("GET", relpath, {{"range", range}}, {}, resp, err);
  if (rc)
    return rc;
  if (resp.status == 416) {
    out.clear();
    return 0;
  }
  if (resp.status != 206 && resp.status != 200) {
    err = "GET status " + std::to_string(resp.status);
    return -httpToErrno(resp.status);
  }
  if (resp.status == 200 && offset > 0) {
    if (offset >= resp.body.size()) {
      out.clear();
      return 0;
    }
    out = resp.body.substr(static_cast<size_t>(offset),
                           static_cast<size_t>(length));
    return 0;
  }
  out = std::move(resp.body);
  if (out.size() > length)
    out.resize(static_cast<size_t>(length));
  return 0;
}

int Client::put(const std::string &relpath, const std::string &body,
                std::string &err)
{
  HttpResponse resp;
  int rc = doReq("PUT", relpath, {{"content-type", "application/octet-stream"}},
                 body, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "PUT status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::write(const std::string &relpath, uint64_t offset,
                  const std::string &data, std::string &err)
{
  if (data.empty())
    return 0;
  const uint64_t last = offset + data.size() - 1;
  const std::string cr = "bytes " + std::to_string(offset) + "-" +
                         std::to_string(last) + "/*";
  HttpResponse resp;
  int rc = doReq("PATCH", relpath,
                 {{"content-type", "application/octet-stream"},
                  {"content-range", cr}},
                 data, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "PATCH status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::mkdir(const std::string &relpath, std::string &err)
{
  HttpResponse resp;
  int rc = doReq("MKCOL", relpath, {}, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "MKCOL status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::unlink(const std::string &relpath, std::string &err)
{
  HttpResponse resp;
  int rc = doReq("DELETE", relpath, {}, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "DELETE status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::rename(const std::string &from, const std::string &to,
                   std::string &err)
{
  HttpResponse resp;
  std::string dest = (base_.tls ? "https://" : "http://") + base_.authority +
                     absPath(to);
  int rc = doReq("MOVE", from, {{"destination", dest}}, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "MOVE status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

} // namespace Kfs
