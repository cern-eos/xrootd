//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioClient.hh"

#include <cerrno>
#include <cstdio>
#include <ctime>
#include <functional>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef __linux__
#include <sys/sysmacros.h>
#endif

namespace XioFS {

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
    case 423:
      return EAGAIN;
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

void addIfHeader(std::vector<std::pair<std::string, std::string>> &hdrs,
                 const char *name, const std::string &val)
{
  if (val.empty())
    return;
  std::string t = val;
  if (t != "*" && t.front() != '"')
    t = "\"" + t + "\"";
  hdrs.emplace_back(name, std::move(t));
}

void attrFromDav(const DavEntry &e, Attr &out)
{
  out.size = e.size < 0 ? 0 : e.size;
  out.mtime = e.mtime;
  out.atime = e.atime ? e.atime : e.mtime;
  out.mode = e.mode;
  out.uid = e.uid;
  out.gid = e.gid;
  out.is_dir = e.is_dir;
  out.is_lnk = e.is_lnk && !e.is_dir;
  out.is_fifo = e.is_fifo;
  out.is_chr = e.is_chr;
  out.is_blk = e.is_blk;
  out.rdev = e.rdev;
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
    if (!lm.empty()) {
      parseHttpDate(lm, out.mtime);
      out.atime = out.mtime;
    }
    return 0;
  }

  const DavEntry &e = ents.front();
  out = Attr{};
  out.path = absPath(relpath);
  attrFromDav(e, out);
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
                std::string &err, const std::string &if_match,
                const std::string &if_none_match)
{
  std::vector<std::pair<std::string, std::string>> hdrs{
      {"content-type", "application/octet-stream"}};
  addIfHeader(hdrs, "if-match", if_match);
  addIfHeader(hdrs, "if-none-match", if_none_match);
  HttpResponse resp;
  int rc = doReq("PUT", relpath, hdrs, body, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "PUT status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::write(const std::string &relpath, uint64_t offset,
                  const std::string &data, std::string &err,
                  const std::string &if_match, std::string *etag_out)
{
  if (data.empty())
    return 0;
  const uint64_t last = offset + data.size() - 1;
  const std::string cr = "bytes " + std::to_string(offset) + "-" +
                         std::to_string(last) + "/*";
  std::vector<std::pair<std::string, std::string>> hdrs{
      {"content-type", "application/octet-stream"},
      {"content-range", cr}};
  addIfHeader(hdrs, "if-match", if_match);
  HttpResponse resp;
  int rc = doReq("PATCH", relpath, hdrs, data, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "PATCH status " + std::to_string(resp.status);
    return -e;
  }
  if (etag_out)
    *etag_out = resp.header("etag");
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

int Client::unlink(const std::string &relpath, std::string &err,
                   const std::string &if_match)
{
  std::vector<std::pair<std::string, std::string>> hdrs;
  addIfHeader(hdrs, "if-match", if_match);
  HttpResponse resp;
  int rc = doReq("DELETE", relpath, hdrs, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "DELETE status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::rename(const std::string &from, const std::string &to,
                   std::string &err, const std::string &if_match)
{
  HttpResponse resp;
  std::string dest = (base_.tls ? "https://" : "http://") + base_.authority +
                     absPath(to);
  std::vector<std::pair<std::string, std::string>> hdrs{{"destination", dest}};
  addIfHeader(hdrs, "if-match", if_match);
  int rc = doReq("MOVE", from, hdrs, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "MOVE status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::chmod(const std::string &relpath, mode_t mode, std::string &err)
{
  char body[512];
  std::snprintf(body, sizeof(body),
                "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
                "<D:set><D:prop><X:mode>%o</X:mode></D:prop></D:set>"
                "</D:propertyupdate>",
                static_cast<unsigned>(mode & 07777));
  return proppatch(relpath, body, err);
}

int Client::proppatch(const std::string &relpath, const std::string &body,
                      std::string &err)
{
  HttpResponse resp;
  int rc = doReq("PROPPATCH", relpath,
                 {{"content-type", "application/xml; charset=\"utf-8\""}},
                 body, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "PROPPATCH status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::chown(const std::string &relpath, uid_t uid, gid_t gid,
                  std::string &err)
{
  if (uid == static_cast<uid_t>(-1) && gid == static_cast<gid_t>(-1))
    return 0;
  std::ostringstream body;
  body << "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
          "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
          "<D:set><D:prop>";
  if (uid != static_cast<uid_t>(-1))
    body << "<X:uid>" << static_cast<unsigned>(uid) << "</X:uid>";
  if (gid != static_cast<gid_t>(-1))
    body << "<X:gid>" << static_cast<unsigned>(gid) << "</X:gid>";
  body << "</D:prop></D:set></D:propertyupdate>";
  return proppatch(relpath, body.str(), err);
}

int Client::utimens(const std::string &relpath, const struct timespec tv[2],
                    std::string &err)
{
#ifndef UTIME_NOW
#define UTIME_NOW  ((1l << 30) - 1l)
#define UTIME_OMIT ((1l << 30) - 2l)
#endif
  long long at = -1;
  long long mt = -1;
  if (tv) {
    if (tv[0].tv_nsec != UTIME_OMIT) {
      if (tv[0].tv_nsec == UTIME_NOW)
        at = static_cast<long long>(time(nullptr));
      else
        at = static_cast<long long>(tv[0].tv_sec);
    }
    if (tv[1].tv_nsec != UTIME_OMIT) {
      if (tv[1].tv_nsec == UTIME_NOW)
        mt = static_cast<long long>(time(nullptr));
      else
        mt = static_cast<long long>(tv[1].tv_sec);
    }
  } else {
    at = mt = static_cast<long long>(time(nullptr));
  }
  if (at < 0 && mt < 0)
    return 0;
  std::ostringstream body;
  body << "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
          "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
          "<D:set><D:prop>";
  if (at >= 0)
    body << "<X:atime>" << at << "</X:atime>";
  if (mt >= 0)
    body << "<X:mtime>" << mt << "</X:mtime>";
  body << "</D:prop></D:set></D:propertyupdate>";
  return proppatch(relpath, body.str(), err);
}

int Client::link(const std::string &from, const std::string &to, std::string &err)
{
  HttpResponse resp;
  std::string dest = (base_.tls ? "https://" : "http://") + base_.authority +
                     absPath(to);
  int rc = doReq("LINK", from, {{"destination", dest}}, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "LINK status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::symlink(const std::string &linkpath, const std::string &target,
                    std::string &err)
{
  HttpResponse resp;
  std::vector<std::pair<std::string, std::string>> hdrs{
      {"xrd-symlink-target", target},
      {"destination", target}};
  int rc = doReq("SYMLINK", linkpath, hdrs, target, resp, err);
  if (rc)
    return rc;
  if (resp.status == 405 || resp.status == 501) {
    hdrs.emplace_back("xrd-link-type", "symbolic");
    rc = doReq("LINK", linkpath, hdrs, {}, resp, err);
    if (rc)
      return rc;
  }
  if (int e = httpToErrno(resp.status)) {
    err = "SYMLINK status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::readlink(const std::string &relpath, std::string &target,
                     std::string &err)
{
  HttpResponse resp;
  int rc = doReq("READLINK", relpath, {}, {}, resp, err);
  if (rc)
    return rc;
  if (resp.status == 405 || resp.status == 501) {
    rc = doReq("GET", relpath, {{"xrd-readlink", "1"}}, {}, resp, err);
    if (rc)
      return rc;
  }
  if (int e = httpToErrno(resp.status)) {
    err = "READLINK status " + std::to_string(resp.status);
    return -e;
  }
  target = resp.body;
  while (!target.empty() &&
         (target.back() == '\n' || target.back() == '\r' ||
          target.back() == '\0'))
    target.pop_back();
  return 0;
}

int Client::mknod(const std::string &relpath, mode_t mode, dev_t rdev,
                  std::string &err)
{
  char modebuf[16];
  std::snprintf(modebuf, sizeof(modebuf), "%o", static_cast<unsigned>(mode));
  char devbuf[32];
  if (S_ISCHR(mode) || S_ISBLK(mode))
    std::snprintf(devbuf, sizeof(devbuf), "%u:%u",
                  static_cast<unsigned>(major(rdev)),
                  static_cast<unsigned>(minor(rdev)));
  else
    std::snprintf(devbuf, sizeof(devbuf), "%llu",
                  static_cast<unsigned long long>(rdev));
  std::vector<std::pair<std::string, std::string>> hdrs{
      {"xrd-mode", modebuf}, {"xrd-dev", devbuf}};
  HttpResponse resp;
  int rc = doReq("MKNOD", relpath, hdrs, {}, resp, err);
  if (rc)
    return rc;
  if (resp.status == 405 || resp.status == 501) {
    hdrs.emplace_back("xrd-mknod", "1");
    rc = doReq("PUT", relpath, hdrs, {}, resp, err);
    if (rc)
      return rc;
  }
  if (int e = httpToErrno(resp.status)) {
    err = "MKNOD status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::getxattr(const std::string &relpath, const std::string &name,
                     std::string &value, std::string &err)
{
  HttpResponse resp;
  int rc = doReq("GET", relpath, {{"xrd-xattr", name}}, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "GET xattr status " + std::to_string(resp.status);
    return -e;
  }
  value = resp.body;
  return 0;
}

int Client::setxattr(const std::string &relpath, const std::string &name,
                     const std::string &value, std::string &err)
{
  std::ostringstream body;
  body << "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
          "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
          "<D:set><D:prop>"
          "<X:xattr-name>"
       << name << "</X:xattr-name><X:xattr-value>" << value
       << "</X:xattr-value></D:prop></D:set></D:propertyupdate>";
  return proppatch(relpath, body.str(), err);
}

int Client::listxattr(const std::string &relpath, std::string &names,
                      std::string &err)
{
  HttpResponse resp;
  int rc = doReq("GET", relpath, {{"xrd-xattr-list", "1"}}, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "GET xattr-list status " + std::to_string(resp.status);
    return -e;
  }
  names = resp.body;
  return 0;
}

int Client::removexattr(const std::string &relpath, const std::string &name,
                        std::string &err)
{
  std::ostringstream body;
  body << "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
          "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
          "<D:set><D:prop><X:xattr-del>"
       << name << "</X:xattr-del></D:prop></D:set></D:propertyupdate>";
  return proppatch(relpath, body.str(), err);
}

int Client::lock(const std::string &relpath, const std::string &cmd,
                 const std::string &type, const std::string &whence,
                 long long start, long long len, std::string &err)
{
  std::vector<std::pair<std::string, std::string>> hdrs{
      {"xrd-lock-cmd", cmd.empty() ? "SETLK" : cmd},
      {"xrd-lock-type", type.empty() ? "WRLCK" : type},
      {"xrd-lock-whence", whence.empty() ? "SET" : whence},
      {"xrd-lock-start", std::to_string(start)},
      {"xrd-lock-len", std::to_string(len)}};
  HttpResponse resp;
  int rc = doReq("LOCK", relpath, hdrs, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "LOCK status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::flock(const std::string &relpath, const std::string &op,
                  std::string &err)
{
  HttpResponse resp;
  int rc = doReq("LOCK", relpath,
                 {{"xrd-lock-cmd", "FLOCK"}, {"xrd-lock-op", op}}, {}, resp,
                 err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "FLOCK status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

int Client::unlock(const std::string &relpath, std::string &err)
{
  HttpResponse resp;
  int rc = doReq("UNLOCK", relpath, {}, {}, resp, err);
  if (rc)
    return rc;
  if (int e = httpToErrno(resp.status)) {
    err = "UNLOCK status " + std::to_string(resp.status);
    return -e;
  }
  return 0;
}

} // namespace XioFS
