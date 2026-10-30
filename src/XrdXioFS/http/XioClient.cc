//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioClient.hh"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <functional>
#include <sstream>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef __linux__
#include <sys/sysmacros.h>
#endif

namespace XioFS {

std::string statusDetail(const char *what, int status, const std::string &body)
{
  std::string e = std::string(what) + " status " + std::to_string(status);
  std::string snip;
  for (unsigned char c : body) {
    if (c == '\r' || c == '\n' || c == '\t')
      c = ' ';
    if (c < 0x20 || c == 0x7f)
      continue;
    if (c == ' ' && (snip.empty() || snip.back() == ' '))
      continue;
    snip.push_back(static_cast<char>(c));
    if (snip.size() >= 160)
      break;
  }
  while (!snip.empty() && snip.back() == ' ')
    snip.pop_back();
  if (!snip.empty()) {
    e += ": ";
    e += snip;
  }
  return e;
}

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

// XrdHttp tags are "<dev:ino>-<ctime>[.ns]-<size>"; the part before the
// first '-' identifies the object across versions.
std::string etagIdentity(const std::string &etag)
{
  std::string id = etag;
  if (!id.empty() && id.front() == '"')
    id.erase(0, 1);
  if (!id.empty() && id.back() == '"')
    id.pop_back();
  auto dash = id.find('-');
  if (dash != std::string::npos)
    id.resize(dash);
  return id;
}

uint64_t makeIno(const std::string &path, const std::string &etag)
{
  // Only the identity prefix may feed the inode number, or every write
  // would change st_ino.
  return std::hash<std::string>{}(path + "\n" + etagIdentity(etag));
}

void addIfHeader(std::vector<std::pair<std::string, std::string>> &hdrs,
                 const char *name, const std::string &val)
{
  if (val.empty())
    return;
  std::string t = val;
  // Mutations assert identity, not version: a bare "<id>" matches any
  // version of the same object on XrdHttp, so a concurrent chmod or a
  // write from another client of ours does not turn into 412.
  if (t != "*" && !strcmp(name, "if-match"))
    t = etagIdentity(t);
  if (t.empty())
    return;
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
  for (auto &kv : data_) {
    if (kv.second.sess)
      kv.second.sess->close();
  }
  data_.clear();
  sticky_.clear();
  opt_ = std::move(opt);
  return sess_.connect(base_, opt_, err);
}

void Client::close()
{
  std::lock_guard<std::mutex> lock(mu_);
  sess_.close();
  for (auto &kv : data_) {
    if (kv.second.sess)
      kv.second.sess->close();
  }
  data_.clear();
  sticky_.clear();
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

namespace {

std::string fileKey(const std::string &abs)
{
  std::string k = abs.empty() ? "/" : abs;
  while (k.size() > 1 && k.back() == '/')
    k.pop_back();
  return k;
}

bool sameEndpoint(const Url &a, const Url &b)
{
  if (a.port != b.port || a.parsock != b.parsock || a.tls != b.tls)
    return false;
  return strcasecmp(a.host.c_str(), b.host.c_str()) == 0;
}

std::string endpointKey(const Url &u, const Http2Session::Options &opt)
{
  std::string k = u.parsock ? "spt://" : (u.tls ? "https://" : "http://");
  k += u.host;
  k += ":";
  k += std::to_string(u.port);
  if (u.parsock) {
    k += "/";
    k += opt.parsock_transport;
    k += "/";
    k += opt.parsock_protection;
  }
  return k;
}

bool isDataOperation(const char *method,
                     const std::vector<std::pair<std::string, std::string>> &hdrs)
{
  if (!method)
    return false;
  if (!strcasecmp(method, "HEAD") || !strcasecmp(method, "PUT") ||
      !strcasecmp(method, "PATCH"))
    return true;
  if (strcasecmp(method, "GET"))
    return false;
  for (const auto &h : hdrs) {
    if (!strcasecmp(h.first.c_str(), "xrd-readlink") ||
        !strcasecmp(h.first.c_str(), "xrd-xattr") ||
        !strcasecmp(h.first.c_str(), "xrd-xattr-list"))
      return false;
  }
  return true;
}

} // namespace

int Client::sessionFor(const Url &url, const Http2Session::Options &opt,
                       bool origin, Http2Session *&out, std::string &err)
{
  out = nullptr;
  if (origin || sameEndpoint(url, base_)) {
    int rc = ensure(err);
    if (rc)
      return rc;
    out = &sess_;
    return 0;
  }
  const std::string key = endpointKey(url, opt);
  DataLink &link = data_[key];
  if (!link.sess)
    link.sess = std::make_unique<Http2Session>();
  if (!link.sess->connected()) {
    link.url = url;
    link.opt = opt;
    int rc = link.sess->connect(url, opt, err);
    if (rc)
      return rc;
  }
  out = link.sess.get();
  return 0;
}

int Client::invoke(const char *method, const std::string &rel,
                   const std::vector<std::pair<std::string, std::string>> &hdrs,
                   const std::string &body, HttpResponse &resp, std::string &err,
                   const std::function<int(const uint8_t *, size_t)> &on_chunk)
{
  const std::string key = fileKey(absPath(rel));
  const bool data_op = isDataOperation(method, hdrs);

  HttpRequest req;
  req.method = method ? method : "GET";
  req.headers = hdrs;
  req.body = body;
  req.on_chunk = on_chunk;

  Url target;
  Http2Session::Options target_opt;
  bool origin = true;
  bool saw_sticky = false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    target = base_;
    target.path = key;
    target_opt = opt_;
    if (data_op) {
      auto it = sticky_.find(key);
      if (it != sticky_.end()) {
        target = it->second.data;
        target_opt = it->second.opt;
        origin = false;
      }
    }
  }
  req.path = target.path.empty() ? "/" : target.path;

  bool send_cred = true;
  for (int hop = 0; hop < 9; ++hop) {
    Http2Session *sess = nullptr;
    {
      std::lock_guard<std::mutex> lock(mu_);
      int src = sessionFor(target, target_opt, origin, sess, err);
      if (src)
        return src;
    }
    int rc = sess->exchange(req, resp, err, send_cred);
    if (rc) {
      std::lock_guard<std::mutex> lock(mu_);
      sess->close();
      const Url &reconnect_to = origin ? base_ : target;
      const Http2Session::Options &reconnect_opt = origin ? opt_ : target_opt;
      if (sess->connect(reconnect_to, reconnect_opt, err))
        return rc;
    }
    if (rc)
      rc = sess->exchange(req, resp, err, send_cred);
    if (rc)
      return rc;

    RedirectPlan plan;
    int prc = planRedirect(sess->peer(), target_opt, req, resp, plan, err);
    if (prc < 0)
      return prc;
    if (prc == 0) {
      if (saw_sticky) {
        std::lock_guard<std::mutex> lock(mu_);
        Affinity aff;
        aff.data = target;
        aff.data.path = req.path;
        aff.opt = target_opt;
        sticky_[key] = std::move(aff);
      }
      return 0;
    }
    if (hop == 8) {
      err = "too many HTTP redirects";
      return -ELOOP;
    }
    if (plan.sticky)
      saw_sticky = true;
    const auto chunk = req.on_chunk;
    req = std::move(plan.request);
    req.on_chunk = chunk;
    send_cred = plan.send_credentials;
    target = plan.url;
    target_opt = plan.opt;
    origin = sameEndpoint(target, base_);
  }
  err = "too many HTTP redirects";
  return -ELOOP;
}

int Client::doReq(const char *method, const std::string &rel,
                  const std::vector<std::pair<std::string, std::string>> &hdrs,
                  const std::string &body, HttpResponse &resp, std::string &err)
{
  return invoke(method, rel, hdrs, body, resp, err, {});
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

int Client::readdir(const std::string &relpath,
                    const std::function<void(DavEntry &&)> &on_entry,
                    std::string &err)
{
  std::string self = hrefBasename(absPath(relpath));
  std::string reqPath = absPath(relpath);
  while (!reqPath.empty() && reqPath.back() == '/')
    reqPath.pop_back();

  int delivered = 0;
  bool first = true;
  DavStreamParser parser([&](DavEntry &&e) {
    if (e.href.empty()) {
      delivered++;
      if (on_entry)
        on_entry(std::move(e));
      return;
    }
    std::string h = e.href;
    while (!h.empty() && h.back() == '/')
      h.pop_back();
    if (h == reqPath || (first && e.is_dir && e.name == self)) {
      first = false;
      return;
    }
    first = false;
    delivered++;
    if (on_entry)
      on_entry(std::move(e));
  });

  std::vector<std::pair<std::string, std::string>> hdrs{{"depth", "1"}};

  HttpResponse resp;
  int rc = doReq("PROPFIND", relpath, hdrs, {}, resp, err);
  if (rc)
    return rc;

  if (resp.status != 207 && resp.status != 200) {
    err = "PROPFIND status " + std::to_string(resp.status);
    return -httpToErrno(resp.status);
  }

  if (!parser.feed(resp.body.data(), resp.body.size(), err) ||
      !parser.finish(err))
    return -EIO;

  if (delivered == 0) {
    std::string snippet = resp.body.substr(0, 180);
    for (char &c : snippet) {
      if (c == '\n' || c == '\r')
        c = ' ';
    }
    err = snippet.empty() ? "empty PROPFIND body" : snippet;
  }

  return 0;
}

int Client::readdir(const std::string &relpath, std::vector<DavEntry> &out,
                    std::string &err)
{
  out.clear();
  return readdir(relpath, [&](DavEntry &&e) {
    out.push_back(std::move(e));
  }, err);
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

int Client::registerDeviceMem(void *dev_ptr, size_t len, int dmabuf_fd,
                              void **mem_handle, std::string &err)
{
  std::lock_guard<std::mutex> lock(mu_);
  int rc = ensure(err);
  if (rc)
    return rc;
  return sess_.registerDeviceMem(dev_ptr, len, dmabuf_fd, mem_handle, err);
}

int Client::deregisterDeviceMem(void *mem_handle, std::string &err)
{
  std::lock_guard<std::mutex> lock(mu_);
  return sess_.deregisterDeviceMem(mem_handle, err);
}

int Client::readDevice(const std::string &relpath, uint64_t offset, uint64_t length,
                       void *dev_ptr, void * /*mem_handle*/, size_t &bytes_read,
                       std::string &err)
{
  bytes_read = 0;
  if (length == 0)
    return 0;
  if (!dev_ptr) {
    err = "null dev_ptr";
    return -EINVAL;
  }

  std::string range = "bytes=" + std::to_string(offset) + "-" +
                      std::to_string(offset + length - 1);
  std::vector<std::pair<std::string, std::string>> hdrs{{"range", range}};

  uint8_t *dst = static_cast<uint8_t *>(dev_ptr);
  size_t written = 0;

  auto on_chunk = [&](const uint8_t *data, size_t len) -> int {
    if (written + len > length) {
      size_t avail = length - written;
      if (avail > 0) {
        memcpy(dst + written, data, avail);
        written += avail;
      }
      return 0;
    }
    memcpy(dst + written, data, len);
    written += len;
    return 0;
  };

  HttpResponse resp;
  int rc = invoke("GET", relpath, hdrs, {}, resp, err, on_chunk);
  if (rc)
    return rc;

  if (resp.status == 416) {
    bytes_read = 0;
    return 0;
  }
  if (resp.status != 206 && resp.status != 200) {
    err = "GET status " + std::to_string(resp.status);
    return -httpToErrno(resp.status);
  }

  bytes_read = written;
  return 0;
}

int Client::writeDevice(const std::string &relpath, uint64_t offset,
                        const void *dev_ptr, void * /*mem_handle*/, size_t length,
                        std::string &err, const std::string &if_match)
{
  if (length == 0)
    return 0;
  if (!dev_ptr) {
    err = "null dev_ptr";
    return -EINVAL;
  }
  std::string data(static_cast<const char *>(dev_ptr), length);
  return write(relpath, offset, data, err, if_match);
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
    err = statusDetail("PROPPATCH", resp.status, resp.body);
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
    err = statusDetail("MKNOD", resp.status, resp.body);
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
                 long long start, long long len, std::string &err,
                 std::string *body_out)
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
    err = statusDetail("LOCK", resp.status, resp.body);
    return -e;
  }
  if (body_out)
    *body_out = resp.body;
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
    err = statusDetail("FLOCK", resp.status, resp.body);
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
