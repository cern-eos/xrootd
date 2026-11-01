//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XrdClXioFilesystem.hh"
#include "XrdClXioUtil.hh"

#include "XioDav.hh"
#include "XioHttp2.hh"

#include <XrdCl/XrdClBuffer.hh>

#include <cctype>
#include <ctime>
#include <memory>
#include <string>
#include <vector>

namespace XrdClXio {

namespace {

std::string httpDigestName(std::string type)
{
  for (char &c : type)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (type == "sha256" || type == "sha-256")
    return "sha-256";
  if (type.empty())
    return "crc32c";
  return type;
}

std::string xrootDigestName(std::string type)
{
  for (char &c : type)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (type == "sha-256")
    return "sha256";
  return type;
}

int b64(char c)
{
  if (c >= 'A' && c <= 'Z')
    return c - 'A';
  if (c >= 'a' && c <= 'z')
    return c - 'a' + 26;
  if (c >= '0' && c <= '9')
    return c - '0' + 52;
  if (c == '+')
    return 62;
  if (c == '/')
    return 63;
  return -1;
}

std::string toHex(const uint8_t *p, size_t n)
{
  static const char *hexd = "0123456789abcdef";
  std::string out(n * 2, '0');
  for (size_t i = 0; i < n; ++i) {
    out[2 * i] = hexd[p[i] >> 4];
    out[2 * i + 1] = hexd[p[i] & 0xf];
  }
  return out;
}

bool isHex(const std::string &s)
{
  if (s.empty() || (s.size() % 2) != 0)
    return false;
  for (unsigned char c : s) {
    if (!std::isxdigit(c))
      return false;
  }
  return true;
}

std::string decodeDigest(std::string value)
{
  while (!value.empty() && (value.front() == ' ' || value.front() == ':'))
    value.erase(value.begin());
  while (!value.empty() && (value.back() == ' ' || value.back() == ':' ||
                            value.back() == ','))
    value.pop_back();
  auto comma = value.find(',');
  if (comma != std::string::npos)
    value.resize(comma);
  if (isHex(value)) {
    for (char &c : value)
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return value;
  }
  std::string raw;
  int val = 0;
  int valb = -8;
  for (unsigned char c : value) {
    if (c == '=')
      break;
    int d = b64(static_cast<char>(c));
    if (d < 0)
      continue;
    val = (val << 6) + d;
    valb += 6;
    if (valb >= 0) {
      raw.push_back(static_cast<char>((val >> valb) & 0xff));
      valb -= 8;
    }
  }
  return toHex(reinterpret_cast<const uint8_t *>(raw.data()), raw.size());
}

std::string stripPath(std::string href)
{
  auto q = href.find('?');
  if (q != std::string::npos)
    href.resize(q);
  auto scheme = href.find("://");
  if (scheme != std::string::npos) {
    auto slash = href.find('/', scheme + 3);
    href = slash == std::string::npos ? "/" : href.substr(slash);
  }
  while (href.size() > 1 && href.back() == '/')
    href.pop_back();
  return href;
}

XrdCl::StatInfo *statFrom(const XioFS::DavEntry &e)
{
  uint32_t flags = XrdCl::StatInfo::Flags::IsReadable;
  if (e.is_dir)
    flags |= XrdCl::StatInfo::Flags::IsDir;
  uint64_t sz = e.size < 0 ? 0 : static_cast<uint64_t>(e.size);
  return new XrdCl::StatInfo("nobody", sz, flags,
                             e.mtime ? e.mtime : std::time(nullptr));
}

} // namespace

Filesystem::Filesystem(const std::string &url) : url_(url)
{
  std::string path;
  std::string err;
  parseTarget(url, endpoint_, path, err);
}

Filesystem::~Filesystem() = default;

std::string Filesystem::fullPath(const std::string &path) const
{
  if (!path.empty() && path[0] == '/')
    return path;
  std::string base = url_.GetPath();
  if (base.empty())
    base = "/";
  if (base.back() != '/')
    base.push_back('/');
  return base + path;
}

std::shared_ptr<XioFS::Http2Session> Filesystem::session(time_t timeout,
                                                        std::string &err)
{
  std::lock_guard<std::mutex> lock(mu_);
  if (endpoint_.host.empty()) {
    err = "invalid filesystem URL";
    return {};
  }
  if (sess_ && sess_->connected())
    return sess_;
  sess_ = connectTarget(endpoint_, timeout, err);
  return sess_;
}

XrdCl::XRootDStatus Filesystem::Stat(const std::string &path,
                                     XrdCl::ResponseHandler *handler,
                                     time_t timeout)
{
  std::string err;
  auto sess = session(timeout, err);
  if (!sess)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errConnectionError, 0, err);

  XioFS::HttpRequest req;
  req.method = "PROPFIND";
  req.path = fullPath(path);
  req.headers.push_back({"depth", "0"});
  applyTimeout(req, timeout);
  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string e) {
    auto st = statusFrom(prc, resp.status, e);
    if (!st.IsOK()) {
      deliver(handler, st, nullptr);
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
    auto *obj = new XrdCl::AnyObject();
    obj->Set(statFrom(ents[0]));
    deliver(handler, XrdCl::XRootDStatus(), obj);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus Filesystem::DirList(const std::string &path,
                                        XrdCl::DirListFlags::Flags,
                                        XrdCl::ResponseHandler *handler,
                                        time_t timeout)
{
  std::string err;
  auto sess = session(timeout, err);
  if (!sess)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errConnectionError, 0, err);

  const std::string target = fullPath(path);
  const std::string self = stripPath(target);
  std::string host = endpoint_.host;
  if (!((endpoint_.tls && endpoint_.port == 443) ||
        (!endpoint_.tls && endpoint_.port == 80)))
    host += ":" + std::to_string(endpoint_.port);

  XioFS::HttpRequest req;
  req.method = "PROPFIND";
  req.path = target;
  req.headers.push_back({"depth", "1"});
  applyTimeout(req, timeout);
  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string e) {
    auto st = statusFrom(prc, resp.status, e);
    if (!st.IsOK()) {
      deliver(handler, st, nullptr);
      return;
    }
    std::vector<XioFS::DavEntry> ents;
    std::string perr;
    if (!XioFS::parseMultistatus(resp.body, ents, perr)) {
      deliver(handler,
              XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInvalidResponse, 0,
                                  perr.empty() ? "PROPFIND parse failed" : perr),
              nullptr);
      return;
    }
    auto *list = new XrdCl::DirectoryList();
    for (const auto &ent : ents) {
      if (stripPath(ent.href) == self)
        continue;
      if (ent.name.empty() || ent.name == "." || ent.name == "..")
        continue;
      list->Add(new XrdCl::DirectoryList::ListEntry(host, ent.name, statFrom(ent)));
    }
    auto *obj = new XrdCl::AnyObject();
    obj->Set(list);
    deliver(handler, XrdCl::XRootDStatus(), obj);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus Filesystem::MkDir(const std::string &path,
                                      XrdCl::MkDirFlags::Flags flags,
                                      XrdCl::Access::Mode, XrdCl::ResponseHandler *handler,
                                      time_t timeout)
{
  std::string err;
  auto sess = session(timeout, err);
  if (!sess)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errConnectionError, 0, err);

  std::vector<std::string> steps;
  const std::string target = fullPath(path);
  if (flags & XrdCl::MkDirFlags::MakePath) {
    std::string cur;
    std::string rest = stripPath(target);
    if (!rest.empty() && rest[0] == '/')
      rest.erase(rest.begin());
    std::string acc;
    while (!rest.empty()) {
      auto slash = rest.find('/');
      std::string comp = slash == std::string::npos ? rest : rest.substr(0, slash);
      if (!comp.empty()) {
        acc += "/" + comp;
        steps.push_back(acc);
      }
      if (slash == std::string::npos)
        break;
      rest.erase(0, slash + 1);
    }
  }
  if (steps.empty())
    steps.push_back(target);

  struct Job {
    std::shared_ptr<XioFS::Http2Session> sess;
    std::vector<std::string> steps;
    size_t index{0};
    bool parents{false};
    time_t timeout{0};
    XrdCl::ResponseHandler *handler{nullptr};

    void next()
    {
      if (index >= steps.size()) {
        deliver(handler, XrdCl::XRootDStatus(), nullptr);
        delete this;
        return;
      }
      XioFS::HttpRequest req;
      req.method = "MKCOL";
      req.path = steps[index];
      applyTimeout(req, timeout);
      auto self = this;
      int rc = sess->start(std::move(req), [self](int prc, XioFS::HttpResponse resp,
                                                  std::string e) {
        const bool exists = self->parents && prc == 0 &&
                            (resp.status == 405 || resp.status == 409 ||
                             resp.status == 301);
        if (!exists) {
          auto st = statusFrom(prc, resp.status, e);
          if (!st.IsOK()) {
            deliver(self->handler, st, nullptr);
            delete self;
            return;
          }
        }
        ++self->index;
        self->next();
      });
      if (rc) {
        deliver(handler, statusFrom(rc, 0, "HTTP/2 request was not queued"), nullptr);
        delete this;
      }
    }
  };

  auto *job = new Job();
  job->sess = sess;
  job->steps = std::move(steps);
  job->parents = (flags & XrdCl::MkDirFlags::MakePath) != 0;
  job->timeout = timeout;
  job->handler = handler;
  job->next();
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus Filesystem::Rm(const std::string &path,
                                   XrdCl::ResponseHandler *handler,
                                   time_t timeout)
{
  std::string err;
  auto sess = session(timeout, err);
  if (!sess)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errConnectionError, 0, err);
  XioFS::HttpRequest req;
  req.method = "DELETE";
  req.path = fullPath(path);
  applyTimeout(req, timeout);
  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string e) {
    deliver(handler, statusFrom(prc, resp.status, e), nullptr);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

XrdCl::XRootDStatus Filesystem::RmDir(const std::string &path,
                                      XrdCl::ResponseHandler *handler,
                                      time_t timeout)
{
  return Rm(path, handler, timeout);
}

XrdCl::XRootDStatus Filesystem::Query(XrdCl::QueryCode::Code queryCode,
                                      const XrdCl::Buffer &arg,
                                      XrdCl::ResponseHandler *handler,
                                      time_t timeout)
{
  if (queryCode != XrdCl::QueryCode::Checksum) {
    if (handler)
      return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errQueryNotSupported, 0,
                                 "unsupported query");
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errQueryNotSupported);
  }

  std::string err;
  auto sess = session(timeout, err);
  if (!sess)
    return XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errConnectionError, 0, err);

  std::string text = arg.ToString();
  std::string path = fullPath(text);
  std::string digest = "crc32c";
  auto pit = url_.GetParams().find("cks.type");
  if (pit != url_.GetParams().end())
    digest = httpDigestName(pit->second);
  auto sp = text.find(' ');
  if (sp != std::string::npos && text.find('/') != std::string::npos &&
      text.find('/') > sp) {
    digest = httpDigestName(text.substr(0, sp));
    path = fullPath(text.substr(sp + 1));
  }

  XioFS::HttpRequest req;
  req.method = "HEAD";
  req.path = path;
  req.headers.push_back({"want-digest", digest});
  applyTimeout(req, timeout);
  int rc = sess->start(std::move(req), [=](int prc, XioFS::HttpResponse resp,
                                           std::string e) {
    auto st = statusFrom(prc, resp.status, e);
    if (!st.IsOK()) {
      deliver(handler, st, nullptr);
      return;
    }
    std::string raw = resp.header("digest");
    if (raw.empty())
      raw = resp.header("repr-digest");
    auto eq = raw.find('=');
    if (eq == std::string::npos) {
      deliver(handler,
              XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errCheckSumError, 0,
                                  "checksum missing from response"),
              nullptr);
      return;
    }
    std::string name = xrootDigestName(raw.substr(0, eq));
    std::string hex = decodeDigest(raw.substr(eq + 1));
    if (hex.empty()) {
      deliver(handler,
              XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errCheckSumError, 0,
                                  "checksum could not be decoded"),
              nullptr);
      return;
    }
    auto *buf = new XrdCl::Buffer();
    buf->FromString(name + " " + hex);
    auto *obj = new XrdCl::AnyObject();
    obj->Set(buf);
    deliver(handler, XrdCl::XRootDStatus(), obj);
  });
  if (rc)
    return statusFrom(rc, 0, "HTTP/2 request was not queued");
  return XrdCl::XRootDStatus();
}

} // namespace XrdClXio
