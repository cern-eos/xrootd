//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioPosixFs.hh"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <fcntl.h>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace XioFS {

namespace {

std::string lower(std::string s)
{
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

bool ieq(const std::string &a, const char *b)
{
  return lower(a) == b;
}

const std::string *findHeader(const FsRequest &req, const char *name)
{
  for (const auto &h : req.headers) {
    if (ieq(h.first, name))
      return &h.second;
  }
  return nullptr;
}

std::string xmlEscape(const std::string &s)
{
  std::string o;
  o.reserve(s.size());
  for (char c : s) {
    if (c == '&')
      o += "&amp;";
    else if (c == '<')
      o += "&lt;";
    else if (c == '>')
      o += "&gt;";
    else
      o.push_back(c);
  }
  return o;
}

std::string httpDate(time_t t)
{
  struct tm tm {};
  gmtime_r(&t, &tm);
  char buf[64];
  if (strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm) == 0)
    return {};
  return buf;
}

bool percentDecode(const std::string &in, std::string &out)
{
  out.clear();
  out.reserve(in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    if (in[i] != '%') {
      if (in[i] == '\0')
        return false;
      out.push_back(in[i]);
      continue;
    }
    if (i + 2 >= in.size())
      return false;
    auto hex = [](char c) -> int {
      if (c >= '0' && c <= '9')
        return c - '0';
      if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
      if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
      return -1;
    };
    int hi = hex(in[i + 1]);
    int lo = hex(in[i + 2]);
    if (hi < 0 || lo < 0)
      return false;
    char dec = static_cast<char>((hi << 4) | lo);
    if (dec == '\0')
      return false;
    out.push_back(dec);
    i += 2;
  }
  return true;
}

// Lexical "/a/../b" collapse. False when the path escapes the root.
bool normalize(const std::string &in, std::string &out)
{
  std::string dec;
  if (!percentDecode(in.empty() ? "/" : in, dec))
    return false;
  if (dec.empty() || dec[0] != '/')
    dec = "/" + dec;
  std::vector<std::string> parts;
  std::string cur;
  for (size_t i = 0; i <= dec.size(); ++i) {
    if (i == dec.size() || dec[i] == '/') {
      if (cur.empty() || cur == ".") {
        cur.clear();
        continue;
      }
      if (cur == "..") {
        if (parts.empty())
          return false;
        parts.pop_back();
      } else {
        parts.push_back(cur);
      }
      cur.clear();
      continue;
    }
    cur.push_back(dec[i]);
  }
  out = "/";
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i)
      out.push_back('/');
    out += parts[i];
  }
  if (parts.empty())
    out = "/";
  return true;
}

bool underRoot(const std::string &root, const std::string &full)
{
  if (full == root)
    return true;
  return full.size() > root.size() &&
         full.compare(0, root.size(), root) == 0 &&
         full[root.size()] == '/';
}

std::string diskPath(const std::string &root, const std::string &url)
{
  if (url == "/")
    return root;
  return root + url;
}

void fail(FsResult &out, int err, int status = 0)
{
  out.err = err;
  out.http_status = status ? status : errnoToHttp(err);
  out.body = strerror(err);
  out.content_type = "text/plain";
}

std::string etagOf(const struct stat &st)
{
  return "\"" + std::to_string(static_cast<unsigned long long>(st.st_ino)) +
         "-" + std::to_string(static_cast<long long>(st.st_mtime)) + "-" +
         std::to_string(static_cast<long long>(st.st_size)) + "\"";
}

std::string etagIdentity(std::string id)
{
  if (!id.empty() && id.front() == '"')
    id.erase(0, 1);
  if (!id.empty() && id.back() == '"')
    id.pop_back();
  auto dash = id.find('-');
  if (dash != std::string::npos)
    id.resize(dash);
  return id;
}

bool ifMatchOk(const FsRequest &req, const struct stat *st)
{
  const std::string *im = findHeader(req, "if-match");
  if (!im || im->empty() || *im == "*")
    return true;
  if (!st)
    return false;
  return etagIdentity(*im) == etagIdentity(etagOf(*st));
}

void fillStat(const std::string &href, const struct stat &st, bool slash,
              std::ostream &xml)
{
  const bool dir = S_ISDIR(st.st_mode);
  xml << "<D:response><D:href>" << xmlEscape(href);
  if (slash && dir && href.size() > 1 && href.back() != '/')
    xml << "/";
  xml << "</D:href><D:propstat><D:prop>"
      << "<D:getcontentlength>" << (dir ? 0 : static_cast<long long>(st.st_size))
      << "</D:getcontentlength>"
      << "<D:getlastmodified>" << httpDate(st.st_mtime)
      << "</D:getlastmodified>"
      << "<D:resourcetype>";
  if (dir)
    xml << "<D:collection/>";
  else if (S_ISLNK(st.st_mode))
    xml << "<D:symlink/>";
  xml << "</D:resourcetype>"
      << "<D:iscollection>" << (dir ? "1" : "0") << "</D:iscollection>"
      << "<X:mode>" << std::oct << (st.st_mode & 07777) << std::dec
      << "</X:mode>"
      << "<X:uid>" << static_cast<unsigned>(st.st_uid) << "</X:uid>"
      << "<X:gid>" << static_cast<unsigned>(st.st_gid) << "</X:gid>"
      << "</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>"
      << "</D:response>";
}

bool parseRange(const std::string &v, uint64_t size, uint64_t &start,
                uint64_t &end)
{
  // bytes=START-END
  std::string s = v;
  auto eq = s.find('=');
  if (eq == std::string::npos)
    return false;
  s = s.substr(eq + 1);
  auto dash = s.find('-');
  if (dash == std::string::npos)
    return false;
  try {
    start = std::stoull(s.substr(0, dash));
    if (dash + 1 >= s.size())
      end = size ? size - 1 : 0;
    else
      end = std::stoull(s.substr(dash + 1));
  } catch (...) {
    return false;
  }
  return true;
}

bool parseContentRange(const std::string &v, uint64_t &start, uint64_t &end)
{
  // bytes START-END/SIZE
  std::string s = v;
  if (s.compare(0, 6, "bytes ") == 0)
    s = s.substr(6);
  auto dash = s.find('-');
  auto slash = s.find('/');
  if (dash == std::string::npos || slash == std::string::npos || slash < dash)
    return false;
  try {
    start = std::stoull(s.substr(0, dash));
    end = std::stoull(s.substr(dash + 1, slash - dash - 1));
  } catch (...) {
    return false;
  }
  return end >= start;
}

std::string propValue(const std::string &body, const char *tag)
{
  std::string open = std::string("<X:") + tag + ">";
  std::string close = std::string("</X:") + tag + ">";
  auto a = body.find(open);
  if (a == std::string::npos) {
    open = std::string("<") + tag + ">";
    close = std::string("</") + tag + ">";
    a = body.find(open);
    if (a == std::string::npos)
      return {};
  }
  a += open.size();
  auto b = body.find(close, a);
  if (b == std::string::npos)
    return {};
  return body.substr(a, b - a);
}

} // namespace

int errnoToHttp(int err)
{
  switch (err) {
    case 0:
      return 200;
    case ENOENT:
      return 404;
    case EACCES:
    case EPERM:
      return 403;
    case EEXIST:
      return 409;
    case EINVAL:
      return 400;
    case ENOSPC:
      return 507;
    case ESTALE:
      return 412;
    case EAGAIN:
      return 423;
    case EISDIR:
    case ENOTDIR:
      return 409;
    default:
      return 500;
  }
}

const std::string *FsRequest::header(const char *name) const
{
  return findHeader(*this, name);
}

PosixFs::PosixFs(std::string root)
  : root_(std::move(root))
{
  while (root_.size() > 1 && root_.back() == '/')
    root_.pop_back();
}

void PosixFs::handle(const FsRequest &req, FsResult &out)
{
  out = FsResult{};
  std::string url;
  if (!normalize(req.path, url)) {
    fail(out, EINVAL);
    return;
  }
  const std::string disk = diskPath(root_, url);
  const std::string method = lower(req.method);

  struct stat st {};
  const bool exists = ::lstat(disk.c_str(), &st) == 0;
  if (!exists && errno != ENOENT && method != "put" && method != "mkcol" &&
      method != "patch") {
    fail(out, errno);
    return;
  }

  auto require = [&](bool ok) -> bool {
    if (ok)
      return true;
    fail(out, exists ? errno : ENOENT);
    return false;
  };

  if (method == "propfind") {
    if (!require(exists))
      return;
    int depth = 0;
    if (const std::string *d = findHeader(req, "depth")) {
      if (*d == "1")
        depth = 1;
      else if (*d == "0")
        depth = 0;
      else if (lower(*d) == "infinity") {
        fail(out, EPERM, 403);
        return;
      }
    }
    std::ostringstream xml;
    xml << "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        << "<D:multistatus xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">";
    fillStat(url, st, false, xml);
    if (depth == 1 && S_ISDIR(st.st_mode)) {
      DIR *dir = opendir(disk.c_str());
      if (!dir) {
        fail(out, errno);
        return;
      }
      while (dirent *de = readdir(dir)) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
          continue;
        std::string child = disk + "/" + de->d_name;
        struct stat cst {};
        if (::lstat(child.c_str(), &cst) != 0)
          continue;
        std::string href = url == "/" ? "/" + std::string(de->d_name)
                                      : url + "/" + de->d_name;
        fillStat(href, cst, false, xml);
      }
      closedir(dir);
    }
    xml << "</D:multistatus>";
    out.http_status = 207;
    out.content_type = "application/xml; charset=\"utf-8\"";
    out.body = xml.str();
    out.headers.emplace_back("etag", etagOf(st));
    return;
  }

  if (method == "head" || method == "get") {
    if (!require(exists))
      return;
    if (S_ISDIR(st.st_mode)) {
      fail(out, EISDIR);
      return;
    }
    out.headers.emplace_back("etag", etagOf(st));
    out.headers.emplace_back("last-modified", httpDate(st.st_mtime));
    if (method == "head") {
      out.http_status = 200;
      out.headers.emplace_back(
          "content-length", std::to_string(static_cast<long long>(st.st_size)));
      return;
    }
    uint64_t start = 0;
    uint64_t end = st.st_size ? static_cast<uint64_t>(st.st_size) - 1 : 0;
    bool partial = false;
    if (const std::string *range = findHeader(req, "range")) {
      partial = true;
      if (!parseRange(*range, static_cast<uint64_t>(st.st_size), start, end)) {
        fail(out, EINVAL);
        return;
      }
      if (st.st_size == 0 || start >= static_cast<uint64_t>(st.st_size)) {
        out.http_status = 416;
        out.headers.emplace_back(
            "content-range",
            "bytes */" + std::to_string(static_cast<long long>(st.st_size)));
        return;
      }
      if (end >= static_cast<uint64_t>(st.st_size))
        end = static_cast<uint64_t>(st.st_size) - 1;
    }
    const uint64_t nbytes =
        st.st_size == 0 ? 0 : (end >= start ? end - start + 1 : 0);
    std::string data;
    if (nbytes > 0) {
      int fd = ::open(disk.c_str(), O_RDONLY);
      if (fd < 0) {
        fail(out, errno);
        return;
      }
      data.resize(static_cast<size_t>(nbytes));
      uint64_t got = 0;
      while (got < nbytes) {
        ssize_t n = ::pread(fd, &data[static_cast<size_t>(got)],
                            static_cast<size_t>(nbytes - got),
                            static_cast<off_t>(start + got));
        if (n < 0) {
          int e = errno;
          ::close(fd);
          fail(out, e);
          return;
        }
        if (n == 0)
          break;
        got += static_cast<uint64_t>(n);
      }
      ::close(fd);
      data.resize(static_cast<size_t>(got));
    }
    out.body = std::move(data);
    if (partial) {
      out.http_status = 206;
      out.headers.emplace_back(
          "content-range",
          "bytes " + std::to_string(start) + "-" +
              std::to_string(start + out.body.size() - (out.body.empty() ? 0 : 1)) +
              "/" + std::to_string(static_cast<long long>(st.st_size)));
    } else {
      out.http_status = 200;
    }
    return;
  }

  if (method == "put" || method == "patch") {
    if (exists && !S_ISREG(st.st_mode)) {
      fail(out, EISDIR);
      return;
    }
    if (exists && !ifMatchOk(req, &st)) {
      fail(out, ESTALE, 412);
      return;
    }
    const std::string *inm = findHeader(req, "if-none-match");
    if (inm && *inm == "*" && exists) {
      fail(out, EEXIST, 412);
      return;
    }
    uint64_t start = 0;
    int flags = O_CREAT | O_WRONLY;
    if (method == "put") {
      flags |= O_TRUNC;
    } else {
      const std::string *cr = findHeader(req, "content-range");
      if (!cr) {
        fail(out, EINVAL);
        return;
      }
      uint64_t end = 0;
      if (!parseContentRange(*cr, start, end)) {
        fail(out, EINVAL);
        return;
      }
      if (req.body.size() != static_cast<size_t>(end - start + 1)) {
        fail(out, EINVAL);
        return;
      }
    }
    int fd = ::open(disk.c_str(), flags, 0644);
    if (fd < 0) {
      fail(out, errno);
      return;
    }
    uint64_t off = 0;
    while (off < req.body.size()) {
      ssize_t n = ::pwrite(fd, req.body.data() + off, req.body.size() - off,
                           static_cast<off_t>(start + off));
      if (n < 0) {
        int e = errno;
        ::close(fd);
        fail(out, e);
        return;
      }
      off += static_cast<uint64_t>(n);
    }
    ::close(fd);
    struct stat nst {};
    if (::lstat(disk.c_str(), &nst) == 0)
      out.headers.emplace_back("etag", etagOf(nst));
    out.http_status = exists ? 204 : 201;
    return;
  }

  if (method == "mkcol") {
    if (exists) {
      fail(out, EEXIST);
      return;
    }
    if (::mkdir(disk.c_str(), 0755) != 0) {
      fail(out, errno);
      return;
    }
    out.http_status = 201;
    return;
  }

  if (method == "delete") {
    if (!require(exists))
      return;
    if (!ifMatchOk(req, &st)) {
      fail(out, ESTALE, 412);
      return;
    }
    int rc = S_ISDIR(st.st_mode) ? ::rmdir(disk.c_str()) : ::unlink(disk.c_str());
    if (rc != 0) {
      fail(out, errno);
      return;
    }
    out.http_status = 204;
    return;
  }

  if (method == "move") {
    if (!require(exists))
      return;
    const std::string *dest = findHeader(req, "destination");
    if (!dest) {
      fail(out, EINVAL);
      return;
    }
    std::string destpath = *dest;
    auto scheme = destpath.find("://");
    if (scheme != std::string::npos) {
      auto slash = destpath.find('/', scheme + 3);
      destpath = slash == std::string::npos ? "/" : destpath.substr(slash);
    }
    std::string desturl;
    if (!normalize(destpath, desturl)) {
      fail(out, EINVAL);
      return;
    }
    std::string to = diskPath(root_, desturl);
    if (!underRoot(root_, to) && to != root_) {
      fail(out, EPERM);
      return;
    }
    if (::rename(disk.c_str(), to.c_str()) != 0) {
      fail(out, errno);
      return;
    }
    out.http_status = 204;
    return;
  }

  if (method == "proppatch") {
    if (!require(exists))
      return;
    const std::string mode = propValue(req.body, "mode");
    const std::string uid = propValue(req.body, "uid");
    const std::string gid = propValue(req.body, "gid");
    const std::string at = propValue(req.body, "atime");
    const std::string mt = propValue(req.body, "mtime");
    if (!mode.empty()) {
      try {
        mode_t m = static_cast<mode_t>(std::stoul(mode, nullptr, 8) & 07777);
        if (::chmod(disk.c_str(), m) != 0) {
          fail(out, errno);
          return;
        }
      } catch (...) {
        fail(out, EINVAL);
        return;
      }
    }
    if (!uid.empty() || !gid.empty()) {
      uid_t u = static_cast<uid_t>(-1);
      gid_t g = static_cast<gid_t>(-1);
      try {
        if (!uid.empty())
          u = static_cast<uid_t>(std::stoul(uid));
        if (!gid.empty())
          g = static_cast<gid_t>(std::stoul(gid));
      } catch (...) {
        fail(out, EINVAL);
        return;
      }
      if (::lchown(disk.c_str(), u, g) != 0) {
        fail(out, errno);
        return;
      }
    }
    if (!at.empty() || !mt.empty()) {
      struct timespec tv[2];
      tv[0].tv_nsec = UTIME_OMIT;
      tv[1].tv_nsec = UTIME_OMIT;
      tv[0].tv_sec = 0;
      tv[1].tv_sec = 0;
      try {
        if (!at.empty()) {
          tv[0].tv_sec = static_cast<time_t>(std::stoll(at));
          tv[0].tv_nsec = 0;
        }
        if (!mt.empty()) {
          tv[1].tv_sec = static_cast<time_t>(std::stoll(mt));
          tv[1].tv_nsec = 0;
        }
      } catch (...) {
        fail(out, EINVAL);
        return;
      }
      if (::utimensat(AT_FDCWD, disk.c_str(), tv, 0) != 0) {
        fail(out, errno);
        return;
      }
    }
    out.http_status = 207;
    out.content_type = "application/xml; charset=\"utf-8\"";
    out.body = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
               "<D:multistatus xmlns:D=\"DAV:\">"
               "<D:response><D:href>" +
               xmlEscape(url) +
               "</D:href><D:propstat><D:status>"
               "HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
               "</D:multistatus>";
    return;
  }

  fail(out, EPERM, 405);
  out.headers.emplace_back("allow",
                           "GET, HEAD, PUT, PATCH, PROPFIND, PROPPATCH, "
                           "MKCOL, DELETE, MOVE");
}

} // namespace XioFS
