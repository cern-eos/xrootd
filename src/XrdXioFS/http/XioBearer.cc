//------------------------------------------------------------------------------
// WLCG bearer token file discovery. Opaque bytes only; no JWT parsing.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioBearer.hh"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace XioFS {
namespace {

bool readSecuredToken(const char *path, uid_t uid, std::string &token,
                      std::string &err, bool *missing)
{
  int fd = ::open(path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) {
      if (missing)
        *missing = true;
      return false;
    }
    err = std::string("open ") + path + ": " + strerror(errno);
    return false;
  }

  struct stat st;
  if (fstat(fd, &st) < 0) {
    int saved = errno;
    close(fd);
    err = std::string("fstat ") + path + ": " + strerror(saved);
    errno = saved;
    return false;
  }

  if (!S_ISREG(st.st_mode)) {
    close(fd);
    err = std::string(path) + " is not a regular file";
    errno = EPERM;
    return false;
  }
  if (st.st_uid != uid) {
    close(fd);
    err = std::string(path) + " is not owned by the calling user";
    errno = EPERM;
    return false;
  }
  if (st.st_mode & (S_IRWXG | S_IRWXO)) {
    close(fd);
    err = std::string(path) + " is group or world accessible";
    errno = EPERM;
    return false;
  }
  if (st.st_size <= 0 || st.st_size >= (off_t)XIOFS_BEARER_MAX) {
    close(fd);
    err = std::string(path) + " has an invalid token size";
    errno = EMSGSIZE;
    return false;
  }

  std::string buf(static_cast<size_t>(st.st_size), '\0');
  ssize_t n = ::read(fd, &buf[0], buf.size());
  int saved = errno;
  close(fd);
  if (n != st.st_size) {
    err = std::string("read ") + path + ": " +
          (n < 0 ? strerror(saved) : "short read");
    errno = n < 0 ? saved : EIO;
    return false;
  }

  while (!buf.empty() &&
         (buf.back() == '\n' || buf.back() == '\r' || buf.back() == ' ' ||
          buf.back() == '\t'))
    buf.pop_back();
  size_t i = 0;
  while (i < buf.size() &&
         (buf[i] == ' ' || buf[i] == '\t' || buf[i] == '\n' || buf[i] == '\r'))
    ++i;
  buf.erase(0, i);
  if (buf.empty()) {
    err = std::string(path) + " is empty";
    errno = ENOENT;
    if (missing)
      *missing = true;
    return false;
  }

  token.swap(buf);
  if (missing)
    *missing = false;
  return true;
}

} // namespace

bool loadBearerToken(uid_t uid, std::string &token, std::string &err,
                     bool *missing)
{
  token.clear();
  if (missing)
    *missing = false;

  std::ostringstream a, b;
  a << "/run/user/" << uid << "/bt_u" << uid;
  b << "/tmp/bt_u" << uid;
  const std::string paths[] = {a.str(), b.str()};

  for (const auto &p : paths) {
    bool miss = false;
    if (readSecuredToken(p.c_str(), uid, token, err, &miss))
      return true;
    if (!miss)
      return false;
  }
  err = "no bearer token file for uid " + std::to_string(uid) +
        " (/run/user/" + std::to_string(uid) + "/bt_u" +
        std::to_string(uid) + " or /tmp/bt_u" + std::to_string(uid) + ")";
  errno = ENOENT;
  if (missing)
    *missing = true;
  return false;
}

} // namespace XioFS
