//------------------------------------------------------------------------------
// High-level KernelFS client: XrdHttp verbs over HTTP/2.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef KFS_CLIENT_HH
#define KFS_CLIENT_HH

#include "KfsDav.hh"
#include "KfsHttp2.hh"
#include "KfsUrl.hh"

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace Kfs {

struct Attr {
  uint64_t ino{0};
  int64_t  size{-1};
  time_t   mtime{0};
  bool     is_dir{false};
  std::string etag;
  std::string path;
};

class Client {
public:
  int open(const std::string &url, Http2Session::Options opt, std::string &err);
  void close();

  int getattr(const std::string &relpath, Attr &out, std::string &err);
  int readdir(const std::string &relpath, std::vector<DavEntry> &out,
              std::string &err);
  int read(const std::string &relpath, uint64_t offset, uint64_t length,
           std::string &out, std::string &err);
  int put(const std::string &relpath, const std::string &body, std::string &err,
          const std::string &if_match = {},
          const std::string &if_none_match = {});
  int write(const std::string &relpath, uint64_t offset, const std::string &data,
            std::string &err, const std::string &if_match = {},
            std::string *etag_out = nullptr);
  int mkdir(const std::string &relpath, std::string &err);
  int unlink(const std::string &relpath, std::string &err,
             const std::string &if_match = {});
  int rename(const std::string &from, const std::string &to, std::string &err,
             const std::string &if_match = {});

  const Url &base() const { return base_; }
  bool connected() const { return sess_.connected(); }

private:
  Url base_;
  Http2Session sess_;
  Http2Session::Options opt_;
  std::mutex mu_;

  std::string absPath(const std::string &rel) const;
  int ensure(std::string &err);
  int doReq(const char *method, const std::string &rel,
            const std::vector<std::pair<std::string, std::string>> &hdrs,
            const std::string &body, HttpResponse &resp, std::string &err);
};

int httpToErrno(int status);

} // namespace Kfs

#endif
