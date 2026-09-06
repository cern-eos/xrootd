//------------------------------------------------------------------------------
// Minimal WebDAV multistatus parser for XrdHttp PROPFIND responses.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_DAV_HH
#define XIOFS_DAV_HH

#include <cstdint>
#include <ctime>
#include <string>
#include <vector>

namespace XioFS {

struct DavEntry {
  std::string name;
  std::string href;
  bool        is_dir{false};
  int64_t     size{-1};
  time_t      mtime{0};
};

// Parse a DAV:multistatus document produced by XrdHttp (207 Multi-Status).
bool parseMultistatus(const std::string &xml, std::vector<DavEntry> &out,
                      std::string &err);

// Last path component of an href, trailing slashes stripped.
std::string hrefBasename(const std::string &href);

// HTTP-date ("Tue, 01 May 2012 02:42:13 GMT") as used by XrdHttp ISOdatetime.
bool parseHttpDate(const std::string &s, time_t &out);

} // namespace XioFS

#endif
