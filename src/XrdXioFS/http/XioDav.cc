//------------------------------------------------------------------------------
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include "XioDav.hh"

#include <cctype>
#include <cstring>
#include <ctime>
#include <sys/stat.h>
#include <sys/types.h>

namespace XioFS {

namespace {

std::string toLower(std::string s)
{
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

std::string localName(const std::string &tag)
{
  auto space = tag.find_first_of(" \t\r\n/");
  std::string t = (space == std::string::npos) ? tag : tag.substr(0, space);
  auto colon = t.rfind(':');
  return (colon == std::string::npos) ? t : t.substr(colon + 1);
}

bool extractLocal(const std::string &xml, const char *local, std::string &inner)
{
  const std::string hay = toLower(xml);
  const std::string loc = toLower(local);
  std::string::size_type pos = 0;
  while (pos < hay.size()) {
    auto lt = hay.find('<', pos);
    if (lt == std::string::npos)
      return false;
    if (lt + 1 < hay.size() && hay[lt + 1] == '/') {
      pos = lt + 1;
      continue;
    }
    auto gt = hay.find('>', lt);
    if (gt == std::string::npos)
      return false;
    std::string tag = hay.substr(lt + 1, gt - lt - 1);
    bool selfclose = false;
    if (!tag.empty() && tag.back() == '/') {
      selfclose = true;
      tag.pop_back();
      while (!tag.empty() && std::isspace(static_cast<unsigned char>(tag.back())))
        tag.pop_back();
    }
    if (localName(tag) != loc) {
      pos = gt + 1;
      continue;
    }
    if (selfclose) {
      inner.clear();
      return true;
    }
    auto close = hay.find("</", gt + 1);
    while (close != std::string::npos) {
      auto cgt = hay.find('>', close);
      if (cgt == std::string::npos)
        return false;
      std::string ctag = hay.substr(close + 2, cgt - close - 2);
      if (localName(ctag) == loc) {
        inner = xml.substr(gt + 1, close - (gt + 1));
        return true;
      }
      close = hay.find("</", cgt + 1);
    }
    return false;
  }
  return false;
}

std::vector<std::string> splitResponses(const std::string &xml)
{
  std::vector<std::string> out;
  const std::string hay = toLower(xml);
  std::string::size_type pos = 0;
  while (pos < hay.size()) {
    auto lt = hay.find('<', pos);
    if (lt == std::string::npos)
      break;
    auto gt = hay.find('>', lt);
    if (gt == std::string::npos)
      break;
    std::string tag = hay.substr(lt + 1, gt - lt - 1);
    if (localName(tag) != "response" || (!tag.empty() && tag[0] == '/')) {
      pos = gt + 1;
      continue;
    }
    auto close = hay.find("</", gt + 1);
    while (close != std::string::npos) {
      auto cgt = hay.find('>', close);
      if (cgt == std::string::npos)
        break;
      std::string ctag = hay.substr(close + 2, cgt - close - 2);
      if (localName(ctag) == "response") {
        out.push_back(xml.substr(lt, cgt + 1 - lt));
        pos = cgt + 1;
        break;
      }
      close = hay.find("</", cgt + 1);
    }
    if (close == std::string::npos)
      break;
  }
  return out;
}

std::string decodeXml(const std::string &href)
{
  std::string decoded;
  decoded.reserve(href.size());
  for (size_t i = 0; i < href.size(); ++i) {
    if (href[i] == '&') {
      if (href.compare(i, 5, "&amp;") == 0) {
        decoded.push_back('&');
        i += 4;
        continue;
      }
      if (href.compare(i, 4, "&lt;") == 0) {
        decoded.push_back('<');
        i += 3;
        continue;
      }
      if (href.compare(i, 4, "&gt;") == 0) {
        decoded.push_back('>');
        i += 3;
        continue;
      }
    }
    decoded.push_back(href[i]);
  }
  return decoded;
}

} // namespace

std::string hrefBasename(const std::string &href)
{
  std::string h = href;
  while (!h.empty() && h.back() == '/')
    h.pop_back();
  auto slash = h.find_last_of('/');
  if (slash == std::string::npos)
    return h;
  return h.substr(slash + 1);
}

bool parseHttpDate(const std::string &s, time_t &out)
{
  struct tm tm {};
  if (!strptime(s.c_str(), "%a, %d %b %Y %H:%M:%S", &tm))
    return false;
  tm.tm_isdst = 0;
#if defined(_WIN32)
  out = _mkgmtime(&tm);
#else
  out = timegm(&tm);
#endif
  return out != static_cast<time_t>(-1);
}

bool parseResponseBlock(const std::string &block, DavEntry &e)
{
  e = DavEntry{};
  std::string href;
  if (extractLocal(block, "href", href)) {
    e.href = decodeXml(href);
    e.name = hrefBasename(e.href);
  }

  std::string len;
  if (extractLocal(block, "getcontentlength", len)) {
    try {
      e.size = std::stoll(len);
    } catch (...) {
      e.size = -1;
    }
  }

  std::string lm;
  if (extractLocal(block, "getlastmodified", lm))
    parseHttpDate(lm, e.mtime);

  std::string rt;
  if (extractLocal(block, "resourcetype", rt)) {
    const std::string rtl = toLower(rt);
    if (rtl.find("collection") != std::string::npos)
      e.is_dir = true;
    else if (rtl.find("symlink") != std::string::npos)
      e.is_lnk = true;
  }

  std::string ic;
  if (extractLocal(block, "iscollection", ic) && !ic.empty() && ic[0] == '1')
    e.is_dir = true;

  std::string other;
  if (extractLocal(block, "other", other) && !other.empty() &&
      (other[0] == '1' || other[0] == 't' || other[0] == 'T'))
    e.is_lnk = true;

  std::string umode;
  if (extractLocal(block, "unix-mode", umode) ||
      extractLocal(block, "mode", umode)) {
    try {
      e.mode = static_cast<mode_t>(std::stoul(umode, nullptr, 8) & 07777);
    } catch (...) {
      e.mode = 0;
    }
  }

  std::string uid;
  if (extractLocal(block, "uid", uid) || extractLocal(block, "owner-uid", uid)) {
    try {
      e.uid = static_cast<uid_t>(std::stoul(uid, nullptr, 10));
    } catch (...) {
    }
  }

  std::string gid;
  if (extractLocal(block, "gid", gid) || extractLocal(block, "owner-gid", gid)) {
    try {
      e.gid = static_cast<gid_t>(std::stoul(gid, nullptr, 10));
    } catch (...) {
    }
  }

  std::string at;
  if (extractLocal(block, "atime", at) ||
      extractLocal(block, "getlastaccessed", at)) {
    try {
      e.atime = static_cast<time_t>(std::stoll(at));
    } catch (...) {
      parseHttpDate(at, e.atime);
    }
  }

  std::string mt;
  if (extractLocal(block, "mtime", mt)) {
    try {
      e.mtime = static_cast<time_t>(std::stoll(mt));
    } catch (...) {
    }
  }

  std::string ftype;
  if (extractLocal(block, "file-type", ftype)) {
    const std::string ft = toLower(ftype);
    if (ft == "fifo")
      e.is_fifo = true;
    else if (ft == "chr")
      e.is_chr = true;
    else if (ft == "blk")
      e.is_blk = true;
    else if (ft == "lnk")
      e.is_lnk = true;
  }

  std::string rdev;
  if (extractLocal(block, "rdev", rdev)) {
    try {
      e.rdev = static_cast<dev_t>(std::stoull(rdev));
    } catch (...) {
    }
  }

  if (e.is_dir)
    e.is_lnk = false;

  return (!e.name.empty() || !e.href.empty());
}

DavStreamParser::DavStreamParser(EntryCallback cb)
    : cb_(std::move(cb))
{
}

bool DavStreamParser::feed(const char *data, size_t len, std::string &/*err*/)
{
  if (!data || len == 0)
    return true;
  buf_.append(data, len);

  size_t pos = 0;
  while (pos < buf_.size()) {
    auto lt = buf_.find('<', pos);
    if (lt == std::string::npos) {
      buf_.clear();
      break;
    }
    auto gt = buf_.find('>', lt);
    if (gt == std::string::npos) {
      if (lt > 0)
        buf_.erase(0, lt);
      break;
    }
    std::string tag = buf_.substr(lt + 1, gt - lt - 1);
    if (tag.empty() || tag[0] == '/') {
      pos = gt + 1;
      continue;
    }
    if (localName(toLower(tag)) != "response") {
      pos = gt + 1;
      continue;
    }

    // Found opening <...response...>, now find matching closing </...response>
    size_t close = gt + 1;
    bool found_close = false;
    while (close < buf_.size()) {
      auto clt = buf_.find("</", close);
      if (clt == std::string::npos)
        break;
      auto cgt = buf_.find('>', clt);
      if (cgt == std::string::npos)
        break;
      std::string ctag = buf_.substr(clt + 2, cgt - clt - 2);
      if (localName(toLower(ctag)) == "response") {
        std::string block = buf_.substr(lt, cgt + 1 - lt);
        DavEntry e;
        if (parseResponseBlock(block, e)) {
          if (cb_)
            cb_(std::move(e));
          count_++;
        }
        buf_.erase(0, cgt + 1);
        pos = 0;
        found_close = true;
        break;
      }
      close = cgt + 1;
    }

    if (!found_close) {
      // Incomplete response element in buffer; keep buffer from lt onwards
      if (lt > 0)
        buf_.erase(0, lt);
      break;
    }
  }

  return true;
}

bool DavStreamParser::finish(std::string &err)
{
  feed("", 0, err);
  buf_.clear();
  return true;
}

bool parseMultistatus(const std::string &xml, std::vector<DavEntry> &out,
                      std::string &err)
{
  out.clear();
  DavStreamParser parser([&](DavEntry &&e) {
    out.push_back(std::move(e));
  });
  if (!parser.feed(xml.data(), xml.size(), err))
    return false;
  if (!parser.finish(err))
    return false;
  if (out.empty()) {
    err = "PROPFIND response contained no DAV:response elements";
    return false;
  }
  return true;
}

} // namespace XioFS
