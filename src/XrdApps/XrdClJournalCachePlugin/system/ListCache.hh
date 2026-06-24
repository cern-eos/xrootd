#pragma once

#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <cstdint>
#include <string>
#include <tuple>

namespace JournalCache {

struct ListCacheHeader {
  bool valid = false;
  uint32_t version = 0;
  XrdCl::DirListFlags::Flags flags = XrdCl::DirListFlags::None;
  uint64_t created = 0;
};

std::string serializeListEntry(const std::string &hostAddress,
                               const std::string &name,
                               const XrdCl::StatInfo *stat);

std::tuple<std::string, std::string, XrdCl::StatInfo *>
deserializeListEntry(const std::string &line);

std::string formatListHeader(XrdCl::DirListFlags::Flags flags, uint64_t created);

ListCacheHeader parseListHeader(const std::string &line);

XrdCl::DirectoryList *loadDirectoryList(const std::string &path,
                                        XrdCl::DirListFlags::Flags expectedFlags,
                                        uint64_t ttlSeconds);

bool saveDirectoryList(const std::string &path, XrdCl::DirectoryList *dirList,
                       XrdCl::DirListFlags::Flags flags);

bool loadStatInfo(const std::string &path, uint64_t ttlSeconds,
                  XrdCl::StatInfo *&stat);

bool saveStatInfo(const std::string &path, const XrdCl::StatInfo *stat);

bool isCacheEntryExpired(uint64_t created, uint64_t ttlSeconds);

} // namespace JournalCache
