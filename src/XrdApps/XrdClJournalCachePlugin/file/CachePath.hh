#pragma once

#include "XrdCl/XrdClFileSystem.hh"
#include <string>

namespace JournalCache {

std::string normalizeRemotePath(const std::string &path);

//! Resolve the local cache directory for a remote path using plugin settings.
std::string resolveCacheDir(const std::string &fsUrl, const std::string &remotePath);

//! Resolve using explicit layout settings (used by tests and plugin code).
std::string resolveCacheDirWithSettings(const std::string &cacheRoot,
                                        const std::string &fsUrl,
                                        const std::string &remotePath,
                                        bool flatHierarchy,
                                        const std::string &basePath);

//! Parent directory of a normalized remote path (returns "/" for top level).
std::string parentRemotePath(const std::string &path);

//! Listing cache file path for a directory and DirList flags.
std::string listingCachePath(const std::string &fsUrl, const std::string &dirPath,
                             XrdCl::DirListFlags::Flags flags);

//! Stat cache file path for a remote path.
std::string statCachePath(const std::string &fsUrl, const std::string &path);

//! Local journal directory for a file URL (without per-thread user suffix).
std::string resolveFileJournalDir(const std::string &cacheKeyUrl);

//! DirList flag combinations that cannot be replayed from a flat listing.
bool isDirListCacheable(XrdCl::DirListFlags::Flags flags);

//! Normalize DirList flags stored in the cache key.
XrdCl::DirListFlags::Flags dirListCacheKeyFlags(XrdCl::DirListFlags::Flags flags);

void invalidateListingCache(const std::string &fsUrl, const std::string &dirPath);
void invalidateListingCacheInDir(const std::string &cacheDir);
void invalidateStatCache(const std::string &fsUrl, const std::string &path);
void invalidateCachesForMutation(const std::string &fsUrl, const std::string &path);

} // namespace JournalCache
