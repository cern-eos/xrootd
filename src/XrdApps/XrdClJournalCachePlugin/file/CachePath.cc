#include "file/CachePath.hh"
#include "file/Digest.hh"
#include "file/XrdClJournalCacheFile.hh"

#include "XrdCl/XrdClURL.hh"

#include <filesystem>
#include <string>

namespace JournalCache {

namespace {

const XrdCl::DirListFlags::Flags kUncacheableDirListFlags =
    static_cast<XrdCl::DirListFlags::Flags>(
        XrdCl::DirListFlags::Recursive | XrdCl::DirListFlags::Chunked |
        XrdCl::DirListFlags::Zip);

const XrdCl::DirListFlags::Flags kDirListKeyFlags =
    static_cast<XrdCl::DirListFlags::Flags>(
        XrdCl::DirListFlags::Stat | XrdCl::DirListFlags::Locate |
        XrdCl::DirListFlags::Merge | XrdCl::DirListFlags::Cksm);

std::string listingFileSuffix(XrdCl::DirListFlags::Flags flags) {
  const auto key = dirListCacheKeyFlags(flags);
  if (key == XrdCl::DirListFlags::Stat) {
    return ".journalcache_list";
  }
  return ".journalcache_list." + std::to_string(static_cast<uint32_t>(key));
}

void removeListingFilesInDir(const std::filesystem::path &dir) {
  if (!std::filesystem::exists(dir)) {
    return;
  }
  std::error_code ec;
  for (const auto &entry : std::filesystem::directory_iterator(dir, ec)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    const auto name = entry.path().filename().string();
    if (name == ".journalcache_list" ||
        name.rfind(".journalcache_list.", 0) == 0) {
      std::filesystem::remove(entry.path(), ec);
    }
  }
}

} // namespace

std::string normalizeRemotePath(const std::string &path) {
  if (path.empty()) {
    return "/";
  }
  if (path[0] == '/') {
    return path;
  }
  return "/" + path;
}

std::string resolveCacheDir(const std::string &fsUrl,
                            const std::string &remotePath) {
  return resolveCacheDirWithSettings(XrdCl::JournalCacheFile::sCachePath, fsUrl,
                                     remotePath,
                                     XrdCl::JournalCacheFile::sFlatHierarchy,
                                     XrdCl::JournalCacheFile::sBasePath);
}

std::string resolveCacheDirWithSettings(const std::string &cacheRoot,
                                        const std::string &fsUrl,
                                        const std::string &remotePath,
                                        bool flatHierarchy,
                                        const std::string &basePath) {
  const std::string normPath = normalizeRemotePath(remotePath);

  if (flatHierarchy) {
    return cacheRoot + computeSHA256(fsUrl + normPath);
  }

  if (!basePath.empty()) {
    const size_t pos = normPath.find(basePath);
    if (pos != std::string::npos) {
      return cacheRoot + normPath.substr(pos);
    }
    XrdCl::URL url(fsUrl);
    const size_t urlPos = url.GetPath().find(basePath);
    if (urlPos != std::string::npos) {
      return cacheRoot + normPath;
    }
  }

  XrdCl::URL url(fsUrl);
  const std::string host =
      url.GetHostName() + ":" + std::to_string(url.GetPort());
  return cacheRoot + host + normPath;
}

std::string parentRemotePath(const std::string &path) {
  const std::string norm = normalizeRemotePath(path);
  if (norm == "/") {
    return "/";
  }
  const auto pos = norm.find_last_of('/');
  if (pos == 0) {
    return "/";
  }
  if (pos == std::string::npos) {
    return "/";
  }
  return norm.substr(0, pos);
}

bool isDirListCacheable(XrdCl::DirListFlags::Flags flags) {
  return (flags & kUncacheableDirListFlags) == 0;
}

XrdCl::DirListFlags::Flags
dirListCacheKeyFlags(XrdCl::DirListFlags::Flags flags) {
  flags |= XrdCl::DirListFlags::Stat;
  return static_cast<XrdCl::DirListFlags::Flags>(flags & kDirListKeyFlags);
}

std::string listingCachePath(const std::string &fsUrl, const std::string &dirPath,
                             XrdCl::DirListFlags::Flags flags) {
  return resolveCacheDir(fsUrl, dirPath) + "/" + listingFileSuffix(flags);
}

std::string statCachePath(const std::string &fsUrl, const std::string &path) {
  return resolveCacheDir(fsUrl, path) + "/.journalcache_stat";
}

std::string resolveFileJournalDir(const std::string &cacheKeyUrl) {
  if (XrdCl::JournalCacheFile::sFlatHierarchy) {
    return XrdCl::JournalCacheFile::sCachePath +
           computeSHA256(cacheKeyUrl);
  }
  XrdCl::URL url(cacheKeyUrl);
  return resolveCacheDir(cacheKeyUrl, url.GetPath());
}

void invalidateListingCache(const std::string &fsUrl, const std::string &dirPath) {
  invalidateListingCacheInDir(resolveCacheDir(fsUrl, dirPath));
}

void invalidateListingCacheInDir(const std::string &cacheDir) {
  removeListingFilesInDir(cacheDir);
}

void invalidateStatCache(const std::string &fsUrl, const std::string &path) {
  std::error_code ec;
  std::filesystem::remove(statCachePath(fsUrl, path), ec);
}

void invalidateCachesForMutation(const std::string &fsUrl,
                                 const std::string &path) {
  const std::string norm = normalizeRemotePath(path);
  invalidateListingCache(fsUrl, parentRemotePath(norm));
  invalidateStatCache(fsUrl, norm);
  invalidateListingCache(fsUrl, norm);
}

} // namespace JournalCache
