#include "file/CachePath.hh"
#include "file/Digest.hh"
#include "file/XrdClJournalCacheFile.hh"

#include "XrdCl/XrdClURL.hh"

#include <filesystem>
#include <string>
#include <sys/stat.h>
#include <vector>

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
  std::string in = path.empty() ? "/" : path;
  if (in.front() != '/') {
    in.insert(in.begin(), '/');
  }

  std::vector<std::string> parts;
  size_t i = 1;
  while (i <= in.size()) {
    const size_t j = (i >= in.size()) ? in.size() : in.find('/', i);
    const size_t end = (j == std::string::npos) ? in.size() : j;
    const std::string seg = in.substr(i, end - i);
    if (seg == ".") {
      // skip
    } else if (seg == "..") {
      if (!parts.empty()) {
        parts.pop_back();
      }
    } else {
      parts.push_back(seg);
    }
    if (end >= in.size()) {
      break;
    }
    i = end + 1;
  }

  if (parts.empty()) {
    return "/";
  }
  std::string out;
  for (const auto &part : parts) {
    out += '/';
    out += part;
  }
  return out;
}

bool isRegularNonSymlinkFile(const std::string &path) {
  struct stat st;
  if (::lstat(path.c_str(), &st) != 0) {
    return false;
  }
  return S_ISREG(st.st_mode) && !S_ISLNK(st.st_mode);
}

bool pathHasPrefix(const std::string &path, const std::string &prefix) {
  std::string p = prefix;
  while (p.size() > 1 && p.back() == '/') {
    p.pop_back();
  }
  if (p.empty() || p == "/") {
    return true;
  }
  if (path.size() < p.size()) {
    return false;
  }
  if (path.compare(0, p.size(), p) != 0) {
    return false;
  }
  return path.size() == p.size() || path[p.size()] == '/';
}

bool pathIsUnderRoot(const std::string &path, const std::string &root) {
  if (root.empty()) {
    return true;
  }
  const auto normalPath = std::filesystem::path(path).lexically_normal();
  const auto normalRoot = std::filesystem::path(root).lexically_normal();
  const auto relative = normalPath.lexically_relative(normalRoot);
  const std::string rel = relative.generic_string();
  return !rel.empty() && rel.rfind("..", 0) != 0 &&
         rel.find("/../") == std::string::npos;
}

std::string confineToCacheRoot(const std::string &resolved,
                               const std::string &cacheRoot,
                               const std::string &fallbackKey) {
  if (pathIsUnderRoot(resolved, cacheRoot)) {
    return resolved;
  }
  return cacheRoot + computeSHA256(fallbackKey);
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
  const std::string key = fsUrl + normPath;
  std::string resolved;

  const std::string normBase =
      basePath.empty() ? std::string() : normalizeRemotePath(basePath);
  if (flatHierarchy) {
    resolved = cacheRoot + computeSHA256(key);
  } else if (!normBase.empty() && pathHasPrefix(normPath, normBase)) {
    resolved = cacheRoot + normPath;
  } else {
    XrdCl::URL url(fsUrl);
    const std::string host =
        url.GetHostName() + ":" + std::to_string(url.GetPort());
    resolved = cacheRoot + host + normPath;
  }

  return confineToCacheRoot(resolved, cacheRoot, key);
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
