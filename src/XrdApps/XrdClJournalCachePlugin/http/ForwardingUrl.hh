#pragma once

#include <string>

namespace JournalCache {

//! Parsed upstream URL embedded in a PSS forwarding-proxy path.
struct EmbeddedFileUrl {
  std::string fileUrl;
  bool valid = false;
};

//! Parse `/https://host/path` or `/http://host/path` into a canonical file URL.
EmbeddedFileUrl parseEmbeddedFileUrl(const std::string &path);

std::string resolveJournalDirWithSettings(const std::string &cacheRoot,
                                          const std::string &serverUrl,
                                          const std::string &remotePath,
                                          bool flatHierarchy,
                                          const std::string &basePath);

//! Resolve on-disk journal path using the same rules as the file plugin.
std::string resolveJournalPathFromCacheKey(const std::string &cacheRoot,
                                           const std::string &cacheKeyUrl,
                                           bool flatHierarchy,
                                           const std::string &basePath);

} // namespace JournalCache
