#pragma once

#include <string>

namespace JournalCache {

//! Parsed upstream URL embedded in a forwarding-proxy path or chained URL.
struct EmbeddedFileUrl {
  std::string fileUrl;
  bool valid = false;
};

//! Parse `/https://host/path`, `/root://host//path`, etc.
EmbeddedFileUrl parseEmbeddedFileUrl(const std::string &path);

//! Parse `root://proxy//root://origin//path` and path-embedded upstream URLs.
EmbeddedFileUrl parseChainedFileUrl(const std::string &url);

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
