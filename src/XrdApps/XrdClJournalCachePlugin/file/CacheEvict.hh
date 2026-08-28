#pragma once

#include <filesystem>
#include <string>

namespace JournalCache {
namespace fs = std::filesystem;

inline bool pathHasXjcComponent(const fs::path &path) {
  for (const auto &part : path) {
    if (part == ".xjc") {
      return true;
    }
  }
  return false;
}

inline bool isEvictableCacheFile(const fs::path &filePath) {
  const std::string name = filePath.filename().string();
  return name == "journal" || name == ".journalcache_stat" ||
         name.rfind(".journalcache_list", 0) == 0;
}

inline void removeEmptyParents(const fs::path &filePath, const fs::path &root) {
  fs::path dir = filePath.parent_path();
  std::error_code ec;
  while (!dir.empty() && dir != root) {
    if (pathHasXjcComponent(dir)) {
      break;
    }
    if (!fs::is_empty(dir, ec) || ec) {
      break;
    }
    fs::remove(dir, ec);
    dir = dir.parent_path();
  }
}

} // namespace JournalCache
