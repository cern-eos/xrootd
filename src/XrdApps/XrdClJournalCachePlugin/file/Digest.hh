#pragma once

#include <string>

namespace JournalCache {

std::string computeSHA256(const std::string &data);
bool ensureCacheDirectory(const std::string &dirName);

} // namespace JournalCache
