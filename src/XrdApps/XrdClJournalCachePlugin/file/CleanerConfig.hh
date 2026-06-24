#pragma once

#include <cstdint>
#include <string>

namespace JournalCache {

//! Hot-reloadable xjccleand settings.
struct CleanerSettings {
  bool enabled = false;
  std::string journal;
  uint64_t highWatermark = 0;
  uint64_t lowWatermark = 0;
  unsigned interval = 60;
  unsigned configPoll = 2;
};

std::string normalizeCacheRoot(std::string path);

bool parseCleanerText(const std::string &text, CleanerSettings &out);
bool loadCleanerFile(const std::string &path, CleanerSettings &out);
bool saveCleanerFile(const std::string &path, const CleanerSettings &settings);
std::string formatCleanerFile(const CleanerSettings &settings);

std::string defaultCleanerPath(const std::string &cacheRoot);

uint64_t effectiveLowWatermark(const CleanerSettings &settings);
bool validateCleanerSettings(const CleanerSettings &settings, std::string &error);

} // namespace JournalCache
