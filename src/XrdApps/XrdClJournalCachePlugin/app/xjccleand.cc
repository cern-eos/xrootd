//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.

#include "file/CleanerConfig.hh"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

namespace {

void usage(const char *prog) {
  std::cerr
      << "Usage: " << prog << " [--journal PATH] [--cleaner PATH]\n"
      << "       " << prog
      << " <directory> <highwatermark> <lowwatermark> <interval>\n"
      << "\n"
      << "Reads $journal/.xjc/cleaner.conf (hot-reloaded on mtime change).\n"
      << "Edit with: xjc cleaner ...\n";
}

void printCurrentTime() {
  auto now = std::chrono::system_clock::now();
  auto now_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
  auto now_ns_since_epoch = now_ns.time_since_epoch();
  auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(now_ns_since_epoch);
  auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
      now_ns_since_epoch - seconds);

  auto now_t = std::chrono::system_clock::to_time_t(now);
  struct tm *tm = std::localtime(&now_t);

  int year = tm->tm_year - 100;

  std::cout << std::setfill('0') << std::setw(2) << year << std::setfill('0')
            << std::setw(4) << (tm->tm_mon + 1) * 100 + tm->tm_mday << " ";
  std::cout << std::setw(2) << std::setfill('0') << tm->tm_hour << ":"
            << std::setw(2) << tm->tm_min << ":" << std::setw(2) << tm->tm_sec
            << " ";
  std::cout << "time=" << seconds.count() << "." << std::setw(9)
            << std::setfill('0') << nanoseconds.count() << " ";
}

time_t getLastAccessTime(const fs::path &filePath) {
  struct stat fileInfo;
  if (stat(filePath.c_str(), &fileInfo) != 0) {
    return -1;
  }
  return fileInfo.st_atime;
}

long long getDirectorySize(const fs::path &directory) {
  long long totalSize = 0;
  for (const auto &entry : fs::recursive_directory_iterator(directory)) {
    if (fs::is_regular_file(entry)) {
      totalSize += fs::file_size(entry);
    }
  }
  return totalSize;
}

std::vector<std::pair<long long, fs::path>>
getFilesByAccessTime(const fs::path &directory) {
  std::vector<std::pair<long long, fs::path>> fileList;
  for (const auto &entry : fs::recursive_directory_iterator(directory)) {
    if (fs::is_regular_file(entry)) {
      auto accessTime = getLastAccessTime(entry.path());
      fileList.emplace_back(accessTime, entry.path());
    }
  }
  std::sort(fileList.begin(), fileList.end());
  return fileList;
}

void cleanDirectory(const fs::path &directory, long long highWatermark,
                    long long lowWatermark) {
  long long currentSize = getDirectorySize(directory);
  if (currentSize <= highWatermark) {
    printCurrentTime();
    std::cout << "Directory size is within the limit. No action needed."
              << std::endl;
    return;
  }

  auto files = getFilesByAccessTime(directory);

  for (const auto &[accessTime, filePath] : files) {
    if (currentSize <= lowWatermark) {
      break;
    }
    long long fileSize = fs::file_size(filePath);
    try {
      fs::remove(filePath);
      currentSize -= fileSize;
      fs::path parentDir = filePath.parent_path();
      std::error_code ec;
      fs::remove_all(parentDir, ec);
      printCurrentTime();
      std::cout << "Deleted: " << filePath << " (Size: " << fileSize
                << " bytes)" << std::endl;
    } catch (const std::exception &e) {
      printCurrentTime();
      std::cerr << "Error deleting " << filePath << ": " << e.what()
                << std::endl;
    }
  }
}

std::chrono::system_clock::time_point fileWriteTime(const std::string &path) {
  std::error_code ec;
  const auto mtime = fs::last_write_time(path, ec);
  if (ec) {
    return {};
  }
#if defined(__cpp_lib_chrono) && __cpp_lib_chrono >= 201907L
  return std::chrono::clock_cast<std::chrono::system_clock>(mtime);
#else
  using namespace std::chrono;
  return system_clock::time_point(
      duration_cast<system_clock::duration>(mtime.time_since_epoch()));
#endif
}

struct ConfigState {
  std::string path;
  JournalCache::CleanerSettings settings;
  std::chrono::system_clock::time_point lastWrite;
  bool hasWrite = false;
};

bool loadConfigState(ConfigState &state) {
  JournalCache::CleanerSettings loaded;
  if (!JournalCache::loadCleanerFile(state.path, loaded)) {
    return false;
  }
  state.settings = loaded;
  state.lastWrite = fileWriteTime(state.path);
  state.hasWrite = state.lastWrite != std::chrono::system_clock::time_point{};
  return true;
}

bool reloadConfigIfChanged(ConfigState &state) {
  if (state.path.empty()) {
    return false;
  }
  const auto writeTime = fileWriteTime(state.path);
  if (writeTime == std::chrono::system_clock::time_point{}) {
    return false;
  }
  if (state.hasWrite && writeTime == state.lastWrite) {
    return false;
  }

  JournalCache::CleanerSettings loaded;
  if (!JournalCache::loadCleanerFile(state.path, loaded)) {
    return false;
  }
  state.settings = loaded;
  state.lastWrite = writeTime;
  state.hasWrite = true;
  return true;
}

void sleepWithConfigPoll(unsigned seconds, unsigned pollSeconds,
                         ConfigState &state) {
  unsigned remaining = seconds;
  while (remaining > 0) {
    const unsigned chunk = std::max(1u, std::min(remaining, pollSeconds));
    std::this_thread::sleep_for(std::chrono::seconds(chunk));
    remaining -= chunk;
    if (reloadConfigIfChanged(state)) {
      printCurrentTime();
      std::cout << "Reloaded cleaner config from " << state.path << std::endl;
    }
  }
}

std::string resolveCleanerPath(int argc, char **argv, int &argi,
                               std::string &journalOut) {
  std::string cleanerPath;
  journalOut.clear();
  if (const char *env = getenv("XRD_JOURNALCACHE_CLEANER")) {
    cleanerPath = env;
  }
  if (const char *cache = getenv("XRD_JOURNALCACHE_CACHE")) {
    journalOut = JournalCache::normalizeCacheRoot(cache);
    if (cleanerPath.empty()) {
      cleanerPath = JournalCache::defaultCleanerPath(journalOut);
    }
  }

  for (; argi < argc; ++argi) {
    const std::string arg = argv[argi];
    if (arg == "--journal" && argi + 1 < argc) {
      journalOut = JournalCache::normalizeCacheRoot(argv[++argi]);
      if (cleanerPath.empty()) {
        cleanerPath = JournalCache::defaultCleanerPath(journalOut);
      }
    } else if (arg == "--cleaner" && argi + 1 < argc) {
      cleanerPath = argv[++argi];
    } else if (arg == "--help" || arg == "-h") {
      usage(argv[0]);
      std::exit(0);
    } else {
      break;
    }
  }
  return cleanerPath;
}

bool bootstrapLegacyConfig(int argc, char **argv, int argi,
                           ConfigState &state) {
  if (argi + 3 >= argc) {
    return false;
  }
  JournalCache::CleanerSettings settings;
  settings.enabled = true;
  settings.journal = JournalCache::normalizeCacheRoot(argv[argi]);
  settings.highWatermark = std::stoull(argv[argi + 1]);
  settings.lowWatermark = std::stoull(argv[argi + 2]);
  settings.interval = static_cast<unsigned>(std::stoul(argv[argi + 3]));
  settings.configPoll = 2;

  state.path = JournalCache::defaultCleanerPath(settings.journal);
  if (!JournalCache::saveCleanerFile(state.path, settings)) {
    std::cerr << "xjccleand: failed to write " << state.path << "\n";
    return false;
  }
  state.settings = settings;
  state.lastWrite = fileWriteTime(state.path);
  state.hasWrite = state.lastWrite != std::chrono::system_clock::time_point{};
  std::cout << "xjccleand: wrote bootstrap config " << state.path << "\n";
  return true;
}

} // namespace

int main(int argc, char *argv[]) {
  int argi = 1;
  std::string journal;
  std::string cleanerPath = resolveCleanerPath(argc, argv, argi, journal);

  ConfigState state;
  if (argi < argc && cleanerPath.empty()) {
    if (!bootstrapLegacyConfig(argc, argv, argi, state)) {
      usage(argv[0]);
      return 1;
    }
  } else {
    if (cleanerPath.empty()) {
      std::cerr << "xjccleand: cleaner path not set; use --journal, "
                   "--cleaner, or XRD_JOURNALCACHE_CACHE\n";
      usage(argv[0]);
      return 1;
    }
    state.path = cleanerPath;
    if (!loadConfigState(state)) {
      std::cerr << "xjccleand: unable to read cleaner config: " << cleanerPath
                << "\n";
      std::cerr << "xjccleand: run xjcd init or create the file with xjc cleaner\n";
      return 1;
    }
  }

  std::cout << "xjccleand: watching " << state.path << std::endl;

  while (true) {
    reloadConfigIfChanged(state);

    if (state.settings.enabled) {
      std::string error;
      if (!JournalCache::validateCleanerSettings(state.settings, error)) {
        printCurrentTime();
        std::cerr << "Invalid cleaner config: " << error << std::endl;
      } else {
        const uint64_t low =
            JournalCache::effectiveLowWatermark(state.settings);
        cleanDirectory(state.settings.journal,
                       static_cast<long long>(state.settings.highWatermark),
                       static_cast<long long>(low));
      }
    } else {
      printCurrentTime();
      std::cout << "Cleaner disabled in config; waiting." << std::endl;
    }

    const unsigned interval =
        state.settings.interval > 0 ? state.settings.interval : 60;
    const unsigned poll =
        state.settings.configPoll > 0 ? state.settings.configPoll : 2;
    sleepWithConfigPoll(interval, poll, state);
  }

  return 0;
}
