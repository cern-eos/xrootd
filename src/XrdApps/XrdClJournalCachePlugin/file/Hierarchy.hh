//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
#pragma once

#include <filesystem>
#include <string>
#include <sys/stat.h>

namespace JournalCache {

static bool makeHierarchy(const std::string &path) {
  try {
    const std::filesystem::path dirPath(path);
    const std::filesystem::path parent = dirPath.parent_path();
    if (parent.empty()) {
      return true;
    }
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return false;
    }
    (void)chmod(parent.c_str(), 0755);
    return true;
  } catch (const std::exception &) {
    return false;
  }
}

} // namespace JournalCache
