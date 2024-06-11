//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/*----------------------------------------------------------------------------*/
#include "cleaner/Cleaner.hh"
#include <sys/statfs.h>
/*----------------------------------------------------------------------------*/

namespace fs = std::filesystem;
namespace JCache {
/*----------------------------------------------------------------------------*/

//----------------------------------------------------------------------------
// @brief getLastAccessTime() returns the last access time of a file.
//
// @param  filePath - path to the file
// @return last access time of the file in seconds
//----------------------------------------------------------------------------
time_t Cleaner::getLastAccessTime(const fs::path &filePath) {
  struct stat fileInfo;
  if (stat(filePath.c_str(), &fileInfo) != 0) {
    return -1; // Error occurred
  }
  return fileInfo.st_atime;
}

//----------------------------------------------------------------------------
// @brief getDirectorySize() returns the total size of all files in a
//        directory.
//
// @param  directory - path to the directory
// @return total size of all files in the directory in bytes
//----------------------------------------------------------------------------
long long Cleaner::getDirectorySize(const fs::path &directory, bool scan) {

  long long totalSize = 0;
  if (scan) {
    for (const auto &entry : fs::recursive_directory_iterator(directory)) {
      if (stopFlag.load()) {
        return 0;
      }
      if (fs::is_regular_file(entry)) {
        totalSize += fs::file_size(entry);
      }
    }
  } else {
    struct statfs stat;

    if (statfs(directory.c_str(), &stat) != 0) {
      mLog->Error(1,"JCache:Cleaner: failed to get directory size using statfs.");
      return 0; 
    }
  }
  return totalSize;
}

//----------------------------------------------------------------------------
// @brief getFilesByAccessTime() returns a list of files sorted by their
//        last access time.
//
// @param  directory - path to the directory
// @return list of files sorted by their last access time
//----------------------------------------------------------------------------
std::vector<std::pair<long long, fs::path>>
Cleaner::getFilesByAccessTime(const fs::path &directory) {
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

//----------------------------------------------------------------------------
// @brief cleanDirectory() deletes files from a directory that are older than
//        a given threshold.
//
// @param  directory - path to the directory
// @param  highWatermark - threshold for high watermark in bytes
// @param  lowWatermark - threshold for low watermark in bytes
// @return none
//----------------------------------------------------------------------------
void Cleaner::cleanDirectory(const fs::path &directory, long long highWatermark,
                             long long lowWatermark) {
  long long currentSize = getDirectorySize(directory);
  if (currentSize <= highWatermark) {
    /*----------------------------------------------------------------------------*/
    mLog->Info(1,"JCache:Cleaner: Directory size is within the limit (%lu/%lu). No action needed.",
    currentSize, highWatermark);
    return;
  }

  auto files = getFilesByAccessTime(directory);

  for (const auto &[accessTime, filePath] : files) {
    if (stopFlag.load()) {
      return;
    }
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
      if (ec) {
        mLog->Error(1, "JCache::Cleaner: error deleting directory '%s'", parentDir.c_str());
      }
      mLog->Info(1, "JCache:Cleaner : deleted '%s' (Size: %lu bytes)", filePath.c_str(),
      fileSize);
    } catch (const std::exception &e) {
      mLog->Error(1,"JCache::Cleaner error deleting '%'", filePath.c_str());
    }
  }
}
} // namespace JCache