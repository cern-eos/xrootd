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

#pragma once
/*----------------------------------------------------------------------------*/
#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClLog.hh"
/*----------------------------------------------------------------------------*/
namespace fs = std::filesystem;

namespace JCache {}
namespace JCache {
class Cleaner {
public:
  // Constructor to initialize the configuration variables
  Cleaner(double lowWatermark, double highWatermark, const std::string &path,
          bool scan, size_t interval)
      : lowWatermark(lowWatermark), highWatermark(highWatermark), subtree(path),
        scan(scan), interval(interval), stopFlag(false) {
          mLog = XrdCl::DefaultEnv::GetLog();
        }

   Cleaner()
      : lowWatermark(0), highWatermark(0), subtree(""),
        scan(true), interval(60), stopFlag(false) {
          mLog = XrdCl::DefaultEnv::GetLog();
        }

  // Method to start the cleaning process in a separate thread
  void run() {
    stopFlag = false;
    cleanerThread = std::thread(&Cleaner::cleanLoop, this);
    cleanerThread.detach();
  }

  // Method to stop the cleaning process and join the thread
  void stop() {
    stopFlag = true;
    if (cleanerThread.joinable()) {
      cleanerThread.join();
    }
  }

  // Destructor to ensure the thread is properly joined
  ~Cleaner() { stop(); }

  // Method to Define Cleaner size
  void SetSize(uint64_t size, const std::string& path) {
    stop();
    if (size > 1024ll*1024ll*1024ll) {
      subtree = path;
      highWatermark = size;
      lowWatermark = size * 0.9;
      run();
    } else {
      mLog->Error(1, "JCache:Cleaner : the size given to the cleaner is less than 1GB - cleaning is disabled!");
    }
  }

  // Method to set the scan option (true means scan, false means don't scan but use statfs!)
  void SetScan(bool sc) {
    scan = sc;
  }

private:
  // Private methods
  time_t getLastAccessTime(const fs::path &filePath);
  long long getDirectorySize(const fs::path &directory, bool scan = true);
  std::vector<std::pair<long long, fs::path>>
  getFilesByAccessTime(const fs::path &directory);
  void cleanDirectory(const fs::path &directory, long long highWatermark,
                      long long lowWatermark);

  // Configuration variables
  double lowWatermark;
  double highWatermark;
  fs::path subtree;
  std::atomic<bool> scan;
  size_t interval;

  // Thread-related variables
  std::thread cleanerThread;
  std::atomic<bool> stopFlag;

  // XRootD logger
  XrdCl::Log *mLog;

  // The method that runs in a loop, calling the clean method every interval
  // seconds
  void cleanLoop() {

    while (!stopFlag.load()) {
      auto start = std::chrono::steady_clock::now();

      cleanDirectory(subtree, highWatermark, lowWatermark);

      auto end = std::chrono::steady_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::seconds>(end - start);

      if ( (size_t)duration.count() < interval) {
        auto s = std::chrono::seconds(interval) - duration;
        std::this_thread::sleep_for(s);
      }
    }
  }

  // The clean method to be called periodically
  void clean() {}
};
} // namespace JCache
