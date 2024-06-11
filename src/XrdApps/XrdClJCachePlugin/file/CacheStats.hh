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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>

#include <string>
#include <thread>
#include <vector>
#include <set>
#include <sys/time.h>
#include <sys/resource.h>
#include "file/XrdClJCacheFile.hh"



namespace JCache 
{
  //! structure for cache hit statistics 
  struct CacheStats {
    CacheStats(bool doe=false) :
      bytesRead(0),
      bytesReadV(0),
      bytesCached(0),
      bytesCachedV(0),
      readOps(0),
      readVOps(0),
      readVreadOps(0),
      nreadfiles(0),
      dumponexit(doe),
      peakrate(0)
    {
      // Get the current real time
      struct timeval now;
      gettimeofday(&now, nullptr);
      startTime = now.tv_sec + now.tv_usec / 1000000.0;
    }

    ~CacheStats() {
      if (dumponexit.load() && totaldatasize) {
	    using namespace std::chrono;
	    std::string jsonpath = XrdCl::JCacheFile::sJsonPath + "jcache.";
	    std::string name = getenv("XRD_APPNAME")?getenv("XRD_APPNAME"):"none"+std::string(".")+std::to_string(getpid());
	    jsonpath += name;
	    jsonpath += ".json";
	    XrdCl::JCacheFile::sStats.GetTimes();

	    XrdCl::JCacheFile::sStats.bytes_per_second = XrdCl::JCacheFile::sStats.bench.GetBins((int)(realTime));
	    XrdCl::JCacheFile::sStats.peakrate = *(std::max_element(XrdCl::JCacheFile::sStats.bytes_per_second.begin(), XrdCl::JCacheFile::sStats.bytes_per_second.end()));
	    if (realTime <1) {
	      XrdCl::JCacheFile::sStats.peakrate = ReadBytes() / realTime;
	    }
	    if (XrdCl::JCacheFile::sJsonPath.length()) {
	        XrdCl::JCacheFile::sStats.persistToJson(jsonpath, name);
	    }
	    if (XrdCl::JCacheFile::sEnableSummary) {
	        std::cerr << CacheStats::GlobalStats(XrdCl::JCacheFile::sStats);
	    }
	    std::vector<uint64_t> bins = XrdCl::JCacheFile::sStats.bench.GetBins(40);
	    JCache::Art art;
	    if (XrdCl::JCacheFile::sEnableSummary) {
	        art.drawCurve(bins, XrdCl::JCacheFile::sStats.bench.GetTimePerBin().count() / 1000000.0, realTime);
	    }
      }
    }

    static std::string bytesToHumanReadable(double bytes) {
      const char* suffixes[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
      const int numSuffixes = sizeof(suffixes) / sizeof(suffixes[0]);
      
      if (bytes == 0) return "0 B";
      
      int exp = std::min((int)(std::log(bytes) / std::log(1000)), numSuffixes - 1);
      double val = bytes / std::pow(1000, exp);
      std::ostringstream oss;
      oss << std::fixed << std::setprecision(2) << val << " " << suffixes[exp];
      return oss.str();
    }
    
    double HitRate() {
      auto n = this->bytesCached.load()+this->bytesRead.load();
      if (!n) return 100.0;      
      return 100.0*(this->bytesCached.load()) / n;
    }
    double HitRateV() {
      auto n = this->bytesCachedV.load()+this->bytesReadV.load();
      if (!n) return 100.0;      
      return 100.0*(this->bytesCachedV.load()) / n;
    }
    double CombinedHitRate() {
      auto n = (this->bytesCached.load()+this->bytesRead.load()+this->bytesCachedV.load()+this->bytesReadV.load());
      if (!n) return 100.0;
      return 100.0*(this->bytesCached.load()+this->bytesCachedV.load()) / n;
    }
    void AddUrl(const std::string& url) {
      std::lock_guard<std::mutex> guard(urlMutex);
      urls.insert(url);
    }
    bool HasUrl(const std::string& url) {
      std::lock_guard<std::mutex> guard(urlMutex);
      return urls.count(url);
    }
    double ReadBytes() {
      return (bytesRead.load()+bytesReadV.load() + bytesCached.load() + bytesCachedV.load());
    }
    
    double Used() {
      if (totaldatasize) {
	    return 100.0*(bytesRead.load()+bytesReadV.load() + bytesCached.load() + bytesCachedV.load()) / totaldatasize;
      } else {
	    return 100.0;
      }
    }

    size_t UniqueUrls() {
      std::lock_guard<std::mutex> guard(urlMutex);
      return urls.size();
    }

    void GetTimes() {
      struct rusage usage;
      struct timeval now;

      // Get the current real time
      gettimeofday(&now, nullptr);
      realTime = now.tv_sec + now.tv_usec / 1000000.0 - startTime;

      // Get resource usage
      getrusage(RUSAGE_SELF, &usage);

      // Get user and system time
      userTime = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1000000.0;
      sysTime = usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1000000.0;
    }

    void persistToJson(const std::string& path, const std::string& name) {
        std::ofstream outFile(path);
        if (!outFile.is_open()) {
            std::cerr << "error: failed to open JSON statistics file: " << path << std::endl;
            return;
        }

        outFile << "{\n";
        outFile << "  \"appname\": \"" << name << "\",\n";
        outFile << "  \"pid\": \"" << getpid() << "\",\n";
        outFile << "  \"bytesRead\": " << bytesRead.load() << ",\n";
        outFile << "  \"bytesReadV\": " << bytesReadV.load() << ",\n";
        outFile << "  \"bytesCached\": " << bytesCached.load() << ",\n";
        outFile << "  \"bytesCachedV\": " << bytesCachedV.load() << ",\n";
        outFile << "  \"readOps\": " << readOps.load() << ",\n";
        outFile << "  \"readVOps\": " << readVOps.load() << ",\n";
        outFile << "  \"readVreadOps\": " << readVreadOps.load() << ",\n";
        outFile << "  \"nreadfiles\": " << nreadfiles.load() << ",\n";
        outFile << "  \"totaldatasize\": " << totaldatasize.load() << ",\n";

        std::lock_guard<std::mutex> lock(urlMutex);
        outFile << "  \"urls\": [";
        for (auto it = urls.begin(); it != urls.end(); ++it) {
            if (it != urls.begin()) {
                outFile << ", ";
            }
            outFile << "\"" << *it << "\"";
        }
        outFile << "],\n";

        outFile << "  \"bytes_per_second\": [";
        for (size_t i = 0; i < bytes_per_second.size(); ++i) {
            if (i != 0) {
                outFile << ", ";
            }
            outFile << bytes_per_second[i];
        }
        outFile << "],\n";

        outFile << std::fixed << std::setprecision(6); // Set precision for double values

        outFile << "  \"userTime\": " << userTime.load() << ",\n";
        outFile << "  \"realTime\": " << realTime.load() << ",\n";
        outFile << "  \"sysTime\": " << sysTime.load() << ",\n";
        outFile << "  \"startTime\": " << startTime.load() << "\n";
        outFile << "}\n";

        outFile.close();
    }

    void AddToStats(CacheStats& gStats) {
        gStats.readOps += readOps.load();
        gStats.readVOps += readVOps.load();
        gStats.readVreadOps += readVreadOps.load();
        gStats.bytesRead += bytesRead.load();
        gStats.bytesReadV += bytesReadV.load();
        gStats.bytesCached += bytesCached.load();
        gStats.bytesCachedV += bytesCachedV.load();
        gStats.nreadfiles += 1;
    }

    static std::string GlobalStats(CacheStats& sStats) {
        std::ostringstream oss;
        oss << "# ----------------------------------------------------------- #" << std::endl;
        oss << "# JCache : cache combined hit rate  : " << std::fixed << std::setprecision(2) << sStats.CombinedHitRate() << " %" << std::endl;
        oss << "# JCache : cache read     hit rate  : " << std::fixed << std::setprecision(2) << sStats.HitRate() << " %" << std::endl;
        oss << "# JCache : cache readv    hit rate  : " << std::fixed << std::setprecision(2) << sStats.HitRateV() << " %" << std::endl;
        oss << "# ----------------------------------------------------------- #" << std::endl;
        oss << "# JCache : total bytes    read      : " << sStats.bytesRead.load()+sStats.bytesCached.load() << std::endl;
        oss << "# JCache : total bytes    readv     : " << sStats.bytesReadV.load()+sStats.bytesCachedV.load() << std::endl;
        oss << "# ----------------------------------------------------------- #" << std::endl;
        oss << "# JCache : total iops     read      : " << sStats.readOps.load() << std::endl;
        oss << "# JCache : total iops     readv     : " << sStats.readVOps.load() << std::endl;
        oss << "# JCache : total iops     readvread : " << sStats.readVreadOps.load() << std::endl;
        oss << "# ----------------------------------------------------------- #" << std::endl;
        oss << "# JCache : open files     read      : " << sStats.nreadfiles.load() << std::endl;
        oss << "# JCache : open unique f. read      : " << sStats.UniqueUrls() << std::endl;
        oss << "# ----------------------------------------------------------- #" << std::endl;
        oss << "# JCache : total unique files bytes : " << sStats.totaldatasize << std::endl;
        oss << "# JCache : total unique files size  : " << sStats.bytesToHumanReadable((double)sStats.totaldatasize) << std::endl;
        oss << "# JCache : percentage dataset read  : " << std::fixed << std::setprecision(2) << sStats.Used() << " %" << std::endl;
        oss << "# ----------------------------------------------------------- #" << std::endl;
        oss << "# JCache : app user time            : " << std::fixed << std::setprecision(2) << sStats.userTime << " s" << std::endl;
        oss << "# JCache : app real time            : " << std::fixed << std::setprecision(2) << sStats.realTime << " s" << std::endl;
        oss << "# JCache : app sys  time            : " << std::fixed << std::setprecision(2) << sStats.sysTime  << " s" << std::endl;
        oss << "# JCache : app acceleration         : " << std::fixed << std::setprecision(2) << sStats.userTime / sStats.realTime  << "x" << std::endl;
        oss << "# JCache : app readrate             : " << std::fixed << std::setprecision(2) << sStats.bytesToHumanReadable((sStats.ReadBytes()/sStats.realTime))  << "/s" << " [ peak (1s) " << sStats.bytesToHumanReadable(sStats.peakrate) << "/s ]" << std::endl;
        oss << "# ----------------------------------------------------------- #" << std::endl;

        return oss.str();
    }

    std::atomic<uint64_t> bytesRead;
    std::atomic<uint64_t> bytesReadV;
    std::atomic<uint64_t> bytesCached;
    std::atomic<uint64_t> bytesCachedV;
    std::atomic<uint64_t> readOps;
    std::atomic<uint64_t> readVOps;
    std::atomic<uint64_t> readVreadOps;
    std::atomic<uint64_t> nreadfiles;
    std::atomic<uint64_t> totaldatasize;
    std::atomic<bool>     dumponexit;
    std::set<std::string> urls;
    std::mutex            urlMutex;
    std::atomic<double>   userTime;
    std::atomic<double>   realTime;
    std::atomic<double>   sysTime;
    std::atomic<double>   startTime;
    JCache::TimeBench     bench;
    std::vector<uint64_t> bytes_per_second;
    std::atomic<double>   peakrate;
  }; // class CacheStats
} // namespace JCache
