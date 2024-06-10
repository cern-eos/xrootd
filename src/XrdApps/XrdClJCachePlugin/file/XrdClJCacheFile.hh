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
#include "XrdCl/XrdClPlugInInterface.hh"
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClLog.hh"
/*----------------------------------------------------------------------------*/
#include "cache/Journal.hh"
#include "vector/XrdClVectorCache.hh"
#include "handler/XrdClJCacheReadHandler.hh"
#include "handler/XrdClJCachePgReadHandler.hh"
#include "handler/XrdClJCacheReadVHandler.hh"
/*----------------------------------------------------------------------------*/
#include <atomic>
/*----------------------------------------------------------------------------*/

namespace XrdCl
{
//----------------------------------------------------------------------------
//! JCache file plugin
//! This XRootD Client Plugin provides a client side read cache.
//! There are two ways of caching, which can be configured individually:
//! - Read Journal Cache (journalling)
//! - Vector Read Cache (vector read responses are stored in binary blobs)
//----------------------------------------------------------------------------
class JCacheFile: public XrdCl::FilePlugIn
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  JCacheFile();
  JCacheFile(const std::string& url);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~JCacheFile();


  //----------------------------------------------------------------------------
  //! @brief Open a file by URL
  //! @param url URL of the file
  //! @param flags Open flags
  //! @param mode Access mode
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Open(const std::string& url,
                            OpenFlags::Flags flags,
                            Access::Mode mode,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief Close a file
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Close(ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief Stat a file
  //! @param force Force stat
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Stat(bool force,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief Read
  //! @param offset Offset in bytes
  //! @param size Size in bytes
  //! @param buffer Buffer to read into
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Read(uint64_t offset,
                            uint32_t size,
                            void* buffer,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief Write
  //! @param offset Offset in bytes
  //! @param size Size in bytes
  //! @param buffer Buffer to write from
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Write(uint64_t offset,
                             uint32_t size,
                             const void* buffer,
                             ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief Sync
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Sync(ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief Truncate
  //! @param size Size in bytes
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Truncate(uint64_t size,
                                ResponseHandler* handler,
                                uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief VectorRead
  //! @param chunks Chunks to read
  //! @param buffer Buffer to read into
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus VectorRead(const ChunkList& chunks,
                                  void* buffer,
                                  ResponseHandler* handler,
                                  uint16_t timeout);

  //------------------------------------------------------------------------
  //! @brief PgRead
  //! @param offset Offset in bytes
  //! @param size Size in bytes
  //! @param buffer Buffer to read into
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //------------------------------------------------------------------------
  virtual XRootDStatus PgRead( uint64_t         offset,
                               uint32_t         size,
                               void            *buffer,
                               ResponseHandler *handler,
                               uint16_t         timeout ) override;

  //------------------------------------------------------------------------
  //! @brief PgWrite
  //! @param offset Offset in bytes
  //! @param nbpgs Number of pages
  //! @param buffer Buffer to write from
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //------------------------------------------------------------------------
  virtual XRootDStatus PgWrite( uint64_t               offset,
                                uint32_t               nbpgs,
                                const void            *buffer,
                                std::vector<uint32_t> &cksums,
                                ResponseHandler       *handler,
                                uint16_t               timeout ) override;


  //------------------------------------------------------------------------
  //! @brief Fcntl
  //! @param arg Argument
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //------------------------------------------------------------------------
  virtual XRootDStatus Fcntl(const Buffer& arg,
                             ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief Visa
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Visa(ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! @brief check if file is open
  //----------------------------------------------------------------------------
  virtual bool IsOpen() const;


  //----------------------------------------------------------------------------
  //! @see XrdCl::File::SetProperty
  //----------------------------------------------------------------------------
  virtual bool SetProperty(const std::string& name,
                           const std::string& value);


  //----------------------------------------------------------------------------
  //! @see XrdCl::File::GetProperty
  //----------------------------------------------------------------------------
  virtual bool GetProperty(const std::string& name,
                           std::string& value) const;


  //----------------------------------------------------------------------------
  //! @brief validate the local cache
  //----------------------------------------------------------------------------
  inline bool IsValid()
  {
    return true;
  }

  //----------------------------------------------------------------------------
  //! @brief set the local cache path and enable/disable journal/vector caches
  //! @param path Local cache path
  //! @param journal Enable/disable journal cache
  //! @param vector Enable/disable vector cache
  //----------------------------------------------------------------------------
  
  static void SetCache(const std::string& path) { sCachePath = path; }
  static void SetJournal(const bool& value) { sEnableJournalCache = value; }
  static void SetVector(const bool& value) { sEnableVectorCache = value; }

  //----------------------------------------------------------------------------
  //! @brief static members pointing to cache settings
  //----------------------------------------------------------------------------
  static std::string sCachePath;
  static bool sEnableVectorCache;
  static bool sEnableJournalCache;
  static JournalManager sJournalManager;
  
  //----------------------------------------------------------------------------
  //! @brief log cache hit statistics
  //----------------------------------------------------------------------------
  void LogStats() {
    mLog->Info(1, "JCache : read:readv-ops:readv-read-ops: %lu:%lu:%lus hit-rate: total [read/readv]=%.02f%% [%.02f%%/%.02f%%] remote-bytes-read/readv: %lu / %lu cached-bytes-read/readv: %lu / %lu",
                pStats.readOps.load(),
                pStats.readVOps.load(),
                pStats.readVreadOps.load(),
                pStats.CombinedHitRate(),
                pStats.HitRate(),
                pStats.HitRateV(),
                pStats.bytesRead.load(), 
                pStats.bytesReadV.load(), 
                pStats.bytesCached.load(),
                pStats.bytesCachedV.load());
  }

  void AddToGlobalStats() {
    sStats.readOps += pStats.readOps.load();
    sStats.readVOps += pStats.readVOps.load();
    sStats.readVreadOps += pStats.readVreadOps.load();
    sStats.bytesRead += pStats.bytesRead.load();
    sStats.bytesReadV += pStats.bytesReadV.load();
    sStats.bytesCached += pStats.bytesCached.load();
    sStats.bytesCachedV += pStats.bytesCachedV.load();
    sStats.nreadfiles += 1;
  }

  static std::string GlobalStats() {
    std::ostringstream oss;
    oss << "# ----------------------------------------------------------- #" << std::endl;
    oss << "# JCache : cache combined hit rate  : " << std::fixed << std::setprecision(2) << sStats.CombinedHitRate() << " %%" << std::endl;
    oss << "# JCache : cache read     hit rate  : " << std::fixed << std::setprecision(2) << sStats.HitRate() << " %%" << std::endl;
    oss << "# JCache : cache readv    hit rate  : " << std::fixed << std::setprecision(2) << sStats.HitRateV() << " %%" << std::endl;
    oss << "# ----------------------------------------------------------- #" << std::endl;
    oss << "# JCache : total bytes    read      : " << sStats.bytesRead.load()+sStats.bytesCached.load() << std::endl;
    oss << "# JCache : total bytes    readv     : " << sStats.bytesReadV.load()+sStats.bytesCachedV.load() << std::endl;
    oss << "# ----------------------------------------------------------- #" << std::endl;
    oss << "# JCache : total iops     read      : " << sStats.readOps.load() << std::endl;
    oss << "# JCache : total iops     readv     : " << sStats.readVOps.load() << std::endl;
    oss << "# JCache : total iops     readvread : " << sStats.readVreadOps.load() << std::endl;
    oss << "# ----------------------------------------------------------- #" << std::endl;
    oss << "# JCache : open files     read      : " << sStats.nreadfiles.load() << std::endl;
    oss << "# JCache : open unique    read      : " << sStats.UniqueUrls() << std::endl;
    oss << "# ----------------------------------------------------------- #" << std::endl;
    return oss.str();
  }
  //! structure about cache hit statistics 
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
      dumponexit(doe)
    {}

    ~CacheStats() {
      if (dumponexit.load()) {
	std::cerr << GlobalStats();
      }
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

    size_t UniqueUrls() {
      std::lock_guard<std::mutex> guard(urlMutex);
      return urls.size();
    }

    std::atomic<uint64_t> bytesRead;
    std::atomic<uint64_t> bytesReadV;
    std::atomic<uint64_t> bytesCached;
    std::atomic<uint64_t> bytesCachedV;
    std::atomic<uint64_t> readOps;
    std::atomic<uint64_t> readVOps;
    std::atomic<uint64_t> readVreadOps;
    std::atomic<uint64_t> nreadfiles;
    std::atomic<bool>     dumponexit;
    std::set<std::string> urls;
    std::mutex            urlMutex;
  };
private:

  //! @brief attach for read
  bool AttachForRead();

  //! @brief atomic variable to track if file is attached for read
  std::atomic<bool> mAttachedForRead;
  //! @brief mutex protecting cache attach procedure
  std::mutex mAttachMutex;
  //! @brief open flags used in Open
  OpenFlags::Flags mFlags;
  //! @brief boolean to track if file is open
  bool mIsOpen;
  //! @brief pointer to the remote file
  XrdCl::File* pFile;
  //! @brief URL of the remote file
  std::string pUrl;
  //! @brief instance of a local journal for this file
  std::shared_ptr<Journal> pJournal;
  //! @brief path to the journal of this file
  std::string pJournalPath;
  //! @brief pointer to logging object
  Log* mLog;

  //! @brief cache hit statistics
  CacheStats pStats;

  //! @brief global plugin cache hit statistics
  static CacheStats sStats;
};

} // namespace XrdCl
