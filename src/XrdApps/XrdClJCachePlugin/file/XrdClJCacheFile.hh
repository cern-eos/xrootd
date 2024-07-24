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
#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClPlugInInterface.hh"
/*----------------------------------------------------------------------------*/
#include "cache/Journal.hh"
#include "cleaner/Cleaner.hh"
#include "file/Art.hh"
#include "file/TimeBench.hh"
#include "handler/XrdClJCacheOpenHandler.hh"
#include "handler/XrdClJCachePgReadHandler.hh"
#include "handler/XrdClJCacheReadHandler.hh"
#include "handler/XrdClJCacheReadVHandler.hh"
#include "vector/XrdClVectorCache.hh"
/*----------------------------------------------------------------------------*/
#include <atomic>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/time.h>
/*----------------------------------------------------------------------------*/

namespace JCache {
class CacheStats;
}

namespace XrdCl {
//----------------------------------------------------------------------------
//! JCache file plugin
//! This XRootD Client Plugin provides a client side read cache.
//! There are two ways of caching, which can be configured individually:
//! - Read Journal Cache (journalling)
//! - Vector Read Cache (vector read responses are stored in binary blobs)
//----------------------------------------------------------------------------
class JCacheFile : public XrdCl::FilePlugIn {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  JCacheFile();
  JCacheFile(const std::string &url);

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
  virtual XRootDStatus Open(const std::string &url, OpenFlags::Flags flags,
                            Access::Mode mode, ResponseHandler *handler,
                            uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief Close a file
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Close(ResponseHandler *handler, uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief Stat a file
  //! @param force Force stat
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Stat(bool force, ResponseHandler *handler,
                            uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief Read
  //! @param offset Offset in bytes
  //! @param size Size in bytes
  //! @param buffer Buffer to read into
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Read(uint64_t offset, uint32_t size, void *buffer,
                            ResponseHandler *handler, uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief Write
  //! @param offset Offset in bytes
  //! @param size Size in bytes
  //! @param buffer Buffer to write from
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Write(uint64_t offset, uint32_t size, const void *buffer,
                             ResponseHandler *handler, uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief Sync
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Sync(ResponseHandler *handler, uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief Truncate
  //! @param size Size in bytes
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Truncate(uint64_t size, ResponseHandler *handler,
                                uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief VectorRead
  //! @param chunks Chunks to read
  //! @param buffer Buffer to read into
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus VectorRead(const ChunkList &chunks, void *buffer,
                                  ResponseHandler *handler, uint16_t timeout);

  //------------------------------------------------------------------------
  //! @brief PgRead
  //! @param offset Offset in bytes
  //! @param size Size in bytes
  //! @param buffer Buffer to read into
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //------------------------------------------------------------------------
  virtual XRootDStatus PgRead(uint64_t offset, uint32_t size, void *buffer,
                              ResponseHandler *handler,
                              uint16_t timeout) override;

  //------------------------------------------------------------------------
  //! @brief PgWrite
  //! @param offset Offset in bytes
  //! @param nbpgs Number of pages
  //! @param buffer Buffer to write from
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //------------------------------------------------------------------------
  virtual XRootDStatus PgWrite(uint64_t offset, uint32_t nbpgs,
                               const void *buffer,
                               std::vector<uint32_t> &cksums,
                               ResponseHandler *handler,
                               uint16_t timeout) override;

  //------------------------------------------------------------------------
  //! @brief Fcntl
  //! @param arg Argument
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //------------------------------------------------------------------------
  virtual XRootDStatus Fcntl(const Buffer &arg, ResponseHandler *handler,
                             uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief Visa
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Visa(ResponseHandler *handler, uint16_t timeout);

  //----------------------------------------------------------------------------
  //! @brief check if file is open
  //----------------------------------------------------------------------------
  virtual bool IsOpen() const;

  //----------------------------------------------------------------------------
  //! @see XrdCl::File::SetProperty
  //----------------------------------------------------------------------------
  virtual bool SetProperty(const std::string &name, const std::string &value);

  //----------------------------------------------------------------------------
  //! @see XrdCl::File::GetProperty
  //----------------------------------------------------------------------------
  virtual bool GetProperty(const std::string &name, std::string &value) const;

  //----------------------------------------------------------------------------
  //! @brief validate the local cache
  //----------------------------------------------------------------------------
  inline bool IsValid() { return true; }

  //----------------------------------------------------------------------------
  //! @brief set the local cache path
  //! @param path Local cache path
  //----------------------------------------------------------------------------
  //! @brief set the local cache path and enable/disable journal/vector caches
  //! @param path Local cache path
  //! @param journal Enable/disable journal cache
  //! @param vector Enable/disable vector cache
  //----------------------------------------------------------------------------

  static void SetCache(const std::string &path) { sCachePath = path; }
  static void SetJournal(const bool &value) { sEnableJournalCache = value; }
  static void SetVector(const bool &value) { sEnableVectorCache = value; }
  static void SetJsonPath(const std::string &path) { sJsonPath = path; }
  static void SetSummary(const bool &value) { sEnableSummary = value; }
  static void SetBypass(const bool &value) { sEnableBypass = value; }
  static void SetSize(uint64_t size) { sCleaner.SetSize(size, sCachePath); }
  static void SetAsync(bool async) { sOpenAsync = async; }
  static void SetFlatHierarchy(bool value) { sFlatHierarchy = value; }

  //----------------------------------------------------------------------------
  //! @brief static members pointing to cache settings
  //----------------------------------------------------------------------------
  static std::string sCachePath;
  static std::string sJsonPath;
  static bool sEnableVectorCache;
  static bool sEnableJournalCache;
  static bool sEnableBypass;
  static bool sEnableSummary;
  static bool sOpenAsync;
  static bool sFlatHierarchy;

  static JournalManager sJournalManager;

  //! @brief set stats interval in seconds
  static void SetStatsInterval(uint64_t interval);

  //----------------------------------------------------------------------------
  //! @brief log cache hit statistics
  //----------------------------------------------------------------------------
  void LogStats();

  //! @brief global plugin cache hit statistics
  static JCache::CacheStats sStats;

  //! @brief cleaner instance
  static JCache::Cleaner sCleaner;

  enum State { CLOSED = 0, OPENING, OPEN, FAILED };
  //! @brief openstate
  std::atomic<int> mOpenState;

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
  //! @brief async open handler
  JCacheOpenHandler *pOpenHandler;
  //! @brief pointer to the remote file
  XrdCl::File *pFile;
  //! @brief boolean if file open is async
  bool mOpenAsync;
  //! @brief URL of the remote file
  std::string pUrl;
  //! @brief instance of a local journal for this file
  std::shared_ptr<Journal> pJournal;
  //! @brief path to the journal of this file
  std::string pJournalPath;
  //! @brief pointer to logging object
  Log *mLog;

  //! @brief cache hit statistics
  JCache::CacheStats *pStats;
};

} // namespace XrdCl
