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
#include "file/CacheHeaders.hh"
#include "file/OriginAllowlist.hh"
#include "file/PolicyConfig.hh"
#include "file/PolicyRuntime.hh"
#include "file/TimeBench.hh"
#include "handler/XrdClJournalCacheOpenHandler.hh"
#include "handler/XrdClJournalCachePgReadHandler.hh"
#include "handler/XrdClJournalCacheReadHandler.hh"
#include "handler/XrdClJournalCacheReadVHandler.hh"
/*----------------------------------------------------------------------------*/
#include <atomic>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <string>
#include <sys/time.h>
#include <sys/syscall.h>
/*----------------------------------------------------------------------------*/

namespace JournalCache {
struct CacheStats;
}

namespace XrdCl {
//----------------------------------------------------------------------------
//! JournalCache file plugin
//! This XRootD Client Plugin provides a client side read cache
//! implemented as a per-file journal.
//----------------------------------------------------------------------------
class JournalCacheFile : public XrdCl::FilePlugIn {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  JournalCacheFile();
  JournalCacheFile(const std::string &url);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~JournalCacheFile();

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
                            time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief Close a file
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Close(ResponseHandler *handler, time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief Stat a file
  //! @param force Force stat
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Stat(bool force, ResponseHandler *handler,
                            time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief Read
  //! @param offset Offset in bytes
  //! @param size Size in bytes
  //! @param buffer Buffer to read into
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Read(uint64_t offset, uint32_t size, void *buffer,
                            ResponseHandler *handler, time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief Write
  //! @param offset Offset in bytes
  //! @param size Size in bytes
  //! @param buffer Buffer to write from
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Write(uint64_t offset, uint32_t size, const void *buffer,
                             ResponseHandler *handler, time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief Sync
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Sync(ResponseHandler *handler, time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief Truncate
  //! @param size Size in bytes
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Truncate(uint64_t size, ResponseHandler *handler,
                                time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief VectorRead
  //! @param chunks Chunks to read
  //! @param buffer Buffer to read into
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus VectorRead(const ChunkList &chunks, void *buffer,
                                  ResponseHandler *handler, time_t timeout) override;

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
                              time_t timeout) override;

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
                               time_t timeout) override;

  //------------------------------------------------------------------------
  //! @brief Fcntl
  //! @param arg Argument
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //------------------------------------------------------------------------
  virtual XRootDStatus Fcntl(const Buffer &arg, ResponseHandler *handler,
                             time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief Visa
  //! @param handler Response handler
  //! @param timeout Timeout in seconds
  //----------------------------------------------------------------------------
  virtual XRootDStatus Visa(ResponseHandler *handler, time_t timeout) override;

  //----------------------------------------------------------------------------
  //! @brief check if file is open
  //----------------------------------------------------------------------------
  virtual bool IsOpen() const override; 

  //----------------------------------------------------------------------------
  //! @see XrdCl::File::SetProperty
  //----------------------------------------------------------------------------
  virtual bool SetProperty(const std::string &name, const std::string &value) override;

  //----------------------------------------------------------------------------
  //! @see XrdCl::File::GetProperty
  //----------------------------------------------------------------------------
  virtual bool GetProperty(const std::string &name, std::string &value) const override;

  //----------------------------------------------------------------------------
  //! @brief validate the local cache
  //----------------------------------------------------------------------------
  inline bool IsValid() { return true; }

  //----------------------------------------------------------------------------
  //! @brief set the local cache path
  //! @param path Local cache path
  //----------------------------------------------------------------------------
  static void SetCache(const std::string &path) { sCachePath = path; }
  static void SetJournal(const bool &value) { sEnableJournalCache = value; }
  static void SetJournalCrc(const bool &value) { sEnableJournalCrc = value; }
  static void SetJsonPath(const std::string &path) { sJsonPath = path; }
  static void SetBasePath(const std::string &path) { sBasePath = path; }
  static void SetSummary(const bool &value) { sEnableSummary = value; }
  static void SetBypass(const bool &value) { sEnableBypass = value; }
  static void SetSize(uint64_t size) { sCleaner.SetSize(size, sCachePath); }
  static void SetAsync(bool async) { sOpenAsync = async; }
  static void SetFlatHierarchy(bool value) { sFlatHierarchy = value; }
  static void SetThreadConnectionDemultiplexing(bool value) {
    sThreadConnectionDemultiplexing = value;
  }
  static void SetMultiOriginUnwrap(bool value) { sMultiOriginUnwrap = value; }
  static void SetAllowedOrigins(const std::string &patterns) {
    sOriginAllowlist.clear();
    sOriginAllowlist.addPatternsFromCsv(patterns);
  }
  static void AddAllowedOriginPattern(const std::string &pattern) {
    sOriginAllowlist.addPattern(pattern);
  }
  static void SetExternalRedirects(const std::string &rules) {
    sExternalRedirect.clear();
    sExternalRedirect.addRulesFromCsv(rules);
  }
  static void AddExternalRedirectRule(const std::string &rule) {
    sExternalRedirect.addRuleFromSpec(rule);
  }
  static std::string ResolveExternalRedirect(const XrdCl::URL &url);
  static JournalCache::PolicySettings activePolicySettings();
  static bool policyBypass();

  //----------------------------------------------------------------------------
  //! @brief static members pointing to cache settings
  //----------------------------------------------------------------------------
  static std::string sCachePath;
  static std::string sJsonPath;
  static std::string sBasePath;
  static bool sEnableJournalCache;
  static bool sEnableJournalCrc;
  static bool sEnableBypass;
  static bool sEnableSummary;
  static bool sOpenAsync;
  static bool sFlatHierarchy;
  static bool sThreadConnectionDemultiplexing;
  static bool sMultiOriginUnwrap;
  static JournalCache::OriginAllowlist sOriginAllowlist;
  static JournalCache::ExternalRedirect sExternalRedirect;

  static JournalManager sJournalManager;

  //! @brief set stats interval in seconds
  static void SetStatsInterval(uint64_t interval);

  //----------------------------------------------------------------------------
  //! @brief log cache hit statistics
  //----------------------------------------------------------------------------
  void LogStats();

  //! @brief global plugin cache hit statistics
  static JournalCache::CacheStats sStats;

  //! @brief cleaner instance
  static JournalCache::Cleaner sCleaner;

  enum State { CLOSED = 0, OPENING, OPEN, FAILED };
  //! @brief openstate
  std::atomic<int> mOpenState;

private:
  //! @brief attach for read
  bool AttachForRead();

  //! @brief apply cache header policy after journal attach
  void ApplyCacheHeaderPolicy(const XrdCl::StatInfo *statInfo = nullptr);

  //! @brief return true when cached reads are allowed this session
  bool CanServeFromJournalCache() const;

  //! @brief return true when journal caching is bypassed for this file
  bool bypassCache() const { return policyBypass() || mFileBypass; }

  //! @brief atomic variable to track if file is attached for read
  std::atomic<bool> mAttachedForRead;
  //! @brief mutex protecting cache attach procedure
  std::mutex mAttachMutex;
  //! @brief open flags used in Open
  OpenFlags::Flags mFlags;
  //! @brief boolean to track if file is open
  bool mIsOpen;
  //! @brief async open handler
  JournalCacheOpenHandler *pOpenHandler;
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
  //! @brief cache headers supplied via CGI on open
  JournalCache::CacheHeaders mOpenCacheHeaders;
  //! @brief validation headers supplied via CGI on open
  JournalCache::CacheValidators mCacheValidators;
  //! @brief skip caching when Cache-Control: no-store is present
  bool mNoStoreCache = false;
  //! @brief per-file bypass via xrd.journalcache.bypass CGI
  bool mFileBypass = false;
  //! @brief require origin stat before serving cached data (no-cache)
  bool mMustRevalidate = false;
  //! @brief set after a successful stat confirms the journal for this session
  bool mRevalidatedThisSession = false;
  //! @brief pointer to logging object
  Log *mLog;

  //! @brief cache hit statistics
  JournalCache::CacheStats *pStats;
};

} // namespace XrdCl
