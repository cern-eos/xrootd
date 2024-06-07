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
#include "handler/XrdClJCacheReadVHandler.hh"
/*----------------------------------------------------------------------------*/
#include <atomic>
/*----------------------------------------------------------------------------*/

namespace XrdCl
{
//----------------------------------------------------------------------------
//! RAIN file plugin
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
  //! Open
  //----------------------------------------------------------------------------
  virtual XRootDStatus Open(const std::string& url,
                            OpenFlags::Flags flags,
                            Access::Mode mode,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Close
  //----------------------------------------------------------------------------
  virtual XRootDStatus Close(ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Stat
  //----------------------------------------------------------------------------
  virtual XRootDStatus Stat(bool force,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Read
  //----------------------------------------------------------------------------
  virtual XRootDStatus Read(uint64_t offset,
                            uint32_t size,
                            void* buffer,
                            ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Write
  //----------------------------------------------------------------------------
  virtual XRootDStatus Write(uint64_t offset,
                             uint32_t size,
                             const void* buffer,
                             ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Sync
  //----------------------------------------------------------------------------
  virtual XRootDStatus Sync(ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Truncate
  //----------------------------------------------------------------------------
  virtual XRootDStatus Truncate(uint64_t size,
                                ResponseHandler* handler,
                                uint16_t timeout);


  //----------------------------------------------------------------------------
  //! VectorRead
  //----------------------------------------------------------------------------
  virtual XRootDStatus VectorRead(const ChunkList& chunks,
                                  void* buffer,
                                  ResponseHandler* handler,
                                  uint16_t timeout);

  //------------------------------------------------------------------------
  //! PgRead
  //------------------------------------------------------------------------
  virtual XRootDStatus PgRead( uint64_t         offset,
                               uint32_t         size,
                               void            *buffer,
                               ResponseHandler *handler,
                               uint16_t         timeout ) override;

  //------------------------------------------------------------------------
  //! PgWrite
  //------------------------------------------------------------------------
  virtual XRootDStatus PgWrite( uint64_t               offset,
                                uint32_t               nbpgs,
                                const void            *buffer,
                                std::vector<uint32_t> &cksums,
                                ResponseHandler       *handler,
                                uint16_t               timeout ) override;


  //------------------------------------------------------------------------
  //! Fcntl
  //------------------------------------------------------------------------
  virtual XRootDStatus Fcntl(const Buffer& arg,
                             ResponseHandler* handler,
                             uint16_t timeout);


  //----------------------------------------------------------------------------
  //! Visa
  //----------------------------------------------------------------------------
  virtual XRootDStatus Visa(ResponseHandler* handler,
                            uint16_t timeout);


  //----------------------------------------------------------------------------
  //! IsOpen
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
  //! validate the local cache
  //----------------------------------------------------------------------------
  inline bool IsValid()
  {
    return true;
  }

  //----------------------------------------------------------------------------
  //! set the local cache path
  //----------------------------------------------------------------------------
  
  static void SetCache(const std::string& path) { sCachePath = path; }
  static void SetJournal(const bool& value) { sEnableJournalCache = value; }
  static void SetVector(const bool& value) { sEnableVectorCache = value; }

  //----------------------------------------------------------------------------
  //! get the local cache path
  //----------------------------------------------------------------------------
  static std::string sCachePath;
  static bool sEnableVectorCache;
  static bool sEnableJournalCache;

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
  //! structure about cache hit statistics 
  struct CacheStats {
    CacheStats() :
      bytesRead(0),
      bytesReadV(0),
      bytesCached(0),
      bytesCachedV(0),
      readOps(0),
      readVOps(0),
      readVreadOps(0)
    {}

    double HitRate() {
      return 100.0*(this->bytesCached.load()+1) /(this->bytesCached.load()+this->bytesRead.load()+1);
    }
    double HitRateV() {
      return 100.0*(this->bytesCachedV.load()+1) /(this->bytesCachedV.load()+this->bytesReadV.load()+1);
    }
    double CombinedHitRate() {
      return 100.0*(this->bytesCached.load()+this->bytesCachedV.load()+1) /(this->bytesCached.load()+this->bytesRead.load()+this->bytesCachedV.load()+this->bytesReadV.load()+1);
    }

    std::atomic<uint64_t> bytesRead;
    std::atomic<uint64_t> bytesReadV;
    std::atomic<uint64_t> bytesCached;
    std::atomic<uint64_t> bytesCachedV;
    std::atomic<uint64_t> readOps;
    std::atomic<uint64_t> readVOps;
    std::atomic<uint64_t> readVreadOps;
  };
private:

  bool AttachForRead();

  std::atomic<bool> mAttachedForRead;
  std::mutex mAttachMutex;
  OpenFlags::Flags mFlags;
  bool mIsOpen;
  XrdCl::File* pFile;
  std::string pUrl;
  Journal pJournal;
  std::string pJournalPath;
  Log* mLog;

  CacheStats pStats;

  std::vector<XrdCl::JCacheReadHandler> mReadHandlers;
};

} // namespace XrdCl