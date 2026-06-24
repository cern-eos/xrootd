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
#include "XrdClJournalCacheFile.hh"
#include "file/CacheStats.hh"
#include "file/Digest.hh"
#include "file/Hierarchy.hh"
#include "file/CachePath.hh"
#include "file/OriginAllowlist.hh"
#include "http/ForwardingUrl.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClMessageUtils.hh"
#include <ctime>
#include <filesystem>
/*----------------------------------------------------------------------------*/

std::string XrdCl::JournalCacheFile::sCachePath = "";
std::string XrdCl::JournalCacheFile::sJsonPath = "";
std::string XrdCl::JournalCacheFile::sBasePath = "";
bool XrdCl::JournalCacheFile::sEnableJournalCache = true;
bool XrdCl::JournalCacheFile::sEnableJournalCrc = false;
bool XrdCl::JournalCacheFile::sEnableSummary = true;
bool XrdCl::JournalCacheFile::sEnableBypass = false;
bool XrdCl::JournalCacheFile::sOpenAsync = false;
bool XrdCl::JournalCacheFile::sFlatHierarchy = false;
bool XrdCl::JournalCacheFile::sThreadConnectionDemultiplexing = false;
bool XrdCl::JournalCacheFile::sMultiOriginUnwrap = false;
JournalCache::OriginAllowlist XrdCl::JournalCacheFile::sOriginAllowlist;

JournalCache::CacheStats XrdCl::JournalCacheFile::sStats(true);
JournalCache::Cleaner XrdCl::JournalCacheFile::sCleaner;
JournalManager XrdCl::JournalCacheFile::sJournalManager;

namespace XrdCl {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JournalCacheFile::JournalCacheFile(const std::string &url)
    : mIsOpen(false), pFile(0), mOpenAsync(false) {
  mAttachedForRead = false;
  mOpenState = JournalCacheFile::CLOSED;
  mLog = DefaultEnv::GetLog();
  pOpenHandler = nullptr;
}
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JournalCacheFile::JournalCacheFile() : mIsOpen(false), pFile(0), mOpenAsync(false) {
  mAttachedForRead = false;
  mOpenState = JournalCacheFile::CLOSED;
  pStats = new JournalCache::CacheStats();
  mLog = DefaultEnv::GetLog();
  pOpenHandler = nullptr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
JournalCacheFile::~JournalCacheFile() {
  LogStats();
  pStats->AddToStats(sStats);
  if (pFile) {
    delete pFile;
  }
  if (pStats) {
    delete pStats;
  }
  if (pOpenHandler) {
    delete pOpenHandler;
  }
}

//------------------------------------------------------------------------------
// Open
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Open(const std::string &url, OpenFlags::Flags flags,
                              Access::Mode mode, ResponseHandler *handler,
                              time_t timeout) {
  XRootDStatus st;
  mFlags = flags;

  if (mIsOpen) {
    st = XRootDStatus(stError, errInvalidOp);
    mLog->Error(1, "File is already opened: %s", pUrl.c_str());
    return st;
  }

  pFile = new XrdCl::File(false);

  XrdCl::URL origUrl(url);
  std::string fetchUrl = url;
  const JournalCache::EmbeddedFileUrl chained =
      JournalCache::parseChainedFileUrl(url);
  if (chained.valid) {
    if (!sOriginAllowlist.isAllowed(chained.fileUrl)) {
      mLog->Error(1, "JournalCache : upstream origin not allowed: %s",
                  chained.fileUrl.c_str());
      return XRootDStatus(stError, errInvalidArgs);
    }
    if (sMultiOriginUnwrap) {
      XrdCl::URL inner(chained.fileUrl);
      XrdCl::URL::ParamsMap params = inner.GetParams();
      for (const auto &entry : origUrl.GetParams()) {
        params[entry.first] = entry.second;
      }
      inner.SetParams(params);
      fetchUrl = inner.GetURL();
      origUrl = inner;
      mLog->Info(1, "JournalCache : multi-origin open to %s",
                 fetchUrl.c_str());
    }
  }

  // sanitize named connections and CGI for the URL to cache
  XrdCl::URL cleanUrl;
  std::string cacheUrl;

  cleanUrl.SetProtocol(origUrl.GetProtocol());
  cleanUrl.SetHostName(origUrl.GetHostName());


  cleanUrl.SetPort(origUrl.GetPort());
  cleanUrl.SetPath(origUrl.GetPath());

  cacheUrl = cleanUrl.GetURL(); // we cannot add the thread id when we use hashing to define the journal name in the cache

  if (sThreadConnectionDemultiplexing) {
    // Lambda function to get the thread ID and return it as an 8-character hexadecimal string
    auto get_thread_id_hex = []() -> std::string {
        // Buffer to hold the hexadecimal string (8 characters + null terminator)
        char hex_str[9];

        // Get the thread ID, apply the modulo operation, and format it directly into the buffer
        sprintf(hex_str, "%08x", (int)(syscall(SYS_gettid)) % 0xFFFFFFFF);

        // Return the formatted hexadecimal string
        return std::string(hex_str);
    };

    cleanUrl.SetUserName(get_thread_id_hex());
  }
  pUrl = cleanUrl.GetURL();

  // allow to enable asynchronous operation globally
  if (sOpenAsync) {
    mOpenAsync = true;
  }

  // allow to enable asynchronous operation by CGI per file
  if (origUrl.GetParams().count("xrd.journalcache.async") &&
      origUrl.GetParams().at("xrd.journalcache.async") == "1") {
    mLog->Info(1, "JournalCache : user allowed async/detached mode");
    mOpenAsync = true;
  }

  mOpenCacheHeaders = JournalCache::CacheHeaders{};
  mCacheValidators = JournalCache::CacheValidators{};
  mNoStoreCache = false;
  mFileBypass = false;
  mMustRevalidate = false;
  mRevalidatedThisSession = false;
  if (JournalCache::extractCacheHeadersFromParams(origUrl.GetParams(),
                                                  mOpenCacheHeaders)) {
    const auto policy =
        JournalCache::parseCacheControl(mOpenCacheHeaders.cacheControl);
    if (policy.noStore) {
      mNoStoreCache = true;
      mLog->Info(1, "JournalCache : Cache-Control no-store disables caching");
    } else if (policy.noCache) {
      mMustRevalidate = true;
      mOpenAsync = false;
      mLog->Info(1,
                 "JournalCache : Cache-Control no-cache requires revalidation");
    }
  }
  JournalCache::extractCacheValidatorsFromParams(origUrl.GetParams(),
                                                 mCacheValidators);

  mFileBypass =
      JournalCache::paramEnabled(origUrl.GetParams(), JournalCache::BYPASS_CGI);
  if (mFileBypass) {
    mLog->Info(1, "JournalCache : per-file bypass enabled for %s",
               origUrl.GetPath().c_str());
  }

  if ((flags & OpenFlags::Flags::Read) == OpenFlags::Flags::Read) {
    pOpenHandler = new JournalCacheOpenHandler(this);
    st = pFile->Open(fetchUrl, flags, mode, pOpenHandler, timeout);

    if (!mOpenAsync) {
      // we have to be sure the file is opened
      st = pOpenHandler->Wait();
    }
    if (st.IsOK()) {
      mIsOpen = true;
      mOpenState = OPENING;
      if (sEnableJournalCache && !bypassCache() && !mNoStoreCache) {
        if ((flags & OpenFlags::Flags::Read) == OpenFlags::Flags::Read) {
          const std::string JournalDir =
              JournalCache::resolveFileJournalDir(cacheUrl);
          pJournalPath = JournalDir + "/journal";
          if (JournalCache::paramEnabled(origUrl.GetParams(),
                                         JournalCache::FORCE_CLEAN_CGI)) {
            std::error_code ec;
            std::filesystem::remove(pJournalPath, ec);
            mLog->Info(1, "JournalCache : force-clean removed journal %s",
                       pJournalPath.c_str());
          }
          if (sFlatHierarchy) {
            if (!JournalCache::ensureCacheDirectory(JournalDir)) {
              st = XRootDStatus(stError, errOSError);
              mLog->Error(1,
                          "JournalCache : unable to create cache directory: %s",
                          JournalDir.c_str());
              return st;
            }
          } else if (!JournalCache::makeHierarchy(pJournalPath)) {
            st = XRootDStatus(stError, errOSError);
            mLog->Error(1,
                        "JournalCache : unable to create cache directory: %s",
                        JournalDir.c_str());
            return st;
          }
        }
      }
      mOpenState = OPENING;
      // call the external handler to pretend all is already good!
      handler->HandleResponseWithHosts(new XRootDStatus(st), 0, 0);
    } else {
      mOpenState = FAILED;
    }
  } else {
    // run with the user handler
    st = pFile->Open(url, flags, mode, handler, timeout);
    mOpenState = OPEN;
    mIsOpen = true;
  }
  return st;
}

//------------------------------------------------------------------------------
// Close
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Close(ResponseHandler *handler, time_t timeout) {
  XRootDStatus st;

  if (mIsOpen) {
    if (mOpenState == OPENING) {
      pOpenHandler->Wait();
    }
    mIsOpen = false;
    mOpenState = CLOSED;
    pUrl = "";
    if (pFile) {
      st = pFile->Close(handler, timeout);
    } else {
      st = XRootDStatus(stOK, 0);
    }
    if (sEnableJournalCache && pJournal) {
      pJournal->detach();
    }
  } else {
    st = XRootDStatus(stOK, 0);
  }

  return st;
}

//------------------------------------------------------------------------------
// Stat
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Stat(bool force, ResponseHandler *handler,
                              time_t timeout) {
  XRootDStatus st;

  if (pFile) {
    if (!force && mOpenAsync) {
      if (sEnableJournalCache && !mNoStoreCache && AttachForRead() &&
          CanServeFromJournalCache() && mOpenAsync) {
        // let's create a stat response using the cache
        AnyObject *obj = new AnyObject();
        std::string id = pUrl;
        auto statInfo = new StatInfo(id, pJournal->getHeaderFileSize(), 0,
                                     pJournal->getHeaderMtime());
        obj->Set(statInfo);
        XRootDStatus *ret_st = new XRootDStatus(XRootDStatus(stOK, 0));
        handler->HandleResponse(ret_st, obj);
        st = XRootDStatus(stOK, 0);
        return st;
      }
    }
    // we have to be sure the file is opened
    if (pOpenHandler) {
      st = pOpenHandler->Wait();
      if (!st.IsOK()) {
        return st;
      }
    }
    st = pFile->Stat(force, handler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}

//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Read(uint64_t offset, uint32_t size, void *buffer,
                              ResponseHandler *handler, time_t timeout) {
  XRootDStatus st;

  if (pFile) {
    sStats.bench.AddMeasurement(size);

    if (!bypassCache() && sEnableJournalCache && !mNoStoreCache &&
        AttachForRead() && CanServeFromJournalCache()) {
      mLog->Info(1,
                 "JournalCache : Read: offset=%llu size=%u buffer=%p path='%s'",
                 static_cast<unsigned long long>(offset), size, buffer,
                 pUrl.c_str());
      bool eof = false;
      auto rb = pJournal->pread(buffer, size, offset, eof);
      if ((rb == size) || (eof && rb)) {
        pStats->bytesCached += rb;
        pStats->readOps++;
        // we can only serve success full reads from the cache for now
        XRootDStatus *ret_st = new XRootDStatus(st);
        ChunkInfo *chunkInfo = new ChunkInfo(offset, rb, buffer);
        AnyObject *obj = new AnyObject();
        obj->Set(chunkInfo);
        handler->HandleResponse(ret_st, obj);
        st = XRootDStatus(stOK, 0);
        return st;
      }
    }

    // we have to be sure the file is opened
    if (pOpenHandler) {
      st = pOpenHandler->Wait();
      if (!st.IsOK()) {
        return st;
      }
    }

    auto jhandler = new JournalCacheReadHandler(
        handler, &pStats->bytesRead,
        sEnableJournalCache && !bypassCache() && !mNoStoreCache ? pJournal.get()
                                                                : nullptr);
    pStats->readOps++;
    st = pFile->Read(offset, size, buffer, jhandler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }
  return st;
}

//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Write(uint64_t offset, uint32_t size,
                               const void *buffer, ResponseHandler *handler,
                               time_t timeout) {
  XRootDStatus st;
  if (pFile) {
    st = pFile->Write(offset, size, buffer, handler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}

//------------------------------------------------------------------------
//! PgRead
//------------------------------------------------------------------------
XRootDStatus JournalCacheFile::PgRead(uint64_t offset, uint32_t size, void *buffer,
                                ResponseHandler *handler, time_t timeout) {
  XRootDStatus st;
  if (pFile) {
    sStats.bench.AddMeasurement(size);

    if (sEnableJournalCache && !mNoStoreCache && AttachForRead() &&
        CanServeFromJournalCache() && !bypassCache()) {
      mLog->Info(1,
                 "JournalCache : PgRead: offset=%llu size=%u buffer=%p path='%s'",
                 static_cast<unsigned long long>(offset), size, buffer,
                 pUrl.c_str());
      bool eof = false;
      auto rb = pJournal->pread(buffer, size, offset, eof);

      mLog->Info(1, "JournalCache : PgRead: rb=%llu size=%u eof=%d path='%s'",
                 static_cast<unsigned long long>(rb), size, eof, pUrl.c_str());
      if ((rb == size) || (eof && rb)) {
        pStats->bytesCached += rb;
        pStats->readOps++;
        // we can only serve complete reads from the cache for now
        XRootDStatus *ret_st = new XRootDStatus(st);
        PageInfo *pageInfo = new PageInfo(offset, rb, buffer);
        AnyObject *obj = new AnyObject();
        obj->Set(pageInfo);
        handler->HandleResponse(ret_st, obj);
        st = XRootDStatus(stOK, 0);
        return st;
      }
    }

    // we have to be sure the file is opened
    if (pOpenHandler) {
      st = pOpenHandler->Wait();
      if (!st.IsOK()) {
        return st;
      }
    }
    auto jhandler = new JournalCachePgReadHandler(
        handler, &pStats->bytesRead,
        sEnableJournalCache && !bypassCache() && !mNoStoreCache ? pJournal.get()
                                                                : nullptr);
    pStats->readOps++;
    st = pFile->PgRead(offset, size, buffer, jhandler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }
  return st;
}

//------------------------------------------------------------------------
//! PgWrite
//------------------------------------------------------------------------
XRootDStatus JournalCacheFile::PgWrite(uint64_t offset, uint32_t nbpgs,
                                 const void *buffer,
                                 std::vector<uint32_t> &cksums,
                                 ResponseHandler *handler, time_t timeout) {
  XRootDStatus st;

  if (pFile) {
    st = pFile->PgWrite(offset, nbpgs, buffer, cksums, handler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}

//------------------------------------------------------------------------------
// Sync
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Sync(ResponseHandler *handler, time_t timeout) {
  XRootDStatus st;

  if (pFile) {
    st = pFile->Sync(handler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}

//------------------------------------------------------------------------------
// Truncate
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Truncate(uint64_t size, ResponseHandler *handler,
                                  time_t timeout) {
  XRootDStatus st;

  if (pFile) {
    st = pFile->Truncate(size, handler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}

//------------------------------------------------------------------------------
// VectorRead
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::VectorRead(const ChunkList &chunks, void *buffer,
                                    ResponseHandler *handler,
                                    time_t timeout) {
  XRootDStatus st;

  if (pFile) {
    uint32_t len = 0;
    for (auto it = chunks.begin(); it != chunks.end(); ++it) {
      len += it->length;
    }

    sStats.bench.AddMeasurement(len);

    if (!bypassCache() && sEnableJournalCache && !mNoStoreCache &&
        AttachForRead() && CanServeFromJournalCache()) {
      bool inJournal = true;
      size_t cachedLen = 0;
      for (auto it = chunks.begin(); it != chunks.end(); ++it) {
        bool eof = false;
        auto rb = pJournal->pread(it->buffer, it->length, it->offset, eof);
        if (rb != it->length) {
          inJournal = false;
          break;
        }
        cachedLen += it->length;
      }
      if (inJournal) {
        pStats->readVOps++;
        pStats->readVreadOps += chunks.size();
        pStats->bytesCachedV += cachedLen;
        XRootDStatus *ret_st = new XRootDStatus(st);
        *ret_st = XRootDStatus(stOK, 0);
        AnyObject *obj = new AnyObject();
        VectorReadInfo *vReadInfo = new VectorReadInfo();
        vReadInfo->SetSize(len);
        ChunkList &vResp = vReadInfo->GetChunks();
        vResp = chunks;
        obj->Set(vReadInfo);
        handler->HandleResponse(ret_st, obj);
        return st;
      }
    }

    // we have to be sure the file is opened
    if (pOpenHandler) {
      st = pOpenHandler->Wait();
      if (!st.IsOK()) {
        return st;
      }
    }

    auto jhandler = new JournalCacheReadVHandler(
        handler, &pStats->bytesReadV,
        sEnableJournalCache && !bypassCache() && !mNoStoreCache ? pJournal.get()
                                                                : nullptr);
    pStats->readVOps++;
    pStats->readVreadOps += chunks.size();

    st = pFile->VectorRead(chunks, buffer, jhandler, timeout);

  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}

//------------------------------------------------------------------------------
// Fcntl
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Fcntl(const XrdCl::Buffer &arg,
                               ResponseHandler *handler, time_t timeout) {
  XRootDStatus st;

  // we have to be sure the file is opened
  if (pOpenHandler) {
    st = pOpenHandler->Wait();
    if (!st.IsOK()) {
      return st;
    }
  }

  if (pFile) {
    st = pFile->Fcntl(arg, handler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}

//------------------------------------------------------------------------------
// Visa
//------------------------------------------------------------------------------
XRootDStatus JournalCacheFile::Visa(ResponseHandler *handler, time_t timeout) {
  XRootDStatus st;

  // we have to be sure the file is opened
  if (pOpenHandler) {
    st = pOpenHandler->Wait();
    if (!st.IsOK()) {
      return st;
    }
  }
  if (pFile) {
    st = pFile->Visa(handler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}

//------------------------------------------------------------------------------
// IsOpen
//------------------------------------------------------------------------------
bool JournalCacheFile::IsOpen() const { return mIsOpen; }

//------------------------------------------------------------------------------
// @see XrdCl::File::SetProperty
//------------------------------------------------------------------------------
bool JournalCacheFile::SetProperty(const std::string &name,
                             const std::string &value) {
  if (pFile) {
    return pFile->SetProperty(name, value);
  } else {
    return false;
  }
}

//------------------------------------------------------------------------------
// @see XrdCl::File::GetProperty
//------------------------------------------------------------------------------
bool JournalCacheFile::GetProperty(const std::string &name,
                             std::string &value) const {
  if (pOpenHandler) {
    if (!pOpenHandler->Wait().IsOK()) {
      return false;
    }
  }
  if (pFile) {
    return pFile->GetProperty(name, value);
  } else {
    return false;
  }
}

void JournalCacheFile::ApplyCacheHeaderPolicy(const StatInfo *statInfo) {
  if (pJournalPath.empty() || !pJournal) {
    return;
  }

  const uint64_t now = static_cast<uint64_t>(std::time(nullptr));

  if (!mOpenCacheHeaders.empty() || statInfo) {
    JournalCache::enrichCacheHeadersFromStat(statInfo, mOpenCacheHeaders);
    if (!mOpenCacheHeaders.empty()) {
      if (!mOpenCacheHeaders.cachedAt) {
        mOpenCacheHeaders.cachedAt = now;
      }
      JournalCache::storeCacheHeaders(pJournalPath, mOpenCacheHeaders, mLog);
    }
  }

  JournalCache::CacheHeaders stored;
  if (!JournalCache::loadCacheHeaders(pJournalPath, stored)) {
    return;
  }

  if (JournalCache::requiresRevalidation(stored)) {
    mMustRevalidate = true;
  }

  if (!JournalCache::shouldUseJournalCache(stored, now)) {
    mLog->Info(1, "JournalCache : cache headers disallow storage for %s",
               pUrl.c_str());
    pJournal->reset();
    mNoStoreCache = true;
    return;
  }

  if (JournalCache::validationRequiresRefresh(stored, mCacheValidators)) {
    mLog->Info(1, "JournalCache : validation headers require refresh for %s",
               pUrl.c_str());
    pJournal->reset();
    mRevalidatedThisSession = false;
    return;
  }

  if (JournalCache::isCacheEntryStale(stored, now)) {
    mLog->Info(1, "JournalCache : cache headers expired for %s", pUrl.c_str());
    pJournal->reset();
    mRevalidatedThisSession = false;
  }
}

bool JournalCacheFile::CanServeFromJournalCache() const {
  if (mNoStoreCache || !mAttachedForRead) {
    return false;
  }
  if (mMustRevalidate && !mRevalidatedThisSession) {
    return false;
  }
  return true;
}

bool JournalCacheFile::AttachForRead() {
  std::lock_guard guard(mAttachMutex);
  if (mAttachedForRead) {
    return CanServeFromJournalCache();
  }
  if (mNoStoreCache || bypassCache()) {
    mAttachedForRead = true;
    return false;
  }
  if ((mFlags & OpenFlags::Flags::Read) == OpenFlags::Flags::Read) {
    // attach to a cache
    if (sEnableJournalCache && pFile) {
      mLog->Info(1, "JournalCache : attaching via journalmanager to '%s'",
                 pUrl.c_str());
      pJournal = sJournalManager.attach(pUrl);

      // try to attach to an existing journal (disconnected mode)
      if (mOpenAsync && !mMustRevalidate) {
        if (!pJournal->attach(pJournalPath, 0, 0, 0, true)) {
          ApplyCacheHeaderPolicy();
          if (mNoStoreCache) {
            mAttachedForRead = true;
            return false;
          }
          if (CanServeFromJournalCache()) {
            if (!sStats.HasUrl(pUrl)) {
              sStats.totaldatasize += pJournal->getHeaderFileSize();
            }
            mLog->Info(1, "JournalCache : attached (async) to cache file: %s",
                       pJournalPath.c_str());
          }
          sStats.AddUrl(pUrl);
          mAttachedForRead = true;
          return CanServeFromJournalCache();
        } else {
          mOpenAsync = false;
        }
      }

      // We need an open file here to proceed
      pOpenHandler->Wait();

      StatInfo *sinfo = 0;
      auto st = pFile->Stat(false, sinfo);
      if (sinfo) {
        // only add a file if it wasn't yet added
        if (!sStats.HasUrl(pUrl)) {
          sStats.totaldatasize += sinfo->GetSize();
          sStats.opentime =
              sStats.opentime.load() + pOpenHandler->GetTimeToOpen();
        }
        if (pJournal->attach(pJournalPath, sinfo->GetModTime(), 0,
                             sinfo->GetSize())) {
          if (!bypassCache()) {
            // when bypass=true this might throw an error because we don't
            // create the journal directory - we just don't want to see this
            mLog->Error(1, "JournalCache : failed to attach to cache file: %s",
                        pJournalPath.c_str());
          }
          mAttachedForRead = true;
          delete sinfo;
          return false;
        }
        ApplyCacheHeaderPolicy(sinfo);
        if (mNoStoreCache) {
          delete sinfo;
          mAttachedForRead = true;
          return false;
        }
        if (mMustRevalidate) {
          mRevalidatedThisSession = true;
        }
        mLog->Info(1, "JournalCache : attached to cache file: %s",
                   pJournalPath.c_str());
        delete sinfo;
      }
    }
  }
  sStats.AddUrl(pUrl);
  mAttachedForRead = true;
  return CanServeFromJournalCache();
}

//----------------------------------------------------------------------------
//! @brief log cache hit statistics
//----------------------------------------------------------------------------
void JournalCacheFile::LogStats() {
  mLog->Info(
      1,
      "JournalCache : read:readv-ops:readv-read-ops: %llu:%llu:%llus hit-rate: "
      "total [read/readv]=%.02f%% [%.02f%%/%.02f%%] remote-bytes-read/readv: "
      "%llu / %llu cached-bytes-read/readv: %llu / %llu",
      static_cast<unsigned long long>(pStats->readOps.load()),
      static_cast<unsigned long long>(pStats->readVOps.load()),
      static_cast<unsigned long long>(pStats->readVreadOps.load()),
      pStats->CombinedHitRate(), pStats->HitRate(), pStats->HitRateV(),
      static_cast<unsigned long long>(pStats->bytesRead.load()),
      static_cast<unsigned long long>(pStats->bytesReadV.load()),
      static_cast<unsigned long long>(pStats->bytesCached.load()),
      static_cast<unsigned long long>(pStats->bytesCachedV.load()));
}

//----------------------------------------------------------------------------
//! @brief set stats interval in CachStats class
//----------------------------------------------------------------------------
void JournalCacheFile::SetStatsInterval(uint64_t interval) {
  sStats.SetInterval(interval);
}

} // namespace XrdCl
