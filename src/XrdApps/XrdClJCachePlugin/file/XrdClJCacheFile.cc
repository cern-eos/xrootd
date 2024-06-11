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
#include "XrdClJCacheFile.hh"
#include "file/CacheStats.hh"
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClMessageUtils.hh"
/*----------------------------------------------------------------------------*/

std::string XrdCl::JCacheFile::sCachePath="";
std::string XrdCl::JCacheFile::sJsonPath="./";
bool XrdCl::JCacheFile::sEnableJournalCache = true;
bool XrdCl::JCacheFile::sEnableVectorCache = false;
bool XrdCl::JCacheFile::sEnableSummary = true;
JCache::CacheStats XrdCl::JCacheFile::sStats(true);

JournalManager XrdCl::JCacheFile::sJournalManager;

namespace XrdCl 
{

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JCacheFile::JCacheFile(const std::string& url):
  mIsOpen(false),
  pFile(0)
{
  mAttachedForRead = false;
  mLog = DefaultEnv::GetLog();
}
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JCacheFile::JCacheFile():
  mIsOpen(false),
  pFile(0)
{
  mAttachedForRead = false;
  mLog = DefaultEnv::GetLog();
  pStats = new JCache::CacheStats();
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
JCacheFile::~JCacheFile()
{
  LogStats();
  pStats->AddToStats(sStats);
  if (pFile) {
    delete pFile;
  }
  if (pStats) {
    delete pStats;
  }
}


//------------------------------------------------------------------------------
// Open
//------------------------------------------------------------------------------
XRootDStatus
JCacheFile::Open(const std::string& url,
		 OpenFlags::Flags flags,
		 Access::Mode mode,
		 ResponseHandler* handler,
		 uint16_t timeout)
{
  XRootDStatus st;
  mFlags = flags;

  if (mIsOpen) {
    st = XRootDStatus(stError, errInvalidOp);
    std::cerr << "error: file is already opened: " << pUrl << std::endl; 
    return st;
  }

  pFile = new XrdCl::File(false);

  // sanitize named connections and CGI for the URL to cache
  XrdCl::URL cleanUrl;
  XrdCl::URL origUrl(url);
  cleanUrl.SetProtocol(origUrl.GetProtocol());
  cleanUrl.SetHostName(origUrl.GetHostName());
  cleanUrl.SetPort(origUrl.GetPort());
  cleanUrl.SetPath(origUrl.GetPath());
  pUrl = cleanUrl.GetURL();
  st = pFile->Open(url, flags, mode, handler, timeout);
  if (st.IsOK()) {
    mIsOpen = true;
    if (sEnableVectorCache || sEnableJournalCache) {
      if ((flags & OpenFlags::Flags::Read) == OpenFlags::Flags::Read) {
	std::string JournalDir = sCachePath + "/" + VectorCache::computeSHA256(pUrl);
	pJournalPath = JournalDir + "/journal";
	// it can be that we cannot write the journal directory
	if (!VectorCache::ensureLastSubdirectoryExists(JournalDir)) {
	  st = XRootDStatus(stError, errOSError);
	  std::cerr << "error: unable to create cache directory: " << JournalDir << std::endl;
        return st;  
	}
      }
    }
  }
  return st;
}


//------------------------------------------------------------------------------
// Close
//------------------------------------------------------------------------------
XRootDStatus
JCacheFile::Close(ResponseHandler* handler,
		  uint16_t timeout)
{
  XRootDStatus st;

  if (mIsOpen) {
    mIsOpen = false;
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
XRootDStatus
JCacheFile::Stat(bool force,
		 ResponseHandler* handler,
		 uint16_t timeout)
{
  XRootDStatus st;

  if (pFile) {
    st = pFile->Stat(force, handler, timeout);
  } else {
    st = XRootDStatus(stError, errInvalidOp);
  }

  return st;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
XRootDStatus
JCacheFile::Read(uint64_t offset,
		 uint32_t size,
		 void* buffer,
		 ResponseHandler* handler,
		 uint16_t timeout)
{
  XRootDStatus st;

  if (pFile) {
    sStats.bench.AddMeasurement(size);
    if (sEnableJournalCache && AttachForRead()) {
      bool eof = false;
      auto rb = pJournal->pread(buffer, size, offset, eof);
      if ((rb == size) || (eof && rb))  {
        pStats->bytesCached += rb;
        pStats->readOps++; 
        // we can only serve success full reads from the cache for now
        XRootDStatus* ret_st = new XRootDStatus(st);
        ChunkInfo* chunkInfo = new ChunkInfo(offset, rb, buffer);
        AnyObject* obj = new AnyObject();
        obj->Set(chunkInfo);
        handler->HandleResponse(ret_st, obj);
        st = XRootDStatus(stOK, 0);
        return st;
      }
    }

    auto jhandler = new JCacheReadHandler(handler, &pStats->bytesRead,sEnableJournalCache?pJournal.get():nullptr);
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
XRootDStatus
JCacheFile::Write(uint64_t offset,
		  uint32_t size,
		  const void* buffer,
		  ResponseHandler* handler,
		  uint16_t timeout)
{
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
XRootDStatus 
JCacheFile::PgRead( uint64_t         offset,
                    uint32_t         size,
                    void            *buffer,
                    ResponseHandler *handler,
                    uint16_t         timeout ) 
{
  XRootDStatus st;
  if (pFile) {
    sStats.bench.AddMeasurement(size);
    if (sEnableJournalCache && AttachForRead()) {
      bool eof = false;
      auto rb = pJournal->pread(buffer, size, offset, eof);
      if ((rb == size) || (eof && rb))  {
        pStats->bytesCached += rb;
        pStats->readOps++; 
        // we can only serve success full reads from the cache for now
        XRootDStatus* ret_st = new XRootDStatus(st);
        ChunkInfo* chunkInfo = new ChunkInfo(offset, rb, buffer);
        AnyObject* obj = new AnyObject();
        obj->Set(chunkInfo);
        handler->HandleResponse(ret_st, obj);
        st = XRootDStatus(stOK, 0);
        return st;
      }
    }

    auto jhandler = new JCachePgReadHandler(handler, &pStats->bytesRead,sEnableJournalCache?pJournal.get():nullptr);
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
XRootDStatus 
JCacheFile::PgWrite(  uint64_t               offset,
                      uint32_t               nbpgs,
                      const void            *buffer,
                      std::vector<uint32_t> &cksums,
                      ResponseHandler       *handler,
                      uint16_t               timeout ) 
{
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
XRootDStatus
JCacheFile::Sync(ResponseHandler* handler,
		 uint16_t timeout)
{
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
XRootDStatus
JCacheFile::Truncate(uint64_t size,
		     ResponseHandler* handler,
		     uint16_t timeout)
{
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
XRootDStatus
JCacheFile::VectorRead(const ChunkList& chunks,
		       void* buffer,
		       ResponseHandler* handler,
		       uint16_t timeout)
{
  XRootDStatus st;

  if (pFile) {
    uint32_t len = 0;
    for (auto it = chunks.begin(); it != chunks.end(); ++it) {
      len += it->length;
    }

    sStats.bench.AddMeasurement(len);
    
    if (sEnableVectorCache) {
      VectorCache cache(chunks, pUrl, buffer?(char*)buffer:(char*)(chunks.begin()->buffer), sCachePath);

      if (cache.retrieve()) {
        XRootDStatus* ret_st = new XRootDStatus(st);
	*ret_st = XRootDStatus(stOK, 0);
        AnyObject* obj = new AnyObject();
        VectorReadInfo* vReadInfo = new VectorReadInfo();
        vReadInfo->SetSize(len);
        ChunkList& vResp = vReadInfo->GetChunks();
        vResp = chunks;
        obj->Set(vReadInfo);
        handler->HandleResponse(ret_st, obj);
	pStats->readVOps++;
	pStats->readVreadOps += chunks.size();
	pStats->bytesCachedV += len;
        return st;
      }
    } else {
      if (sEnableJournalCache) {
	bool inJournal = true;
	size_t len = 0;
	// try to get chunks from journal cache
	for (auto it = chunks.begin(); it != chunks.end(); ++it) {
	  bool eof = false;
	  auto rb = pJournal->pread(it->buffer, it->length, it->offset, eof);
	  if (rb != it->length)  {
	    // interrupt if we miss a piece and go remote
	    inJournal = false;
	    break;
	  } else {
	    len += it->length;
	  }
	}
	if (inJournal) {
	  // we found everything in the journal
	  pStats->readVOps++;
	  pStats->readVreadOps += chunks.size();
	  pStats->bytesCachedV += len;
	  XRootDStatus* ret_st = new XRootDStatus(st);
	  *ret_st = XRootDStatus(stOK, 0);
	  AnyObject* obj = new AnyObject();
	  VectorReadInfo* vReadInfo = new VectorReadInfo();
	  vReadInfo->SetSize(len);
	  ChunkList& vResp = vReadInfo->GetChunks();
	  vResp = chunks;
	  obj->Set(vReadInfo);
	  handler->HandleResponse(ret_st, obj);
	  return st;
	}
      }
    }
      
    
    auto jhandler = new JCacheReadVHandler(handler, &pStats->bytesReadV,sEnableJournalCache?pJournal.get():nullptr, buffer?(char*)buffer:(char*)(chunks.begin()->buffer), sEnableVectorCache?sCachePath:"", pUrl);
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
XRootDStatus
JCacheFile::Fcntl(const XrdCl::Buffer& arg,
		  ResponseHandler* handler,
		  uint16_t timeout)
{
  XRootDStatus st;

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
XRootDStatus
JCacheFile::Visa(ResponseHandler* handler,
		 uint16_t timeout)
{
  XRootDStatus st;

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
bool
JCacheFile::IsOpen() const
{
  return mIsOpen;
}


//------------------------------------------------------------------------------
// @see XrdCl::File::SetProperty
//------------------------------------------------------------------------------
bool
JCacheFile::SetProperty(const std::string& name,
			const std::string& value)
{
  if (pFile) {
    return pFile->SetProperty(name, value);
  } else {
    return false;
  }
}


//------------------------------------------------------------------------------
// @see XrdCl::File::GetProperty
//------------------------------------------------------------------------------
bool
JCacheFile::GetProperty(const std::string& name,
			std::string& value) const
{
  if (pFile) {
    return pFile->GetProperty(name, value);
  } else {
    return false;
  }
}

bool 
JCacheFile::AttachForRead()
{
  std::lock_guard guard(mAttachMutex);
  if (mAttachedForRead) {
    return true;
  }
  if ((mFlags & OpenFlags::Flags::Read) == OpenFlags::Flags::Read) {
    // attach to a cache
    if (sEnableJournalCache && pFile) {
      mLog->Info(1, "JCache : attaching via journalmanager to '%s'", pUrl.c_str());
      pJournal = sJournalManager.attach(pUrl);
      StatInfo* sinfo = 0;
      auto st = pFile->Stat(false, sinfo);
      if (sinfo) {
	      // only add a file if it wasn't yet added
	      if (!sStats.HasUrl(pUrl)) {
	        sStats.totaldatasize+=sinfo->GetSize();
	      }
        if (pJournal->attach(pJournalPath, sinfo->GetModTime(),0, sinfo->GetSize())) {
          mLog->Error(1, "JCache : failed to attach to cache directory: %s", pJournalPath.c_str());
          mAttachedForRead = true;
          return false;
        } else {
          mLog->Info(1, "JCache : attached to cache directory: %s", pJournalPath.c_str());
        } 
      }
    }
  }
  sStats.AddUrl(pUrl);  
  mAttachedForRead = true;
  return true;
}


//----------------------------------------------------------------------------
//! @brief log cache hit statistics
//----------------------------------------------------------------------------
void JCacheFile::LogStats() {
  mLog->Info(1, "JCache : read:readv-ops:readv-read-ops: %lu:%lu:%lus hit-rate: total [read/readv]=%.02f%% [%.02f%%/%.02f%%] remote-bytes-read/readv: %lu / %lu cached-bytes-read/readv: %lu / %lu",
              pStats->readOps.load(),
              pStats->readVOps.load(),
              pStats->readVreadOps.load(),
              pStats->CombinedHitRate(),
              pStats->HitRate(),
              pStats->HitRateV(),
              pStats->bytesRead.load(), 
              pStats->bytesReadV.load(), 
              pStats->bytesCached.load(),
              pStats->bytesCachedV.load());
  }
} // namespace XrdCl 

