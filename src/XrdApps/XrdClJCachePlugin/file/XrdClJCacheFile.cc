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
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClMessageUtils.hh"
/*----------------------------------------------------------------------------*/

std::string XrdCl::JCacheFile::sCachePath="";
bool XrdCl::JCacheFile::sEnableJournalCache = true;
bool XrdCl::JCacheFile::sEnableVectorCache = true;

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
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
JCacheFile::~JCacheFile()
{
  LogStats();
  if (pFile) {
    delete pFile;
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
  pUrl = url;
  st = pFile->Open(url, flags, mode, handler, timeout);
  
  if (st.IsOK()) {
    mIsOpen = true;
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
    if (sEnableJournalCache) {
      pJournal.detach();
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
    if (sEnableJournalCache && AttachForRead()) {
      auto rb = pJournal.pread(buffer, size, offset);
      if (rb == size)  {
        pStats.bytesCached += rb;
        pStats.readOps++; 
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

    auto jhandler = new JCacheReadHandler(handler, &pStats.bytesRead,sEnableJournalCache?&pJournal:nullptr);
    pStats.readOps++;
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
    if (sEnableJournalCache && AttachForRead()) {
      auto rb = pJournal.pread(buffer, size, offset);
      if (rb == size)  {
        pStats.bytesCached += rb;
        pStats.readOps++; 
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

    auto jhandler = new JCachePgReadHandler(handler, &pStats.bytesRead,sEnableJournalCache?&pJournal:nullptr);
    pStats.readOps++;
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
    VectorCache cache(chunks, pUrl, (const char*)buffer, sCachePath);
    if (sEnableVectorCache) {
      if (cache.retrieve()) {
        // Compute total length of readv request
        uint32_t len = 0;
        for (auto it = chunks.begin(); it != chunks.end(); ++it) {
          len += it->length;
        }

        XRootDStatus* ret_st = new XRootDStatus(st);
        AnyObject* obj = new AnyObject();
        VectorReadInfo* vReadInfo = new VectorReadInfo();
        vReadInfo->SetSize(len);
        ChunkList vResp = vReadInfo->GetChunks();
        vResp = chunks;
        obj->Set(vReadInfo);
        handler->HandleResponse(ret_st, obj);
        return st;
      }
    }
    
    auto jhandler = new JCacheReadVHandler(handler, &pStats.bytesReadV,sEnableJournalCache?&pJournal:nullptr, buffer, sEnableVectorCache?sCachePath:"", pUrl);
    pStats.readVOps++; 
    pStats.readVreadOps += chunks.size();

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
      StatInfo* sinfo = 0;
      auto st = pFile->Stat(false, sinfo);
      if (sinfo) {
        if (pJournal.attach(pJournalPath,sinfo->GetSize(),sinfo->GetModTime(),0)) {
          mLog->Error(1, "JCache : failed to attach to cache directory: %s", pJournalPath.c_str());
          mAttachedForRead = true;
          return false;
        } else {
          mLog->Info(1, "JCache : attached to cache directory: %s", pJournalPath.c_str());
        } 
      }
    }
  }
  mAttachedForRead = true;
  return true;
}

} // namespace XrdCl 

