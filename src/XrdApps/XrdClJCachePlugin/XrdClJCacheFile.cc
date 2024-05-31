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

std::string JCacheFile::sCachePath="";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
JCacheFile::JCacheFile():
  mIsOpen(false),
  pFile(0)
{
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
JCacheFile::~JCacheFile()
{

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

  if (mIsOpen) {
    st = XRootDStatus(stError, errInvalidOp);
    return st;
  }

  pFile = new XrdCl::File(false);
  st = pFile->Open(url, flags, mode, handler, timeout);
  
  if (st.IsOK()) {
    mIsOpen = true;
  }

  
  if ((flags & OpenFlags::Flags::Read) == OpenFlags::Flags::Read) {
    // attach to a cache
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

    if (pFile) {
      st = pFile->Close(handler, timeout);
    }
  } else {
    // File already closed
    st = XRootDStatus(stError, errInvalidOp);
    XRootDStatus* ret_st = new XRootDStatus(st);
    handler->HandleResponse(ret_st, 0);
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
    st = pFile->Read(offset, size, buffer, handler, timeout);
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
    st = pFile->VectorRead(chunks, buffer, handler, timeout);
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

