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

#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClPlugInInterface.hh"
#include <string>

namespace XrdCl {
//----------------------------------------------------------------------------
//! JCache system plugin
//! This XRootD Client Plugin provides a client side listing cache.
//----------------------------------------------------------------------------
class JCacheSystem : public XrdCl::FileSystemPlugIn {
public:
  //------------------------------------------------------------------------
  //! Constructor
  //------------------------------------------------------------------------
  JCacheSystem(const URL &url) {
    pSystem = new XrdCl::FileSystem(url, false);
    mUrl = url.GetURL();
  }

  //------------------------------------------------------------------------
  //! Destructor
  //------------------------------------------------------------------------
  virtual ~JCacheSystem() { delete pSystem; }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Locate
  //------------------------------------------------------------------------
  virtual XRootDStatus Locate(const std::string &path, OpenFlags::Flags flags,
                              ResponseHandler *handler, uint16_t timeout) {
    return pSystem->Locate(path, flags, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::DeepLocate
  //------------------------------------------------------------------------
  virtual XRootDStatus DeepLocate(const std::string &path,
                                  OpenFlags::Flags flags,
                                  ResponseHandler *handler, uint16_t timeout) {
    return pSystem->DeepLocate(path, flags, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Mv
  //------------------------------------------------------------------------
  virtual XRootDStatus Mv(const std::string &source, const std::string &dest,
                          ResponseHandler *handler, uint16_t timeout) {
    return pSystem->Mv(source, dest, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Query
  //------------------------------------------------------------------------
  virtual XRootDStatus Query(QueryCode::Code queryCode, const Buffer &arg,
                             ResponseHandler *handler, uint16_t timeout) {
    return pSystem->Query(queryCode, arg, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Truncate
  //------------------------------------------------------------------------
  virtual XRootDStatus Truncate(const std::string &path, uint64_t size,
                                ResponseHandler *handler, uint16_t timeout) {
    return pSystem->Truncate(path, size, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Rm
  //------------------------------------------------------------------------
  virtual XRootDStatus Rm(const std::string &path, ResponseHandler *handler,
                          uint16_t timeout) {
    return pSystem->Rm(path, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::MkDir
  //------------------------------------------------------------------------
  virtual XRootDStatus MkDir(const std::string &path, MkDirFlags::Flags flags,
                             Access::Mode mode, ResponseHandler *handler,
                             uint16_t timeout) {
    return pSystem->MkDir(path, flags, mode, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::RmDir
  //------------------------------------------------------------------------
  virtual XRootDStatus RmDir(const std::string &path, ResponseHandler *handler,
                             uint16_t timeout) {
    return pSystem->RmDir(path, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::ChMod
  //------------------------------------------------------------------------
  virtual XRootDStatus ChMod(const std::string &path, Access::Mode mode,
                             ResponseHandler *handler, uint16_t timeout) {
    return pSystem->ChMod(path, mode, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Ping
  //------------------------------------------------------------------------
  virtual XRootDStatus Ping(ResponseHandler *handler, uint16_t timeout) {
    return pSystem->Ping(handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Stat
  //------------------------------------------------------------------------
  virtual XRootDStatus Stat(const std::string &path, ResponseHandler *handler,
                            uint16_t timeout);

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::StatVFS
  //------------------------------------------------------------------------
  virtual XRootDStatus StatVFS(const std::string &path,
                               ResponseHandler *handler, uint16_t timeout) {
    return pSystem->StatVFS(path, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Protocol
  //------------------------------------------------------------------------
  virtual XRootDStatus Protocol(ResponseHandler *handler,
                                uint16_t timeout = 0) {
    return pSystem->Protocol(handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::DirlList
  //------------------------------------------------------------------------
  virtual XRootDStatus DirList(const std::string &path,
                               DirListFlags::Flags flags,
                               ResponseHandler *handler, uint16_t timeout);

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::SendInfo
  //------------------------------------------------------------------------
  virtual XRootDStatus SendInfo(const std::string &info,
                                ResponseHandler *handler, uint16_t timeout) {
    return pSystem->SendInfo(info, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::Prepare
  //------------------------------------------------------------------------
  virtual XRootDStatus Prepare(const std::vector<std::string> &fileList,
                               PrepareFlags::Flags flags, uint8_t priority,
                               ResponseHandler *handler, uint16_t timeout) {
    return pSystem->Prepare(fileList, flags, priority, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::SetXAttr
  //------------------------------------------------------------------------
  virtual XRootDStatus SetXAttr(const std::string &path,
                                const std::vector<xattr_t> &attrs,
                                ResponseHandler *handler, uint16_t timeout) {
    return pSystem->SetXAttr(path, attrs, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::GetXAttr
  //------------------------------------------------------------------------
  virtual XRootDStatus GetXAttr(const std::string &path,
                                const std::vector<std::string> &attrs,
                                ResponseHandler *handler, uint16_t timeout) {
    return pSystem->GetXAttr(path, attrs, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::DelXAttr
  //------------------------------------------------------------------------
  virtual XRootDStatus DelXAttr(const std::string &path,
                                const std::vector<std::string> &attrs,
                                ResponseHandler *handler, uint16_t timeout) {
    return pSystem->DelXAttr(path, attrs, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::ListXAttr
  //------------------------------------------------------------------------
  virtual XRootDStatus ListXAttr(const std::string &path,
                                 ResponseHandler *handler, uint16_t timeout) {
    return pSystem->ListXAttr(path, handler, timeout);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::SetProperty
  //------------------------------------------------------------------------
  virtual bool SetProperty(const std::string &name, const std::string &value) {
    return pSystem->SetProperty(name, value);
  }

  //------------------------------------------------------------------------
  //! @see XrdCl::FileSystem::GetProperty
  //------------------------------------------------------------------------
  virtual bool GetProperty(const std::string &name, std::string &value) const {
    return pSystem->GetProperty(name, value);
  }

private:
  XrdCl::FileSystem *pSystem;
  std::string mUrl;

  DirectoryList* LoadDirList(const std::string& path);
  bool SaveDirList(const std::string& path, DirectoryList* dirList);

  std::string Serialize(const std::string& hostaddress, const std::string& name, XrdCl::StatInfo* stat);
  std::tuple<std::string, std::string, XrdCl::StatInfo*> Deserialize(const std::string& data);
};

}