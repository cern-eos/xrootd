//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters / CERN EOS Project <andreas.joachim.peters@cern.ch>
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
//------------------------------------------------------------------------------

#pragma once

#ifdef __linux__

#include <sys/fsuid.h>

namespace S3 {

//------------------------------------------------------------------------------
//! Scoped fsuid and fsgid setter changing the fsuid and fsgid of the calling
//! process to the given values during the lifetime of the object.
//! On destruction the fsuid and fsgid are restored to their original values.
//------------------------------------------------------------------------------
class ScopedFsId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ScopedFsId(uid_t fsuid_, gid_t fsgid_)
    : fsuid(fsuid_), fsgid(fsgid_)
  {
    ok = true;
    prevFsuid = -1;
    prevFsgid = -1;

    //--------------------------------------------------------------------------
    //! Set fsuid
    //--------------------------------------------------------------------------
    if (fsuid >= 0) {
      prevFsuid = setfsuid(fsuid);

      if (setfsuid(fsuid) != fsuid) {
        std::cerr << "Error: Unable to set fsuid to " << fsuid << "." <<std::endl;
        ok = false;
        return;
      }
    }

    //--------------------------------------------------------------------------
    //! Set fsgid
    //--------------------------------------------------------------------------
    if (fsgid >= 0) {
      prevFsgid = setfsgid(fsgid);

      if (setfsgid(fsgid) != fsgid) {
        std::cerr << "Error: Unable to set fsgid to " << fsgid << "." <<std::endl;
        ok = false;
        return;
      }
    }
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ScopedFsId()
  {
    if (prevFsuid >= 0) {
      setfsuid(prevFsuid);
    }

    if (prevFsgid >= 0) {
      setfsgid(prevFsgid);
    }
  }

  bool IsOk() const
  {
    return ok;
  }

  static void Validate() {
    ScopedFsId scope(geteuid()+1, geteuid()+1);
    if (!scope.IsOk()) {
      throw std::runtime_error("XrdS3 misses the capability to set the filesystem IDs on the fly!");
    }
  }
private:
  int fsuid;
  int fsgid;

  int prevFsuid;
  int prevFsgid;

  bool ok;
};

} // namespace S3

#else
// Dummy implementation for non-linux platforms
class ScopedFsId
{
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  ScopedFsId(uid_t fsuid_, gid_t fsgid_)
    : fsuid(fsuid_), fsgid(fsgid_) {}

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~ScopedFsId() {}

  bool IsOk() const { return true;}
};

} // namespace S3
#endif
