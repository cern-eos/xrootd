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
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/
#include <mutex>
#include <condition_variable>
/*----------------------------------------------------------------------------*/


namespace XrdCl {

class JCacheFile;

class JCacheOpenHandler : public XrdCl::ResponseHandler
// ---------------------------------------------------------------------- //
{
public:
  JCacheOpenHandler() : ready(false), pFile(nullptr), t2open(0) {}  
  JCacheOpenHandler(XrdCl::JCacheFile* file) 
    : ready(false), pFile(file), t2open(0) {
      creationTime = std::chrono::steady_clock::now();
    }

  virtual ~JCacheOpenHandler() {}

  void HandleResponseWithHosts(XrdCl::XRootDStatus* pStatus,
			       XrdCl::AnyObject* pResponse,
			       XrdCl::HostList* pHostList);  

  XrdCl::XRootDStatus Wait();    
  bool ready;  

  double GetTimeToOpen() {return t2open;}

private:
  XrdCl::JCacheFile* pFile;
  XrdCl::XRootDStatus mStatus;
  std::mutex mtx;
  std::condition_variable cv;
  std::atomic<double> t2open;
  std::chrono::time_point<std::chrono::steady_clock> creationTime;
  std::chrono::time_point<std::chrono::steady_clock> openedTime;
};

} // namespace XrdCl
