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

/*----------------------------------------------------------------------------*/
#include "file/XrdClJCacheFile.hh"
#include "handler/XrdClJCacheOpenHandler.hh"
/*----------------------------------------------------------------------------*/


namespace XrdCl {
// ---------------------------------------------------------------------- //
void 
JCacheOpenHandler::HandleResponseWithHosts(XrdCl::XRootDStatus* pStatus,
			                                     XrdCl::AnyObject* pResponse,
			                                     XrdCl::HostList* pHostList) {

   
  openedTime = std::chrono::steady_clock::now();
  std::chrono::duration<double> topen = openedTime - creationTime;
  t2open = topen.count();

  if (pHostList) {                                         
    delete pHostList;
    pHostList = nullptr;
  }
  // Response shoud be nullptr in general                                                                                                                                
  if (pResponse) {
    delete pResponse;
    pResponse = nullptr;
  }
  if (pStatus->IsOK()) {
    pFile->mOpenState = JCacheFile::OPEN;
  } else {
    pFile->mOpenState = JCacheFile::FAILED;
  }
  mStatus = *pStatus;
  std::lock_guard<std::mutex> lock(mtx);
  ready = true;
  cv.notify_one(); // Notify Wait()
}

XRootDStatus
JCacheOpenHandler::Wait() {
  // quick bypass, we know we have opened
  if (pFile && pFile->mOpenState == JCacheFile::OPEN)
    return mStatus;

  // slower condition variable path
  std::unique_lock<std::mutex> lock(mtx);
  // Wait until `ready` becomes true
  cv.wait(lock, [this] { return this->ready; });
  return mStatus;
} 

} // namespace XrdCl
