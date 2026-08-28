//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------

#include "handler/XrdClJournalCacheOpenHandler.hh"
#include "file/XrdClJournalCacheFile.hh"

namespace XrdCl {

void JournalCacheOpenHandler::HandleResponseWithHosts(XrdCl::XRootDStatus *pStatus,
                                                XrdCl::AnyObject *pResponse,
                                                XrdCl::HostList *pHostList) {
  openedTime = std::chrono::steady_clock::now();
  std::chrono::duration<double> topen = openedTime - creationTime;
  t2open = topen.count();

  {
    std::lock_guard<std::mutex> lock(mtx);
    if (pStatus) {
      mStatus = *pStatus;
    }
    ready = true;
  }

  if (pFile) {
    if (pStatus && pStatus->IsOK()) {
      pFile->mOpenState = JournalCacheFile::OPEN;
    } else {
      pFile->mOpenState = JournalCacheFile::FAILED;
    }
  }
  cv.notify_one();

  delete pStatus;
  delete pResponse;
  delete pHostList;
}

XRootDStatus JournalCacheOpenHandler::Wait() {
  std::unique_lock<std::mutex> lock(mtx);
  cv.wait(lock, [this] { return this->ready; });
  return mStatus;
}

} // namespace XrdCl
