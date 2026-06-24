#include "handler/XrdClJournalCacheDirListHandler.hh"
#include "system/XrdClJournalCacheSystem.hh"

namespace XrdCl {

void JournalCacheDirListHandler::HandleResponse(XrdCl::XRootDStatus *pStatus,
                                                XrdCl::AnyObject *pResponse) {
  if (pStatus->IsOK() && pResponse) {
    XrdCl::DirectoryList *lList = nullptr;
    pResponse->Get(lList);
    if (lList) {
      pSystem->SaveDirList(mPath, lList, mFlags);
    }
  }
  mHandler->HandleResponse(pStatus, pResponse);
  delete this;
}

} // namespace XrdCl
