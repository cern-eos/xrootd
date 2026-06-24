#include "handler/XrdClJournalCacheStatHandler.hh"
#include "system/XrdClJournalCacheSystem.hh"

namespace XrdCl {

void JournalCacheStatHandler::HandleResponse(XrdCl::XRootDStatus *pStatus,
                                             XrdCl::AnyObject *pResponse) {
  if (pStatus->IsOK() && pResponse) {
    XrdCl::StatInfo *statInfo = nullptr;
    pResponse->Get(statInfo);
    if (statInfo) {
      pSystem->SaveStat(mPath, statInfo);
    }
  }
  mHandler->HandleResponse(pStatus, pResponse);
  delete this;
}

} // namespace XrdCl
