#pragma once
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
/*----------------------------------------------------------------------------*/
#include "cache/Journal.hh"
/*----------------------------------------------------------------------------*/

namespace XrdCl {

class JournalCacheReadVHandler : public XrdCl::ResponseHandler
// ---------------------------------------------------------------------- //
{
public:
  JournalCacheReadVHandler() {}

  JournalCacheReadVHandler(JournalCacheReadVHandler *other) {
    journal = other->journal;
    rvbytes = other->rvbytes;
  }

  JournalCacheReadVHandler(XrdCl::ResponseHandler *handler,
                           std::atomic<uint64_t> *rvbytes, Journal *journal)
      : handler(handler), rvbytes(rvbytes), journal(journal) {}

  virtual ~JournalCacheReadVHandler() {}

  virtual void HandleResponse(XrdCl::XRootDStatus *pStatus,
                              XrdCl::AnyObject *pResponse) {
    if (pStatus->IsOK()) {
      if (pResponse) {
        VectorReadInfo *vReadInfo;
        pResponse->Get(vReadInfo);
        ChunkList *chunks = &(vReadInfo->GetChunks());
        if (journal) {
          for (auto it = chunks->begin(); it != chunks->end(); ++it) {
            journal->pwrite(it->GetBuffer(), it->GetLength(), it->GetOffset());
          }
        }
        for (auto it = chunks->begin(); it != chunks->end(); ++it) {
          *rvbytes += it->GetLength();
        }
      }
    }
    handler->HandleResponse(pStatus, pResponse);
    delete this;
  }

  XrdCl::ResponseHandler *handler;
  std::atomic<uint64_t> *rvbytes;
  Journal *journal;
};

} // namespace XrdCl
