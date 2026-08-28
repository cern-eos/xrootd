#pragma once
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "cache/Journal.hh"
#include <memory>

namespace XrdCl {

class JournalCacheReadVHandler : public XrdCl::ResponseHandler {
public:
  JournalCacheReadVHandler(XrdCl::ResponseHandler *handler,
                           std::atomic<uint64_t> *rvbytes,
                           std::shared_ptr<Journal> journal)
      : handler(handler), rvbytes(rvbytes), journal(std::move(journal)) {}

  virtual ~JournalCacheReadVHandler() {}

  virtual void HandleResponse(XrdCl::XRootDStatus *pStatus,
                              XrdCl::AnyObject *pResponse) {
    if (pStatus->IsOK() && pResponse) {
      VectorReadInfo *vReadInfo = nullptr;
      pResponse->Get(vReadInfo);
      if (vReadInfo) {
        ChunkList *chunks = &(vReadInfo->GetChunks());
        for (auto it = chunks->begin(); it != chunks->end(); ++it) {
          if (journal) {
            journal->pwrite(it->GetBuffer(), it->GetLength(), it->GetOffset());
          }
          if (rvbytes) {
            *rvbytes += it->GetLength();
          }
        }
      }
    }
    handler->HandleResponse(pStatus, pResponse);
    delete this;
  }

  XrdCl::ResponseHandler *handler;
  std::atomic<uint64_t> *rvbytes;
  std::shared_ptr<Journal> journal;
};

} // namespace XrdCl
