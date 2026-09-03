//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
#pragma once
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "cache/Journal.hh"
#include <memory>

namespace XrdCl {

class JournalCacheReadHandler : public XrdCl::ResponseHandler {
public:
  JournalCacheReadHandler(XrdCl::ResponseHandler *handler,
                    std::atomic<uint64_t> *rbytes,
                    std::shared_ptr<Journal> journal)
      : handler(handler), rbytes(rbytes), journal(std::move(journal)) {}

  virtual ~JournalCacheReadHandler() {}

  virtual void HandleResponse(XrdCl::XRootDStatus *pStatus,
                              XrdCl::AnyObject *pResponse) {
    if (pStatus->IsOK() && pResponse) {
      XrdCl::ChunkInfo *chunkInfo = nullptr;
      pResponse->Get(chunkInfo);
      if (chunkInfo) {
        if (auto j = journal.lock()) {
          (void)j->pwrite(chunkInfo->GetBuffer(), chunkInfo->GetLength(),
                          chunkInfo->GetOffset());
        }
        if (rbytes) {
          *rbytes += chunkInfo->GetLength();
        }
      }
    }
    handler->HandleResponse(pStatus, pResponse);
    delete this;
  }

  XrdCl::ResponseHandler *handler;
  std::atomic<uint64_t> *rbytes;
  std::weak_ptr<Journal> journal;
};

} // namespace XrdCl
