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

class JournalCachePgReadHandler : public XrdCl::ResponseHandler {
public:
  JournalCachePgReadHandler(XrdCl::ResponseHandler *handler,
                      std::atomic<uint64_t> *rbytes,
                      std::shared_ptr<Journal> journal)
      : handler(handler), rbytes(rbytes), journal(std::move(journal)) {}

  virtual ~JournalCachePgReadHandler() {}

  virtual void HandleResponse(XrdCl::XRootDStatus *pStatus,
                              XrdCl::AnyObject *pResponse) {
    if (pStatus->IsOK() && pResponse) {
      XrdCl::PageInfo *pageInfo = nullptr;
      pResponse->Get(pageInfo);
      if (pageInfo) {
        if (auto j = journal.lock()) {
          (void)j->pwrite(pageInfo->GetBuffer(), pageInfo->GetLength(),
                          pageInfo->GetOffset());
        }
        if (rbytes) {
          *rbytes += pageInfo->GetLength();
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
