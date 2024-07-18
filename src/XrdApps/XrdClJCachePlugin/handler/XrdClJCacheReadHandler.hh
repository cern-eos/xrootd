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
#include "cache/Journal.hh"
/*----------------------------------------------------------------------------*/

namespace XrdCl {

class JCacheReadHandler : public XrdCl::ResponseHandler
// ---------------------------------------------------------------------- //
{
public:
  JCacheReadHandler() {}

  JCacheReadHandler(JCacheReadHandler *other) {
    rbytes = other->rbytes;
    journal = other->journal;
  }

  JCacheReadHandler(XrdCl::ResponseHandler *handler,
                    std::atomic<uint64_t> *rbytes, Journal *journal)
      : handler(handler), rbytes(rbytes), journal(journal) {}

  virtual ~JCacheReadHandler() {}

  virtual void HandleResponse(XrdCl::XRootDStatus *pStatus,
                              XrdCl::AnyObject *pResponse) {

    XrdCl::ChunkInfo *chunkInfo;
    if (pStatus->IsOK()) {
      if (pResponse) {
        pResponse->Get(chunkInfo);
        // store successfull reads in the journal
        if (journal)
          journal->pwrite(chunkInfo->GetBuffer(), chunkInfo->GetLength(),
                          chunkInfo->GetOffset());
        *rbytes += chunkInfo->GetLength();
      }
    }
    handler->HandleResponse(pStatus, pResponse);
    delete this;
  }

  XrdCl::ResponseHandler *handler;
  std::atomic<uint64_t> *rbytes;
  Journal *journal;
};

} // namespace XrdCl