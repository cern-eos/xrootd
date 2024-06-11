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

class JCachePgReadHandler : public XrdCl::ResponseHandler
// ---------------------------------------------------------------------- //
{
public:
  JCachePgReadHandler() {}

  JCachePgReadHandler(JCachePgReadHandler *other) {
    rbytes = other->rbytes;
    journal = other->journal;
  }

  JCachePgReadHandler(XrdCl::ResponseHandler *handler,
                      std::atomic<uint64_t> *rbytes, Journal *journal)
      : handler(handler), rbytes(rbytes), journal(journal) {}

  virtual ~JCachePgReadHandler() {}

  virtual void HandleResponse(XrdCl::XRootDStatus *pStatus,
                              XrdCl::AnyObject *pResponse) {

    XrdCl::PageInfo *pageInfo;
    if (pStatus->IsOK()) {
      if (pResponse) {
        pResponse->Get(pageInfo);
        // store successfull reads in the journal
        if (journal)
          journal->pwrite(pageInfo->GetBuffer(), pageInfo->GetLength(),
                          pageInfo->GetOffset());
        *rbytes += pageInfo->GetLength();
      }
    }
    handler->HandleResponse(pStatus, pResponse);
  }

  XrdCl::ResponseHandler *handler;
  std::atomic<uint64_t> *rbytes;
  Journal *journal;
};

} // namespace XrdCl
