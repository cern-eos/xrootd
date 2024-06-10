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
#include "vector/XrdClVectorCache.hh"
/*----------------------------------------------------------------------------*/

namespace XrdCl {

class JCacheReadVHandler : public XrdCl::ResponseHandler
  // ---------------------------------------------------------------------- //
{
public:
    JCacheReadVHandler() { }

    JCacheReadVHandler(JCacheReadVHandler* other) {
      
      journal = other->journal;
      buffer = other->buffer;
      rvbytes = other->rvbytes;
      vcachepath = other->vcachepath;
      url = other->url;
    }

    JCacheReadVHandler(XrdCl::ResponseHandler* handler, 
                      std::atomic<uint64_t>* rvbytes,
                      Journal* journal,
                      void* buffer,
                      const std::string& vcachepath,
                      const std::string& url) : handler(handler), rvbytes(rvbytes), journal(journal), buffer(buffer), vcachepath(vcachepath), url(url) {}

    virtual ~JCacheReadVHandler() {}

    virtual void HandleResponse(XrdCl::XRootDStatus* pStatus,
                                XrdCl::AnyObject* pResponse) {              
                                  if (pStatus->IsOK()) {
                                    if (pResponse) {
                                      VectorReadInfo* vReadInfo;
                                      pResponse->Get(vReadInfo);
				      ChunkList* chunks = &(vReadInfo->GetChunks());
                                      // store successfull reads in the journal if there is no vector cache
                                      if (journal) {
                                        if (vcachepath.empty()) {
                                          for (auto it = chunks->begin(); it != chunks->end(); ++it) {
                                            journal->pwrite(it->GetBuffer(), it->GetLength(), it->GetOffset());
                                          }
                                        } else {
                                          VectorCache cache(*chunks, url, (const char*)buffer, vcachepath);
                                          cache.store();
                                        }
                                      }  
                                      for (auto it = chunks->begin(); it != chunks->end(); ++it) {
                                        *rvbytes += it->GetLength();
                                      }
                                    }
                                  }                          
                                  handler->HandleResponse(pStatus, pResponse);
                                }

    XrdCl::ResponseHandler* handler;
    std::atomic<uint64_t>* rvbytes;
    Journal* journal;
    void* buffer;
    std::string vcachepath;
    std::string url;
};

} // namespace XrdCl
