//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Mano Segransan / CERN EOS Project <andreas.joachim.peters@cern.ch>
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
//------------------------------------------------------------------------------

#pragma once

//------------------------------------------------------------------------------
#include <string>
#include <vector>
//------------------------------------------------------------------------------
#include "XrdS3ObjectStore.hh"
#include "XrdS3Req.hh"
//------------------------------------------------------------------------------

namespace S3 {

int ListBucketsResponse(XrdS3Req& req, const std::string& id,
                        const std::string& display_name,
                        const std::vector<S3ObjectStore::BucketInfo>& buckets);

int ListObjectVersionsResponse(XrdS3Req& req, const std::string& bucket,
                               bool encode, char delimiter, int max_keys,
                               const std::string& prefix,
                               const ListObjectsInfo& vinfo);

int DeleteObjectsResponse(XrdS3Req& req, bool quiet,
                          const std::vector<DeletedObject>& deleted,
                          const std::vector<ErrorObject>& err);

int ListObjectsV2Response(XrdS3Req& req, const std::string& bucket,
                          const std::string& prefix,
                          const std::string& continuation_token, char delimiter,
                          int max_keys, bool fetch_owner,
                          const std::string& start_after, bool encode,
                          const ListObjectsInfo& oinfo);

int ListObjectsResponse(XrdS3Req& req, const std::string& bucket,
                        const std::string& prefix, char delimiter,
                        const std::string& marker, int max_keys, bool encode,
                        const ListObjectsInfo& objects);

int CopyObjectResponse(XrdS3Req& req, const std::string& etag);

int CreateMultipartUploadResponse(XrdS3Req& req, const std::string& upload_id);

int ListMultipartUploadResponse(
    XrdS3Req& req,
    const std::vector<S3ObjectStore::MultipartUploadInfo>& uploads);

int ListPartsResponse(XrdS3Req& req, const std::string& upload_id,
                      const std::vector<S3ObjectStore::PartInfo>& parts);

int CompleteMultipartUploadResponse(XrdS3Req& req);

int GetAclResponse(XrdS3Req& req, const S3Auth::Bucket& bucket);

}  // namespace S3
