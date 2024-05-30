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
#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <utility>

#include "XrdS3Auth.hh"
#include "XrdS3ObjectStore.hh"
#include "XrdS3Req.hh"
#include "XrdS3Log.hh"

namespace S3 {

//------------------------------------------------------------------------------
//! \brief S3 API class
//!
//! This class is the main entry point to the S3 API.
//! It is responsible for parsing the request and calling the appropriate
//! handler.
//------------------------------------------------------------------------------
class S3Api {
 public:
  S3Api() = default;
  S3Api(S3Log& log, const std::string &config_path, const std::string &region, const std::string &service,
        const std::string &mtpu_path)
    : mLog(&log),objectStore(config_path, mtpu_path), auth(config_path, region, service) {}

  ~S3Api() = default;

  // Bucket Operations
  int CreateBucketHandler(XrdS3Req &req);
  int DeleteBucketHandler(XrdS3Req &req);
  int HeadBucketHandler(XrdS3Req &req);
  int ListBucketsHandler(XrdS3Req &req);

  // Object Operations
  int ListObjectsHandler(XrdS3Req &req);
  int ListObjectsV2Handler(XrdS3Req &req);
  int GetObjectHandler(XrdS3Req &req);
  int HeadObjectHandler(XrdS3Req &req);
  int PutObjectHandler(XrdS3Req &req);
  int DeleteObjectHandler(XrdS3Req &req);
  int DeleteObjectsHandler(XrdS3Req &req);
  int ListObjectVersionsHandler(XrdS3Req &req);
  int CopyObjectHandler(XrdS3Req &req);
  int CreateMultipartUploadHandler(XrdS3Req &req);
  int ListMultipartUploadsHandler(XrdS3Req &req);
  int AbortMultipartUploadHandler(XrdS3Req &req);
  int ListPartsHandler(XrdS3Req &req);
  int UploadPartHandler(XrdS3Req &req);
  int CompleteMultipartUploadHandler(XrdS3Req &req);

  int GetBucketAclHandler(XrdS3Req &req);
  int GetObjectAclHandler(XrdS3Req &req);

  // Not implemented

  int UploadPartCopyHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketAnalyticsConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketCorsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketEncryptionHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketIntelligentTieringConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketInventoryConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketLifecycleHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketMetricsConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketPolicyHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketReplicationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketTaggingHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketWebsiteHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteObjectTaggingHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeletePublicAccessBlockHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketAccelerateConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketAnalyticsConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketCorsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketEncryptionHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketIntelligentTieringConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketInventoryConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketLifecycleHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketLifecycleConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketLocationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketLoggingHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketMetricsConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketNotificationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketNotificationConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketPolicyHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketPolicyStatusHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketReplicationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketRequestPaymentHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketTaggingHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketVersioningHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketWebsiteHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetObjectAttributesHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetObjectLegalHoldHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetObjectLockConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetObjectRetentionHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetObjectTaggingHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetObjectTorrentHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetPublicAccessBlockHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int ListBucketAnalyticsConfigurationsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int ListBucketIntelligentTieringConfigurationsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int ListBucketInventoryConfigurationsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int ListBucketMetricsConfigurationsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketAccelerateConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketAnalyticsConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketCorsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketEncryptionHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketIntelligentTieringConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketInventoryConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketLifecycleHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketLifecycleConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketLoggingHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketMetricsConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketNotificationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketNotificationConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketPolicyHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketReplicationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketRequestPaymentHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketTaggingHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketVersioningHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketWebsiteHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutObjectLegalHoldHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutObjectLockConfigurationHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutObjectRetentionHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutObjectTaggingHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutPublicAccessBlockHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int RestoreObjectHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int SelectObjectContentHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int WriteGetObjectResponseHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int GetBucketOwnershipControlsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketOwnershipControlsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int DeleteBucketOwnershipControlsHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutBucketAclHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }
  int PutObjectAclHandler(XrdS3Req &req) {
    return req.S3ErrorResponse(S3Error::NotImplemented);
  }

 private:
  S3Log* mLog;
  S3ObjectStore objectStore;
  S3Auth auth;
};

}  // namespace S3

