//
// Created by segransm on 11/16/23.
//

#ifndef XROOTD_XRDS3API_HH
#define XROOTD_XRDS3API_HH

#include <utility>

#include "XrdS3Auth.hh"
#include "XrdS3ObjectStore.hh"
#include "XrdS3Req.hh"

namespace S3 {

class S3Api {
 public:
  S3Api() = default;
  S3Api(const std::string& data_path, const std::string &auth_path)
      : objectStore(data_path), auth(auth_path) {}

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
  int DeleteBucketOwnershipControlsHandler(XrdS3Req &req) {
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
  int GetBucketAclHandler(XrdS3Req &req) {
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
  int GetBucketOwnershipControlsHandler(XrdS3Req &req) {
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
  int GetObjectAclHandler(XrdS3Req &req) {
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
  int PutBucketAclHandler(XrdS3Req &req) {
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
  int PutBucketOwnershipControlsHandler(XrdS3Req &req) {
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
  int PutObjectAclHandler(XrdS3Req &req) {
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

 private:
  S3ObjectStore objectStore;
  S3Auth auth;
};

}  // namespace S3

#endif  // XROOTD_XRDS3API_HH
