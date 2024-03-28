//
// Created by segransm on 11/16/23.
//

#ifndef XROOTD_XRDS3RESPONSE_HH
#define XROOTD_XRDS3RESPONSE_HH

#include <string>
#include <vector>

#include "XrdS3ObjectStore.hh"
#include "XrdS3Req.hh"

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

#endif  // XROOTD_XRDS3RESPONSE_HH
