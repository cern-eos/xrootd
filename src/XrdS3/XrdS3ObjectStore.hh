//
// Created by segransm on 11/17/23.
//

#ifndef XROOTD_XRDS3OBJECTSTORE_HH
#define XROOTD_XRDS3OBJECTSTORE_HH

#include <filesystem>
#include <fstream>
#include <functional>
#include <set>
#include <tuple>
#include <vector>

#include "XrdS3ErrorResponse.hh"
#include "XrdS3Req.hh"

namespace S3 {

struct ObjectInfo {
  std::string name;
  time_t last_modified;
  std::string size;
  std::string owner;
};

struct ListObjectsInfo {
  bool is_truncated;
  std::string key_marker;
  std::string next_marker;
  std::string vid_marker;
  std::string next_vid_marker;
  std::vector<ObjectInfo> objects;
  std::set<std::string> common_prefixes;
};

struct DeletedObject {
  std::string key;
  std::string version_id;
  bool delete_marker;
  std::string delete_marker_version_id;
};

struct ErrorObject {
  S3Error code;
  std::string key;
  std::string message;
  std::string version_id;
};

struct SimpleObject {
  std::string key;
  std::string version_id;
};

class S3ObjectStore {
 public:
  S3ObjectStore() = default;

  explicit S3ObjectStore(std::string path);

  ~S3ObjectStore() = default;

  struct BucketInfo {
    std::string name;
    std::string created;
  };
  class Object {
   public:
    Object() = default;
    ~Object() = default;

    S3Error Init(const std::filesystem::path &path);

    std::ifstream &GetStream() { return ifs; };

    size_t GetSize() const { return size; };
    const std::map<std::string, std::string> &GetAttributes() const {
      return attributes;
    };

   private:
    // 32 MB
    const int bufsize = 32000000;
    size_t read{};
    std::string name{};
    size_t size{};
    std::ifstream ifs{};
    std::map<std::string, std::string> attributes{};
  };

  // todo: handle location constraint
  S3Error CreateBucket(const std::string &id, const std::string &bucket,
                       const std::string &_location);
  S3Error DeleteBucket(const std::string &bucket);
  S3Error GetObject(const std::string &bucket, const std::string &object,
                    Object &obj);
  S3Error DeleteObject(const std::string &bucket, const std::string &object);
  std::string GetBucketOwner(const std::string &bucket) const;
  static S3Error SetMetadata(
      const std::string &object,
      const std::map<std::string, std::string> &metadata);
  std::vector<BucketInfo> ListBuckets(const std::string &id) const;
  ListObjectsInfo ListObjectVersions(const std::string &bucket,
                                     const std::string &prefix,
                                     const std::string &key_marker,
                                     const std::string &version_id_marker,
                                     char delimiter, int max_keys);
  ListObjectsInfo ListObjectsV2(const std::string &bucket,
                                const std::string &prefix,
                                const std::string &continuation_token,
                                char delimiter, int max_keys, bool fetch_owner,
                                const std::string &start_after);
  ListObjectsInfo ListObjects(const std::string &bucket,
                              const std::string &prefix,
                              const std::string &marker, char delimiter,
                              int max_keys);
  S3Error PutObject(XrdS3Req &req, unsigned long size, bool chunked,
                    Headers &headers);

  std::tuple<std::vector<DeletedObject>, std::vector<ErrorObject>>
  DeleteObjects(const std::string &bucket,
                const std::vector<SimpleObject> &objects);

  S3Error CopyObject(const std::string &bucket, const std::string &object,
                     Object &source_obj, const Headers &reqheaders,
                     Headers &headers);

  std::string CreateMultipartUpload(XrdS3Req &req, const std::string &bucket,
                                    const std::string &object);

  struct Part {
    std::string etag;
    std::filesystem::file_time_type last_modified;
    size_t size;
  };
  struct MultipartUpload {
    std::string key;
    std::map<size_t, Part> parts;
  };
  struct MultipartUploadInfo {
    std::string key;
    std::string upload_id;
  };
  struct PartInfo {
    std::string etag;
    std::filesystem::file_time_type last_modified;
    size_t part_number;
    size_t size;
  };

  std::vector<MultipartUploadInfo> ListMultipartUploads(
      const std::string &bucket);

  S3Error AbortMultipartUpload(const string &bucket, const string &key,
                               const string &upload_id);

  typedef std::map<std::string, MultipartUpload> MultipartUploads;
  std::pair<S3Error, std::vector<S3ObjectStore::PartInfo>> ListParts(
      const string &bucket, const string &key, const string &upload_id);

  S3Error UploadPart(XrdS3Req &req, const string &upload_id, size_t part_number,
                    unsigned long size, bool chunked, Headers &headers);
  S3Error CompleteMultipartUpload(XrdS3Req &req, const string &bucket,
                                  const string &key, const string &upload_id,
                                  const std::vector<PartInfo> &parts);

 private:
  static const int BUFFSIZE = 8000;
  char BUFFER[BUFFSIZE]{};
  static bool ValidateBucketName(const std::string &name);

  std::filesystem::path path;

  std::map<std::string, std::string> bucketOwners;
  std::map<std::string, BucketInfo> bucketInfo;
  std::map<std::string, MultipartUploads> multipartUploads;

  ListObjectsInfo ListObjectsCommon(
      const std::string &bucket, std::string prefix, const std::string &marker,
      char delimiter, int max_keys, bool get_versions,
      const std::function<ObjectInfo(const std::filesystem::path &,
                                     const std::string &)> &f);
};

}  // namespace S3

#endif  // XROOTD_XRDS3OBJECTSTORE_HH
