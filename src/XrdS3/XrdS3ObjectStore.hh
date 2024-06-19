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

#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdS3Auth.hh"
#include "XrdS3ErrorResponse.hh"
#include "XrdS3Log.hh"
#include "XrdS3Req.hh"

namespace S3 {

struct ObjectInfo {
  std::string name;
  std::string etag;
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

  S3ObjectStore(const std::string &config, const std::string &mtpu);

  ~S3ObjectStore() = default;

  struct BucketInfo {
    std::string name;
    std::string created;
  };
  class Object {
   public:
    Object() = default;
    ~Object();

    S3Error Init(const std::filesystem::path &path, uid_t uid, gid_t gid);

    [[nodiscard]] ssize_t GetSize() const { return size; };
    [[nodiscard]] size_t BufferSize() const { return buffer_size; };
    time_t LastModified() const { return last_modified; }
    ssize_t Read(size_t length, char **data);
    off_t Lseek(off_t offset, int whence);
    std::string Name() const { return name; }

    const std::map<std::string, std::string> &GetAttributes() const {
      return attributes;
    };

   private:
    static constexpr size_t MAX_BUFFSIZE = 32000000;
    bool init{};
    std::vector<char> buffer{};
    std::string name{};
    size_t buffer_size{};
    size_t size{};
    time_t last_modified{};
    int fd{};
    uid_t uid;
    gid_t gid;
    std::map<std::string, std::string> attributes{};
  };

  class ExclusiveLocker {
   public:
    // Default constructor
    ExclusiveLocker() = default;

    // Copy constructor
    ExclusiveLocker(const ExclusiveLocker &other) {}

    virtual ~ExclusiveLocker() {}

    // Dummy Assignment operator
    ExclusiveLocker &operator=(const ExclusiveLocker &other) { return *this; }

    // Function to acquire a lock for a given name
    void lock(const std::string &name) {
      std::unique_lock<std::mutex> map_lock(map_mutex_);
      std::shared_ptr<std::mutex> mutex = getOrCreateMutex(name);
      map_lock.unlock();  // Release the map lock before acquiring the
                          // object-specific lock
      mutex->lock();
    }

    // Function to release a lock for a given name
    void unlock(const std::string &name) {
      std::unique_lock<std::mutex> map_lock(map_mutex_);
      auto it = mutex_map_.find(name);
      if (it != mutex_map_.end()) {
        it->second->unlock();
        if (it->second.use_count() == 1) {
          // If this was the last reference, remove the entry from the map
          mutex_map_.erase(it);
        }
      }
    }

   private:
    std::unordered_map<std::string, std::shared_ptr<std::mutex>> mutex_map_;
    std::mutex map_mutex_;

    // Function to get or create a mutex for a given name
    std::shared_ptr<std::mutex> getOrCreateMutex(const std::string &name) {
      auto it = mutex_map_.find(name);
      if (it == mutex_map_.end()) {
        auto mutex = std::make_shared<std::mutex>();
        mutex_map_[name] = mutex;
        return mutex;
      } else {
        return it->second;
      }
    }
  };

  S3Error CreateBucket(S3Auth &auth, S3Auth::Bucket bucket,
                       const std::string &_location);
  S3Error DeleteBucket(S3Auth &auth, const S3Auth::Bucket &bucket);
  S3Error GetObject(const S3Auth::Bucket &bucket, const std::string &object,
                    Object &obj);
  S3Error DeleteObject(const S3Auth::Bucket &bucket, const std::string &object);
  std::string GetBucketOwner(const std::string &bucket) const;
  static S3Error SetMetadata(
      const std::string &object,
      const std::map<std::string, std::string> &metadata);
  std::vector<BucketInfo> ListBuckets(const std::string &id) const;
  ListObjectsInfo ListObjectVersions(const S3Auth::Bucket &bucket,
                                     const std::string &prefix,
                                     const std::string &key_marker,
                                     const std::string &version_id_marker,
                                     char delimiter, int max_keys);
  ListObjectsInfo ListObjectsV2(const S3Auth::Bucket &bucket,
                                const std::string &prefix,
                                const std::string &continuation_token,
                                char delimiter, int max_keys, bool fetch_owner,
                                const std::string &start_after);
  ListObjectsInfo ListObjects(const S3Auth::Bucket &bucket,
                              const std::string &prefix,
                              const std::string &marker, char delimiter,
                              int max_keys);
  S3Error PutObject(XrdS3Req &req, const S3Auth::Bucket &bucket,
                    unsigned long size, bool chunked, Headers &headers);

  std::tuple<std::vector<DeletedObject>, std::vector<ErrorObject>>
  DeleteObjects(const S3Auth::Bucket &bucket,
                const std::vector<SimpleObject> &objects);

  S3Error CopyObject(const S3Auth::Bucket &bucket, const std::string &object,
                     Object &source_obj, const Headers &reqheaders,
                     Headers &headers);
  std::pair<std::string, S3Error> CreateMultipartUpload(
      const S3Auth::Bucket &bucket, const std::string &object);

  struct Part {
    std::string etag;
    time_t last_modified;
    size_t size;
  };
  struct MultipartUpload {
    std::string key;
    std::map<size_t, Part> parts;
    std::set<size_t> progress;
    bool optimized;
    size_t last_part_number;
    size_t part_size;
    size_t last_part_size;
  };
  struct MultipartUploadInfo {
    std::string key;
    std::string upload_id;
  };
  struct PartInfo {
    std::string etag;
    time_t last_modified;
    size_t part_number;
    size_t size;
    std::string str() {
      return "# " + std::to_string(part_number) +
             " size: " + std::to_string(size) + " etag: " + etag +
             " modified: " + std::to_string(last_modified);
    }
    std::string nstr() { return std::to_string(part_number); }
  };

  std::vector<MultipartUploadInfo> ListMultipartUploads(
      const std::string &bucket);

  S3Error AbortMultipartUpload(const S3Auth::Bucket &bucket,
                               const std::string &key,
                               const std::string &upload_id);

  typedef std::map<std::string, MultipartUpload> MultipartUploads;
  std::pair<S3Error, std::vector<S3ObjectStore::PartInfo>> ListParts(
      const std::string &bucket, const std::string &key,
      const std::string &upload_id);

  S3Error UploadPart(XrdS3Req &req, const std::string &upload_id,
                     size_t part_number, unsigned long size, bool chunked,
                     Headers &headers);
  S3Error CompleteMultipartUpload(XrdS3Req &req, const S3Auth::Bucket &bucket,
                                  const std::string &key,
                                  const std::string &upload_id,
                                  const std::vector<PartInfo> &parts);

  static ExclusiveLocker s_exclusive_locker;

 private:
  static bool ValidateBucketName(const std::string &name);

  std::filesystem::path config_path;
  std::filesystem::path user_map;
  std::filesystem::path mtpu_path;

  ListObjectsInfo ListObjectsCommon(
      const S3Auth::Bucket &bucket, std::string prefix,
      const std::string &marker, char delimiter, int max_keys,
      bool get_versions,
      const std::function<ObjectInfo(const std::filesystem::path &,
                                     const std::string &)> &f);
  static S3Error UploadPartOptimized(XrdS3Req &req, const std::string &tmp_path,
                                     size_t part_size, size_t part_number,
                                     size_t size, Headers &headers);

  static std::vector<std::string> GetPartsNumber(const std::string &path);

  static S3Error SetPartsNumbers(const std::string &path,
                                 std::vector<std::string> &parts);

  static S3Error AddPartAttr(const std::string &object, size_t part_number);

  [[nodiscard]] std::string GetUserDefaultBucketPath(
      const std::string &user_id) const;

  bool KeepOptimize(const std::filesystem::path &upload_path,
                    size_t part_number, unsigned long size,
                    const std::string &tmp_path, size_t part_size,
                    std::vector<std::string> &parts);
  [[nodiscard]] static S3Error ValidateMultipartUpload(
      const std::string &upload_path, const std::string &key);
  S3Error DeleteMultipartUpload(const S3Auth::Bucket &bucket,
                                const std::string &key,
                                const std::string &upload_id);

  static bool CompleteOptimizedMultipartUpload(
      const std::filesystem::path &final_path,
      const std::filesystem::path &tmp_path,
      const std::vector<PartInfo> &parts);
};

}  // namespace S3
