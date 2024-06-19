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

//------------------------------------------------------------------------------
#include "XrdS3ObjectStore.hh"
//------------------------------------------------------------------------------
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
//------------------------------------------------------------------------------
#include <algorithm>
#include <ctime>
#include <filesystem>
#include <stack>
#include <utility>
//------------------------------------------------------------------------------
#include <XrdOuc/XrdOucTUtils.hh>

#include "XrdCks/XrdCksCalcmd5.hh"
#include "XrdPosix/XrdPosixExtern.hh"
#include "XrdS3.hh"
#include "XrdS3Auth.hh"
#include "XrdS3Req.hh"
#include "XrdS3ScopedFsId.hh"
//------------------------------------------------------------------------------
S3::S3ObjectStore::ExclusiveLocker S3::S3ObjectStore::s_exclusive_locker;
//------------------------------------------------------------------------------

namespace S3 {
//------------------------------------------------------------------------------
//! \brief S3ObjectStore Constructor
//! \param config Path to the configuration file
//! \param mtpu Path to the MTPU directory
//------------------------------------------------------------------------------
S3ObjectStore::S3ObjectStore(const std::string &config, const std::string &mtpu)
    : config_path(config), mtpu_path(mtpu) {
  user_map = config_path / "users";

  XrdPosix_Mkdir(user_map.c_str(), S_IRWXU | S_IRWXG);
  XrdPosix_Mkdir(mtpu_path.c_str(), S_IRWXU | S_IRWXG);
}

//------------------------------------------------------------------------------
//! \brief ValidateBucketName
//! \param name Bucket name to validate
//! \return true if the name is valid, false otherwise
//------------------------------------------------------------------------------
bool S3ObjectStore::ValidateBucketName(const std::string &name) {
  if (name.size() < 3 || name.size() > 63) {
    return false;
  }

  if (!isalnum(name[0]) || !isalnum(name[name.size() - 1])) {
    return false;
  }

  return std::all_of(name.begin(), name.end(), [](const auto &c) {
    return islower(c) || isdigit(c) || c == '.' || c == '-';
  });
}

//------------------------------------------------------------------------------
//! \brief GetUserDefaultBucketPath Get the default bucket path for a user
//! \param user_id User ID
//! \return Default bucket path for the user
//------------------------------------------------------------------------------
std::string S3ObjectStore::GetUserDefaultBucketPath(
    const std::string &user_id) const {
  return S3Utils::GetXattr(user_map / user_id, "new_bucket_path");
}

//------------------------------------------------------------------------------
//! \brief SetMetadata Set metadata for a file or directory
//! \param object Object path
//! \param metadata Metadata to set
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::SetMetadata(
    const std::string &object,
    const std::map<std::string, std::string> &metadata) {
  for (const auto &meta : metadata) {
    S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::SetMetaData",
                                 "%s:=%s on %s", meta.first.c_str(),
                                 meta.second.c_str(), object.c_str());
    if (S3Utils::SetXattr(object, meta.first, meta.second, 0)) {
      S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::SetMetaData", "failed to set %s:=%s on %s", meta.first.c_str(), meta.second.c_str(), object.c_str());
      return S3Error::InternalError;
    }
  }
  return S3Error::None;
}

//------------------------------------------------------------------------------
//! \brief GetPartsNumber Get the parts number for a file
//! \param path Object path
//! \return Parts number for the file
//------------------------------------------------------------------------------
std::vector<std::string> S3ObjectStore::GetPartsNumber(
    const std::string &path) {
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::GetPartsNumber", "%s",
                               path.c_str());
  auto p = S3Utils::GetXattr(path, "parts");

  if (p.empty()) {
    return {};
  }

  std::vector<std::string> res;
  XrdOucTUtils::splitString(res, p, ",");

  return res;
}

//------------------------------------------------------------------------------
//! SetPartsNumber Set the parts number for a file
//! \param path Object path
//! \param parts Parts number to set
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::SetPartsNumbers(const std::string &path,
                                       std::vector<std::string> &parts) {
  auto p = S3Utils::stringJoin(',', parts);
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::SetPartsNumber",
                               "%s : %s", path.c_str(), p.c_str());

  if (S3Utils::SetXattr(path.c_str(), "parts", p, 0)) {
    return S3Error::InternalError;
  }
  return S3Error::None;
}

// TODO: We need a mutex here as multiple threads can write to the same file
//------------------------------------------------------------------------------
//! \brief AddPartAttr Add a part attribute for a file
//! \param object Object path
//! \param part_number Part number
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::AddPartAttr(const std::string &object,
                                   size_t part_number) {
  s_exclusive_locker.lock(object);
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::AddPartAttr", "%s : %u",
                               object.c_str(), part_number);
  auto parts = GetPartsNumber(object);

  auto n = std::to_string(part_number);
  if (std::find(parts.begin(), parts.end(), n) == parts.end()) {
    parts.push_back(n);
    return SetPartsNumbers(object, parts);
  }
  s_exclusive_locker.unlock(object);
  return S3Error::None;
}

//------------------------------------------------------------------------------
//! \brief CreateBucket Create a bucket
//! \param auth Authentication object
//! \param bucket Bucket to create
//! \param _location Location to create the bucket in
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::CreateBucket(S3Auth &auth, S3Auth::Bucket bucket,
                                    const std::string &_location) {
  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::CreateBucket",
                               "%s => %s", bucket.name.c_str(),
                               _location.c_str());
  if (!ValidateBucketName(bucket.name)) {
    return S3Error::InvalidBucketName;
  }

  bucket.path =
      std::filesystem::path(GetUserDefaultBucketPath(bucket.owner.id)) /
      bucket.name;

  auto err = auth.CreateBucketInfo(bucket);
  if (err != S3Error::None) {
    return err;
  }

  auto userInfoBucket = user_map / bucket.owner.id / bucket.name;

  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::CreateBucket",
                               "bucket-path:%s : user-info:%s",
                               bucket.path.c_str(), userInfoBucket.c_str());

  auto fd = XrdPosix_Open(userInfoBucket.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          S_IRWXU | S_IRWXG);
  if (fd <= 0) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::CreateBucket",
                                 "bucket-path:%s failed to open user-info:%s",
                                 bucket.path.c_str(), userInfoBucket.c_str());
    auth.DeleteBucketInfo(bucket);
    return S3Error::InternalError;
  }
  XrdPosix_Close(fd);

  if (S3Utils::SetXattr(userInfoBucket, "createdAt",
                        std::to_string(std::time(nullptr)), XATTR_CREATE)) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::CreateBucket",
        "bucket-path:%s failed to set creation time at user-info:%s",
        bucket.path.c_str(), userInfoBucket.c_str());
    auth.DeleteBucketInfo(bucket);
    XrdPosix_Unlink(userInfoBucket.c_str());
    return S3Error::InternalError;
  }

  if (XrdPosix_Mkdir((mtpu_path / bucket.name).c_str(), S_IRWXU | S_IRWXG)) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::CreateBucket",
                                 "bucket-path:%s failed to create temporary "
                                 "multipart upload directory %s",
                                 bucket.path.c_str(),
                                 (mtpu_path / bucket.name).c_str());
    auth.DeleteBucketInfo(bucket);
    XrdPosix_Unlink(userInfoBucket.c_str());
    return S3Error::InternalError;
  }

  int mkdir_retc = 0;
  {
    // Create the backend directory with the users filesystem id
    ScopedFsId scop(bucket.owner.uid, bucket.owner.gid);
    mkdir_retc = XrdPosix_Mkdir(
        bucket.path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  }
  if (mkdir_retc) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::CreateBucket",
                                 "failed to create bucket-path:%s",
                                 bucket.path.c_str());
    auth.DeleteBucketInfo(bucket);
    XrdPosix_Unlink(userInfoBucket.c_str());
    XrdPosix_Rmdir((mtpu_path / bucket.name).c_str());
    return S3Error::InternalError;
  }

  return S3Error::None;
}

//------------------------------------------------------------------------------
//! BaseDir Split a path into a base directory and a file name
//! \param p Path to split
//! \return Pair of base directory and file name
//------------------------------------------------------------------------------
std::pair<std::string, std::string> BaseDir(std::string p) {
  std::string basedir;
  auto pos = p.rfind('/');
  if (pos != std::string::npos) {
    basedir = p.substr(0, pos);
    p.erase(0, pos + 1);
  }

  return {basedir, p};
}

//------------------------------------------------------------------------------
//! DeleteBucket - Delete a bucket and all its content
//! - we do this only it is empty and the backend bucket directory is not
//! removed! \param auth Authentication object \param bucket Bucket to delete
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::DeleteBucket(S3Auth &auth,
                                    const S3Auth::Bucket &bucket) {
  S3::S3Handler::Logger()->Log(
      S3::INFO, "ObjectStore::DeleteBucket", "bucket-name:%s owner(%u:%u)",
      bucket.name.c_str(), bucket.owner.uid, bucket.owner.gid);
  {
    // Check the backend directory with the users filesystem id
    ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);

    if (!S3Utils::IsDirEmpty(bucket.path)) {
      S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::DeleteBucket",
                                   "error bucket-name:%s is not empty!",
                                   bucket.name.c_str());
      return S3Error::BucketNotEmpty;
    }
  }

  auto upload_path = mtpu_path / bucket.name;
  auto rm_mtpu_uploads = [&upload_path](dirent *entry) {
    if (entry->d_name[0] == '.') {
      return;
    }

    auto dir_name = upload_path / entry->d_name;
    auto rm_files = [&dir_name](dirent *entry) {
      if (entry->d_name[0] == '.') {
        return;
      }
      // TODO: error handling
      XrdPosix_Unlink((dir_name / entry->d_name).c_str());
    };

    S3Utils::DirIterator(dir_name, rm_files);
    // TODO: error handling
    XrdPosix_Rmdir((upload_path / entry->d_name).c_str());
  };

  S3Utils::DirIterator(upload_path, rm_mtpu_uploads);

  // TODO: error handling
  XrdPosix_Rmdir(bucket.path.c_str());
  XrdPosix_Rmdir(upload_path.c_str());
  auth.DeleteBucketInfo(bucket);
  XrdPosix_Unlink((user_map / bucket.owner.id / bucket.name).c_str());

  return S3Error::None;
}

//------------------------------------------------------------------------------
//! \brief Object destructor - close the file descriptor if open
//------------------------------------------------------------------------------
S3ObjectStore::Object::~Object() {
  if (fd != 0) {
    XrdPosix_Close(fd);
  }
}

// TODO: Replace with the real XrdPosix_Listxattr once implemented.
#include "XrdS3XAttr.hh"
#define XrdPosix_Listxattr listxattr

//------------------------------------------------------------------------------
//! \brief Object init
//! \param p Path to the object
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::Object::Init(const std::filesystem::path &p, uid_t uid,
                                    gid_t gid) {
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::Object::Init",
                               "object-path=%s owner(%u:%u)", p.c_str(), uid,
                               gid);
  struct stat buf;

  // Do the backend operations with the users filesystem id
  ScopedFsId scope(uid, gid);
  if (XrdPosix_Stat(p.c_str(), &buf) || S_ISDIR(buf.st_mode)) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::Object::Init",
                                 "no such object - object-path=%s owner(%u:%u)",
                                 p.c_str(), uid, gid);
    return S3Error::NoSuchKey;
  }

  std::vector<std::string> attrnames;
  std::vector<char> attrlist;
  auto attrlen = XrdPosix_Listxattr(p.c_str(), nullptr, 0);
  attrlist.resize(attrlen);
  XrdPosix_Listxattr(p.c_str(), attrlist.data(), attrlen);
  auto i = attrlist.begin();
  while (i != attrlist.end()) {
    auto tmp = std::find(i, attrlist.end(), 0);
    attrnames.emplace_back(i, tmp);
    i = tmp + 1;
  }
  std::vector<char> value;
  for (const auto &attr : attrnames) {
    if (attr.substr(0, 8) != "user.s3.") continue;
    attributes.insert({attr.substr(8), S3Utils::GetXattr(p, attr.substr(8))});
  }

  this->init = true;
  this->name = p;
  this->size = buf.st_size;
  this->buffer_size = std::min(this->size, MAX_BUFFSIZE);
  this->last_modified = buf.st_mtim.tv_sec;

  // store the ownership
  this->uid = uid;
  this->gid = gid;

  return S3Error::None;
}

//------------------------------------------------------------------------------
//! \brief Read a file
//! \param length Number of bytes to read
//! \param ptr Pointer to the buffer
//! \return Number of bytes read
//------------------------------------------------------------------------------
ssize_t S3ObjectStore::Object::Read(size_t length, char **ptr) {
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::Object::Read",
                               "object-path=%s owner(%u:%u) length=%u",
                               name.c_str(), uid, gid, length);
  if (!init) {
    return 0;
  }

  if (fd == 0) {
    this->buffer.resize(this->buffer_size);
    this->fd = XrdPosix_Open(name.c_str(), O_RDONLY);
  }

  size_t l = std::min(length, buffer.size());

  auto ret = XrdPosix_Read(fd, buffer.data(), l);

  *ptr = buffer.data();

  return ret;
}

//------------------------------------------------------------------------------
//! \brief lseek a file
//! \param offset Offset
//! \param whence Whence
//! \return Offset
//------------------------------------------------------------------------------
off_t S3ObjectStore::Object::Lseek(off_t offset, int whence) {
  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::Object::Seek",
      "object-path=%s owner(%u:%u) offset=%lu whence:%d", name.c_str(), uid,
      gid, offset, whence);
  if (!init) {
    return -1;
  }

  if (fd == 0) {
    this->buffer.resize(this->buffer_size);
    this->fd = XrdPosix_Open(name.c_str(), O_RDONLY);
  }

  return XrdPosix_Lseek(fd, offset, whence);
}

//------------------------------------------------------------------------------
//! \brief GetObject - Get an object from the store
//! \param bucket Bucket name
//! \param object Object name
//! \param obj Object to fill
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::GetObject(const S3Auth::Bucket &bucket,
                                 const std::string &object, Object &obj) {
  return obj.Init(bucket.path / object, bucket.owner.uid, bucket.owner.gid);
}

//------------------------------------------------------------------------------
//! \brief DeleteObject - Delete an object from the store
//! \param bucket Bucket name
//! \param object Object name
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::DeleteObject(const S3Auth::Bucket &bucket,
                                    const std::string &key) {
  std::string base, obj;

  auto full_path = bucket.path / key;
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::DeleteObject",
                               "object-path=%s", full_path.c_str());

  if (XrdPosix_Unlink(full_path.c_str())) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::DeleteObject",
                                 "failed to delete object-path=%s",
                                 full_path.c_str());
    return S3Error::NoSuchKey;
  }

  do {
    full_path = full_path.parent_path();
  } while (full_path != bucket.path && !XrdPosix_Rmdir(full_path.c_str()));

  return S3Error::None;
}

//------------------------------------------------------------------------------
//! \brief ListBuckets - List all buckets in the store for a user
//! \param id User ID
//! \param buckets Buckets to fill
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
std::vector<S3ObjectStore::BucketInfo> S3ObjectStore::ListBuckets(
    const std::string &id) const {
  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::ListBuckets", "id:%s",
                               id.c_str());
  std::vector<BucketInfo> buckets;
  auto get_entry = [this, &buckets, &id](dirent *entry) {
    if (entry->d_name[0] == '.') {
      return;
    }

    auto created_at =
        S3Utils::GetXattr(user_map / id / entry->d_name, "createdAt");

    if (created_at.empty()) {
      created_at = "0";
    }

    buckets.push_back(BucketInfo{entry->d_name, created_at});
  };

  S3Utils::DirIterator(user_map / id, get_entry);

  return buckets;
}

// TODO: At the moment object versioning is not supported, this returns a
//  hardcoded object version.
//------------------------------------------------------------------------------
//! \brief ListObjectVersions - List all versions of an object in the store
//! \param bucket Bucket name
//! \param object Object name
//! \param versions Versions to fill
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
ListObjectsInfo S3ObjectStore::ListObjectVersions(
    const S3Auth::Bucket &bucket, const std::string &prefix,
    const std::string &key_marker, const std::string &version_id_marker,
    const char delimiter, int max_keys) {
  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::ListObjectVersions",
      "bucket:%s prefix:%s marker:%s vmarker:%s delimt=%c max-keys:%d",
      bucket.name.c_str(), prefix.c_str(), key_marker.c_str(),
      version_id_marker.c_str(), delimiter, max_keys);
  auto f = [](const std::filesystem::path &root, const std::string &object) {
    struct stat buf;
    if (!stat((root / object).c_str(), &buf)) {
      return ObjectInfo{object, S3Utils::GetXattr(root / object, "etag"),
                        buf.st_mtim.tv_sec, std::to_string(buf.st_size),
                        S3Utils::GetXattr(root / object, "owner")};
    }
    return ObjectInfo{};
  };

  return ListObjectsCommon(bucket, prefix, key_marker, delimiter, max_keys,
                           true, f);
}

//------------------------------------------------------------------------------
//! \brief CopyObject - Copy an object from the store
//! \param bucket Bucket name
//! \param key Object name
//! \param source_obj Object to copy
//! \param reqheaders Request headers
//! \param headers Response headers
//! \return S3Error::None if successful, S3Error::InternalError otherwise
//------------------------------------------------------------------------------
S3Error S3ObjectStore::CopyObject(const S3Auth::Bucket &bucket,
                                  const std::string &key, Object &source_obj,
                                  const Headers &reqheaders, Headers &headers) {
  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::CopyObject",
                               "bucket:%s key:%s src=:%s", bucket.name.c_str(),
                               key.c_str(), source_obj.Name().c_str());
  auto final_path = bucket.path / key;

  struct stat buf;
  if (!XrdPosix_Stat(final_path.c_str(), &buf) && S_ISDIR(buf.st_mode)) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::CopyObject",
        "target:%s is directory => bucket:%s key:%s src=:%s",
        final_path.c_str(), bucket.name.c_str(), source_obj.Name().c_str());
    return S3Error::ObjectExistAsDir;
  }

  auto tmp_path =
      bucket.path / ("." + key + "." + std::to_string(std::time(nullptr)) +
                     std::to_string(std::rand()));

  auto err = S3Utils::makePath((char *)final_path.parent_path().c_str(),
                               S_IRWXU | S_IRWXG);

  if (err == ENOTDIR) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::CopyObject",
        "target:%s exists already=> bucket:%s key:%s src=:%s",
        final_path.c_str(), bucket.name.c_str(), source_obj.Name().c_str());
    return S3Error::ObjectExistInObjectPath;
  } else if (err != 0) {
    return S3Error::InternalError;
  }

  auto fd = XrdPosix_Open(tmp_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          S_IRWXU | S_IRGRP);

  if (fd < 0) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::CopyObject",
        "target:%s failed to create => bucket:%s key:%s src=:%s",
        final_path.c_str(), bucket.name.c_str(), source_obj.Name().c_str());
    S3Utils::RmPath(final_path.parent_path(), bucket.path);
    return S3Error::InternalError;
  }

  XrdCksCalcmd5 xs;
  xs.Init();

  char *ptr;

  ssize_t len = source_obj.GetSize();
  ssize_t i = 0;
  while ((i = source_obj.Read(len, &ptr)) > 0) {
    len -= i;
    xs.Update(ptr, i);
    // TODO: error handling
    XrdPosix_Write(fd, ptr, i);
  }

  // TODO: error handling
  XrdPosix_Close(fd);

  char *fxs = xs.Final();
  std::vector<unsigned char> md5(fxs, fxs + 16);

  std::map<std::string, std::string> metadata = source_obj.GetAttributes();
  auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';
  metadata["etag"] = md5hex;
  headers.clear();
  headers["ETag"] = md5hex;

  if (S3Utils::MapHasEntry(reqheaders, "x-amz-metadata-directive", "REPLACE")) {
    auto add_header = [&metadata, &reqheaders](const std::string &name) {
      if (S3Utils::MapHasKey(reqheaders, name)) {
        metadata[name] = reqheaders.find(name)->second;
      }
    };

    add_header("cache-control");
    add_header("content-disposition");
    add_header("content-type");

    //    metadata.insert({"last-modified",
    //    std::to_string(std::time(nullptr))});
  }

  auto error = SetMetadata(tmp_path, metadata);
  if (error != S3Error::None) {
    XrdPosix_Unlink(tmp_path.c_str());
    S3Utils::RmPath(final_path.parent_path(), bucket.path);
    return error;
  }

  // TODO: error handling
  XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());

  return error;
}

//! When doing multipart uploads, we can do an optimisation for the most common
//! case, by directly writing to the final file, to avoid doing a concatenation
//! later. In order to do this optimisation, the multipart upload needs to
//! fulfill multiple conditions:
//! - Every part must be the same size (except the last part)\n
//! - There must be no gap in the part numbers (this will be checked when
//! completing the multipart upload)\n
//! - There cannot be another upload in progress of the same part.\n

//------------------------------------------------------------------------------
//! \brief KeepOptimize - Optimize the file by keeping only the parts that are
//! needed \param upload_path - The path to the file to optimize \param
//! part_number - The part number of the part to optimize \param size - The size
//! of the part to optimize \param tmp_path - The path to the temporary file
//! \param part_size - The size of the part
//! \return true if the file can be optimized, false otherwise
//------------------------------------------------------------------------------
bool S3ObjectStore::KeepOptimize(const std::filesystem::path &upload_path,
                                 size_t part_number, unsigned long size,
                                 const std::string &tmp_path, size_t part_size,
                                 std::vector<std::string> &parts) {
  // for the time being we disable optimized uploads

  return false;
  auto p = S3Utils::stringJoin(',', parts);
  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::KeepOptimize",
      "upload-path:%s part:%u size:%lu tmp-path=%s, part-size:%u, parts:%s",
      upload_path.c_str(), part_number, size, tmp_path.c_str(), part_size,
      p.c_str());
  size_t last_part_size;
  try {
    last_part_size =
        std::stoul(S3Utils::GetXattr(upload_path, "last_part_size"));
  } catch (std::exception &) {
    S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::KeepOptimize",
                                 "disabling - last_part_size is not set");
    return false;
  }

  if (last_part_size == 0 && part_number != 1) {
    S3::S3Handler::Logger()->Log(
        S3::DEBUG, "ObjectStore::KeepOptimize",
        "disabling - last_part_size is 0 and part_number is not 1 !");
    return false;
  }

  if (part_size == 0) {
    S3Utils::SetXattr(upload_path, "part_size", std::to_string(size),
                      XATTR_REPLACE);
    S3Utils::SetXattr(upload_path, "last_part_size", std::to_string(size),
                      XATTR_REPLACE);
    S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::KeepOptimize",
                                 "setting part_size:%u last_part_size:%u", size,
                                 size);
    return true;
  }

  size_t last_part_number = 0;
  for (auto p : parts) {
    size_t n;
    try {
      n = std::stoul(p);
    } catch (std::exception &) {
      return false;
    }
    if (part_number == n) {
      return false;
    }
    if (n > last_part_number) {
      last_part_number = n;
    }
  }

  if (part_number < last_part_number) {
    // Part must be the same size as the other parts
    if (part_size != size) {
      return false;
    }
  } else {
    // This is the new last part, if `last_part_size` was already set, all the
    // parts do not have the same size.
    if (last_part_size != part_size) {
      return false;
    }
    S3Utils::SetXattr(upload_path, "last_part_size", std::to_string(size),
                      XATTR_REPLACE);
  }

  return true;
}

//------------------------------------------------------------------------------
//! \brief ReadBufferAt - Read a buffer from the request
//! \param req - The request to read from
//! \param md5XS - The md5 checksum
//! \param sha256XS - The sha256 checksum
//! \param fd - The file descriptor to write to
//! \param length - The length of the buffer to read
//! \return S3Error::None if successful, S3Error::IncompleteBody otherwise
//------------------------------------------------------------------------------

S3Error ReadBufferAt(XrdS3Req &req, XrdCksCalcmd5 &md5XS,
                     S3Crypt::S3SHA256 &sha256XS, int fd,
                     unsigned long length) {
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::ReadBufferAt",
                               "fd:%d length:%lu", fd, length);
  int buflen = 0;
  unsigned long readlen = 0;
  char *ptr;
  while (length > 0 &&
         (buflen = req.ReadBody(
              length > INT_MAX ? INT_MAX : static_cast<int>(length), &ptr,
              true)) > 0) {
    readlen = buflen;
    if (length < readlen) {
      return S3Error::IncompleteBody;
    }
    length -= readlen;
    md5XS.Update(ptr, buflen);
    sha256XS.Update(ptr, buflen);

    if (XrdPosix_Write(fd, ptr, buflen) == -1) {
      return S3Error::InternalError;
    }
  }
  if (buflen < 0 || length != 0) {
    return S3Error::IncompleteBody;
  }
  return S3Error::None;
}

//------------------------------------------------------------------------------
//! \brief ReadBufferIntoFile - Read a buffer from the request into a file
//! \param req - The request to read from
//! \param md5XS - The md5 checksum
//! \param sha256XS - The sha256 checksum
//! \param fd - The file descriptor to write to
//! \param length - The length of the buffer to read
//! \return S3Error::None if successful, S3Error::IncompleteBody otherwise
//------------------------------------------------------------------------------
std::pair<S3Error, size_t> ReadBufferIntoFile(XrdS3Req &req,
                                              XrdCksCalcmd5 &md5XS,
                                              S3Crypt::S3SHA256 &sha256XS,
                                              int fd, bool chunked,
                                              unsigned long size) {
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::ReadBufferIntoFile",
                               "fd:%d size:%lu chunked:%d", fd, size, chunked);
#define PUT_LIMIT 5000000000
  auto reader = [&req, &md5XS, &sha256XS, fd](unsigned long length) {
    return ReadBufferAt(req, md5XS, sha256XS, fd, length);
  };

  if (!chunked) {
    return {reader(size), size};
  }

  int length;
  unsigned long final_size = 0;
  XrdOucString chunk_header;

  auto error = S3Error::None;

  do {
    req.BuffgetLine(chunk_header);
    // TODO: Handle chunk extension parameters
    chunk_header.erasefromend(2);
    try {
      length = std::stoi(chunk_header.c_str(), nullptr, 16);
    } catch (std::exception &) {
      return {S3Error::InvalidRequest, 0};
    }
    final_size += length;
    if (final_size > PUT_LIMIT) {
      return {S3Error::EntityTooLarge, 0};
    }
    error = reader(length);
    req.BuffgetLine(chunk_header);
  } while (error == S3Error::None && length != 0);

  return {error, final_size};
#undef PUT_LIMIT
}

//------------------------------------------------------------------------------
//! \breif FileUploadResult - Result of uploading a file
//------------------------------------------------------------------------------
struct FileUploadResult {
  S3Error result;
  sha256_digest sha256;
  std::string md5hex;
  size_t size;
};

//------------------------------------------------------------------------------
//! \brief FileUploader - Upload a file
//! \param req - The request to read from
//! \param chunked - Whether the file is chunked
//! \param size - The size of the file
//! \param path - The path to the file
//! \return The result of the upload
//------------------------------------------------------------------------------
FileUploadResult FileUploader(XrdS3Req &req, bool chunked, size_t size,
                              std::filesystem::path &path) {
  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::FileUploader",
                               "%s path:%s chunked:%d size:%lu",
                               req.trace.c_str(), path.c_str(), chunked, size);
  auto fd = XrdPosix_Open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          S_IRWXU | S_IRGRP);

  if (fd < 0) {
    if (errno == 13) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::FileUploader",
          "access denied : errno:13 path:%s chunked:%d size:%lu", path.c_str(),
          chunked, size);
      return FileUploadResult{S3Error::AccessDenied, {}, {}, {}};
    } else {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::FileUploader",
          "internal error : errno:%d path:%s chunked:%d size:%lu", errno,
          path.c_str(), chunked, size);
      return FileUploadResult{S3Error::InternalError, {}, {}, {}};
    }
  }

  // TODO: Implement handling of different checksum types.
  XrdCksCalcmd5 md5XS;
  md5XS.Init();
  S3Crypt::S3SHA256 sha256XS;
  sha256XS.Init();

  auto [error, final_size] =
      ReadBufferIntoFile(req, md5XS, sha256XS, fd, chunked, size);

  XrdPosix_Close(fd);

  if (error != S3Error::None) {
    XrdPosix_Unlink(path.c_str());
    return FileUploadResult{error, {}, {}, {}};
  }

  char *md5f = md5XS.Final();
  sha256_digest sha256 = sha256XS.Finish();
  std::vector<unsigned char> md5(md5f, md5f + 16);

  if (!req.md5.empty() && req.md5 != md5) {
    error = S3Error::BadDigest;
  } else if (!S3Utils::MapHasEntry(req.lowercase_headers,
                                   "x-amz-content-sha256",
                                   S3Utils::HexEncode(sha256))) {
    error = S3Error::XAmzContentSHA256Mismatch;
  }

  const auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';

  return FileUploadResult{error, sha256, md5hex, final_size};
}

//------------------------------------------------------------------------------
//! \brief UploadPartOptimized - Upload a part of a file
//! \param req - The request to read from
//! \param tmp_path - The path to the temporary file
//! \param part_size - The size of the part
//! \param part_number - The part number
//! \param size - The size of the file
//! \param headers - The headers to set
//! \return The result of the upload
//------------------------------------------------------------------------------
S3Error S3ObjectStore::UploadPartOptimized(XrdS3Req &req,
                                           const std::string &tmp_path,
                                           size_t part_size, size_t part_number,
                                           size_t size, Headers &headers) {
  S3::S3Handler::Logger()->Log(
      S3::INFO, "ObjectStore::UploadPartOptimized",
      "%s tmp-path:%s part-size:%u part-number:%u size:%u", req.trace.c_str(),
      tmp_path.c_str(), part_size, part_number, size);
  auto fd = XrdPosix_Open(tmp_path.c_str(), O_WRONLY);

  if (fd < 0) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::UploadPartOptimized",
                                 "failed to open tmp-path:%s errno:%d",
                                 tmp_path.c_str(), errno);
    return S3Error::InternalError;
  }

  XrdCksCalcmd5 md5XS;
  md5XS.Init();
  S3Crypt::S3SHA256 sha256XS;
  sha256XS.Init();

  long long offset = (long long)part_size * (part_number - 1);

  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::UploadPartOptimized",
                               "tmp-path:%s computed offset:%lld",
                               tmp_path.c_str(), offset);
  XrdPosix_Lseek(fd, offset, SEEK_SET);

  // TODO: error handling
  auto [error, final_size] =
      ReadBufferIntoFile(req, md5XS, sha256XS, fd, false, size);

  XrdPosix_Close(fd);

  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::UploadPartOptimized", "tmp-path:%s upload complete", tmp_path.c_str(), offset);
  if (error != S3Error::None) {
    return error;
  }
  char *md5f = md5XS.Final();
  sha256XS.Finish();
  std::vector<unsigned char> md5(md5f, md5f + 16);

  if (!req.md5.empty() && req.md5 != md5) {
    S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::UploadPartOptimized",
                                 "bad digest - tmp-path:%s", tmp_path.c_str());
    return S3Error::BadDigest;
  }

  const auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';
  std::map<std::string, std::string> metadata;

  auto prefix = "part" + std::to_string(part_number) + '.';
  metadata.insert({prefix + "etag", md5hex});
  headers.insert({"ETag", md5hex});
  metadata.insert({prefix + "start", std::to_string(offset)});

  error = SetMetadata(tmp_path, metadata);
  if (error == S3Error::None) {
    AddPartAttr(tmp_path, part_number);
  }

  return error;
}

// TODO: Needs a mutex for some operations
// TODO: Can be optimized by keeping in memory part information data instead of
//  using xattr

//------------------------------------------------------------------------------
//! \brief UploadPart - Upload a part of a file
//! \param req - The request to read from
//! \param part_number - The part number
//! \param size - The size of the file
//! \param chunked - Whether the file is chunked
//! \param headers - The headers to set
//! \return The result of the upload
//------------------------------------------------------------------------------
S3Error S3ObjectStore::UploadPart(XrdS3Req &req, const std::string &upload_id,
                                  size_t part_number, unsigned long size,
                                  bool chunked, Headers &headers) {
  auto upload_path = mtpu_path / req.bucket / upload_id;

  S3::S3Handler::Logger()->Log(
      S3::INFO, "ObjectStore::UploadPart",
      "%s upload-id:%s part-number:%u size:%lu chunked:%d", req.trace.c_str(),
      upload_id.c_str(), part_number, size, chunked);

  auto err = ValidateMultipartUpload(upload_path, req.object);
  if (err != S3Error::None) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::UploadPart",
        "validation failed - upload-id:%s part-number:%u size:%lu chunked:%d",
        upload_id.c_str(), part_number, size, chunked);
    return err;
  }

  auto optimized = S3Utils::GetXattr(upload_path, "optimized");
  uid_t uid;
  gid_t gid;

  try {
    uid = std::stoul(S3Utils::GetXattr(upload_path, "uid"));
    gid = std::stoul(S3Utils::GetXattr(upload_path, "gid"));
  } catch (std::exception &) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::UploadPart",
        "get attributes for (uid,gid) failed  - upload-id:%s part-number:%u "
        "size:%lu chunked:%d",
        upload_id.c_str(), part_number, size, chunked);

    return S3Error::InternalError;
  }

  // Chunked uploads disables optimizations as we do not know the part size.
  if (chunked && !optimized.empty()) {
    S3::S3Handler::Logger()->Log(
        S3::DEBUG, "ObjectStore::UploadPart",
        "disabling optimization for chunked uploads - upload-id:%s "
        "part-number:%u size:%lu chunked:%d",
        upload_id.c_str(), part_number, size, chunked);

    S3Utils::SetXattr(upload_path, "optimized", "", XATTR_REPLACE);
  }

  if (!optimized.empty()) {
    auto tmp_path = S3Utils::GetXattr(upload_path, "tmp");

    size_t part_size;
    try {
      part_size = std::stoul(S3Utils::GetXattr(upload_path, "part_size"));
    } catch (std::exception &) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::UploadPart",
          "failed to get 'part_size' attribute on '%s' - upload-id:%s "
          "part-number:%u size:%lu chunked:%d",
          upload_path.c_str(), upload_id.c_str(), part_number, size, chunked);

      return S3Error::InternalError;
    }
    {
      std::vector<std::string> parts;
      parts = GetPartsNumber(upload_path);
      if (KeepOptimize(upload_path, part_number, size, tmp_path, part_size,
                       parts)) {
        ScopedFsId scope(uid, gid);
        return UploadPartOptimized(req, tmp_path, part_size, part_number, size,
                                   headers);
      }
    }
    S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::UploadPart",
                                 "disabling optimization - upload-id:%s "
                                 "part-number:%u size:%lu chunked:%d",
                                 upload_id.c_str(), part_number, size, chunked);

    S3Utils::SetXattr(upload_path, "optimized", "", XATTR_REPLACE);
  }

  auto tmp_path = upload_path / ("." + std::to_string(part_number) + "." +
                                 std::to_string(std::time(nullptr)) +
                                 std::to_string(std::rand()));
  auto final_path = upload_path / std::to_string(part_number);

  auto [error, _, md5hex, final_size] =
      FileUploader(req, chunked, size, tmp_path);

  if (error != S3Error::None) {
    return error;
  }

  std::map<std::string, std::string> metadata;
  // Metadata
  metadata.insert({"etag", md5hex});
  headers.insert({"ETag", md5hex});

  error = SetMetadata(tmp_path, metadata);
  if (error != S3Error::None) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::UploadPart",
        "error setting meta-data - unlinking path:%s - upload-id:%s "
        "part-number:%u size:%lu chunked:%d",
        tmp_path.c_str(), upload_id.c_str(), part_number, size, chunked);
    XrdPosix_Unlink(tmp_path.c_str());
  }

  // TODO: handle error
  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::UploadPart",
      "rename s:'%s'=>d:'%s' - upload-id:%s part-number:%u size:%lu chunked:%d",
      tmp_path.c_str(), final_path.c_str(), upload_id.c_str(), part_number,
      size, chunked);
  XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());
  return error;
}

//------------------------------------------------------------------------------
//! \brief PutObject - Put an object
//! \param req - The request to read from
//! \param bucket - The bucket to put the object in
//! \param size - The size of the file
//! \param chunked - Whether the file is chunked
//! \param headers - The headers to set
//! \return The result of the upload
//------------------------------------------------------------------------------
S3Error S3ObjectStore::PutObject(XrdS3Req &req, const S3Auth::Bucket &bucket,
                                 unsigned long size, bool chunked,
                                 Headers &headers) {
  ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
  auto final_path = bucket.path / req.object;

  S3::S3Handler::Logger()->Log(
      S3::INFO, "ObjectStore::PutObject",
      "%s path:%s object-path:%s owner(%u:%u), chunked:%d size:%lu",
      req.trace.c_str(), bucket.path.c_str(), final_path.c_str(),
      bucket.owner.uid, bucket.owner.gid, chunked, size);

  struct stat buf;
  if (!XrdPosix_Stat(final_path.c_str(), &buf) && S_ISDIR(buf.st_mode)) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::PutObject",
                                 "path:%s is a directory", final_path.c_str());
    return S3Error::ObjectExistAsDir;
  }

  auto tmp_path =
      final_path.parent_path() /
      ("." + final_path.filename().string() + "." +
       std::to_string(std::time(nullptr)) + std::to_string(std::rand()));

  auto err = S3Utils::makePath((char *)final_path.parent_path().c_str(),
                               S_IRWXU | S_IRGRP);
  if (err == ENOTDIR) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::PutObject",
                                 "object exists in object path -  path:%s",
                                 final_path.c_str());
    return S3Error::ObjectExistInObjectPath;
  } else if (err != 0) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::PutObject",
                                 "internal error makeing parent : path:%s",
                                 final_path.c_str());
    return S3Error::InternalError;
  }

  auto [error, _, md5hex, final_size] =
      FileUploader(req, chunked, size, tmp_path);

  if (error != S3Error::None) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::PutObject",
                                 "upload to path:%s failed - unlink path:%s",
                                 tmp_path.c_str(), tmp_path.c_str());
    XrdPosix_Unlink(tmp_path.c_str());
    S3Utils::RmPath(final_path.parent_path(), bucket.path);
    return error;
  }

  std::map<std::string, std::string> metadata;
  // Metadata
  auto add_header = [&metadata, &req](const std::string &name) {
    if (S3Utils::MapHasKey(req.lowercase_headers, name)) {
      metadata.insert({name, req.lowercase_headers.find(name)->second});
    }
  };

  add_header("cache-control");
  add_header("content-disposition");
  add_header("content-type");

  // TODO: etag is not always md5.
  // (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)
  metadata.insert({"etag", md5hex});
  headers.insert({"ETag", md5hex});
  // TODO: Add metadata from other headers.
  // TODO: Handle non asccii characters.
  // (https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)
  for (const auto &hd : req.lowercase_headers) {
    if (hd.first.substr(0, 11) == "x-amz-meta-") {
      metadata.insert({hd.first, hd.second});
    }
  }
  error = SetMetadata(tmp_path, metadata);
  if (error != S3Error::None) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::PutObject",
        "setting meta-data on path:%s failed - unlink path:%s",
        tmp_path.c_str(), tmp_path.c_str());
    XrdPosix_Unlink(tmp_path.c_str());
    S3Utils::RmPath(final_path.parent_path(), bucket.path);
    return error;
  }

  // TODO: error handling
  XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());

  return error;
}

//------------------------------------------------------------------------------
//! \brief DeleteObjects - Delete multiple objects
//! \param bucket - The bucket to delete the objects in
//! \param objects - The objects to delete
//! \return The result of the delete
//------------------------------------------------------------------------------
std::tuple<std::vector<DeletedObject>, std::vector<ErrorObject>>
S3ObjectStore::DeleteObjects(const S3Auth::Bucket &bucket,
                             const std::vector<SimpleObject> &objects) {
  std::vector<DeletedObject> deleted;
  std::vector<ErrorObject> error;

  for (const auto &o : objects) {
    S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::DeleteObjects",
                                 "bucket:%s object:%s", bucket.name.c_str(),
                                 o.key.c_str());
    auto err = DeleteObject(bucket, o.key);
    if (err == S3Error::None || err == S3Error::NoSuchKey) {
      if (err == S3Error::None) {
        S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::DeleteObjects",
                                     "deleted bucket:%s object:%s",
                                     bucket.name.c_str(), o.key.c_str());
      } else {
        S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::DeleteObjects",
                                     "no such key - bucket:%s object:%s",
                                     bucket.name.c_str(), o.key.c_str());
      }
      deleted.push_back({o.key, o.version_id, false, ""});
    } else {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::DeleteObjects",
          "internal error when delting bucket:%s object:%s",
          bucket.name.c_str(), o.key.c_str());
      error.push_back({S3Error::InternalError, o.key, "", o.version_id});
    }
  }
  return {deleted, error};
}

//------------------------------------------------------------------------------
//! \brief ListObjectsV2 - List objects in a bucket
//! \param bucket - The bucket to list the objects in
//! \param prefix - The prefix to filter the objects by
//! \param continuation_token - The continuation token to use for pagination
//! \param delimiter - The delimiter to use for grouping objects
//! \param max_keys - The maximum number of keys to return
//! \param fetch_owner - Whether to fetch the owner of the objects
//! \param start_after - The key to start after
//! \return The result of the list
//------------------------------------------------------------------------------
ListObjectsInfo S3ObjectStore::ListObjectsV2(
    const S3Auth::Bucket &bucket, const std::string &prefix,
    const std::string &continuation_token, const char delimiter, int max_keys,
    bool fetch_owner, const std::string &start_after) {
  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::ListObjectsV2",
                               "bucket:%s prefix:%s cont-token:%s delimiter:%c "
                               "max-keys:%d fetch-owner:%d start-after:%s",
                               bucket.name.c_str(), prefix.c_str(),
                               continuation_token.c_str(), delimiter, max_keys,
                               fetch_owner, start_after.c_str());

  auto f = [fetch_owner](const std::filesystem::path &root,
                         const std::string &object) {
    struct stat buf;

    std::string owner;
    if (fetch_owner) {
      owner = S3Utils::GetXattr(root / object, "owner");
    }

    if (!stat((root / object).c_str(), &buf)) {
      return ObjectInfo{object, S3Utils::GetXattr(root / object, "etag"),
                        buf.st_mtim.tv_sec, std::to_string(buf.st_size), owner};
    }
    return ObjectInfo{};
  };

  return ListObjectsCommon(
      bucket, prefix,
      continuation_token.empty() ? start_after : continuation_token, delimiter,
      max_keys, false, f);
}

//------------------------------------------------------------------------------
//! \brief ListObjects - List objects in a bucket
//! \param bucket - The bucket to list the objects in
//! \param prefix - The prefix to filter the objects by
//! \param marker - The marker to use for pagination
//! \param delimiter - The delimiter to use for grouping objects
//! \param max_keys - The maximum number of keys to return
//! \return The result of the list
//------------------------------------------------------------------------------
ListObjectsInfo S3ObjectStore::ListObjects(const S3Auth::Bucket &bucket,
                                           const std::string &prefix,
                                           const std::string &marker,
                                           const char delimiter, int max_keys) {
  S3::S3Handler::Logger()->Log(
      S3::INFO, "ObjectStore::ListObjects",
      "bucket:%s prefix:%s marker:%s delimiter:%c max-keys:%d",
      bucket.name.c_str(), prefix.c_str(), marker.c_str(), delimiter, max_keys);

  auto f = [](const std::filesystem::path &root, const std::string &object) {
    struct stat buf;

    if (!stat((root / object).c_str(), &buf)) {
      return ObjectInfo{object, S3Utils::GetXattr(root / object, "etag"),
                        buf.st_mtim.tv_sec, std::to_string(buf.st_size),
                        S3Utils::GetXattr(root / object, "owner")};
    }
    return ObjectInfo{};
  };

  return ListObjectsCommon(bucket, prefix, marker, delimiter, max_keys, false,
                           f);
}

// TODO: Replace with real XrdPosix_Scandir once implemented, or replace with
//  custom function.
#define XrdPosix_Scandir scandir

//------------------------------------------------------------------------------
//! \brief ListObjectsCommon - Common logic for listing objects
//! \param bucket - The bucket to list the objects in
//! \param prefix - The prefix to filter the objects by
//! \param marker - The marker to use for pagination
//! \param delimiter - The delimiter to use for grouping objects
//! \param max_keys - The maximum number of keys to return
//! \param get_versions - Whether to get versions of the objects
//! \param f - The function to call for each object
//! \return The result of the list
//------------------------------------------------------------------------------
ListObjectsInfo S3ObjectStore::ListObjectsCommon(
    const S3Auth::Bucket &bucket, std::string prefix, const std::string &marker,
    char delimiter, int max_keys, bool get_versions,
    const std::function<ObjectInfo(const std::filesystem::path &,
                                   const std::string &)> &f) {
  std::string basedir;
  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::ListObjectsCommon",
      "bucket:%s prefix:%s marker:%s delimiter:%c max-keys:%d get-version:%d",
      bucket.name.c_str(), prefix.c_str(), marker.c_str(), delimiter, max_keys,
      get_versions);

  if (prefix == "/" || max_keys == 0) {
    return {};
  }

  std::tie(basedir, prefix) = BaseDir(prefix);

  if (!basedir.empty()) {
    basedir += '/';
  }

  auto fullpath = bucket.path;
  struct BasicPath {
    std::string base;
    std::string name;
    unsigned char d_type;
  };

  std::deque<BasicPath> entries;

  struct dirent **ent = nullptr;
  int n;
  if ((n = XrdPosix_Scandir((fullpath / basedir).c_str(), &ent, nullptr,
                            alphasort)) < 0) {
    return {};
  }
  for (auto i = 0; i < n; i++) {
    if (prefix.compare(0, prefix.size(), std::string(ent[i]->d_name), 0,
                       prefix.size()) == 0) {
      entries.push_back({basedir, ent[i]->d_name, ent[i]->d_type});
    }
    free(ent[i]);
  }
  free(ent);
  ent = nullptr;

  ListObjectsInfo list{};

  while (!entries.empty()) {
    auto entry = entries.front();
    entries.pop_front();

    if (entry.name == "." || entry.name == "..") {
      continue;
    }

    auto entry_path = entry.base + entry.name;

    // Skip to marker
    if (entry_path.compare(0, marker.size(), marker) < 0) {
      continue;
    }
    // When listing versions, the marker indicated the key to start with, and
    // not the last key to skip
    if (!get_versions) {
      if (!marker.empty() && entry_path == marker) {
        continue;
      }
    }

    if (list.objects.size() + list.common_prefixes.size() >= (size_t)max_keys) {
      list.is_truncated = true;
      list.next_marker = entry_path;
      list.next_vid_marker = "1";
      return list;
    }

    size_t m;
    if ((m = entry_path.find(delimiter, prefix.length() + basedir.length() +
                                            1)) != std::string::npos) {
      list.common_prefixes.insert(entry_path.substr(0, m + 1));
      list.key_marker = entry_path.substr(0, m + 1);
      list.vid_marker = "1";
      continue;
    }

    if (entry.d_type == DT_UNKNOWN) {
      continue;
    }

    if (entry.d_type == DT_DIR) {
      if (delimiter == '/') {
        list.common_prefixes.insert(entry_path + '/');
        list.key_marker = entry_path + '/';
        list.vid_marker = "1";
        continue;
      }

      if ((n = scandir((fullpath / entry_path).c_str(), &ent, nullptr,
                       alphasort)) < 0) {
        return {};
      }
      for (size_t i = n; i > 0; i--) {
        entries.push_front(
            {entry_path + '/', ent[i - 1]->d_name, ent[i - 1]->d_type});
        free(ent[i - 1]);
      }
      free(ent);
      continue;
    }

    list.objects.push_back(f(fullpath, entry_path));
    list.key_marker = entry_path;
    list.vid_marker = "1";
  }
  list.next_marker = "";
  list.next_vid_marker = "";
  return list;
}

//------------------------------------------------------------------------------
//! \brief CreateMultipartUpload - Create a multipart upload
//! \param bucket - The bucket to create the upload in
//! \param key - The key to create the upload for
//! \return The result of the upload
//------------------------------------------------------------------------------
std::pair<std::string, S3Error> S3ObjectStore::CreateMultipartUpload(
    const S3Auth::Bucket &bucket, const std::string &key) {
  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::CreateMultipartUpload",
                               "bucket:%s key:%s", bucket.name.c_str(),
                               key.c_str());
  // TODO: Metadata uploaded with the create multipart upload operation is not
  //  saved to the final file.

  auto upload_id = S3Utils::HexEncode(
      S3Crypt::SHA256_OS(bucket.name + key + std::to_string(std::rand())));

  auto final_path = bucket.path / key;
  auto tmp_path =
      final_path.parent_path() /
      ("." + final_path.filename().string() + "." +
       std::to_string(std::time(nullptr)) + std::to_string(std::rand()));

  auto pp = mtpu_path / bucket.name;
  auto p = mtpu_path / bucket.name / upload_id;

  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::CreateMultipartUpload",
      "bucket:%s key:%s tmp-upload-path:%s final-path:%s", bucket.name.c_str(),
      key.c_str(), p.c_str(), final_path.c_str());
  // TODO: error handling
  XrdPosix_Mkdir(pp.c_str(), S_IRWXU | S_IRGRP);
  XrdPosix_Mkdir(p.c_str(), S_IRWXU | S_IRGRP);

  {
    // we have to do this as the owner of the bucket
    ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
    auto err = S3Utils::makePath((char *)final_path.parent_path().c_str(),
                                 S_IRWXU | S_IRGRP);

    if (err == ENOTDIR) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::CreateMultipartUpload",
          "bucket:%s key:%s object exists in path:%s", bucket.name.c_str(),
          key.c_str(), final_path.c_str());
      return {{}, S3Error::ObjectExistInObjectPath};
    } else if (err != 0) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::CreateMultipartUpload",
          "internal error - bucket:%s key:%s tmp-upload-path:%s final-path:%s",
          bucket.name.c_str(), key.c_str(), p.c_str(), final_path.c_str());
      return {{}, S3Error::InternalError};
    }

    auto fd = XrdPosix_Open(tmp_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                            S_IRWXU | S_IRGRP);

    if (fd < 0) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::CreateMultipartUpload",
          "bucket:%s key:%s failed to create tmp-upload-path:%s",
          bucket.name.c_str(), key.c_str(), tmp_path.c_str());
      S3Utils::RmPath(final_path.parent_path(), bucket.path);
      return {{}, S3Error::InternalError};
    }
  }

  S3Utils::SetXattr(p, "key", key, XATTR_CREATE);
  S3Utils::SetXattr(p, "optimized", "1", XATTR_CREATE);
  S3Utils::SetXattr(p, "tmp", tmp_path, XATTR_CREATE);
  S3Utils::SetXattr(p, "part_size", "0", XATTR_CREATE);
  S3Utils::SetXattr(p, "last_part_size", "0", XATTR_CREATE);
  S3Utils::SetXattr(p, "uid", std::to_string(bucket.owner.uid).c_str(),
                    XATTR_CREATE);
  S3Utils::SetXattr(p, "gid", std::to_string(bucket.owner.gid).c_str(),
                    XATTR_CREATE);

  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::CreateMultipartUpload",
                               "bucket:%s key:%s upload-id:%s",
                               bucket.name.c_str(), key.c_str(),
                               upload_id.c_str());
  return {upload_id, S3Error::None};
}

//------------------------------------------------------------------------------
//! \brief ListMultipartUploads - List all multipart uploads for a bucket
//! \param bucket - bucket name
//! \return vector of multipart upload info
//------------------------------------------------------------------------------
std::vector<S3ObjectStore::MultipartUploadInfo>
S3ObjectStore::ListMultipartUploads(const std::string &bucket) {
  auto upload_path = mtpu_path / bucket;

  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::ListMultipartUpload",
                               "bucket:%s upload-path:%s", bucket.c_str(),
                               upload_path.c_str());
  std::vector<MultipartUploadInfo> uploads;
  auto parse_upload = [upload_path, &uploads](dirent *entry) {
    if (entry->d_name[0] == '.') {
      return;
    }

    auto key = S3Utils::GetXattr(upload_path / entry->d_name, "key");

    uploads.push_back({key, entry->d_name});
  };

  S3Utils::DirIterator(upload_path, parse_upload);

  return uploads;
}

//------------------------------------------------------------------------------
//! \brief AbortMultipartUpload - Abort a multipart upload
//! \param bucket - The bucket to abort the upload in
//! \param key - The key to abort the upload for
//! \param upload_id - The upload id to abort
//! \return The result of the abort
//------------------------------------------------------------------------------
S3Error S3ObjectStore::AbortMultipartUpload(const S3Auth::Bucket &bucket,
                                            const std::string &key,
                                            const std::string &upload_id) {
  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::AbortMultipartUpload",
                               "bucket:%s key:%s upload-id:%s",
                               bucket.name.c_str(), key.c_str(),
                               upload_id.c_str());

  auto upload_path = mtpu_path / bucket.name / upload_id;
  auto tmp_path = S3Utils::GetXattr(upload_path, "tmp");
  auto err = DeleteMultipartUpload(bucket, key, upload_id);
  if (err != S3Error::None) {
    return err;
  }

  // TODO: error handling
  XrdPosix_Unlink(tmp_path.c_str());
  S3Utils::RmPath(std::filesystem::path(tmp_path), bucket.path);

  return err;
};

//------------------------------------------------------------------------------
//! \brief DeleteMultipartUpload - Delete a multipart upload
//! \param bucket - The bucket to delete the upload in
//! \param key - The key to delete the upload for
//! \param upload_id - The upload id to delete
//! \return The result of the delete
//------------------------------------------------------------------------------
S3Error S3ObjectStore::DeleteMultipartUpload(const S3Auth::Bucket &bucket,
                                             const std::string &key,
                                             const std::string &upload_id) {
  auto upload_path = mtpu_path / bucket.name / upload_id;

  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::DeleteMultipartUpload",
                               "bucket:%s key:%s upload-id:%s",
                               bucket.name.c_str(), key.c_str(),
                               upload_id.c_str());

  auto err = ValidateMultipartUpload(upload_path, key);
  if (err != S3Error::None) {
    return err;
  }

  auto rm_files = [&upload_path](dirent *entry) {
    if (entry->d_name[0] == '.') {
      return;
    }
    // TODO: error handling
    XrdPosix_Unlink((upload_path / entry->d_name).c_str());
  };

  S3Utils::DirIterator(upload_path, rm_files);

  // TODO: error handling
  XrdPosix_Rmdir(upload_path.c_str());

  return S3Error::None;
}

//------------------------------------------------------------------------------
//! \brief ValidateMultipartUpload - Validate a multipart upload
//! \param upload_path - The path to the upload
//! \param key - The key to validate the upload for
//! \return The result of the validation
//------------------------------------------------------------------------------
S3Error S3ObjectStore::ValidateMultipartUpload(const std::string &upload_path,
                                               const std::string &key) {
  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::ValidateMultipartUpload",
      "key:%s upload-path:%s", key.c_str(), upload_path.c_str());

  struct stat buf;

  if (XrdPosix_Stat(upload_path.c_str(), &buf)) {
    return S3Error::NoSuchUpload;
  }

  auto upload_key = S3Utils::GetXattr(upload_path, "key");
  if (upload_key != key) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::ValidateMultipartUpload",
        "key:%s upload-key:%s - keys do not match - invalid request",
        key.c_str(), upload_key.c_str());
    return S3Error::InvalidRequest;
  }

  return S3Error::None;
}

//------------------------------------------------------------------------------
//! \brief ListParts - List all parts for a multipart upload
//! \param bucket - The bucket to list the parts in
//! \param key - The key to list the parts for
//! \param upload_id - The upload id to list the parts for
//! \return The result of the list
//------------------------------------------------------------------------------
std::pair<S3Error, std::vector<S3ObjectStore::PartInfo>>
S3ObjectStore::ListParts(const std::string &bucket, const std::string &key,
                         const std::string &upload_id) {
  auto upload_path = mtpu_path / bucket / upload_id;

  S3::S3Handler::Logger()->Log(S3::DEBUG, "ObjectStore::ListParts",
                               "bucket:%s key:%s upload-id:%s upload-path:%s",
                               bucket.c_str(), key.c_str(), upload_id.c_str(),
                               upload_path.c_str());
  auto err = ValidateMultipartUpload(upload_path, key);
  if (err != S3Error::None) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::ListParts",
        "bucket:%s key:%s upload-id:%s upload-path:%s validation failed",
        bucket.c_str(), key.c_str(), upload_id.c_str(), upload_path.c_str());
    return {err, {}};
  }

  std::vector<PartInfo> parts;
  auto parse_part = [upload_path, &parts](dirent *entry) {
    if (entry->d_name[0] == '.') {
      return;
    }

    auto etag = S3Utils::GetXattr(upload_path / entry->d_name, "etag");

    struct stat buf;
    // TODO: error handling
    XrdPosix_Stat((upload_path / entry->d_name).c_str(), &buf);

    unsigned long n;
    try {
      n = std::stoul(entry->d_name);
    } catch (std::exception &e) {
      return;
    }
    parts.push_back(PartInfo{etag, buf.st_mtim.tv_sec, n,
                             static_cast<size_t>(buf.st_size)});
  };

  S3Utils::DirIterator(upload_path, parse_part);

  return {S3Error::None, parts};
}

//------------------------------------------------------------------------------
//! \brief Complete an optimized multipart upload
//!
//! \param final_path Path to the final file
//! \param tmp_path Path to the temporary directory
//! \param parts Vector of parts to upload
//!
//! \return True if the upload was successful, false otherwise
//------------------------------------------------------------------------------
bool S3ObjectStore::CompleteOptimizedMultipartUpload(
    const std::filesystem::path &final_path,
    const std::filesystem::path &tmp_path, const std::vector<PartInfo> &parts) {
  std::string p;
  for (auto i : parts) {
    p += i.nstr();
    p += ",";
  }
  p.pop_back();
  S3::S3Handler::Logger()->Log(S3::INFO, "ObjectStore::CompleteOptimizedMultipartUpload", "final-path:%s tmp-path:%s parts:%s",
			       final_path.c_str(), tmp_path.c_str(), p.c_str());
  size_t e = 1;

  for (const auto &[etag, _, n, __] : parts) {
    S3::S3Handler::Logger()->Log(
        S3::DEBUG, "ObjectStore::CompleteOptimizedMultipartUpload",
        "final-path:%s tmp-path:%s part:%d etag:%s", final_path.c_str(),
        tmp_path.c_str(), n, etag.c_str());
    if (e != n) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::CompleteOptimizedMultipartUpload",
          "final-path:%s tmp-path:%s part:%d etag:%s e!=n", final_path.c_str(),
          tmp_path.c_str(), n, etag.c_str());
      return false;
    }

    e++;
    auto id = "part" + std::to_string(n);
    if (S3Utils::GetXattr(tmp_path, id + ".start").empty()) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::CompleteOptimizedMultipartUpload",
          "final-path:%s tmp-path:%s part:%d etag:%s '.start' attribute is "
          "empty",
          final_path.c_str(), tmp_path.c_str(), n, etag.c_str());
      return false;
    }

    if (S3Utils::GetXattr(tmp_path, id + ".etag") != etag) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::CompleteOptimizedMultipartUpload",
          "final-path:%s tmp-path:%s part:%d etag:%s '.etag' attribute is "
          "empty",
          final_path.c_str(), tmp_path.c_str(), n, etag.c_str());
      return false;
    }
  }

  // TODO: error handling
  XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());
  S3::S3Handler::Logger()->Log(
      S3::INFO, "ObjectStore::CompleteOptimizedMultipartUpload",
      "final-path:%s tmp-path:%s parts:%s has been successfully finalized",
      final_path.c_str(), tmp_path.c_str(), p.c_str());

  return true;
}

//------------------------------------------------------------------------------
//! \brief CompleteMultipartUpload - Complete a multipart upload
//! \param req - The request object
//! \param bucket - The bucket to complete the upload in
//! \param key - The key to complete the upload for
//! \param upload_id - The upload id to complete
//! \param parts - The parts to complete the upload with
//! \return The result of the complete
//------------------------------------------------------------------------------
S3Error S3ObjectStore::CompleteMultipartUpload(
    XrdS3Req &req, const S3Auth::Bucket &bucket, const std::string &key,
    const std::string &upload_id, const std::vector<PartInfo> &parts) {
  auto upload_path = mtpu_path / req.bucket / upload_id;
  std::string p;
  for (auto i : parts) {
    p += i.nstr();
    p += ",";
  }
  p.pop_back();

  S3::S3Handler::Logger()->Log(
      S3::INFO, "ObjectStore::CompleteMultipartUpload",
      "%s bucket:%s key:%s upload-id:%s parts:%s upload-path:%s",
      req.trace.c_str(), bucket.name.c_str(), key.c_str(), upload_id.c_str(),
      p.c_str(), upload_path.c_str());
  auto err = ValidateMultipartUpload(upload_path, req.object);
  if (err != S3Error::None) {
    S3::S3Handler::Logger()->Log(
        S3::ERROR, "ObjectStore::CompleteMultipartUpload",
        "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s didn't get "
        "validated",
        bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
        upload_path.c_str());
    return err;
  }

  auto final_path = bucket.path / req.object;
  auto opt_path = S3Utils::GetXattr(upload_path, "tmp");

  auto optimized = S3Utils::GetXattr(upload_path, "optimized");

  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
      "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
      "opt-path:%s final-path:%s",
      bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
      upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
      final_path.c_str());

  // Check if we are able to complete the multipart upload with only a mv
  // operation.
  if (!optimized.empty()) {
    ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
    if (!CompleteOptimizedMultipartUpload(final_path, opt_path, parts)) {
      return DeleteMultipartUpload(bucket, key, upload_id);
    }
  } else {
    // Otherwise we will need to concatenate parts

    // First check that all the parts are in order.
    // We first check if a file named partN exists in the multipart upload dir,
    // then check if it exists in the optimized upload tmp path.
    size_t max = 0;
    struct stat buf;
    for (const auto &[etag, _, n, __] : parts) {
      if (n <= max) {
        S3::S3Handler::Logger()->Log(
            S3::ERROR, "ObjectStore::CompleteMultipartUpload",
            "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s "
            "optimized:%d opt-path:%s final-path:%s invalid part ordering",
            bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
            upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
            final_path.c_str());
        return S3Error::InvalidPartOrder;
      }
      max = n;

      auto path = upload_path / std::to_string(n);
      // TODO: error handling
      if (XrdPosix_Stat(path.c_str(), &buf)) {
        auto id = "part" + std::to_string(n);
        if (S3Utils::GetXattr(opt_path, id + ".start").empty()) {
          S3::S3Handler::Logger()->Log(
              S3::ERROR, "ObjectStore::CompleteMultipartUpload",
              "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s "
              "optimized:%d opt-path:%s final-path:%s invalid part .start "
              "attribute",
              bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
              upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
              final_path.c_str());

          return S3Error::InvalidPart;
        }
        if (S3Utils::GetXattr(opt_path, id + ".etag") != etag) {
          S3::S3Handler::Logger()->Log(
              S3::ERROR, "ObjectStore::CompleteMultipartUpload",
              "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s "
              "optimized:%d opt-path:%s final-path:%s invalid part .etag "
              "attribute",
              bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
              upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
              final_path.c_str());

          return S3Error::InvalidPart;
        }
      } else {
        if (S3Utils::GetXattr(path, "etag") != etag) {
          S3::S3Handler::Logger()->Log(
              S3::ERROR, "ObjectStore::CompleteMultipartUpload",
              "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s "
              "optimized:%d opt-path:%s final-path:%s invalid .etag attribute",
              bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
              upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
              final_path.c_str());
          return S3Error::InvalidPart;
        }
      }
    }
  }

  {
    // Check if the final file exists in the backend and is a directory
    ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
    struct stat buf;
    if (!XrdPosix_Stat(final_path.c_str(), &buf)) {
      if (S_ISDIR(buf.st_mode)) {
        S3::S3Handler::Logger()->Log(
            S3::ERROR, "ObjectStore::CompleteMultipartUpload",
            "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s "
            "optimized:%d opt-path:%s final-path:%s final-path is a directory!",
            bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
            upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
            final_path.c_str());
        return S3Error::ObjectExistAsDir;
      }
    } else {
      if (errno != ENOENT) {
        S3::S3Handler::Logger()->Log(
            S3::ERROR, "ObjectStore::CompleteMultipartUpload",
            "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s "
            "optimized:%d opt-path:%s final-path:%s final-path cannot be "
            "accessed!",
            bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
            upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
            final_path.c_str());
        return S3Error::AccessDenied;
      }
    }
  }

  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
      "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
      "opt-path:%s final-path:%s copying parts to final destination via "
      "tempfile+rename ...",
      bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
      upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
      final_path.c_str());

  // Then we copy all the parts into a tmp file, which will be renamed to the
  // final file.
  auto tmp_path = bucket.path /
                  ("." + req.object + "." + std::to_string(std::time(nullptr)) +
                   std::to_string(std::rand()));

  int fd = 0;
  {
    // The temp file has to created using the filesystem id of the owner
    ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
    fd = XrdPosix_Open(tmp_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                       S_IRWXU | S_IRGRP);
  }

  if (fd < 0) {
    S3::S3Handler::Logger()->Log(S3::ERROR, "ObjectStore::CompleteMultipartUpload", "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d opt-path:%s final-path:%s final-path:%s failed to open tmp-path:%s!",
				 bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(), upload_path.c_str(), optimized.length()?1:0, opt_path.c_str(), final_path.c_str(), tmp_path.c_str());
    return S3Error::InternalError;
  }

  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
      "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
      "opt-path:%s final-path:%s starting checksummming ...",
      bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
      upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
      final_path.c_str());

  XrdCksCalcmd5 xs;
  xs.Init();

  Object optimized_obj;
  optimized_obj.Init(opt_path, bucket.owner.uid, bucket.owner.gid);

  ssize_t opt_len;
  try {
    ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
    opt_len = std::stol(S3Utils::GetXattr(opt_path, "part_size"));
  } catch (std::exception &) {
    S3::S3Handler::Logger()->Log(
        S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
        "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
        "opt-path:%s final-path:%s part size not set on opt-path:%s!",
        bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
        upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
        final_path.c_str(), opt_path.c_str());
    opt_len = 0;
  }

  char *ptr;

  for (const auto &part : parts) {
    Object obj;

    if (obj.Init(upload_path / std::to_string(part.part_number), geteuid(),
                 getegid()) != S3Error::None) {
      S3::S3Handler::Logger()->Log(
          S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
          "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
          "opt-path:%s final-path:%s using optimized part %s ...",
          bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
          upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
          final_path.c_str(),
          (upload_path / std::to_string(part.part_number)).c_str());

      // use the optimized part
      ssize_t start;

      try {
        start = std::stol(S3Utils::GetXattr(
            opt_path, "part" + std::to_string(part.part_number) + ".start"));
      } catch (std::exception &) {
        return S3Error::InternalError;
      }

      optimized_obj.Lseek(start, SEEK_SET);

      ssize_t i = 0;
      ssize_t len = opt_len;
      while ((i = optimized_obj.Read(len, &ptr)) > 0) {
        if (len < i) {
          ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
          XrdPosix_Close(fd);
          XrdPosix_Unlink(tmp_path.c_str());
          S3Utils::RmPath(final_path.parent_path(), bucket.path);
          return S3Error::InternalError;
        }
        len -= i;
        xs.Update(ptr, i);
        XrdPosix_Write(fd, ptr, i);
      }
    } else {
      S3::S3Handler::Logger()->Log(
          S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
          "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
          "opt-path:%s final-path:%s using temporary part ...",
          bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
          upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
          final_path.c_str());
      ssize_t len = obj.GetSize();
      ssize_t i = 0;

      while ((i = obj.Read(len, &ptr)) > 0) {
        if (len < i) {
          ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
          XrdPosix_Close(fd);
          XrdPosix_Unlink(tmp_path.c_str());
          S3Utils::RmPath(final_path.parent_path(), bucket.path);
          S3::S3Handler::Logger()->Log(
              S3::ERROR, "ObjectStore::CompleteMultipartUpload",
              "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s "
              "optimized:%d opt-path:%s final-path:%s read error on temporary "
              "part ...",
              bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
              upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
              final_path.c_str());
          return S3Error::InternalError;
        }
        len -= i;
        xs.Update(ptr, i);
        // TODO: error handling
        XrdPosix_Write(fd, ptr, i);
        S3::S3Handler::Logger()->Log(
            S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
            "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s "
            "optimized:%d opt-path:%s final-path:%s writing part ...",
            bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
            upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
            final_path.c_str());
      }
    }
  }

  XrdPosix_Close(fd);

  S3::S3Handler::Logger()->Log(
      S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
      "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
      "opt-path:%s final-path:%s finalizing checksum ...",
      bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
      upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
      final_path.c_str());
  char *fxs = xs.Final();
  std::vector<unsigned char> md5(fxs, fxs + 16);
  auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';
  std::map<std::string, std::string> metadata;

  metadata.insert({"etag", md5hex});

  {
    ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
    S3Error error = SetMetadata(tmp_path, metadata);
    if (error != S3Error::None) {
      S3::S3Handler::Logger()->Log(
          S3::ERROR, "ObjectStore::CompleteMultipartUpload",
          "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
          "opt-path:%s final-path:%s error setting metadata on tmp-path:%s",
          bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
          upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
          final_path.c_str(), tmp_path.c_str());
      // TODO: error handling
      XrdPosix_Unlink(tmp_path.c_str());
      S3Utils::RmPath(final_path.parent_path(), bucket.path);
      return error;
    }
  }

  {
    S3::S3Handler::Logger()->Log(
        S3::DEBUG, "ObjectStore::CompleteMultipartUpload",
        "bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
        "opt-path:%s final-path:%s renaming %s => %s",
        bucket.name.c_str(), key.c_str(), upload_id.c_str(), p.c_str(),
        upload_path.c_str(), optimized.length() ? 1 : 0, opt_path.c_str(),
        final_path.c_str(), tmp_path.c_str(), final_path.c_str());
    // Rename using the owner filesystem id
    ScopedFsId scope(bucket.owner.uid, bucket.owner.gid);
    // TODO: error handling
    XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());
    // TODO: error handling
    XrdPosix_Unlink(opt_path.c_str());
  }

  DeleteMultipartUpload(bucket, key, upload_id);
  S3::S3Handler::Logger()->Log(
      S3::INFO, "ObjectStore::CompleteMultipartUpload",
      "%s bucket:%s key:%s upload-id:%s parts;%s upload-path:%s optimized:%d "
      "opt-path:%s final-path:%s multipart upload complete!",
      req.trace.c_str(), bucket.name.c_str(), key.c_str(), upload_id.c_str(),
      p.c_str(), upload_path.c_str(), optimized.length() ? 1 : 0,
      opt_path.c_str(), final_path.c_str());
  return S3Error::None;
}

}  // namespace S3
