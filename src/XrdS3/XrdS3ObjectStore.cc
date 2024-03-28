//
// Created by segransm on 11/17/23.
//

#include "XrdS3ObjectStore.hh"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/xattr.h>

#include <XrdOuc/XrdOucTUtils.hh>
#include <algorithm>
#include <ctime>
#include <filesystem>
#include <stack>
#include <utility>

#include "XrdCks/XrdCksCalcmd5.hh"
#include "XrdPosix/XrdPosixExtern.hh"
#include "XrdS3Auth.hh"
#include "XrdS3Req.hh"

namespace S3 {

S3ObjectStore::S3ObjectStore(const std::string &config, const std::string &mtpu)
    : config_path(config), mtpu_path(mtpu) {
  user_map = config_path / "users";

  XrdPosix_Mkdir(user_map.c_str(), S_IRWXU | S_IRWXG);
  XrdPosix_Mkdir(mtpu_path.c_str(), S_IRWXU | S_IRWXG);
}

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

std::string S3ObjectStore::GetUserDefaultBucketPath(
    const std::string &user_id) const {
  return S3Utils::GetXattr(user_map / user_id, "new_bucket_path");
}

S3Error S3ObjectStore::SetMetadata(
    const std::string &object,
    const std::map<std::string, std::string> &metadata) {
  for (const auto &meta : metadata) {
    if (S3Utils::SetXattr(object, meta.first, meta.second, 0)) {
      return S3Error::InternalError;
    }
  }
  return S3Error::None;
}

std::vector<std::string> S3ObjectStore::GetPartsNumber(
    const std::string &path) {
  auto p = S3Utils::GetXattr(path, "parts");

  if (p.empty()) {
    return {};
  }

  std::vector<std::string> res;
  XrdOucTUtils::splitString(res, p, ",");

  return res;
}

// todo:
S3Error S3ObjectStore::SetPartsNumbers(const std::string &path,
                                       std::vector<std::string> &parts) {
  auto p = S3Utils::stringJoin(',', parts);
  if (setxattr(path.c_str(), "user.parts", p.c_str(), p.size(), 0)) {
    return S3Error::InternalError;
  }
  return S3Error::None;
}

// TODO: We need a mutex here as multiple threads can write to the same file
S3Error S3ObjectStore::AddPartAttr(const std::string &object,
                                   size_t part_number) {
  auto parts = GetPartsNumber(object);

  auto n = std::to_string(part_number);
  if (std::find(parts.begin(), parts.end(), n) == parts.end()) {
    parts.push_back(n);
    return SetPartsNumbers(object, parts);
  }
  return S3Error::None;
}

S3Error S3ObjectStore::CreateBucket(S3Auth &auth, S3Auth::Bucket bucket,
                                    const std::string &_location) {
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
  auto fd = XrdPosix_Open(userInfoBucket.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          S_IRWXU | S_IRWXG);
  if (fd <= 0) {
    auth.DeleteBucketInfo(bucket);
    return S3Error::InternalError;
  }
  XrdPosix_Close(fd);

  if (S3Utils::SetXattr(userInfoBucket, "createdAt",
                        std::to_string(std::time(nullptr)), XATTR_CREATE)) {
    auth.DeleteBucketInfo(bucket);
    XrdPosix_Unlink(userInfoBucket.c_str());
    return S3Error::InternalError;
  }

  if (XrdPosix_Mkdir((mtpu_path / bucket.name).c_str(), S_IRWXU | S_IRWXG)) {
    auth.DeleteBucketInfo(bucket);
    XrdPosix_Unlink(userInfoBucket.c_str());
    return S3Error::InternalError;
  }

  if (XrdPosix_Mkdir(bucket.path.c_str(), S_IRWXU | S_IRWXG)) {
    auth.DeleteBucketInfo(bucket);
    XrdPosix_Unlink(userInfoBucket.c_str());
    XrdPosix_Rmdir((mtpu_path / bucket.name).c_str());
    return S3Error::InternalError;
  }

  return S3Error::None;
}

std::pair<std::string, std::string> BaseDir(std::string p) {
  std::string basedir;
  auto pos = p.rfind('/');
  if (pos != std::string::npos) {
    basedir = p.substr(0, pos);
    p.erase(0, pos + 1);
  }

  return {basedir, p};
}

S3Error S3ObjectStore::DeleteBucket(S3Auth &auth,
                                    const S3Auth::Bucket &bucket) {
  if (!S3Utils::IsDirEmpty(bucket.path)) {
    return S3Error::BucketNotEmpty;
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

      XrdPosix_Unlink((dir_name / entry->d_name).c_str());
    };

    S3Utils::DirIterator(dir_name, rm_files);

    XrdPosix_Rmdir((upload_path / entry->d_name).c_str());
  };

  S3Utils::DirIterator(upload_path, rm_mtpu_uploads);

  XrdPosix_Rmdir(bucket.path.c_str());
  XrdPosix_Rmdir(upload_path.c_str());
  auth.DeleteBucketInfo(bucket);
  XrdPosix_Unlink((user_map / bucket.owner.id / bucket.name).c_str());

  return S3Error::None;
}

S3ObjectStore::Object::~Object() {
  if (fd != 0) {
    XrdPosix_Close(fd);
  }
}

// TODO: Replace with the real XrdPosix_Listxattr once implemented.
#define XrdPosix_Listxattr listxattr

S3Error S3ObjectStore::Object::Init(const std::filesystem::path &p) {
  struct stat buf;

  if (XrdPosix_Stat(p.c_str(), &buf) || S_ISDIR(buf.st_mode)) {
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

  return S3Error::None;
}

ssize_t S3ObjectStore::Object::Read(size_t length, char **ptr) {
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

off_t S3ObjectStore::Object::Lseek(off_t offset, int whence) {
  if (!init) {
    return -1;
  }

  if (fd == 0) {
    this->buffer.resize(this->buffer_size);
    this->fd = XrdPosix_Open(name.c_str(), O_RDONLY);
  }

  return XrdPosix_Lseek(fd, offset, whence);
}

S3Error S3ObjectStore::GetObject(const S3Auth::Bucket &bucket,
                                 const std::string &object, Object &obj) {
  return obj.Init(bucket.path / object);
}

S3Error S3ObjectStore::DeleteObject(const S3Auth::Bucket &bucket,
                                    const std::string &key) {
  std::string base, obj;

  auto full_path = bucket.path / key;

  if (XrdPosix_Unlink(full_path.c_str())) {
    return S3Error::NoSuchKey;
  }

  do {
    full_path = full_path.parent_path();
  } while (full_path != bucket.path && !XrdPosix_Rmdir(full_path.c_str()));

  return S3Error::None;
}

std::vector<S3ObjectStore::BucketInfo> S3ObjectStore::ListBuckets(
    const std::string &id) const {
  std::vector<BucketInfo> buckets;
  auto get_entry = [this, &buckets, &id](dirent *entry) {
    if (entry->d_name[0] == '.') {
      return;
    }

    auto created_at =
        S3Utils::GetXattr(user_map / id / entry->d_name, "createdAt");

    buckets.push_back(BucketInfo{entry->d_name, created_at});
  };

  S3Utils::DirIterator(user_map / id, get_entry);

  return buckets;
}

// TODO: At the moment object versioning is not supported, this returns a
//  hardcoded object version.
ListObjectsInfo S3ObjectStore::ListObjectVersions(
    const S3Auth::Bucket &bucket, const std::string &prefix,
    const std::string &key_marker, const std::string &version_id_marker,
    const char delimiter, int max_keys) {
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

S3Error S3ObjectStore::CopyObject(const S3Auth::Bucket &bucket,
                                  const std::string &key, Object &source_obj,
                                  const Headers &reqheaders, Headers &headers) {
  auto final_path = bucket.path / key;

  struct stat buf;
  if (!XrdPosix_Stat(final_path.c_str(), &buf) && S_ISDIR(buf.st_mode)) {
    return S3Error::ObjectExistAsDir;
  }

  auto tmp_path =
      bucket.path / ("." + key + "." + std::to_string(std::time(nullptr)) +
                     std::to_string(std::rand()));

  auto err = S3Utils::makePath((char *)final_path.parent_path().c_str(),
                               S_IRWXU | S_IRWXG);

  if (err == ENOTDIR) {
    return S3Error::ObjectExistInObjectPath;
  } else if (err != 0) {
    return S3Error::InternalError;
  }

  auto fd = XrdPosix_Open(tmp_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          S_IRWXU | S_IRWXG);

  if (fd < 0) {
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
    XrdPosix_Write(fd, ptr, i);
  }

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
bool S3ObjectStore::KeepOptimize(const std::filesystem::path &upload_path,
                                 size_t part_number, unsigned long size,
                                 const std::string &tmp_path,
                                 size_t part_size) {
  size_t last_part_size;
  try {
    last_part_size =
        std::stoul(S3Utils::GetXattr(upload_path, "last_part_size"));
  } catch (std::exception &) {
    return false;
  }

  if (part_size == 0) {
    S3Utils::SetXattr(upload_path, "part_size", std::to_string(size),
                      XATTR_REPLACE);
    S3Utils::SetXattr(upload_path, "last_part_size", std::to_string(size),
                      XATTR_REPLACE);
    return true;
  }

  auto parts = GetPartsNumber(tmp_path);

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

S3Error ReadBufferAt(XrdS3Req &req, XrdCksCalcmd5 &md5XS,
                     S3Crypt::S3SHA256 &sha256XS, int fd,
                     unsigned long length) {
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

std::pair<S3Error, size_t> ReadBufferIntoFile(XrdS3Req &req,
                                              XrdCksCalcmd5 &md5XS,
                                              S3Crypt::S3SHA256 &sha256XS,
                                              int fd, bool chunked,
                                              unsigned long size) {
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

struct FileUploadResult {
  S3Error result;
  sha256_digest sha256;
  std::string md5hex;
  size_t size;
};

FileUploadResult FileUploader(XrdS3Req &req, bool chunked, size_t size,
                              std::filesystem::path &path) {
  auto fd = XrdPosix_Open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          S_IRWXU | S_IRWXG);

  if (fd < 0) {
    return FileUploadResult{S3Error::InternalError, {}, {}, {}};
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

S3Error S3ObjectStore::UploadPartOptimized(XrdS3Req &req,
                                           const std::string &tmp_path,
                                           size_t part_size, size_t part_number,
                                           size_t size, Headers &headers) {
  auto fd = XrdPosix_Open(tmp_path.c_str(), O_WRONLY);

  if (fd < 0) {
    return S3Error::InternalError;
  }

  XrdCksCalcmd5 md5XS;
  md5XS.Init();
  S3Crypt::S3SHA256 sha256XS;
  sha256XS.Init();

  long long offset = (long long)part_size * (part_number - 1);

  XrdPosix_Lseek(fd, offset, SEEK_SET);

  auto [error, final_size] =
      ReadBufferIntoFile(req, md5XS, sha256XS, fd, false, size);

  XrdPosix_Close(fd);

  if (error != S3Error::None) {
    return error;
  }
  char *md5f = md5XS.Final();
  sha256XS.Finish();
  std::vector<unsigned char> md5(md5f, md5f + 16);

  if (!req.md5.empty() && req.md5 != md5) {
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
S3Error S3ObjectStore::UploadPart(XrdS3Req &req, const std::string &upload_id,
                                  size_t part_number, unsigned long size,
                                  bool chunked, Headers &headers) {
  auto upload_path = mtpu_path / req.bucket / upload_id;

  auto err = ValidateMultipartUpload(upload_path, req.object);
  if (err != S3Error::None) {
    return err;
  }

  auto optimized = S3Utils::GetXattr(upload_path, "optimized");

  // Chunked uploads disables optimizations as we do not know the part size.
  if (chunked && !optimized.empty()) {
    S3Utils::SetXattr(upload_path, "optimized", "", XATTR_REPLACE);
  }

  if (!optimized.empty()) {
    auto tmp_path = S3Utils::GetXattr(upload_path, "tmp");

    size_t part_size;
    try {
      part_size = std::stoul(S3Utils::GetXattr(upload_path, "part_size"));
    } catch (std::exception &) {
      return S3Error::InternalError;
    }
    if (KeepOptimize(upload_path, part_number, size, tmp_path, part_size)) {
      return UploadPartOptimized(req, tmp_path, part_size, part_number, size,
                                 headers);
    }
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
    XrdPosix_Unlink(tmp_path.c_str());
  }

  XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());
  return error;
}

S3Error S3ObjectStore::PutObject(XrdS3Req &req, const S3Auth::Bucket &bucket,
                                 unsigned long size, bool chunked,
                                 Headers &headers) {
  auto final_path = bucket.path / req.object;

  struct stat buf;
  if (!XrdPosix_Stat(final_path.c_str(), &buf) && S_ISDIR(buf.st_mode)) {
    return S3Error::ObjectExistAsDir;
  }

  auto tmp_path =
      final_path.parent_path() /
      ("." + final_path.filename().string() + "." +
       std::to_string(std::time(nullptr)) + std::to_string(std::rand()));

  auto err = S3Utils::makePath((char *)final_path.parent_path().c_str(),
                               S_IRWXU | S_IRWXG);

  if (err == ENOTDIR) {
    return S3Error::ObjectExistInObjectPath;
  } else if (err != 0) {
    return S3Error::InternalError;
  }

  auto [error, _, md5hex, final_size] =
      FileUploader(req, chunked, size, tmp_path);

  if (error != S3Error::None) {
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
    XrdPosix_Unlink(tmp_path.c_str());
    S3Utils::RmPath(final_path.parent_path(), bucket.path);
    return error;
  }

  XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());

  return error;
}

std::tuple<std::vector<DeletedObject>, std::vector<ErrorObject>>
S3ObjectStore::DeleteObjects(const S3Auth::Bucket &bucket,
                             const std::vector<SimpleObject> &objects) {
  std::vector<DeletedObject> deleted;
  std::vector<ErrorObject> error;

  for (const auto &o : objects) {
    auto err = DeleteObject(bucket, o.key);
    if (err == S3Error::None || err == S3Error::NoSuchKey) {
      deleted.push_back({o.key, o.version_id, false, ""});
    } else {
      error.push_back({S3Error::InternalError, o.key, "", o.version_id});
    }
  }
  return {deleted, error};
}

ListObjectsInfo S3ObjectStore::ListObjectsV2(
    const S3Auth::Bucket &bucket, const std::string &prefix,
    const std::string &continuation_token, const char delimiter, int max_keys,
    bool fetch_owner, const std::string &start_after) {
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

ListObjectsInfo S3ObjectStore::ListObjects(const S3Auth::Bucket &bucket,
                                           const std::string &prefix,
                                           const std::string &marker,
                                           const char delimiter, int max_keys) {
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

ListObjectsInfo S3ObjectStore::ListObjectsCommon(
    const S3Auth::Bucket &bucket, std::string prefix, const std::string &marker,
    char delimiter, int max_keys, bool get_versions,
    const std::function<ObjectInfo(const std::filesystem::path &,
                                   const std::string &)> &f) {
  std::string basedir;

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

std::pair<std::string, S3Error> S3ObjectStore::CreateMultipartUpload(
    const S3Auth::Bucket &bucket, const std::string &key) {
  // TODO: Metadata uploaded with the create multipart upload operation is not
  //  saved to the final file.

  auto upload_id = S3Utils::HexEncode(
      S3Crypt::SHA256_OS(bucket.name + key + std::to_string(std::rand())));

  auto final_path = bucket.path / key;
  auto tmp_path =
      final_path.parent_path() /
      ("." + final_path.filename().string() + "." +
       std::to_string(std::time(nullptr)) + std::to_string(std::rand()));

  auto p = mtpu_path / bucket.name / upload_id;
  XrdPosix_Mkdir(p.c_str(), S_IRWXU | S_IRWXG);

  auto err = S3Utils::makePath((char *)final_path.parent_path().c_str(),
                               S_IRWXU | S_IRWXG);

  if (err == ENOTDIR) {
    return {{}, S3Error::ObjectExistInObjectPath};
  } else if (err != 0) {
    return {{}, S3Error::InternalError};
  }

  auto fd = XrdPosix_Open(tmp_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                          S_IRWXU | S_IRWXG);

  if (fd < 0) {
    S3Utils::RmPath(final_path.parent_path(), bucket.path);
    return {{}, S3Error::InternalError};
  }

  S3Utils::SetXattr(p, "key", key, XATTR_CREATE);
  S3Utils::SetXattr(p, "optimized", "1", XATTR_CREATE);
  S3Utils::SetXattr(p, "tmp", tmp_path, XATTR_CREATE);
  S3Utils::SetXattr(p, "part_size", "0", XATTR_CREATE);
  S3Utils::SetXattr(p, "last_part_size", "0", XATTR_CREATE);

  return {upload_id, S3Error::None};
}

std::vector<S3ObjectStore::MultipartUploadInfo>
S3ObjectStore::ListMultipartUploads(const std::string &bucket) {
  auto upload_path = mtpu_path / bucket;

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

S3Error S3ObjectStore::AbortMultipartUpload(const S3Auth::Bucket &bucket,
                                            const std::string &key,
                                            const std::string &upload_id) {
  auto upload_path = mtpu_path / bucket.name / upload_id;
  auto tmp_path = S3Utils::GetXattr(upload_path, "tmp");
  auto err = DeleteMultipartUpload(bucket, key, upload_id);
  if (err != S3Error::None) {
    return err;
  }

  XrdPosix_Unlink(tmp_path.c_str());
  S3Utils::RmPath(std::filesystem::path(tmp_path), bucket.path);

  return err;
};

S3Error S3ObjectStore::DeleteMultipartUpload(const S3Auth::Bucket &bucket,
                                             const std::string &key,
                                             const std::string &upload_id) {
  auto upload_path = mtpu_path / bucket.name / upload_id;

  auto err = ValidateMultipartUpload(upload_path, key);
  if (err != S3Error::None) {
    return err;
  }

  auto rm_files = [&upload_path](dirent *entry) {
    if (entry->d_name[0] == '.') {
      return;
    }

    XrdPosix_Unlink((upload_path / entry->d_name).c_str());
  };

  S3Utils::DirIterator(upload_path, rm_files);

  XrdPosix_Rmdir(upload_path.c_str());

  return S3Error::None;
}

S3Error S3ObjectStore::ValidateMultipartUpload(const std::string &upload_path,
                                               const std::string &key) {
  struct stat buf;

  if (XrdPosix_Stat(upload_path.c_str(), &buf)) {
    return S3Error::NoSuchUpload;
  }

  auto upload_key = S3Utils::GetXattr(upload_path, "key");
  if (upload_key != key) {
    return S3Error::InvalidRequest;
  }

  return S3Error::None;
}

std::pair<S3Error, std::vector<S3ObjectStore::PartInfo>>
S3ObjectStore::ListParts(const std::string &bucket, const std::string &key,
                         const std::string &upload_id) {
  auto upload_path = mtpu_path / bucket / upload_id;

  auto err = ValidateMultipartUpload(upload_path, key);
  if (err != S3Error::None) {
    return {err, {}};
  }

  std::vector<PartInfo> parts;
  auto parse_part = [upload_path, &parts](dirent *entry) {
    if (entry->d_name[0] == '.') {
      return;
    }

    auto etag = S3Utils::GetXattr(upload_path / entry->d_name, "etag");

    struct stat buf;
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

bool S3ObjectStore::CompleteOptimizedMultipartUpload(
    const std::filesystem::path &final_path,
    const std::filesystem::path &tmp_path, const std::vector<PartInfo> &parts) {
  size_t e = 1;
  for (const auto &[etag, _, n, __] : parts) {
    if (e != n) {
      return false;
    }

    e++;
    auto id = "part" + std::to_string(n);
    if (S3Utils::GetXattr(tmp_path, id + ".start").empty()) {
      return false;
    }

    if (S3Utils::GetXattr(tmp_path, id + ".etag") != etag) {
      return false;
    }
  }

  XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());
  return true;
}

S3Error S3ObjectStore::CompleteMultipartUpload(
    XrdS3Req &req, const S3Auth::Bucket &bucket, const std::string &key,
    const std::string &upload_id, const std::vector<PartInfo> &parts) {
  auto upload_path = mtpu_path / req.bucket / upload_id;

  auto err = ValidateMultipartUpload(upload_path, req.object);
  if (err != S3Error::None) {
    return err;
  }

  auto final_path = bucket.path / req.object;
  auto opt_path = S3Utils::GetXattr(upload_path, "tmp");

  auto optimized = S3Utils::GetXattr(upload_path, "optimized");
  // Check if we are able to complete the multipart upload with only a mv
  // operation.
  if (!optimized.empty() &&
      CompleteOptimizedMultipartUpload(final_path, opt_path, parts)) {
    return DeleteMultipartUpload(bucket, key, upload_id);
  }
  // Otherwise we will need to concatenate parts

  // First check that all the parts are in order.
  // We first check if a file named partN exists in the multipart upload dir,
  // then check if it exists in the optimized upload tmp path.
  size_t max = 0;
  struct stat buf;
  for (const auto &[etag, _, n, __] : parts) {
    if (n <= max) {
      return S3Error::InvalidPartOrder;
    }
    max = n;

    auto path = upload_path / std::to_string(n);
    if (XrdPosix_Stat(path.c_str(), &buf)) {
      auto id = "part" + std::to_string(n);
      if (S3Utils::GetXattr(opt_path, id + ".start").empty()) {
        return S3Error::InvalidPart;
      }
      if (S3Utils::GetXattr(opt_path, id + ".etag") != etag) {
        return S3Error::InvalidPart;
      }
    } else {
      if (S3Utils::GetXattr(path, "etag") != etag) {
        return S3Error::InvalidPart;
      }
    }
  }

  if (!XrdPosix_Stat(final_path.c_str(), &buf) && S_ISDIR(buf.st_mode)) {
    return S3Error::ObjectExistAsDir;
  }

  // Then we copy all the parts into a tmp file, which will be renamed to the
  // final file.
  auto tmp_path = bucket.path /
                  ("." + req.object + "." + std::to_string(std::time(nullptr)) +
                   std::to_string(std::rand()));
  auto fd = XrdPosix_Open(tmp_path.c_str(), O_CREAT | O_EXCL | O_WRONLY);

  if (fd < 0) {
    return S3Error::InternalError;
  }

  XrdCksCalcmd5 xs;
  xs.Init();

  Object optimized_obj;
  optimized_obj.Init(opt_path);

  ssize_t opt_len;
  try {
    opt_len = std::stol(S3Utils::GetXattr(opt_path, "part_size"));
  } catch (std::exception &) {
    return S3Error::InternalError;
  }

  char *ptr;

  for (const auto &part : parts) {
    Object obj;

    if (obj.Init(upload_path / std::to_string(part.part_number)) !=
        S3Error::None) {
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
      ssize_t len = obj.GetSize();
      ssize_t i = 0;

      while ((i = obj.Read(len, &ptr)) > 0) {
        if (len < i) {
          XrdPosix_Close(fd);
          XrdPosix_Unlink(tmp_path.c_str());
          S3Utils::RmPath(final_path.parent_path(), bucket.path);
          return S3Error::InternalError;
        }
        len -= i;
        xs.Update(ptr, i);
        XrdPosix_Write(fd, ptr, i);
      }
    }
  }

  XrdPosix_Close(fd);

  char *fxs = xs.Final();
  std::vector<unsigned char> md5(fxs, fxs + 16);
  auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';
  std::map<std::string, std::string> metadata;

  metadata.insert({"etag", md5hex});

  S3Error error = SetMetadata(tmp_path, metadata);
  if (error != S3Error::None) {
    XrdPosix_Unlink(tmp_path.c_str());
    S3Utils::RmPath(final_path.parent_path(), bucket.path);
    return error;
  }

  XrdPosix_Rename(tmp_path.c_str(), final_path.c_str());

  XrdPosix_Unlink(opt_path.c_str());
  DeleteMultipartUpload(bucket, key, upload_id);

  return S3Error::None;
  //  if (upload->second.optimized) {
  //    auto it = upload->second.parts.end();
  //    size_t e = 1;
  //    for (const auto &[etag, _, n, __] : parts) {
  //      if (e != n) {
  //        throw std::runtime_error("Multipart is not continuous, todo!");
  //      }
  //      if ((it = upload->second.parts.find(n)) == upload->second.parts.end())
  //      {
  //        return S3Error::InvalidPart;
  //      }
  //      if (it->second.etag != etag) {
  //        return S3Error::InvalidPart;
  //      }
  //      e++;
  //    }
  //    auto p = std::filesystem::path(path) / bucket / key;
  //    create_directories(p.parent_path());
  //
  //    XrdCksCalcmd5 xs;
  //    xs.Init();
  //
  //    auto fname = path / XRD_MULTIPART_UPLOAD_DIR / bucket / upload_id /
  //    "file"; auto size = file_size(fname); auto fd = open(fname.c_str(),
  //    O_RDONLY);
  //
  //    auto file_buffer = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
  //
  //    xs.Update((const char *)file_buffer, size);
  //
  //    munmap(file_buffer, size);
  //
  //    char *fxs = xs.Final();
  //    std::vector<unsigned char> md5(fxs, fxs + 16);
  //    auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';
  //    std::map<std::string, std::string> metadata;
  //
  //    metadata.insert({"last-modified", std::to_string(std::time(nullptr))});
  //    metadata.insert({"etag", md5hex});
  //    // todo: add headers from create multipart upload
  //
  //    S3Error error = SetMetadata(fname, metadata);
  //    if (error != S3Error::None) {
  //      return error;
  //    }
  //
  //    std::filesystem::rename(fname, p);
  //    buploads->second.erase(upload);
  //
  //    return S3Error::None;
  //  } else {
  //    throw std::runtime_error("Not optimized mtpu not implemented");
  //  }

  //  auto it = parts.begin();
  //  auto ret = S3Error::None;
  //
  //
  //
  //  auto validate_part = [&it, &parts, &ret](dirent *entry) {
  //    if (ret != S3Error::None || entry->d_name[0] == '.') {
  //      return;
  //    }
  //
  //    if (it == parts.end()) {
  //      ret = S3Error::InvalidPart;
  //      return;
  //    }
  //
  //
  //
  //  };
}

}  // namespace S3
