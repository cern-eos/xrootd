//
// Created by segransm on 11/17/23.
//

#include "XrdS3ObjectStore.hh"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stack>
#include <stdexcept>
#include <utility>

#include "XrdCks/XrdCksCalcmd5.hh"
#include "XrdS3Req.hh"

#define XRD_MULTIPART_UPLOAD_DIR "__XRD__MULTIPART__"

namespace S3 {

std::string GetDirName(const std::filesystem::path &p) {
  std::string name;
  for (const auto &v : p) {
    if (!v.empty()) {
      name = v;
    }
  }
  return name;
}

std::string GetXattr(const std::filesystem::path &path,
                     const std::string &key) {
  std::string res;

  res.resize(getxattr(path.c_str(), key.c_str(), nullptr, 0));
  getxattr(path.c_str(), key.c_str(), res.data(), res.size());

  return res;
}

S3ObjectStore::S3ObjectStore(std::string p) : path(p) {
  std::filesystem::create_directory(std::filesystem::path(path) /
                                    XRD_MULTIPART_UPLOAD_DIR);

  for (const auto &bucket : std::filesystem::directory_iterator(path)) {
    ssize_t i;
    auto name = GetDirName(bucket.path());
    if (name == XRD_MULTIPART_UPLOAD_DIR) {
      continue;
    }
    if ((i = getxattr(bucket.path().c_str(), "user.owner", BUFFER, BUFFSIZE)) >=
        0) {
      BUFFER[i] = 0;
      bucketOwners.insert({name, BUFFER});
    }
    if ((i = getxattr(bucket.path().c_str(), "user.createdAt", BUFFER,
                      BUFFSIZE)) >= 0) {
      BUFFER[i] = 0;
      bucketInfo.insert({name, {name, BUFFER}});
    }

    MultipartUploads uploads;
    for (const auto &entry : std::filesystem::directory_iterator(
             std::filesystem::path(path) / XRD_MULTIPART_UPLOAD_DIR / name)) {
      if (entry.is_directory()) {
        MultipartUpload upload{GetXattr(entry.path(), "user.key"), {}};

        for (const auto &part : std::filesystem::directory_iterator(entry)) {
          auto etag = GetXattr(part.path(), "user.etag");
          auto last_modified = part.last_write_time();
          auto size = part.file_size();
          upload.parts.insert({std::stoul(part.path().filename()),
                               {etag, last_modified, size}});
        }
        uploads.insert({GetDirName(entry.path()), upload});
      }
    }
    multipartUploads.insert({name, uploads});
  }
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

std::string S3ObjectStore::GetBucketOwner(const std::string &bucket) const {
  auto it = bucketOwners.find(bucket);
  if (it != bucketOwners.end()) {
    return it->second;
  }
  return {};
}

S3Error S3ObjectStore::SetMetadata(
    const std::string &object,
    const std::map<std::string, std::string> &metadata) {
  for (const auto &meta : metadata) {
    if (setxattr(object.c_str(), ("user." + meta.first).c_str(),
                 meta.second.c_str(), meta.second.size(), 0)) {
      // fprintf(stderr, "UNABLE TO SET XATTR: %s", metadata)
      return S3Error::InternalError;
    }
  }
  return S3Error::None;
}

S3Error S3ObjectStore::CreateBucket(const std::string &id,
                                    const std::string &bucket,
                                    const std::string &_location) {
  if (!ValidateBucketName(bucket)) {
    return S3Error::InvalidBucketName;
  }

  auto owner = GetBucketOwner(bucket);
  if (!owner.empty()) {
    if (owner == id) {
      return S3Error::BucketAlreadyOwnedByYou;
    }
    return S3Error::BucketAlreadyExists;
  }

  auto bucketPath = path / bucket;
  if (!std::filesystem::create_directory(std::filesystem::path(path) /
                                         XRD_MULTIPART_UPLOAD_DIR / bucket) ||
      !std::filesystem::create_directory(std::filesystem::path(path) /
                                         bucket)) {
    return S3Error::InternalError;
  }

  if (setxattr(bucketPath.c_str(), "user.owner", id.c_str(), id.size(),
               XATTR_CREATE)) {
    fprintf(stderr, "Unable to set xattr...\n");
    std::filesystem::remove_all(bucketPath.c_str());
    return S3Error::InternalError;
  }

  auto now = std::to_string(std::time(nullptr));
  if (setxattr(bucketPath.c_str(), "user.createdAt", now.c_str(), now.size(),
               XATTR_CREATE)) {
    fprintf(stderr, "Unable to set xattr...\n");
    std::filesystem::remove_all(bucketPath.c_str());
    return S3Error::InternalError;
  }

  fprintf(stderr, "Created bucket %s with owner %s\n", bucketPath.c_str(),
          id.c_str());

  bucketOwners.insert({bucket, id});
  bucketInfo.insert({bucket, {bucket, now}});
  multipartUploads.insert({bucket, {}});

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

S3Error S3ObjectStore::DeleteBucket(const std::string &bucket) {
  if (!ValidateBucketName(bucket)) {
    return S3Error::InvalidBucketName;
  }

  if (GetBucketOwner(bucket).empty()) {
    return S3Error::NoSuchBucket;
  }

  auto bucketPath = path / bucket;

  if (!std::filesystem::is_empty(bucketPath) ||
      !std::filesystem::is_empty(path / XRD_MULTIPART_UPLOAD_DIR / bucket)) {
    return S3Error::BucketNotEmpty;
  }

  std::filesystem::remove(bucketPath);
  std::filesystem::remove(path / XRD_MULTIPART_UPLOAD_DIR / bucket);

  bucketOwners.erase(bucket);
  bucketInfo.erase(bucket);

  return S3Error::None;
}

S3Error S3ObjectStore::Object::Init(const std::filesystem::path &p) {
  if (!exists(p) || is_directory(p)) {
    return S3Error::NoSuchKey;
  }

  std::vector<std::string> attrnames;
  std::vector<char> attrlist;
  auto attrlen = listxattr(p.c_str(), nullptr, 0);
  attrlist.resize(attrlen);
  listxattr(p.c_str(), attrlist.data(), attrlen);
  auto i = attrlist.begin();
  while (i != attrlist.end()) {
    auto tmp = std::find(i, attrlist.end(), 0);
    attrnames.emplace_back(i, tmp);
    i = tmp + 1;
  }
  std::vector<char> value;
  for (const auto &attr : attrnames) {
    if (attr.substr(0, 5) != "user.") continue;
    attrlen = getxattr(p.c_str(), attr.c_str(), nullptr, 0);
    value.resize(attrlen);
    if (attrlen > 0) {
      value[attrlen - 1] = 0;
    }
    getxattr(p.c_str(), attr.c_str(), value.data(), attrlen);
    attributes.insert({attr.substr(5), {value.begin(), value.end()}});
  }

  name = p;
  this->size = file_size(p);
  this->ifs = std::ifstream(p, std::ios_base::binary);

  return S3Error::None;
}

S3Error S3ObjectStore::GetObject(const std::string &bucket,
                                 const std::string &object, Object &obj) {
  auto objectPath = path / bucket / object;

  return obj.Init(objectPath);
}

S3Error S3ObjectStore::DeleteObject(const std::string &bucket,
                                    const std::string &key) {
  std::string base, obj;

  auto full_path = path / bucket / key;

  if (!std::filesystem::remove(full_path)) {
    return S3Error::NoSuchKey;
  }

  std::error_code ec;
  do {
    full_path = full_path.parent_path();
  } while (full_path != path / bucket &&
           std::filesystem::remove(full_path, ec));

  return S3Error::None;
}

std::vector<S3ObjectStore::BucketInfo> S3ObjectStore::ListBuckets(
    const std::string &id) const {
  std::vector<BucketInfo> buckets;
  for (const auto &b : bucketOwners) {
    if (b.second == id) {
      // todo:
      buckets.push_back(bucketInfo.find(b.first)->second);
    }
  }
  return buckets;
}

// todo: this should only be a simple wrapper around a LostObjects that is used
//  with ListObjectsV2 ListObjects ListObjectVersion, etc.
ListObjectsInfo S3ObjectStore::ListObjectVersions(
    const std::string &bucket, const std::string &prefix,
    const std::string &key_marker, const std::string &version_id_marker,
    const char delimiter, int max_keys) {
  auto f = [](const std::filesystem::path &root, const std::string &object) {
    struct stat buf;

    if (!stat((root / object).c_str(), &buf)) {
      return ObjectInfo{object, buf.st_mtim.tv_sec, std::to_string(buf.st_size),
                        ""};
    }
    return ObjectInfo{};
  };

  // todo: vid_marker
  return ListObjectsCommon(bucket, prefix, key_marker, delimiter, max_keys,
                           true, f);
}

int mkpath(std::string s, mode_t mode, size_t pos = 0) {
  std::string dir;
  int mdret;

  while ((pos = s.find_first_of('/', pos)) != std::string::npos) {
    dir = s.substr(0, pos++);
    if (dir.size() == 0) continue;  // if leading / first time is 0 length
    fprintf(stderr, "MKPATH: MKDIR: %s\n", dir.c_str());
    if ((mdret = mkdir(dir.c_str(), mode)) && errno != EEXIST) {
      fprintf(stderr, "--> MKDIR: ERRNO: %s %s\n", dir.c_str(),
              strerror(errno));
      return mdret;
    }
    fprintf(stderr, "MKDIR: ERRNO: %s %s\n", dir.c_str(), strerror(errno));
  }
  return mdret;
}
// todo:
#define PUT_LIMIT 5000000000

S3Error S3ObjectStore::CopyObject(const std::string &bucket,
                                  const std::string &key, Object &source_obj,
                                  const Headers &reqheaders, Headers &headers) {
  auto final_path = path / bucket / key;
  auto tmp_path = path / XRD_MULTIPART_UPLOAD_DIR / bucket /
                  S3Utils::HexEncode(key + std::to_string(std::rand()));
  std::error_code ec;

  if (is_directory(final_path)) {
    return S3Error::ObjectExistAsDir;
  }

  if (!std::filesystem::create_directories(final_path.parent_path(), ec)) {
    if (ec.value() == ENOTDIR) {
      return S3Error::ObjectExistInObjectPath;
    } else if (ec.value() != 0) {
      throw std::runtime_error("INTERNAL ERROR");
      return S3Error::InternalError;
    }
  }

  std::ofstream ofs(tmp_path, std::ios_base::binary | std::ios_base::trunc);

  if (!ofs.is_open()) {
    throw std::runtime_error("INTERNAL ERROR");
    return S3Error::InternalError;
  }

  XrdCksCalcmd5 xs;
  xs.Init();
  size_t final_size = 0;

  auto stream = &source_obj.GetStream();
  streamsize i = 0;
  while ((i = stream->readsome(BUFFER, BUFFSIZE)) > 0) {
    xs.Update(BUFFER, i);
    final_size += i;
    ofs.write(BUFFER, i);
  }
  ofs.close();

  char *fxs = xs.Final();
  std::vector<unsigned char> md5(fxs, fxs + 16);
  auto error = S3Error::None;

  std::map<std::string, std::string> metadata;
  if (S3Utils::HeaderEq(headers, "x-amz-metadata-directive", "REPLACE")) {
    auto add_header = [&metadata, &headers](const std::string &name) {
      if (S3Utils::HasHeader(headers, name)) {
        metadata.insert({name, headers.find(name)->second});
      }
    };

    add_header("cache-control");
    add_header("content-disposition");
    add_header("content-type");

    metadata.insert({"last-modified", std::to_string(std::time(nullptr))});
  } else {
    metadata = source_obj.GetAttributes();
    // Metadata
    // todo: validate headers

    // todo: validate calculated md5
    // todo: etag not always md5
    // (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)
    auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';
    metadata.insert({"etag", md5hex});
    headers.clear();
    headers.insert({"ETag", md5hex});
  }
  error = SetMetadata(tmp_path, metadata);
  if (error != S3Error::None) {
    // todo: remove path too
    std::filesystem::remove(tmp_path);
  }

  std::filesystem::rename(tmp_path, final_path);

  return error;
}

// todo: check path of multipart upload on creation
//  todo: deny uploads with path of multipart upload if in progress, maybe
//  create a temp object

S3Error S3ObjectStore::UploadPart(XrdS3Req &req, const std::string &upload_id,
                                  size_t part_number, unsigned long size,
                                  bool chunked, Headers &headers) {
  auto buploads = multipartUploads.find(req.bucket);
  if (buploads == multipartUploads.end()) {
    return S3Error::InternalError;
  }

  auto upload = buploads->second.find(upload_id);
  if (upload == buploads->second.end()) {
    return S3Error::NoSuchUpload;
  }
  if (upload->second.key != req.object) {
    return S3Error::InvalidRequest;
  }

  auto part_path = std::filesystem::path(path) / XRD_MULTIPART_UPLOAD_DIR /
                   req.bucket / upload_id / std::to_string(part_number);

  fprintf(stderr, "Opening part %s\n", part_path.c_str());
  auto *f = fopen(part_path.c_str(), "w");
  if (f == nullptr) {
    fprintf(stderr, "ERRNO: %s\n", strerror(errno));
    return S3Error::InternalError;
  }

  auto error = S3Error::None;
  // todo: dont check if not neededd, handle different cheksum types
  XrdCksCalcmd5 xs;
  xs.Init();
  S3Crypt::S3SHA256 sha;
  sha.Init();

  auto readBuffer = [&req, &xs, &sha, f](unsigned long length) {
    int buflen = 0;
    unsigned long readlen = 0;
    char *ptr;
    while (length > 0 &&
           (buflen = req.ReadBody(
                length > INT_MAX ? INT_MAX : static_cast<int>(length), &ptr,
                true)) > 0) {
      readlen = buflen;
      if (length < readlen) {
        fprintf(stderr, "Read too many bytes: %zu < %zu\n", length, readlen);
        return S3Error::IncompleteBody;
      }
      length -= readlen;
      xs.Update(ptr, buflen);
      sha.Update(ptr, buflen);
      if (fwrite(ptr, 1, readlen, f) != readlen) {
        return S3Error::InternalError;
      }
    }
    if (buflen < 0 || length != 0) {
      fprintf(stderr, "Length mismatch: %d %zu\n", buflen, length);
      return S3Error::IncompleteBody;
    }
    return S3Error::None;
  };

  unsigned long final_size = size;
  if (chunked) {
    int length;
    final_size = 0;
    XrdOucString chunk_size;

    do {
      req.BuffgetLine(chunk_size);
      chunk_size.erasefromend(2);
      try {
        length = std::stoi(chunk_size.c_str(), nullptr, 16);
      } catch (std::exception &) {
        error = S3Error::InvalidRequest;
        break;
      }
      final_size += length;
      if (final_size > PUT_LIMIT) {
        error = S3Error::EntityTooLarge;
        break;
      }
      error = readBuffer(length);
      req.BuffgetLine(chunk_size);
    } while (error == S3Error::None && length != 0);
  } else {
    error = readBuffer(size);
  }

  fclose(f);

  char *fxs = xs.Final();
  sha256_digest sha256 = sha.Finish();
  std::vector<unsigned char> md5(fxs, fxs + 16);
  if (error == S3Error::None) {
    if (!req.md5.empty() && req.md5 != md5) {
      error = S3Error::BadDigest;
    } else if (!S3Utils::HeaderEq(req.lowercase_headers, "x-amz-content-sha256",
                                  S3Utils::HexEncode(sha256))) {
      error = S3Error::XAmzContentSHA256Mismatch;
    }
  }

  if (error != S3Error::None) {
    // todo: remove path too
    std::remove(part_path.c_str());
    return error;
  }

  std::map<std::string, std::string> metadata;
  // Metadata
  // todo: validate headers
  // todo: etag not always md5
  // (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)
  auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';
  metadata.insert({"etag", md5hex});
  headers.insert({"ETag", md5hex});

  error = SetMetadata(part_path, metadata);
  if (error != S3Error::None) {
    // todo: remove path too
    fprintf(stderr, "meta error %d\n", (int)error);
    std::remove(part_path.c_str());
  } else {
    upload->second.parts[part_number] = {md5hex, last_write_time(part_path),
                                         final_size};
  }

  return error;
}

S3Error S3ObjectStore::PutObject(XrdS3Req &req, unsigned long size,
                                 bool chunked, Headers &headers) {
  auto final_path = path / req.bucket / req.object;
  auto tmp_path = path / XRD_MULTIPART_UPLOAD_DIR / req.bucket /
                  S3Utils::HexEncode(req.object + std::to_string(std::rand()));

  std::error_code ec;

  if (is_directory(final_path)) {
    return S3Error::ObjectExistAsDir;
  }

  if (!std::filesystem::create_directories(final_path.parent_path(), ec)) {
    if (ec.value() == ENOTDIR) {
      return S3Error::ObjectExistInObjectPath;
    } else if (ec.value() != 0) {
      fprintf(stderr, "Unable to create directories... %d\n", ec.value());
      return S3Error::InternalError;
    }
  }

  std::ofstream ofs(tmp_path, std::ios_base::binary | std::ios_base::trunc);

  if (!ofs.is_open()) {
    return S3Error::InternalError;
  }

  auto error = S3Error::None;
  // todo: dont check if not needed, handle different cheksum types
  XrdCksCalcmd5 xs;
  xs.Init();
  S3Crypt::S3SHA256 sha;
  sha.Init();

  auto readBuffer = [&req, &xs, &sha, &ofs](unsigned long length) {
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
      xs.Update(ptr, buflen);
      sha.Update(ptr, buflen);
      ofs.write(ptr, readlen);
    }
    if (buflen < 0 || length != 0) {
      return S3Error::IncompleteBody;
    }
    return S3Error::None;
  };

  unsigned long final_size = size;
  if (chunked) {
    int length;
    final_size = 0;
    XrdOucString chunk_size;

    do {
      req.BuffgetLine(chunk_size);
      chunk_size.erasefromend(2);
      try {
        length = std::stoi(chunk_size.c_str(), nullptr, 16);
      } catch (std::exception &) {
        error = S3Error::InvalidRequest;
        break;
      }
      final_size += length;
      if (final_size > PUT_LIMIT) {
        error = S3Error::EntityTooLarge;
        break;
      }
      error = readBuffer(length);
      req.BuffgetLine(chunk_size);
    } while (error == S3Error::None && length != 0);
  } else {
    error = readBuffer(size);
  }

  ofs.close();

  char *fxs = xs.Final();
  sha256_digest sha256 = sha.Finish();
  std::vector<unsigned char> md5(fxs, fxs + 16);
  if (error == S3Error::None) {
    if (!req.md5.empty() && req.md5 != md5) {
      error = S3Error::BadDigest;
    } else if (!S3Utils::HeaderEq(req.lowercase_headers, "x-amz-content-sha256",
                                  S3Utils::HexEncode(sha256))) {
      error = S3Error::XAmzContentSHA256Mismatch;
    }
  }

  if (error != S3Error::None) {
    // todo: remove path too
    std::filesystem::remove(tmp_path);
    return error;
  }

  std::map<std::string, std::string> metadata;
  // Metadata
  // todo: validate headers
  auto add_header = [&metadata, &req](const std::string &name) {
    if (S3Utils::HasHeader(req.lowercase_headers, name)) {
      metadata.insert({name, req.lowercase_headers.find(name)->second});
    }
  };

  add_header("cache-control");
  add_header("content-disposition");
  add_header("content-type");

  metadata.insert({"last-modified", std::to_string(std::time(nullptr))});
  // todo: etag not always md5
  // (https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html)
  auto md5hex = '"' + S3Utils::HexEncode(md5) + '"';
  metadata.insert({"etag", md5hex});
  headers.insert({"ETag", md5hex});
  // todo: all other system-headers

  // todo: handle non asccii chars:
  // (https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html)
  for (const auto &hd : req.lowercase_headers) {
    if (hd.first.substr(0, 11) == "x-amz-meta-") {
      metadata.insert({hd.first, hd.second});
    }
  }
  error = SetMetadata(tmp_path, metadata);
  if (error != S3Error::None) {
    // todo: remove path too
    fprintf(stderr, "meta error %d\n", (int)error);
    std::filesystem::remove(tmp_path);
  }

  std::filesystem::rename(tmp_path, final_path);

  return error;
}

std::tuple<std::vector<DeletedObject>, std::vector<ErrorObject>>
S3ObjectStore::DeleteObjects(const std::string &bucket,
                             const std::vector<SimpleObject> &objects) {
  auto bucketPath = path / bucket;

  std::vector<DeletedObject> deleted;
  std::vector<ErrorObject> error;

  for (const auto &o : objects) {
    fprintf(stderr, "Deleting object %s -> %s\n", bucket.c_str(),
            o.key.c_str());
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
    const std::string &bucket, const std::string &prefix,
    const std::string &continuation_token, const char delimiter, int max_keys,
    bool fetch_owner, const std::string &start_after) {
  auto f = [fetch_owner](const std::filesystem::path &root,
                         const std::string &object) {
    struct stat buf;

    auto owner = "";
    if (fetch_owner) {
      // todo
      owner = "abc";
    }

    if (!stat((root / object).c_str(), &buf)) {
      return ObjectInfo{object, buf.st_mtim.tv_sec, std::to_string(buf.st_size),
                        owner};
    }
    return ObjectInfo{};
  };

  return ListObjectsCommon(
      bucket, prefix,
      continuation_token.empty() ? start_after : continuation_token, delimiter,
      max_keys, false, f);
}

ListObjectsInfo S3ObjectStore::ListObjects(const std::string &bucket,
                                           const std::string &prefix,
                                           const std::string &marker,
                                           const char delimiter, int max_keys) {
  auto f = [](const std::filesystem::path &root, const std::string &object) {
    struct stat buf;

    if (!stat((root / object).c_str(), &buf)) {
      return ObjectInfo{object, buf.st_mtim.tv_sec, std::to_string(buf.st_size),
                        ""};
    }
    return ObjectInfo{};
  };

  return ListObjectsCommon(bucket, prefix, marker, delimiter, max_keys, false,
                           f);
}

ListObjectsInfo S3ObjectStore::ListObjectsCommon(
    const std::string &bucket, std::string prefix, const std::string &marker,
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
  auto fullpath = path / bucket;

  struct BasicPath {
    std::string base;
    std::string name;
    unsigned char d_type;
  };

  fprintf(stderr, "fp: %s bd: %s, pf: %s\n", fullpath.c_str(), basedir.c_str(),
          prefix.c_str());
  std::deque<BasicPath> entries;

  struct dirent **ent = nullptr;
  int n;
  if ((n = scandir((fullpath / basedir).c_str(), &ent, nullptr, alphasort)) <
      0) {
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

    fprintf(stderr, "Checking entry: %s\n", entry_path.c_str());
    size_t m;
    if ((m = entry_path.find(delimiter, prefix.length() + basedir.length() +
                                            1)) != std::string::npos) {
      fprintf(stderr, "common prefix: %s %zu\n", entry_path.c_str(), m);
      list.common_prefixes.insert(entry_path.substr(0, m + 1));
      list.key_marker = entry_path.substr(0, m + 1);
      list.vid_marker = "1";
      continue;
    }

    if (entry.d_type == DT_UNKNOWN) {
      // todo
      throw std::runtime_error("Unknown entry type");
    }

    if (entry.d_type == DT_DIR) {
      if (delimiter == '/') {
        fprintf(stderr, "common prefix dir: %s\n", entry_path.c_str());
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

std::string S3ObjectStore::CreateMultipartUpload(XrdS3Req &req,
                                                 const std::string &bucket,
                                                 const std::string &key) {
  // todo: handle metadata
  auto upload_id = S3Utils::HexEncode(
      S3Crypt::SHA256_OS(bucket + key + std::to_string(std::rand())));

  auto p = std::filesystem::path(path) / XRD_MULTIPART_UPLOAD_DIR / bucket /
           upload_id;
  if (!std::filesystem::create_directory(p)) {
    return "";
  }

  if (setxattr(p.c_str(), "user.key", key.c_str(), key.size(), XATTR_CREATE)) {
    std::filesystem::remove(p);
    return "";
  }

  auto it = multipartUploads.find(bucket);
  if (it != multipartUploads.end()) {
    it->second.insert({upload_id, {key, {}}});
  } else {
    multipartUploads.insert({bucket, {{upload_id, {key, {}}}}});
  }

  return upload_id;
}

std::vector<S3ObjectStore::MultipartUploadInfo>
S3ObjectStore::ListMultipartUploads(const std::string &bucket) {
  auto it = multipartUploads.find(bucket);
  if (it == multipartUploads.end()) {
    return {};
  }

  std::vector<MultipartUploadInfo> res;
  res.reserve(it->second.size());
  for (const auto &[id, info] : it->second) {
    res.push_back({info.key, id});
  }

  return res;
}

S3Error S3ObjectStore::AbortMultipartUpload(const std::string &bucket,
                                            const std::string &key,
                                            const std::string &upload_id) {
  auto buploads = multipartUploads.find(bucket);
  if (buploads == multipartUploads.end()) {
    return S3Error::InternalError;
  }

  auto upload = buploads->second.find(upload_id);
  if (upload == buploads->second.end()) {
    return S3Error::NoSuchUpload;
  }
  if (upload->second.key != key) {
    return S3Error::InvalidRequest;
  }

  buploads->second.erase(upload_id);
  std::filesystem::remove_all(std::filesystem::path(path) /
                              XRD_MULTIPART_UPLOAD_DIR / bucket / upload_id);

  return S3Error::None;
}

std::pair<S3Error, std::vector<S3ObjectStore::PartInfo>>
S3ObjectStore::ListParts(const std::string &bucket, const std::string &key,
                         const std::string &upload_id) {
  auto buploads = multipartUploads.find(bucket);
  if (buploads == multipartUploads.end()) {
    return {S3Error::InternalError, {}};
  }

  auto upload = buploads->second.find(upload_id);
  if (upload == buploads->second.end()) {
    return {S3Error::NoSuchUpload, {}};
  }
  if (upload->second.key != key) {
    return {S3Error::InvalidRequest, {}};
  }

  std::vector<PartInfo> parts;

  for (const auto &[n, info] : upload->second.parts) {
    parts.push_back({info.etag, info.last_modified, n, info.size});
  }

  return {S3Error::None, parts};
}

S3Error S3ObjectStore::CompleteMultipartUpload(
    XrdS3Req &req, const std::string &bucket, const std::string &key,
    const std::string &upload_id, const std::vector<PartInfo> &parts) {
  auto buploads = multipartUploads.find(bucket);
  if (buploads == multipartUploads.end()) {
    return S3Error::InternalError;
  }

  auto upload = buploads->second.find(upload_id);
  if (upload == buploads->second.end()) {
    return S3Error::NoSuchUpload;
  }
  if (upload->second.key != key) {
    return S3Error::InvalidRequest;
  }

  size_t max = 0;
  auto it = upload->second.parts.end();
  for (const auto &[etag, _, n, __] : parts) {
    if ((it = upload->second.parts.find(n)) == upload->second.parts.end()) {
      return S3Error::InvalidPart;
    }
    if (it->second.etag != etag) {
      return S3Error::InvalidPart;
    }
    if (n <= max) {
      return S3Error::InvalidPartOrder;
    }
    max = n;
  }

  auto p = std::filesystem::path(path) / bucket / key;
  create_directories(p.parent_path());

  std::ofstream ofs(p, std::ios_base::binary | std::ios_base::trunc);

  // todo: send spaces to avoid timing out connection
  for (const auto &part : parts) {
    std::ifstream ifs(std::filesystem::path(path) / XRD_MULTIPART_UPLOAD_DIR /
                          bucket / upload_id / std::to_string(part.part_number),
                      std::ios_base::binary);

    ofs << ifs.rdbuf();
    ifs.close();
  }

  // todo: set metadata
  ofs.close();

  std::filesystem::remove_all(std::filesystem::path(path) /
                              XRD_MULTIPART_UPLOAD_DIR / bucket / upload_id);
  buploads->second.erase(upload);

  return S3Error::None;
}

}  // namespace S3
