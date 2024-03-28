//
// Created by segransm on 11/16/23.
//

#include "XrdS3Api.hh"

#include <tinyxml2.h>

#include "XrdCks/XrdCksCalcmd5.hh"
#include "XrdS3Auth.hh"
#include "XrdS3ErrorResponse.hh"
#include "XrdS3Response.hh"

namespace S3 {

#define VALIDATE_REQUEST(action)                                 \
  auto [err, bucket] =                                           \
      auth.ValidateRequest(req, action, req.bucket, req.object); \
  if (err != S3Error::None) {                                    \
    return req.S3ErrorResponse(err);                             \
  }

#define RET_ON_ERROR(action)         \
  err = action;                      \
  if (err != S3Error::None) {        \
    return req.S3ErrorResponse(err); \
  }

bool ParseCreateBucketBody(char* body, int length, std::string& location) {
  tinyxml2::XMLDocument doc;

  doc.Parse(body, length);

  if (doc.Error()) {
    return false;
  }

  tinyxml2::XMLElement* elem = doc.RootElement();

  if (elem == nullptr ||
      std::string(elem->Name()) != "CreateBucketConfiguration") {
    return false;
  }

  elem = elem->FirstChildElement();
  if (elem == nullptr) {
    return false;
  }
  if (std::string(elem->Name()) != "LocationConstraint") {
    return false;
  }

  location = elem->GetText();

  if (elem->NextSiblingElement() != nullptr) {
    return false;
  }
  return true;
}

int S3Api::CreateBucketHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::CreateBucket)

  int length = 0;
  auto it = req.lowercase_headers.find("content-length");
  if (it != req.lowercase_headers.end()) {
    try {
      length = std::stoi(it->second);
    } catch (std::exception&) {
      return req.S3ErrorResponse(S3Error::InvalidArgument);
    }
  }
  if (length < 0) {
    return req.S3ErrorResponse(S3Error::InvalidArgument);
  }

  std::string location;
  if (length > 0) {
    char* ptr;
    if (req.ReadBody(length, &ptr, true) != length) {
      return req.S3ErrorResponse(S3Error::IncompleteBody);
    }

    if (!ParseCreateBucketBody(ptr, length, location) || location.empty()) {
      return req.S3ErrorResponse(S3Error::MalformedXML);
    }
  }

  bucket.owner.id = req.id;
  bucket.name = req.bucket;

  RET_ON_ERROR(objectStore.CreateBucket(auth, bucket, location))

  Headers headers = {{"Location", '/' + req.bucket}};
  return req.S3Response(200, headers, "");
}

int S3Api::ListBucketsHandler(S3::XrdS3Req& req) {
  VALIDATE_REQUEST(Action::ListBuckets)

  auto buckets = objectStore.ListBuckets(req.id);

  return ListBucketsResponse(req, req.id, req.id, buckets);
}

int S3Api::HeadBucketHandler(S3::XrdS3Req& req) {
  auto [err, bucket] =
      auth.ValidateRequest(req, Action::HeadBucket, req.bucket, req.object);
  // Head bucket does not return a body when an error occurs
  if (err != S3Error::None) {
    return req.S3Response(S3ErrorMap.find(err)->second.httpCode);
  }

  return req.Ok();
}

int S3Api::DeleteBucketHandler(S3::XrdS3Req& req) {
  VALIDATE_REQUEST(Action::DeleteBucket)

  RET_ON_ERROR(objectStore.DeleteBucket(auth, bucket))

  return req.S3Response(204);
}

int S3Api::DeleteObjectHandler(S3::XrdS3Req& req) {
  VALIDATE_REQUEST(Action::DeleteObject)

  RET_ON_ERROR(objectStore.DeleteObject(bucket, req.object))

  return req.S3Response(204);
}

S3Error ValidatePreconditions(const std::string& etag, time_t last_modified,
                              const Headers& headers) {
  // See https://datatracker.ietf.org/doc/html/rfc7232#section-6 for
  // precondition evaluation order
  auto it = headers.end();
  if ((it = headers.find("if-match")) != headers.end()) {
    if (it->second != etag) {
      return S3Error::PreconditionFailed;
    }
  } else {
    if ((it = headers.find("if-unmodified-since")) != headers.end()) {
      tm date{};
      auto ret =
          strptime(it->second.c_str(), "%a, %d %b %Y %H:%M:%S GMT", &date);
      if (ret == nullptr || *ret != '\0') {
        return S3Error::InvalidArgument;
      }
      if (last_modified > timegm(&date)) {
        return S3Error::PreconditionFailed;
      }
    }
  }
  if ((it = headers.find("if-none-match")) != headers.end()) {
    if (it->second == etag) {
      return S3Error::NotModified;
    }
  } else {
    if ((it = headers.find("if-modified-since")) != headers.end()) {
      tm date{};
      auto ret =
          strptime(it->second.c_str(), "%a, %d %b %Y %H:%M:%S GMT", &date);
      if (ret == nullptr || *ret != '\0') {
        return S3Error::InvalidArgument;
      }
      if (last_modified < timegm(&date)) {
        return S3Error::NotModified;
      }
    }
  }
  return S3Error::None;
}

int S3Api::GetObjectHandler(S3::XrdS3Req& req) {
  VALIDATE_REQUEST(Action::GetObject)

  S3ObjectStore::Object obj;

  RET_ON_ERROR(objectStore.GetObject(bucket, req.object, obj))

  std::map<std::string, std::string> headers = obj.GetAttributes();
  if (!S3Utils::MapHasKey(headers, "etag")) {
    return req.S3ErrorResponse(S3Error::InternalError);
  }

  std::string etag = headers["etag"];
  time_t last_modified = obj.LastModified();

  RET_ON_ERROR(
      ValidatePreconditions(etag, last_modified, req.lowercase_headers))

  ssize_t length = obj.GetSize();
  ssize_t start = 0;
  if (S3Utils::MapHasKey(req.lowercase_headers, "range")) {
    if (req.lowercase_headers["range"].find("bytes=") != 0) {
      return req.S3ErrorResponse(S3Error::InvalidRange);
    }

    auto split_at = req.lowercase_headers["range"].find('-', 6);

    if (split_at == std::string::npos) {
      return req.S3ErrorResponse(S3Error::InvalidRange);
    }

    if (split_at != 6) {
      try {
        start = (ssize_t)std::stoul(
            req.lowercase_headers["range"].substr(6, split_at));
      } catch (std::exception&) {
        return req.S3ErrorResponse(S3Error::InvalidRange);
      }
    } else {
      // Range in format bytes=-123
      start = 0;
    }

    ssize_t end = 0;
    if (split_at == req.lowercase_headers["range"].length() - 1) {
      // Range in format bytes=123-
      end = obj.GetSize();
    } else {
      try {
        end = (ssize_t)std::stoul(
            req.lowercase_headers["range"].substr(split_at + 1));
      } catch (std::exception&) {
        return req.S3ErrorResponse(S3Error::InvalidRange);
      }
    }

    if (end < start || end > obj.GetSize()) {
      return req.S3ErrorResponse(S3Error::InvalidRange);
    }

    length = end - start;
  }

  headers["last-modified"] = S3Utils::timestampToIso8016(last_modified);

  if (length == 0) {
    return req.S3Response(200, headers, nullptr, 0);
  }

  if (obj.Lseek(start, SEEK_SET) == -1) {
    return req.S3ErrorResponse(S3Error::InternalError);
  }

  char* ptr;

  if ((size_t)length <= obj.BufferSize()) {
    auto i = obj.Read(length, &ptr);
    if (i != length) {
      return req.S3ErrorResponse(S3Error::InternalError);
    }
    return req.S3Response(200, headers, ptr, i);
  } else {
    auto ret = req.StartChunkedResp(200, headers);
    if (ret < 0) {
      return ret;
    }

    ssize_t i = 0;
    while ((i = obj.Read(length, &ptr)) > 0) {
      if (length < i) {
        return -1;
      }
      length -= i;
      ret = req.ChunkResp(ptr, i);
      if (ret < 0) {
        return ret;
      }
    }

    return req.ChunkResp(nullptr, 0);
  }
}

S3Error ParseCommonQueryParams(
    const std::map<std::string, std::string>& query_params, char& delimiter,
    bool& encode_values, int& max_keys, std::string& prefix) {
  auto it = query_params.end();

  delimiter = 0;
  if ((it = query_params.find("delimiter")) != query_params.end()) {
    if (it->second.length() > 1) {
      return S3Error::InvalidArgument;
    }
    if (it->second.empty()) {
      delimiter = 0;
    } else {
      delimiter = it->second[0];
    }
  }

  encode_values = false;
  if ((it = query_params.find("encoding-type")) != query_params.end()) {
    if (it->second != "url") {
      return S3Error::InvalidArgument;
    }
    encode_values = true;
  }

  // 1k by default, change depending on url params
  max_keys = 1000;
  if ((it = query_params.find("max-keys")) != query_params.end()) {
    try {
      max_keys = std::stoi(it->second);
    } catch (std::exception& e) {
      return S3Error::InvalidArgument;
    }
  }

  prefix = "";
  if ((it = query_params.find("prefix")) != query_params.end()) {
    prefix = it->second;
  }

  return S3Error::None;
}

int S3Api::ListObjectVersionsHandler(S3::XrdS3Req& req) {
  VALIDATE_REQUEST(Action::ListObjectVersions)

  char delimiter;
  std::string prefix;
  bool encode_values;
  int max_keys;

  RET_ON_ERROR(ParseCommonQueryParams(req.query, delimiter, encode_values,
                                      max_keys, prefix))

  auto it = req.query.end();

  std::string key_marker;
  if ((it = req.query.find("key-marker")) != req.query.end()) {
    key_marker = it->second;
  }
  std::string version_id_marker;
  if ((it = req.query.find("version-id-marker")) != req.query.end()) {
    version_id_marker = it->second;
  }

  auto vinfo = objectStore.ListObjectVersions(
      bucket, prefix, key_marker, version_id_marker, delimiter, max_keys);

  return ListObjectVersionsResponse(req, req.bucket, encode_values, delimiter,
                                    max_keys, prefix, vinfo);
}

#define PUT_LIMIT 5000000000

int S3Api::CopyObjectHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::CopyObject)

  auto source =
      req.ctx->utils.UriDecode(req.lowercase_headers["x-amz-copy-source"]);
  auto pos = source.find('/');
  if (pos == std::string::npos) {
    return req.S3ErrorResponse(S3Error::InvalidArgument);
  }
  auto bucket_src = source.substr(0, pos);
  auto object_src = source.substr(pos + 1);

  auto [error, source_bucket] =
      auth.ValidateRequest(req, Action::GetObject, bucket_src, object_src);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

  if (bucket_src == req.bucket && object_src == req.object) {
    return req.S3ErrorResponse(S3Error::InvalidRequest);
  }

  S3ObjectStore::Object obj;
  RET_ON_ERROR(objectStore.GetObject(source_bucket, object_src, obj))

  if (!S3Utils::MapHasKey(obj.GetAttributes(), "etag")) {
    return req.S3ErrorResponse(S3Error::InternalError);
  }

  //  std::string etag = headers["etag"];
  //  time_t last_modified = std::stoul(headers["last-modified"]);

  // err = ValidatePreconditions(etag, last_modified, req.lowercase_headers);
  // if (err != S3Error::None) {
  // return req.S3ErrorResponse(err, "", "");
  // }

  if (obj.GetSize() > PUT_LIMIT) {
    return req.S3ErrorResponse(S3Error::EntityTooLarge);
  }

  Headers hd = {{"Content-Type", "application/xml"}};
  req.StartChunkedResp(200, hd);

  std::map<std::string, std::string> headers;
  err = objectStore.CopyObject(bucket, req.object, obj, req.lowercase_headers,
                               headers);
  if (err != S3Error::None) {
    req.S3ErrorResponse(err, "", "", true);
  } else {
    CopyObjectResponse(req, headers["ETag"]);
  }

  return req.ChunkResp(nullptr, 0);
}

int S3Api::PutObjectHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::PutObject)

  auto chunked = false;
  unsigned long length = 0;
  auto it = req.lowercase_headers.find("content-length");
  if (it == req.lowercase_headers.end()) {
    if (S3Utils::MapHasEntry(req.lowercase_headers, "transfer-encoding",
                             "chunked")) {
      chunked = true;
    } else {
      return req.S3ErrorResponse(S3Error::MissingContentLength);
    }
  } else {
    try {
      length = std::stoul(it->second);
    } catch (std::exception&) {
      return req.S3ErrorResponse(S3Error::InvalidArgument);
    }

    if (length > PUT_LIMIT) {
      return req.S3ErrorResponse(S3Error::EntityTooLarge);
    }
  }

  S3ObjectStore::Object obj;
  err = objectStore.GetObject(bucket, req.object, obj);
  if (err == S3Error::None) {
    std::map<std::string, std::string> headers = obj.GetAttributes();
    if (!S3Utils::MapHasKey(headers, "etag")) {
      return req.S3ErrorResponse(S3Error::InternalError);
    }

    std::string etag = headers["etag"];
    time_t last_modified = obj.LastModified();

    RET_ON_ERROR(
        ValidatePreconditions(etag, last_modified, req.lowercase_headers))
  }

  std::map<std::string, std::string> headers;
  RET_ON_ERROR(objectStore.PutObject(req, bucket, length, chunked, headers))

  return req.S3Response(200, headers, "");
}

int S3Api::HeadObjectHandler(XrdS3Req& req) {
  auto [err, bucket] =
      auth.ValidateRequest(req, Action::HeadObject, req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3Response(S3ErrorMap.find(err)->second.httpCode);
  }

  S3ObjectStore::Object obj;

  err = objectStore.GetObject(bucket, req.object, obj);
  if (err != S3Error::None) {
    return req.S3Response(S3ErrorMap.find(err)->second.httpCode);
  }

  std::map<std::string, std::string> headers = obj.GetAttributes();

  auto last_modified = obj.LastModified();

  headers["last-modified"] = S3Utils::timestampToIso8016(last_modified);

  auto content_length = obj.GetSize();

  return req.S3Response(200, headers, nullptr, (long long)content_length);
}

struct DeleteObjectsQuery {
  bool quiet;
  std::vector<SimpleObject> objects;
};

bool ParseDeleteObjectsBody(char* body, int length, DeleteObjectsQuery& query) {
  tinyxml2::XMLDocument doc;

  doc.Parse(body, length);

  if (doc.Error()) {
    return false;
  }

  tinyxml2::XMLElement* elem = doc.RootElement();

  if (elem == nullptr || std::string(elem->Name()) != "Delete") {
    return false;
  }

  elem = elem->FirstChildElement();
  if (elem == nullptr) {
    return false;
  }

  tinyxml2::XMLElement* child = nullptr;
  const char* str = nullptr;
  while (elem) {
    const std::string name(elem->Name());
    if (name == "Object") {
      if ((child = elem->FirstChildElement("Key")) == nullptr) {
        return false;
      }

      if ((str = child->GetText()) == nullptr) {
        return false;
      }
      std::string version_id;
      if ((child = elem->FirstChildElement("VersionId")) != nullptr) {
        version_id = child->GetText();
      }
      query.objects.push_back({str, version_id});
    } else if (name == "Quiet") {
      if (elem->QueryBoolText(&query.quiet)) {
        return false;
      }
    } else {
      return false;
    }
    elem = elem->NextSiblingElement();
  }
  return true;
}

int S3Api::DeleteObjectsHandler(S3::XrdS3Req& req) {
  VALIDATE_REQUEST(Action::DeleteObjects)

  auto it = req.lowercase_headers.find("content-length");
  if (it == req.lowercase_headers.end()) {
    return req.S3ErrorResponse(S3Error::MissingContentLength);
  }
  int length;
  try {
    length = std::stoi(it->second);
  } catch (std::exception&) {
    return req.S3ErrorResponse(S3Error::InvalidArgument);
  }
  if (length < 0) {
    return req.S3ErrorResponse(S3Error::InvalidArgument);
  }

  char* ptr;
  if (req.ReadBody(length, &ptr, true) != length) {
    return req.S3ErrorResponse(S3Error::IncompleteBody);
  }

  DeleteObjectsQuery query;
  if (!ParseDeleteObjectsBody(ptr, length, query) || query.objects.empty()) {
    return req.S3ErrorResponse(S3Error::MalformedXML);
  }

  if (query.objects.size() > 1000) {
    return req.S3ErrorResponse(S3Error::InvalidRequest);
  }

  std::vector<DeletedObject> deleted;
  std::vector<ErrorObject> error;

  std::tie(deleted, error) = objectStore.DeleteObjects(bucket, query.objects);

  return DeleteObjectsResponse(req, query.quiet, deleted, error);
}

int S3Api::ListObjectsV2Handler(S3::XrdS3Req& req) {
  VALIDATE_REQUEST(Action::ListObjectsV2)

  char delimiter;
  std::string prefix;
  bool encode_values;
  int max_keys;

  RET_ON_ERROR(ParseCommonQueryParams(req.query, delimiter, encode_values,
                                      max_keys, prefix))

  auto it = req.query.end();
  std::string continuation_token;
  if ((it = req.query.find("continuation-token")) != req.query.end()) {
    continuation_token = it->second;
  }
  std::string start_after;
  if ((it = req.query.find("start-after")) != req.query.end()) {
    start_after = it->second;
  }

  bool fetch_owner = false;
  if ((it = req.query.find("fetch-owner")) != req.query.end()) {
    if (it->second == "true") {
      fetch_owner = true;
    } else if (it->second == "false") {
      fetch_owner = false;
    } else {
      return req.S3ErrorResponse(S3Error::InvalidArgument);
    }
  }

  auto objectinfo =
      objectStore.ListObjectsV2(bucket, prefix, continuation_token, delimiter,
                                max_keys, fetch_owner, start_after);

  return ListObjectsV2Response(req, req.bucket, prefix, continuation_token,
                               delimiter, max_keys, fetch_owner, start_after,
                               encode_values, objectinfo);
}

int S3Api::ListObjectsHandler(S3::XrdS3Req& req) {
  VALIDATE_REQUEST(Action::ListObjects)

  char delimiter;
  std::string prefix;
  bool encode_values;
  int max_keys;

  RET_ON_ERROR(ParseCommonQueryParams(req.query, delimiter, encode_values,
                                      max_keys, prefix))

  auto it = req.query.end();
  std::string marker;
  if ((it = req.query.find("marker")) != req.query.end()) {
    marker = it->second;
  }
  auto objectinfo =
      objectStore.ListObjects(bucket, prefix, marker, delimiter, max_keys);
  return ListObjectsResponse(req, req.bucket, prefix, delimiter, marker,
                             max_keys, encode_values, objectinfo);
}
int S3Api::CreateMultipartUploadHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::CreateMultipartUpload)

  auto [upload_id, error] =
      objectStore.CreateMultipartUpload(bucket, req.object);

  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }
  return CreateMultipartUploadResponse(req, upload_id);
}

int S3Api::ListMultipartUploadsHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::ListMultipartUploads)

  auto multipart_uploads = objectStore.ListMultipartUploads(req.bucket);

  return ListMultipartUploadResponse(req, multipart_uploads);
}

int S3Api::AbortMultipartUploadHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::AbortMultipartUpload)

  // This function will never be called if the query params do not contain
  // `uploadId`.
  auto upload_id = req.query["uploadId"];

  RET_ON_ERROR(objectStore.AbortMultipartUpload(bucket, req.object, upload_id))

  return req.S3Response(204);
}

int S3Api::ListPartsHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::ListParts)

  // This function will never be called if the query params do not contain
  // `uploadId`.
  auto upload_id = req.query["uploadId"];

  auto [error, parts] =
      objectStore.ListParts(req.bucket, req.object, upload_id);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

  return ListPartsResponse(req, upload_id, parts);
}
int S3Api::UploadPartHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::UploadPart)

  // This function will never be called if the query params do not contain
  // `uploadId` and `partNumber`.
  auto upload_id = req.query["uploadId"];
  size_t part_number;
  try {
    part_number = std::stoul(req.query["partNumber"]);
  } catch (std::exception& e) {
    return req.S3ErrorResponse(S3Error::InvalidRequest);
  }

  if (part_number < 1 || part_number > 10000) {
    return req.S3ErrorResponse(S3Error::InvalidRequest);
  }

  bool chunked = false;
  size_t length = 0;
  auto it = req.lowercase_headers.find("content-length");
  if (it == req.lowercase_headers.end()) {
    if (S3Utils::MapHasEntry(req.lowercase_headers, "transfer-encoding",
                             "chunked")) {
      chunked = true;
    } else {
      return req.S3ErrorResponse(S3Error::MissingContentLength);
    }
  } else {
    try {
      length = std::stoul(it->second);
    } catch (std::exception&) {
      return req.S3ErrorResponse(S3Error::InvalidArgument);
    }
    // TODO: Parts have a minimum size if they are not the last part,
    //  they are currently not rejected.
    if (length > PUT_LIMIT) {
      return req.S3ErrorResponse(S3Error::EntityTooLarge);
    }
  }

  std::map<std::string, std::string> headers;

  RET_ON_ERROR(objectStore.UploadPart(req, upload_id, part_number, length,
                                      chunked, headers))

  return req.S3Response(200, headers, "");
}

bool ParseCompleteMultipartUploadBody(
    char* body, int length, std::vector<S3ObjectStore::PartInfo>& query) {
  tinyxml2::XMLDocument doc;

  doc.Parse(body, length);

  if (doc.Error()) {
    return false;
  }

  tinyxml2::XMLElement* elem = doc.RootElement();

  if (elem == nullptr ||
      std::string(elem->Name()) != "CompleteMultipartUpload") {
    return false;
  }

  elem = elem->FirstChildElement();
  if (elem == nullptr) {
    return false;
  }

  tinyxml2::XMLElement* child = nullptr;
  while (elem) {
    const std::string name(elem->Name());
    if (name == "Part") {
      if ((child = elem->FirstChildElement("ETag")) == nullptr) {
        return false;
      }

      if (child->GetText() == nullptr) {
        return false;
      }

      std::string etag(child->GetText());

      if ((child = elem->FirstChildElement("PartNumber")) == nullptr) {
        return false;
      }
      if (child->GetText() == nullptr) {
        return false;
      }

      query.push_back({etag, {}, std::stoul(child->GetText()), {}});
    } else {
      return false;
    }
    elem = elem->NextSiblingElement();
  }
  return true;
}

int S3Api::CompleteMultipartUploadHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::CompleteMultipartUpload)

  auto it = req.lowercase_headers.find("content-length");
  if (it == req.lowercase_headers.end()) {
    return req.S3ErrorResponse(S3Error::MissingContentLength);
  }
  int length;
  try {
    length = std::stoi(it->second);
  } catch (std::exception&) {
    return req.S3ErrorResponse(S3Error::InvalidArgument);
  }
  if (length < 0) {
    return req.S3ErrorResponse(S3Error::InvalidArgument);
  }

  char* ptr;
  if (req.ReadBody(length, &ptr, true) != length) {
    return req.S3ErrorResponse(S3Error::IncompleteBody);
  }

  std::vector<S3ObjectStore::PartInfo> query;
  if (!ParseCompleteMultipartUploadBody(ptr, length, query) || query.empty()) {
    return req.S3ErrorResponse(S3Error::MalformedXML);
  }

  if (query.size() > 10000) {
    return req.S3ErrorResponse(S3Error::InvalidRequest);
  }

  auto upload_id = req.query["uploadId"];

  RET_ON_ERROR(objectStore.CompleteMultipartUpload(req, bucket, req.object,
                                                   upload_id, query))

  return CompleteMultipartUploadResponse(req);
}

int S3Api::GetBucketAclHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::GetBucketAcl)

  return GetAclResponse(req, bucket);
}

int S3Api::GetObjectAclHandler(XrdS3Req& req) {
  VALIDATE_REQUEST(Action::GetObjectAcl)

  S3ObjectStore::Object obj;

  RET_ON_ERROR(objectStore.GetObject(bucket, req.object, obj))

  return GetAclResponse(req, bucket);
}

#undef VALIDATE_REQUEST
#undef RET_ON_ERROR

}  // namespace S3
