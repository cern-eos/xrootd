//
// Created by segransm on 11/16/23.
//

#include "XrdS3Api.hh"

#include <sys/stat.h>
#include <tinyxml2.h>

#include "S3Response.hh"
#include "XrdCks/XrdCksCalcmd5.hh"
#include "XrdS3Auth.hh"
#include "XrdS3ErrorResponse.hh"

namespace S3 {

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
  auto err = auth.ValidateRequest(req, objectStore, Action::CreateBucket,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

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

  S3Error error = objectStore.CreateBucket(req.id, req.bucket, location);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

  Headers headers = {{"Location", '/' + req.bucket}};
  return req.S3Response(200, headers, "");
}

int S3Api::ListBucketsHandler(S3::XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::ListBuckets,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  auto buckets = objectStore.ListBuckets(req.id);

  // todo: display name
  return ListBucketsResponse(req, req.id, "display name", buckets);
}

int S3Api::HeadBucketHandler(S3::XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::HeadBucket,
                                  req.bucket, req.object);
  // Head bucket does not return a body when an error occurs
  if (err != S3Error::None) {
    return req.S3Response(S3ErrorMap.find(err)->second.httpCode);
  }

  return req.Ok();
}

int S3Api::DeleteBucketHandler(S3::XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::DeleteBucket,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  // todo: we assume that bucket is owner by req.id;
  S3Error error = objectStore.DeleteBucket(req.bucket);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

  return req.S3Response(204);
}

int S3Api::DeleteObjectHandler(S3::XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::DeleteObject,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  // todo: req.path should be req.object
  S3Error error = objectStore.DeleteObject(req.bucket, req.object);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

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
      fprintf(stderr, "FOUND PRECONDITION! `%s` `%s` `%s`\n",
              it->second.c_str(), to_string(last_modified).c_str(),
              to_string(timegm(&date)).c_str());
      if (last_modified < timegm(&date)) {
        fprintf(stderr, "ERROR\n");
        return S3Error::NotModified;
      }
    }
  }
  return S3Error::None;
  // todo range header
}

#define LOG(msg) std::cerr << msg << std::endl;

int S3Api::GetObjectHandler(S3::XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::GetObject,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  S3ObjectStore::Object obj;

  auto error = objectStore.GetObject(req.bucket, req.object, obj);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

  std::map<std::string, std::string> headers = obj.GetAttributes();
  if (!S3Utils::HasHeader(headers, "etag") ||
      !S3Utils::HasHeader(headers, "last-modified")) {
    return req.S3ErrorResponse(S3Error::InternalError);
  }

  std::string etag = headers["etag"];
  time_t last_modified = std::stol(headers["last-modified"]);

  error = ValidatePreconditions(etag, last_modified, req.lowercase_headers);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

  headers["last-modified"] = S3Utils::timestampToIso8016(last_modified);

  if (obj.GetSize() == 0) {
    return req.S3Response(200, headers, nullptr, 0);
  } else if (obj.GetSize() <= 32000000) {
    char* ptr = new char[obj.GetSize()];
    auto i = obj.GetStream().readsome(ptr, obj.GetSize());
    if (i == 0) {
      delete[] ptr;
      return req.S3ErrorResponse(S3Error::InternalError);
    }
    auto ret = req.S3Response(200, headers, ptr, i);
    delete[] ptr;

    return ret;
  } else {
    auto ret = req.StartChunkedResp(200, headers);

    char* ptr = new char[32000000];
    auto stream = &obj.GetStream();
    while (stream->good()) {
      auto i = stream->readsome(ptr, 32000000);

      req.ChunkResp(ptr, i);
    }
    req.ChunkResp(nullptr, 0);
    return ret;
  }

  // todo: autodetect content type on put object
  // headers.insert({"Content-Type", "text/plain"});
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
  auto err = auth.ValidateRequest(req, objectStore, Action::ListObjectVersions,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  char delimiter;
  std::string prefix;
  bool encode_values;
  int max_keys;

  auto error = ParseCommonQueryParams(req.query, delimiter, encode_values,
                                      max_keys, prefix);

  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

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
      req.bucket, prefix, key_marker, version_id_marker, delimiter, max_keys);

  // todo: key_marker is lastt key returned, next_key_marker is the one after
  // key_marker todo: next vid marker && vid marker
  return ListObjectVersionsResponse(req, req.bucket, encode_values, delimiter,
                                    max_keys, prefix, vinfo);
}

#define VALIDATE_REQUEST(action)                                              \
  auto err =                                                                  \
      auth.ValidateRequest(req, objectStore, action, req.bucket, req.object); \
  if (err != S3Error::None) {                                                 \
    return req.S3ErrorResponse(err);                                          \
  }

#define PUT_LIMIT 5000000000

int S3Api::CopyObjectHandler(XrdS3Req& req) {
  // return req.S3ErrorResponse(S3Error::NotImplemented, "", "");
  // // todo: combine code for all functions that call ValidateRequest then ret
  VALIDATE_REQUEST(Action::CopyObject)

  auto source =
      req.ctx->utils.UriDecode(req.lowercase_headers["x-amz-copy-source"]);
  // todo: validate name
  auto pos = source.find('/');
  if (pos == std::string::npos) {
    return req.S3ErrorResponse(S3Error::InvalidArgument);
  }
  auto bucket_src = source.substr(0, pos);
  auto object_src = source.substr(pos + 1);

  // // todo: do this in AuthorizeRequest
  err = auth.ValidateRequest(req, objectStore, Action::GetObject, bucket_src,
                             object_src);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  if (bucket_src == req.bucket && object_src == req.object) {
    return req.S3ErrorResponse(S3Error::InvalidRequest);
  }

  S3ObjectStore::Object obj;
  err = objectStore.GetObject(bucket_src, object_src, obj);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  std::map<std::string, std::string> headers = obj.GetAttributes();
  if (!S3Utils::HasHeader(headers, "etag") ||
      !S3Utils::HasHeader(headers, "last-modified")) {
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

  auto error = objectStore.CopyObject(req.bucket, req.object, obj,
                                      req.lowercase_headers, headers);
  if (error != S3Error::None) {
    req.S3ErrorResponse(error, "", "", true);
  } else {
    CopyObjectResponse(req, headers["ETag"]);
  }

  return req.ChunkResp(nullptr, 0);
}

int S3Api::PutObjectHandler(XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::PutObject,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  auto chunked = false;
  unsigned long length = 0;
  auto it = req.lowercase_headers.find("content-length");
  if (it == req.lowercase_headers.end()) {
    if (S3Utils::HeaderEq(req.lowercase_headers, "transfer-encoding",
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
  auto error = objectStore.GetObject(req.bucket, req.object, obj);
  if (error == S3Error::None) {
    std::map<std::string, std::string> headers = obj.GetAttributes();
    if (!S3Utils::HasHeader(headers, "etag") ||
        !S3Utils::HasHeader(headers, "last-modified")) {
      return req.S3ErrorResponse(S3Error::InternalError);
    }

    std::string etag = headers["etag"];
    time_t last_modified = std::stol(headers["last-modified"]);

    error = ValidatePreconditions(etag, last_modified, req.lowercase_headers);
    if (error != S3Error::None) {
      return req.S3ErrorResponse(error);
    }
  }

  std::map<std::string, std::string> headers;
  error = objectStore.PutObject(req, length, chunked, headers);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

  return req.S3Response(200, headers, "");
}

int S3Api::HeadObjectHandler(XrdS3Req& req) {
  auto error = auth.ValidateRequest(req, objectStore, Action::HeadObject,
                                    req.bucket, req.object);
  if (error != S3Error::None) {
    return req.S3Response(S3ErrorMap.find(error)->second.httpCode);
  }

  S3ObjectStore::Object obj;

  error = objectStore.GetObject(req.bucket, req.object, obj);
  if (error != S3Error::None) {
    return req.S3Response(S3ErrorMap.find(error)->second.httpCode);
  }

  std::map<std::string, std::string> headers = obj.GetAttributes();

  // todo: autodetect content type on put object
  // headers.insert({"Content-Type", "text/plain"});

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
    fprintf(stderr, "DOC ERROR: %d\n", doc.ErrorID());
    return false;
  }

  tinyxml2::XMLElement* elem = doc.RootElement();

  if (elem == nullptr || std::string(elem->Name()) != "Delete") {
    fprintf(stderr, "no delete elem\n");
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
        fprintf(stderr, "no key elem\n");
        return false;
      }

      fprintf(stderr, "firstchild: %p\n", child->FirstChild());
      fprintf(stderr, "getext: %s\n", child->GetText());
      if ((str = child->GetText()) == nullptr) {
        //        fprintf(stderr, "cant get text of key %p\n",
        //        child->FirstChild());
        return false;
      }
      std::string version_id;
      if ((child = elem->FirstChildElement("VersionId")) != nullptr) {
        version_id = child->GetText();
      }
      // todo: parse objects that end with /
      query.objects.push_back({str, version_id});
    } else if (name == "Quiet") {
      if (elem->QueryBoolText(&query.quiet)) {
        fprintf(stderr, "quiet is not bool\n");
        return false;
      }
    } else {
      fprintf(stderr, "unknown element name: %s\n", name.c_str());
      return false;
    }
    elem = elem->NextSiblingElement();
  }
  return true;
}

int S3Api::DeleteObjectsHandler(S3::XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::DeleteObjects,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

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

  std::tie(deleted, error) =
      objectStore.DeleteObjects(req.bucket, query.objects);

  return DeleteObjectsResponse(req, query.quiet, deleted, error);
}

int S3Api::ListObjectsV2Handler(S3::XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::ListObjectsV2,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  char delimiter;
  std::string prefix;
  bool encode_values;
  int max_keys;
  auto error = ParseCommonQueryParams(req.query, delimiter, encode_values,
                                      max_keys, prefix);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }

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
      objectStore.ListObjectsV2(req.bucket, prefix, continuation_token,
                                delimiter, max_keys, fetch_owner, start_after);

  return ListObjectsV2Response(req, req.bucket, prefix, continuation_token,
                               delimiter, max_keys, fetch_owner, start_after,
                               encode_values, objectinfo);
}

int S3Api::ListObjectsHandler(S3::XrdS3Req& req) {
  auto err = auth.ValidateRequest(req, objectStore, Action::ListObjects,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  char delimiter;
  std::string prefix;
  bool encode_values;
  int max_keys;
  auto error = ParseCommonQueryParams(req.query, delimiter, encode_values,
                                      max_keys, prefix);
  if (error != S3Error::None) {
    return req.S3ErrorResponse(error);
  }
  auto it = req.query.end();
  std::string marker;
  if ((it = req.query.find("marker")) != req.query.end()) {
    marker = it->second;
  }
  auto objectinfo =
      objectStore.ListObjects(req.bucket, prefix, marker, delimiter, max_keys);
  return ListObjectsResponse(req, req.bucket, prefix, delimiter, marker,
                             max_keys, encode_values, objectinfo);
}
int S3Api::CreateMultipartUploadHandler(XrdS3Req& req) {
  auto err = auth.ValidateRequest(
      req, objectStore, Action::CreateMultipartUpload, req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  std::string upload_id =
      objectStore.CreateMultipartUpload(req, req.bucket, req.object);

  if (upload_id.empty()) {
    return req.S3ErrorResponse(S3Error::InternalError);
  }
  return CreateMultipartUploadResponse(req, upload_id);
}

int S3Api::ListMultipartUploadsHandler(XrdS3Req& req) {
  auto err = auth.ValidateRequest(
      req, objectStore, Action::ListMultipartUploads, req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  auto multipart_uploads = objectStore.ListMultipartUploads(req.bucket);

  return ListMultipartUploadResponse(req, multipart_uploads);
}

int S3Api::AbortMultipartUploadHandler(XrdS3Req& req) {
  auto err = auth.ValidateRequest(
      req, objectStore, Action::AbortMultipartUpload, req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  // This function will never be called if the query params do not contain
  // `uploadId`.
  auto upload_id = req.query["uploadId"];

  err = objectStore.AbortMultipartUpload(req.bucket, req.object, upload_id);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  return req.S3Response(204);
}

int S3Api::ListPartsHandler(XrdS3Req& req) {
  auto err = auth.ValidateRequest(
      req, objectStore, Action::AbortMultipartUpload, req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

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
  auto err = auth.ValidateRequest(req, objectStore, Action::UploadPart,
                                  req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

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
    if (S3Utils::HeaderEq(req.lowercase_headers, "transfer-encoding",
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
    // todo: check minimum size if not last part
    if (length > PUT_LIMIT) {
      return req.S3ErrorResponse(S3Error::EntityTooLarge);
    }
  }

  std::map<std::string, std::string> headers;
  err = objectStore.UploadPart(req, upload_id, part_number, length, chunked,
                               headers);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

  return req.S3Response(200, headers, "");
}

// todo: handle checksum

bool ParseCompleteMultipartUploadBody(
    char* body, int length, std::vector<S3ObjectStore::PartInfo>& query) {
  tinyxml2::XMLDocument doc;

  doc.Parse(body, length);

  if (doc.Error()) {
    fprintf(stderr, "DOC ERROR: %d\n", doc.ErrorID());
    return false;
  }

  tinyxml2::XMLElement* elem = doc.RootElement();

  if (elem == nullptr ||
      std::string(elem->Name()) != "CompleteMultipartUpload") {
    fprintf(stderr, "no delete elem\n");
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
        fprintf(stderr, "no etag\n");
        return false;
      }

      if (child->GetText() == nullptr) {
        fprintf(stderr, "cant get text of etag %p\n", child->FirstChild());
        return false;
      }

      std::string etag(child->GetText());

      if ((child = elem->FirstChildElement("PartNumber")) == nullptr) {
        fprintf(stderr, "no part number\n");
        return false;
      }
      if (child->GetText() == nullptr) {
        fprintf(stderr, "cant get text of part_number %p\n",
                child->FirstChild());
        return false;
      }

      query.push_back({etag, {}, std::stoul(child->GetText()), {}});
    } else {
      fprintf(stderr, "unknown element name: %s\n", name.c_str());
      return false;
    }
    elem = elem->NextSiblingElement();
  }
  return true;
}

int S3Api::CompleteMultipartUploadHandler(XrdS3Req& req) {
  auto err =
      auth.ValidateRequest(req, objectStore, Action::CompleteMultipartUpload,
                           req.bucket, req.object);
  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }

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

  err = objectStore.CompleteMultipartUpload(req, req.bucket, req.object,
                                            upload_id, std::move(query));

  if (err != S3Error::None) {
    return req.S3ErrorResponse(err);
  }
  return CompleteMultipartUploadResponse(req);
}

}  // namespace S3
