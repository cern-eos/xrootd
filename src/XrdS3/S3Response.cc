//
// Created by segransm on 11/16/23.
//

#include "S3Response.hh"

#include <set>

#include "XrdS3ObjectStore.hh"
#include "XrdS3Xml.hh"

namespace S3 {

int ListBucketsResponse(XrdS3Req& req, const std::string& id,
                        const std::string& display_name,
                        const std::vector<S3ObjectStore::BucketInfo>& buckets) {
  S3Xml printer;

  printer.OpenElement("ListAllMyBucketsResult");
  printer.OpenElement("Owner");
  printer.AddElement("ID", id);
  printer.AddElement("DisplayName", display_name);
  printer.CloseElement();

  printer.OpenElement("Buckets");

  for (const auto& bucket : buckets) {
    printer.OpenElement("Bucket");
    printer.AddElement("Name", bucket.name);
    printer.AddElement("CreationDate",
                       S3Utils::timestampToIso8016(bucket.created));
    printer.CloseElement();
  }

  printer.CloseElement();
  printer.CloseElement();

  std::string hd = "Content-Type: application/xml";
  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}

int ListObjectVersionsResponse(XrdS3Req& req, const std::string& bucket,
                               const bool encode, const char delimiter,
                               const int max_keys, const std::string& prefix,
                               const ListObjectsInfo& vinfo) {
  S3Xml printer;
  auto encoder = [req, encode](const std::string& str) {
    return encode ? req.ctx->utils.ObjectUriEncode(str) : str;
  };

  printer.OpenElement("ListVersionsResult");

  // todo: put common code like this in separate func
  for (const auto& pfx : vinfo.common_prefixes) {
    printer.OpenElement("CommonPrefixes");
    printer.AddElement("Prefix", encoder(pfx));
    printer.CloseElement();
  }

  // todo: delete marker
  //  printer.AddElement("DeleteMarker", "");

  if (delimiter) {
    printer.AddElement("Delimiter", encoder(std::string(1, delimiter)));
  }

  if (encode) {
    printer.AddElement("EncodingType", "url");
  }

  printer.AddElement("IsTruncated", vinfo.is_truncated);

  printer.AddElement("KeyMarker", encoder(vinfo.key_marker));

  printer.AddElement("MaxKeys", max_keys);
  printer.AddElement("Name", bucket);

  if (!vinfo.next_marker.empty()) {
    printer.AddElement("NextKeyMarker", encoder(vinfo.next_marker));
  }
  if (!vinfo.next_vid_marker.empty()) {
    printer.AddElement("NextVersionIdMarker", vinfo.next_vid_marker);
  }
  printer.AddElement("VersionIdMarker", vinfo.vid_marker);
  printer.AddElement("Prefix", encoder(prefix));
  for (const auto& version : vinfo.objects) {
    printer.OpenElement("Version");
    // todo: all fields
    //  https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectVersion.html

    printer.AddElement("Key", encoder(version.name));
    printer.AddElement("LastModified",
                       S3Utils::timestampToIso8016(version.last_modified));
    printer.AddElement("Size", version.size);
    // todo:
    printer.AddElement("VersionId", "1");
    printer.CloseElement();
  }

  printer.CloseElement();
  std::string hd = "Content-Type: application/xml";
  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}

int DeleteObjectsResponse(XrdS3Req& req, bool quiet,
                          const std::vector<DeletedObject>& deleted,
                          const std::vector<ErrorObject>& err) {
  S3Xml printer;

  printer.OpenElement("DeleteResult");

  if (!quiet) {
    for (const auto& d : deleted) {
      printer.OpenElement("Deleted");
      printer.AddElement("DeleteMarker", d.delete_marker);
      printer.AddElement("DeleteMarkerVersionId", d.delete_marker_version_id);
      printer.AddElement("Key", d.key);
      printer.AddElement("VersionId", d.version_id);
      printer.CloseElement();
    }
  }

  for (const auto& e : err) {
    printer.OpenElement("Error");
    printer.AddElement("Code", S3ErrorMap.find(e.code)->second.code);
    printer.AddElement("Key", e.key);
    printer.AddElement("Message", e.message);
    printer.AddElement("VersionId", e.version_id);
    printer.CloseElement();
  }

  printer.CloseElement();

  std::string hd = "Content-Type: application/xml";
  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}

int ListObjectsV2Response(XrdS3Req& req, const std::string& bucket,
                          const std::string& prefix,
                          const std::string& continuation_token,
                          const char delimiter, const int max_keys,
                          const bool fetch_owner,
                          const std::string& start_after, const bool encode,
                          const ListObjectsInfo& oinfo) {
  S3Xml printer;
  auto encoder = [req, encode](const std::string& str) {
    return encode ? req.ctx->utils.ObjectUriEncode(str) : str;
  };

  printer.OpenElement("ListBucketResult");
  printer.AddElement("Name", bucket);
  printer.AddElement("MaxKeys", max_keys);
  printer.AddElement("ContinuationToken", encoder(continuation_token));
  if (encode) {
    printer.AddElement("EncodingType", "url");
  }

  if (delimiter) {
    printer.AddElement("Delimiter", encoder(std::string(1, delimiter)));
  }

  if (!start_after.empty()) {
    printer.AddElement("StartAfter", encoder(start_after));
  }

  printer.AddElement("Prefix", encoder(prefix));

  printer.AddElement("KeyCount",
                     oinfo.objects.size() + oinfo.common_prefixes.size());
  printer.AddElement("IsTruncated", oinfo.is_truncated);
  if (oinfo.is_truncated) {
    printer.AddElement("NextContinuationToken", encoder(oinfo.key_marker));
  }

  for (const auto& object : oinfo.objects) {
    printer.OpenElement("Contents");
    printer.AddElement("Key", encoder(object.name));
    printer.AddElement("LastModified",
                       S3Utils::timestampToIso8016(object.last_modified));
    printer.AddElement("Size", object.size);
    if (fetch_owner) {
      printer.OpenElement("Owner");
      // todo: display name
      printer.AddElement("ID", object.owner);
      printer.CloseElement();
    }
    printer.CloseElement();
  }

  for (const auto& pfx : oinfo.common_prefixes) {
    printer.OpenElement("CommonPrefixes");
    printer.AddElement("Prefix", encoder(pfx));
    printer.CloseElement();
  }

  printer.CloseElement();

  std::string hd = "Content-Type: application/xml";
  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}

int ListObjectsResponse(XrdS3Req& req, const std::string& bucket,
                        const std::string& prefix, const char delimiter,
                        const std::string& marker, const int max_keys,
                        const bool encode, const ListObjectsInfo& objects) {
  S3Xml printer;
  auto encoder = [req, encode](const std::string& str) {
    return encode ? req.ctx->utils.ObjectUriEncode(str) : str;
  };

  printer.OpenElement("ListBucketResult");

  for (const auto& pfx : objects.common_prefixes) {
    printer.OpenElement("CommonPrefixes");
    printer.AddElement("Prefix", encoder(pfx));
    printer.CloseElement();
  }

  for (const auto& object : objects.objects) {
    printer.OpenElement("Contents");
    printer.AddElement("Key", encoder(object.name));
    printer.AddElement("LastModified",
                       S3Utils::timestampToIso8016(object.last_modified));
    printer.AddElement("Size", object.size);
    printer.CloseElement();
  }

  if (delimiter) {
    printer.AddElement("Delimiter", encoder(std::string(1, delimiter)));
  }

  if (encode) {
    printer.AddElement("EncodingType", "url");
  }
  printer.AddElement("IsTruncated", objects.is_truncated);

  printer.AddElement("Marker", encoder(marker));
  printer.AddElement("MaxKeys", max_keys);
  printer.AddElement("Name", bucket);

  if (objects.is_truncated && delimiter != 0) {
    printer.AddElement("NextMarker", encoder(objects.key_marker));
  }

  printer.AddElement("Prefix", prefix);

  printer.CloseElement();

  std::string hd = "Content-Type: application/xml";
  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}

int CopyObjectResponse(XrdS3Req& req, const std::string& etag) {
  S3Xml printer;

  printer.OpenElement("CopyObjectResult");

  printer.AddElement("ETag", etag);
  printer.CloseElement();

  return req.ChunkResp(printer.CStr(), printer.CStrSize() - 1);
}

int CreateMultipartUploadResponse(XrdS3Req& req, const std::string& upload_id) {
  S3Xml printer;
  printer.OpenElement("InitiateMultipartUploadResult");

  printer.AddElement("Bucket", req.bucket);
  printer.AddElement("Key", req.object);
  printer.AddElement("UploadId", upload_id);

  printer.CloseElement();

  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}
int ListMultipartUploadResponse(
    XrdS3Req& req,
    const std::vector<S3ObjectStore::MultipartUploadInfo>& uploads) {
  S3Xml printer;
  printer.OpenElement("ListMultipartUploadsResult");

  printer.AddElement("Bucket", req.bucket);

  for (const auto& [key, upload_id] : uploads) {
    printer.OpenElement("Upload");

    printer.AddElement("Key", key);
    printer.AddElement("UploadId", upload_id);

    printer.CloseElement();
  }

  printer.CloseElement();

  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}

int ListPartsResponse(XrdS3Req& req, const std::string& upload_id,
                      const std::vector<S3ObjectStore::PartInfo>& parts) {
  S3Xml printer;
  printer.OpenElement("ListPartsResult");

  printer.AddElement("Bucket", req.bucket);
  printer.AddElement("Key", req.object);
  printer.AddElement("UploadId", upload_id);

  for (const auto& [etag, last_modified, part_number, size] : parts) {
    printer.OpenElement("Part");

    printer.AddElement("ETag", etag);
    printer.AddElement("LastModified",
                       S3Utils::timestampToIso8016(last_modified));
    printer.AddElement("PartNumber", part_number);
    printer.AddElement("Size", size);

    printer.CloseElement();
  }

  printer.CloseElement();

  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}

int CompleteMultipartUploadResponse(XrdS3Req& req) {
  S3Xml printer;
  printer.OpenElement("CompleteMultipartUploadResult");

  printer.AddElement("Bucket", req.bucket);
  printer.AddElement("Key", req.object);

  printer.CloseElement();

  return req.S3Response(200, {}, printer.CStr(), printer.CStrSize() - 1);
}

}  // namespace S3
