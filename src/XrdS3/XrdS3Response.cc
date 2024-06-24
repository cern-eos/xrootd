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
#include "XrdS3Response.hh"
//------------------------------------------------------------------------------
#include <set>
//------------------------------------------------------------------------------
#include "XrdS3ObjectStore.hh"
#include "XrdS3Xml.hh"
//------------------------------------------------------------------------------

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
                       S3Utils::timestampToIso8601(bucket.created));
    printer.CloseElement();
  }

  printer.CloseElement();
  printer.CloseElement();

  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
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

  for (const auto& pfx : vinfo.common_prefixes) {
    printer.OpenElement("CommonPrefixes");
    printer.AddElement("Prefix", encoder(pfx));
    printer.CloseElement();
  }

  // TODO: Add delete marker support
  //  printer.AddElement("DeleteMarker", ...);

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
    // TODO: Implement all Version fields:
    //  https://docs.aws.amazon.com/AmazonS3/latest/API/API_ObjectVersion.html

    printer.AddElement("Key", encoder(version.name));
    printer.AddElement("LastModified",
                       S3Utils::timestampToIso8601(version.last_modified));
    printer.AddElement("Size", version.size);
    printer.AddElement("VersionId", "1");
    printer.CloseElement();
  }

  printer.CloseElement();
  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
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

  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
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

  printer.AddElement("KeyCount", (int64_t)(oinfo.objects.size() +
                                           oinfo.common_prefixes.size()));
  printer.AddElement("IsTruncated", oinfo.is_truncated);
  if (oinfo.is_truncated) {
    printer.AddElement("NextContinuationToken", encoder(oinfo.key_marker));
  }

  for (const auto& object : oinfo.objects) {
    printer.OpenElement("Contents");
    printer.AddElement("ETag", object.etag);
    printer.AddElement("Key", encoder(object.name));
    printer.AddElement("LastModified",
                       S3Utils::timestampToIso8601(object.last_modified));
    printer.AddElement("Size", object.size);
    if (fetch_owner) {
      printer.OpenElement("Owner");
      // TODO: Owner display name should probably be different from owner id
      printer.AddElement("ID", object.owner);
      printer.AddElement("DisplayName", object.owner);
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

  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
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
    printer.AddElement("ETag", object.etag);
    printer.AddElement("Key", encoder(object.name));
    printer.AddElement("LastModified",
                       S3Utils::timestampToIso8601(object.last_modified));
    printer.AddElement("Size", object.size);
    printer.OpenElement("Owner");

    printer.AddElement("ID", object.owner);
    printer.AddElement("DisplayName", object.owner);

    printer.CloseElement();

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

  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
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

  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
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
                       S3Utils::timestampToIso8601(last_modified));
    printer.AddElement("PartNumber", (int64_t)part_number);
    printer.AddElement("Size", (int64_t)size);

    printer.CloseElement();
  }

  printer.CloseElement();

  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
}

int CompleteMultipartUploadResponse(XrdS3Req& req) {
  S3Xml printer;
  printer.OpenElement("CompleteMultipartUploadResult");

  printer.AddElement("Bucket", req.bucket);
  printer.AddElement("Key", req.object);

  printer.CloseElement();
  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
}

int GetAclResponse(XrdS3Req& req, const S3Auth::Bucket& bucket) {
  S3Xml printer;
  printer.OpenElement("AccessControlPolicy");

  printer.OpenElement("Owner");

  printer.AddElement("DisplayName", bucket.owner.display_name);
  printer.AddElement("ID", bucket.owner.id);

  printer.CloseElement();

  // This is hardcoded as we do not support setting different acls.
  printer.OpenElement("AccessControlList");

  printer.OpenElement("Grant");

  printer.OpenElement("Grantee");
  printer.PushAttribute("xmlns:xsi",
                        "http://www.w3.org/2001/XMLSchema-instance");
  printer.PushAttribute("xsi:type", "CanonicalUser");

  printer.AddElement("Type", "CanonicalUser");

  printer.CloseElement();

  printer.AddElement("Permission", "FULL_CONTROL");

  printer.CloseElement();

  printer.CloseElement();

  printer.CloseElement();

  return req.S3Response(200, {{"Content-Type", "application/xml"}},
                        printer.CStr(), printer.CStrSize() - 1);
}

}  // namespace S3