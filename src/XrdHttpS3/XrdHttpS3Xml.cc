#include "XrdHttpS3Xml.hh"

#include <atomic>
#include <cstdio>
#include <ctime>
#include <sstream>

namespace XrdHttpS3 {

std::string XmlEscape(const std::string &s) {
  std::string out;
  out.reserve(s.size());
  for (unsigned char c : s) {
    switch (c) {
      case '&': out += "&amp;"; break;
      case '<': out += "&lt;"; break;
      case '>': out += "&gt;"; break;
      case '"': out += "&quot;"; break;
      case '\'': out += "&apos;"; break;
      default:
        if (c < 0x20 && c != '\t' && c != '\n' && c != '\r')
          break;
        out.push_back(static_cast<char>(c));
    }
  }
  return out;
}

std::string HttpDate(time_t t) {
  std::tm tm{};
  gmtime_r(&t, &tm);
  char buf[64];
  strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
  return buf;
}

std::string IsoDate(time_t t) {
  std::tm tm{};
  gmtime_r(&t, &tm);
  char buf[40];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S.000Z", &tm);
  return buf;
}

std::string RequestId() {
  static std::atomic<unsigned> seq{0};
  char buf[32];
  snprintf(buf, sizeof(buf), "%08lX%08X", static_cast<unsigned long>(time(nullptr)),
           ++seq);
  return buf;
}

std::string ErrorXml(const std::string &code, const std::string &message,
                     const std::string &resource) {
  return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>" + XmlEscape(code) +
         "</Code><Message>" + XmlEscape(message) + "</Message><Resource>" +
         XmlEscape(resource) + "</Resource><RequestId>" + RequestId() +
         "</RequestId></Error>\n";
}

std::string ListBucketsXml(const std::string &owner, const std::vector<ListedBucket> &buckets) {
  std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                    "<Owner><ID>" + XmlEscape(owner) + "</ID><DisplayName>" + XmlEscape(owner) +
                    "</DisplayName></Owner><Buckets>";
  for (const auto &b : buckets) {
    xml += "<Bucket><Name>" + XmlEscape(b.name) + "</Name><CreationDate>" +
           IsoDate(b.created) + "</CreationDate></Bucket>";
  }
  xml += "</Buckets></ListAllMyBucketsResult>\n";
  return xml;
}

namespace {

std::string GrantXml(const std::string &who, const std::string &permission) {
  std::string g = "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ";
  if (who.compare(0, 4, "uri=") == 0) {
    g += "xsi:type=\"Group\"><URI>" + XmlEscape(who.substr(4)) + "</URI></Grantee>";
  } else {
    std::string id = who.compare(0, 3, "id=") == 0 ? who.substr(3) : who;
    g += "xsi:type=\"CanonicalUser\"><ID>" + XmlEscape(id) + "</ID><DisplayName>" +
         XmlEscape(id) + "</DisplayName></Grantee>";
  }
  g += "<Permission>" + XmlEscape(permission) + "</Permission></Grant>";
  return g;
}

std::string PermName(const std::string &key) {
  if (key == "read")
    return "READ";
  if (key == "write")
    return "WRITE";
  if (key == "read-acp")
    return "READ_ACP";
  if (key == "write-acp")
    return "WRITE_ACP";
  if (key == "full-control")
    return "FULL_CONTROL";
  return "FULL_CONTROL";
}

}

std::string AclXml(const std::string &owner, const std::string &canned,
                   const std::map<std::string, std::string> &grants) {
  std::string id = XmlEscape(owner);
  std::string body;
  if (!grants.empty()) {
    for (const auto &kv : grants) {
      std::stringstream ss(kv.second);
      std::string item;
      while (std::getline(ss, item, ',')) {
        size_t a = 0;
        while (a < item.size() && item[a] == ' ')
          a++;
        if (a)
          item.erase(0, a);
        if (!item.empty())
          body += GrantXml(item, PermName(kv.first));
      }
    }
  } else {
    body = GrantXml("id=" + owner, "FULL_CONTROL");
    const std::string all = "uri=http://acs.amazonaws.com/groups/global/AllUsers";
    const std::string auth = "uri=http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
    if (canned == "public-read" || canned == "public-read-write")
      body += GrantXml(all, "READ");
    if (canned == "public-read-write")
      body += GrantXml(all, "WRITE");
    if (canned == "authenticated-read")
      body += GrantXml(auth, "READ");
  }
  return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
         "<AccessControlPolicy><Owner><ID>" + id + "</ID><DisplayName>" + id +
         "</DisplayName></Owner><AccessControlList>" + body +
         "</AccessControlList></AccessControlPolicy>\n";
}

std::string ListObjectsXml(const std::string &bucket, const std::string &prefix,
                           const std::string &delimiter, int maxKeys,
                           const std::vector<ListedObject> &page, bool truncated,
                           const std::string &token) {
  int keys = 0;
  std::string body;
  for (const auto &item : page) {
    keys++;
    if (item.common) {
      body += "<CommonPrefixes><Prefix>" + XmlEscape(item.key) + "</Prefix></CommonPrefixes>";
    } else {
      body += "<Contents><Key>" + XmlEscape(item.key) + "</Key><LastModified>" +
              IsoDate(item.mtime) + "</LastModified><ETag>&quot;" + XmlEscape(item.etag) +
              "&quot;</ETag><Size>" + std::to_string(item.size) +
              "</Size><StorageClass>STANDARD</StorageClass></Contents>";
    }
  }
  std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                    "<Name>" + XmlEscape(bucket) + "</Name><Prefix>" + XmlEscape(prefix) +
                    "</Prefix><KeyCount>" + std::to_string(keys) + "</KeyCount><MaxKeys>" +
                    std::to_string(maxKeys) + "</MaxKeys><IsTruncated>" +
                    (truncated ? "true" : "false") + "</IsTruncated>";
  if (!delimiter.empty())
    xml += "<Delimiter>" + XmlEscape(delimiter) + "</Delimiter>";
  if (!token.empty())
    xml += "<ContinuationToken>" + XmlEscape(token) + "</ContinuationToken>";
  if (truncated && !page.empty())
    xml += "<NextContinuationToken>" + XmlEscape(page.back().key) + "</NextContinuationToken>";
  xml += body;
  xml += "</ListBucketResult>\n";
  return xml;
}

std::string ListObjectsV1Xml(const std::string &bucket, const std::string &prefix,
                             const std::string &delimiter, const std::string &marker,
                             int maxKeys, const std::vector<ListedObject> &page,
                             bool truncated) {
  std::string body;
  for (const auto &item : page) {
    if (item.common) {
      body += "<CommonPrefixes><Prefix>" + XmlEscape(item.key) + "</Prefix></CommonPrefixes>";
    } else {
      body += "<Contents><Key>" + XmlEscape(item.key) + "</Key><LastModified>" +
              IsoDate(item.mtime) + "</LastModified><ETag>&quot;" + XmlEscape(item.etag) +
              "&quot;</ETag><Size>" + std::to_string(item.size) +
              "</Size><StorageClass>STANDARD</StorageClass></Contents>";
    }
  }
  std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                    "<Name>" + XmlEscape(bucket) + "</Name><Prefix>" + XmlEscape(prefix) +
                    "</Prefix><Marker>" + XmlEscape(marker) + "</Marker><MaxKeys>" +
                    std::to_string(maxKeys) + "</MaxKeys><IsTruncated>" +
                    (truncated ? "true" : "false") + "</IsTruncated>";
  if (!delimiter.empty())
    xml += "<Delimiter>" + XmlEscape(delimiter) + "</Delimiter>";
  if (truncated && !page.empty())
    xml += "<NextMarker>" + XmlEscape(page.back().key) + "</NextMarker>";
  xml += body;
  xml += "</ListBucketResult>\n";
  return xml;
}

std::string CopyObjectXml(const std::string &etag, time_t modified) {
  return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
         "<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
         "<LastModified>" + IsoDate(modified) + "</LastModified><ETag>&quot;" +
         XmlEscape(etag) + "&quot;</ETag></CopyObjectResult>\n";
}

std::string DeleteObjectsXml(const std::vector<std::string> &deleted,
                             const std::vector<std::string> &errors,
                             const std::vector<std::string> &errorCodes,
                             const std::vector<std::string> &errorMessages) {
  std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
  for (const auto &key : deleted)
    xml += "<Deleted><Key>" + XmlEscape(key) + "</Key></Deleted>";
  for (size_t i = 0; i < errors.size(); i++) {
    xml += "<Error><Key>" + XmlEscape(errors[i]) + "</Key><Code>" +
           XmlEscape(i < errorCodes.size() ? errorCodes[i] : "InternalError") +
           "</Code><Message>" +
           XmlEscape(i < errorMessages.size() ? errorMessages[i] : "") +
           "</Message></Error>";
  }
  xml += "</DeleteResult>\n";
  return xml;
}

std::string CreateMultipartXml(const std::string &bucket, const std::string &key,
                               const std::string &uploadId) {
  return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
         "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
         "<Bucket>" + XmlEscape(bucket) + "</Bucket><Key>" + XmlEscape(key) +
         "</Key><UploadId>" + XmlEscape(uploadId) + "</UploadId>"
         "</InitiateMultipartUploadResult>\n";
}

std::string CompleteMultipartXml(const std::string &bucket, const std::string &key,
                                 const std::string &etag) {
  return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
         "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
         "<Location>/" + XmlEscape(bucket) + "/" + XmlEscape(key) + "</Location><Bucket>" +
         XmlEscape(bucket) + "</Bucket><Key>" + XmlEscape(key) + "</Key><ETag>&quot;" +
         XmlEscape(etag) + "&quot;</ETag></CompleteMultipartUploadResult>\n";
}

std::string ListPartsXml(const std::string &bucket, const std::string &key,
                         const std::string &uploadId, int maxParts, int marker,
                         const std::vector<ListedPart> &parts, bool truncated) {
  std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    "<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                    "<Bucket>" + XmlEscape(bucket) + "</Bucket><Key>" + XmlEscape(key) +
                    "</Key><UploadId>" + XmlEscape(uploadId) + "</UploadId><PartNumberMarker>" +
                    std::to_string(marker) + "</PartNumberMarker><MaxParts>" +
                    std::to_string(maxParts) + "</MaxParts><IsTruncated>" +
                    (truncated ? "true" : "false") + "</IsTruncated>";
  if (truncated && !parts.empty())
    xml += "<NextPartNumberMarker>" + std::to_string(parts.back().number) +
           "</NextPartNumberMarker>";
  for (const auto &part : parts) {
    xml += "<Part><PartNumber>" + std::to_string(part.number) +
           "</PartNumber><LastModified>" + IsoDate(part.mtime) + "</LastModified><ETag>&quot;" +
           XmlEscape(part.etag) + "&quot;</ETag><Size>" + std::to_string(part.size) +
           "</Size></Part>";
  }
  xml += "</ListPartsResult>\n";
  return xml;
}

std::string ListUploadsXml(const std::string &bucket, const std::string &prefix,
                           int maxUploads, const std::vector<ListedUpload> &uploads,
                           bool truncated) {
  std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    "<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                    "<Bucket>" + XmlEscape(bucket) + "</Bucket><Prefix>" + XmlEscape(prefix) +
                    "</Prefix><MaxUploads>" + std::to_string(maxUploads) +
                    "</MaxUploads><IsTruncated>" + (truncated ? "true" : "false") +
                    "</IsTruncated>";
  for (const auto &up : uploads) {
    xml += "<Upload><Key>" + XmlEscape(up.key) + "</Key><UploadId>" + XmlEscape(up.uploadId) +
           "</UploadId><Initiated>" + IsoDate(up.initiated) + "</Initiated></Upload>";
  }
  xml += "</ListMultipartUploadsResult>\n";
  return xml;
}

std::string LocationXml(const std::string &region) {
  std::string body = region == "us-east-1" ? std::string() : XmlEscape(region);
  return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
         "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
         body + "</LocationConstraint>\n";
}

}
