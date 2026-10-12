#include "XrdHttpS3Xml.hh"

#include <atomic>
#include <cstdio>
#include <ctime>

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

}
