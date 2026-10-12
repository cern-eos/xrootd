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

}
