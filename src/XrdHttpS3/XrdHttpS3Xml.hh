#ifndef XRDHTTPS3_XML_HH
#define XRDHTTPS3_XML_HH

#include <string>
#include <time.h>
#include <vector>

namespace XrdHttpS3 {

std::string XmlEscape(const std::string &s);
std::string HttpDate(time_t t);
std::string IsoDate(time_t t);
std::string RequestId();

std::string ErrorXml(const std::string &code, const std::string &message,
                     const std::string &resource);

struct ListedBucket {
  std::string name;
  time_t created{0};
};

std::string ListBucketsXml(const std::string &owner, const std::vector<ListedBucket> &buckets);

struct ListedObject {
  bool common{false};
  std::string key;
  long long size{0};
  time_t mtime{0};
  std::string etag;
};

std::string ListObjectsXml(const std::string &bucket, const std::string &prefix,
                           const std::string &delimiter, int maxKeys,
                           const std::vector<ListedObject> &page, bool truncated,
                           const std::string &token);

}

#endif
