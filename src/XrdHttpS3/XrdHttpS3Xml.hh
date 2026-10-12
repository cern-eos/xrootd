#ifndef XRDHTTPS3_XML_HH
#define XRDHTTPS3_XML_HH

#include <map>
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
std::string AclXml(const std::string &owner, const std::string &canned = "private",
                    const std::map<std::string, std::string> &grants = {});

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
std::string ListObjectsV1Xml(const std::string &bucket, const std::string &prefix,
                             const std::string &delimiter, const std::string &marker,
                             int maxKeys, const std::vector<ListedObject> &page,
                             bool truncated);

std::string CopyObjectXml(const std::string &etag, time_t modified);
std::string DeleteObjectsXml(const std::vector<std::string> &deleted,
                             const std::vector<std::string> &errors,
                             const std::vector<std::string> &errorCodes,
                             const std::vector<std::string> &errorMessages);
std::string CreateMultipartXml(const std::string &bucket, const std::string &key,
                               const std::string &uploadId);
std::string CompleteMultipartXml(const std::string &bucket, const std::string &key,
                                 const std::string &etag);

struct ListedPart {
  int number{0};
  long long size{0};
  time_t mtime{0};
  std::string etag;
};

std::string ListPartsXml(const std::string &bucket, const std::string &key,
                         const std::string &uploadId, int maxParts, int marker,
                         const std::vector<ListedPart> &parts, bool truncated);

struct ListedUpload {
  std::string key;
  std::string uploadId;
  time_t initiated{0};
};

std::string ListUploadsXml(const std::string &bucket, const std::string &prefix,
                           int maxUploads, const std::vector<ListedUpload> &uploads,
                           bool truncated);

std::string LocationXml(const std::string &region);

}

#endif
