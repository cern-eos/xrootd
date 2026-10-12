#ifndef XRDHTTPS3_PATHS_HH
#define XRDHTTPS3_PATHS_HH

#include "XrdHttpS3Config.hh"

#include <string>

namespace XrdHttpS3 {

bool ValidUser(const std::string &user);
bool ValidAccessKey(const std::string &akid);
bool ValidBucket(const std::string &bucket);
bool ValidObjectKey(const std::string &key);

std::string JoinPath(const std::string &a, const std::string &b);
std::string UserHome(const Config &cfg, const std::string &user);
std::string UserMeta(const Config &cfg, const std::string &user);
std::string KeyIndex(const Config &cfg, const std::string &akid);
std::string BucketIndex(const Config &cfg, const std::string &bucket);
std::string UserKeyFile(const Config &cfg, const std::string &user,
                        const std::string &akid);
std::string UserBucketFile(const Config &cfg, const std::string &user,
                           const std::string &bucket);
std::string ObjectFile(const Config &cfg, const std::string &user,
                       const std::string &bucket, const std::string &key);
std::string ObjectRecord(const Config &cfg, const std::string &user,
                         const std::string &bucket, const std::string &key);
std::string BucketData(const Config &cfg, const std::string &user,
                       const std::string &bucket);
std::string UploadDir(const Config &cfg, const std::string &user,
                      const std::string &uploadId);
std::string UploadMeta(const Config &cfg, const std::string &user,
                       const std::string &uploadId);
std::string UploadPartFile(const Config &cfg, const std::string &user,
                           const std::string &uploadId, const std::string &part);
std::string BucketDoc(const Config &cfg, const std::string &user, const std::string &bucket,
                      const std::string &kind);
std::string ObjectTags(const Config &cfg, const std::string &user, const std::string &bucket,
                       const std::string &key);

}

#endif
