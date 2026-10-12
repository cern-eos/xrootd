#include "XrdHttpS3Paths.hh"

#include <cctype>

namespace XrdHttpS3 {
namespace {

bool SegmentOk(const std::string &seg) {
  return !seg.empty() && seg != "." && seg != ".." && seg != ".s3" &&
         seg != ".s3folder";
}

}

bool ValidUser(const std::string &user) {
  if (user.empty() || user.size() > 128)
    return false;
  if (user == "." || user == ".." || user[0] == '.')
    return false;
  for (unsigned char c : user) {
    if (c == '/' || c == '\\' || c < 0x20)
      return false;
  }
  return true;
}

bool ValidAccessKey(const std::string &akid) {
  if (akid.size() < 16 || akid.size() > 128)
    return false;
  for (unsigned char c : akid) {
    if (!std::isalnum(c))
      return false;
  }
  return true;
}

bool ValidBucket(const std::string &bucket) {
  if (bucket.size() < 3 || bucket.size() > 63)
    return false;
  if (!std::isalnum(static_cast<unsigned char>(bucket.front())) ||
      !std::isalnum(static_cast<unsigned char>(bucket.back())))
    return false;
  int dots = 0;
  int numRun = 0;
  bool ipLike = true;
  bool prevDot = false;
  for (unsigned char c : bucket) {
    if (c == '.') {
      if (prevDot || numRun == 0)
        ipLike = false;
      prevDot = true;
      dots++;
      numRun = 0;
      continue;
    }
    prevDot = false;
    if (std::isdigit(c)) {
      numRun++;
      continue;
    }
    ipLike = false;
    if (!(std::islower(c) || c == '-'))
      return false;
  }
  if (ipLike && dots == 3)
    return false;
  return true;
}

bool ValidObjectKey(const std::string &key) {
  if (key.empty() || key.size() > 1024 || key[0] == '/')
    return false;
  std::string seg;
  for (size_t i = 0; i <= key.size(); i++) {
    if (i == key.size() || key[i] == '/') {
      if (seg.empty()) {
        if (i != key.size())
          return false;
      } else if (!SegmentOk(seg)) {
        return false;
      } else {
        for (unsigned char c : seg) {
          if (c < 0x20 || c == '\\')
            return false;
        }
      }
      seg.clear();
    } else {
      seg.push_back(key[i]);
    }
  }
  return true;
}

std::string JoinPath(const std::string &a, const std::string &b) {
  if (a.empty())
    return b;
  if (b.empty())
    return a;
  if (a.back() == '/')
    return a + b;
  return a + "/" + b;
}

std::string UserHome(const Config &cfg, const std::string &user) {
  std::string home = cfg.userprefix;
  if (cfg.userhash && !user.empty()) {
    char h = user[0];
    if (!(std::isalnum(static_cast<unsigned char>(h))))
      h = '_';
    home = JoinPath(home, std::string(1, h));
  }
  return JoinPath(home, user);
}

std::string UserMeta(const Config &cfg, const std::string &user) {
  return JoinPath(UserHome(cfg, user), ".s3");
}

std::string KeyIndex(const Config &cfg, const std::string &akid) {
  return JoinPath(JoinPath(cfg.sysmeta, "keys"), akid);
}

std::string BucketIndex(const Config &cfg, const std::string &bucket) {
  return JoinPath(JoinPath(cfg.sysmeta, "buckets"), bucket);
}

std::string UserKeyFile(const Config &cfg, const std::string &user,
                        const std::string &akid) {
  return JoinPath(JoinPath(UserMeta(cfg, user), "keys"), akid);
}

std::string UserBucketFile(const Config &cfg, const std::string &user,
                           const std::string &bucket) {
  return JoinPath(JoinPath(UserMeta(cfg, user), "buckets"), bucket);
}

std::string BucketData(const Config &cfg, const std::string &user,
                       const std::string &bucket) {
  return JoinPath(UserHome(cfg, user), bucket);
}

std::string ObjectFile(const Config &cfg, const std::string &user,
                       const std::string &bucket, const std::string &key) {
  std::string path = BucketData(cfg, user, bucket);
  if (!key.empty() && key.back() == '/')
    return JoinPath(JoinPath(path, key.substr(0, key.size() - 1)), ".s3folder");
  return JoinPath(path, key);
}

std::string ObjectRecord(const Config &cfg, const std::string &user,
                         const std::string &bucket, const std::string &key) {
  std::string rel = key;
  if (!rel.empty() && rel.back() == '/') {
    rel.pop_back();
    rel += ".s3dir";
  }
  return JoinPath(JoinPath(JoinPath(UserMeta(cfg, user), "objmeta"), bucket), rel);
}

std::string UploadDir(const Config &cfg, const std::string &user,
                      const std::string &uploadId) {
  return JoinPath(JoinPath(UserMeta(cfg, user), "uploads"), uploadId);
}

std::string UploadMeta(const Config &cfg, const std::string &user,
                       const std::string &uploadId) {
  return JoinPath(UploadDir(cfg, user, uploadId), "meta");
}

std::string UploadPartFile(const Config &cfg, const std::string &user,
                           const std::string &uploadId, const std::string &part) {
  return JoinPath(JoinPath(UploadDir(cfg, user, uploadId), "parts"), part);
}

}
