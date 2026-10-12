#ifndef XRDHTTPS3_META_HH
#define XRDHTTPS3_META_HH

#include "XrdHttpS3Config.hh"
#include "XrdSec/XrdSecEntity.hh"

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

class XrdSfsFileSystem;
class XrdSysError;

namespace XrdHttpS3 {

class Fs;

struct KeyRecord {
  std::string accessKey;
  std::string secret;
  std::string user;
  std::string status{"active"};
  long long expires{0};
};

struct BucketRecord {
  std::string name;
  std::string owner;
  long long created{0};
  std::string acl{"private"};
  std::string region;
};

struct ObjectMeta {
  std::string etag;
  std::string contentType{"application/octet-stream"};
  std::map<std::string, std::string> user;
};

class Meta {
public:
  Meta(XrdSfsFileSystem &sfs, const Config &cfg, XrdSysError &log);
  ~Meta();

  const XrdSecEntity *MetaWho() const;
  Fs &Store() { return *m_fs; }

  int LoadKey(const std::string &akid, KeyRecord &out, int &err);
  int LoadBucket(const std::string &name, BucketRecord &out, int &err);
  int StoreBucket(const BucketRecord &rec, int &err);
  int RemoveBucket(const std::string &name, const std::string &owner, int &err);
  int ListUserBuckets(const std::string &user, std::vector<BucketRecord> &out, int &err);

  int ReadObjectMeta(const std::string &user, const std::string &bucket,
                     const std::string &key, ObjectMeta &out);
  int WriteObjectMeta(const std::string &user, const std::string &bucket,
                      const std::string &key, const ObjectMeta &meta, int &err);
  int RemoveObjectMeta(const std::string &user, const std::string &bucket,
                       const std::string &key);

private:
  int ReadProps(const std::string &path, const XrdSecEntity *who,
                std::map<std::string, std::string> &props, int &err);
  int WriteProps(const std::string &path, const XrdSecEntity *who,
                 const std::map<std::string, std::string> &props, mode_t mode, int &err);

  const Config &m_cfg;
  XrdSysError &m_log;
  std::unique_ptr<Fs> m_fs;
  XrdSecEntity m_ent;
  std::string m_entName;
  std::mutex m_mu;
  struct Stamp {
    std::chrono::steady_clock::time_point exp;
    KeyRecord key;
    bool keyOk{false};
    BucketRecord bucket;
    bool bucketOk{false};
  };
  std::map<std::string, Stamp> m_keys;
  std::map<std::string, Stamp> m_buckets;
};

std::map<std::string, std::string> ParseProps(const std::string &body);
std::string FormatProps(const std::map<std::string, std::string> &props);

}

#endif
