#include "XrdHttpS3Meta.hh"

#include "XrdHttpS3Fs.hh"
#include "XrdHttpS3Paths.hh"
#include "XrdSys/XrdSysError.hh"

#include <cerrno>
#include <cstdlib>
#include <memory>
#include <sstream>

namespace XrdHttpS3 {

std::map<std::string, std::string> ParseProps(const std::string &body) {
  std::map<std::string, std::string> out;
  size_t i = 0;
  while (i < body.size()) {
    size_t nl = body.find('\n', i);
    if (nl == std::string::npos)
      nl = body.size();
    std::string line = body.substr(i, nl - i);
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    i = nl + 1;
    if (line.empty() || line[0] == '#')
      continue;
    size_t eq = line.find('=');
    if (eq == std::string::npos || eq == 0)
      continue;
    out[line.substr(0, eq)] = line.substr(eq + 1);
  }
  return out;
}

std::string FormatProps(const std::map<std::string, std::string> &props) {
  std::ostringstream out;
  for (const auto &kv : props)
    out << kv.first << '=' << kv.second << '\n';
  return out.str();
}

Meta::Meta(XrdSfsFileSystem &sfs, const Config &cfg, XrdSysError &log)
    : m_cfg(cfg), m_log(log), m_fs(new Fs(sfs)), m_ent("s3") {
  if (!cfg.metaentity.empty()) {
    m_entName = cfg.metaentity;
    m_ent.name = &m_entName[0];
  }
}

Meta::~Meta() = default;

const XrdSecEntity *Meta::MetaWho() const {
  return m_cfg.metaentity.empty() ? nullptr : &m_ent;
}

int Meta::ReadProps(const std::string &path, const XrdSecEntity *who,
                    std::map<std::string, std::string> &props, int &err) {
  std::string body;
  if (m_fs->ReadAll(path, who, body, err) != 0)
    return -1;
  props = ParseProps(body);
  return 0;
}

int Meta::WriteProps(const std::string &path, const XrdSecEntity *who,
                     const std::map<std::string, std::string> &props, mode_t mode,
                     int &err) {
  return m_fs->WriteAll(path, who, FormatProps(props), mode, err);
}

int Meta::LoadKey(const std::string &akid, KeyRecord &out, int &err) {
  err = 0;
  if (!ValidAccessKey(akid)) {
    err = EINVAL;
    return -1;
  }
  if (m_cfg.metacache > 0) {
    std::lock_guard<std::mutex> lock(m_mu);
    auto it = m_keys.find(akid);
    if (it != m_keys.end() && std::chrono::steady_clock::now() < it->second.exp) {
      if (!it->second.keyOk) {
        err = ENOENT;
        return -1;
      }
      out = it->second.key;
      return 0;
    }
  }

  std::map<std::string, std::string> props;
  int rc = ReadProps(KeyIndex(m_cfg, akid), MetaWho(), props, err);
  Stamp stamp;
  stamp.exp = std::chrono::steady_clock::now() + std::chrono::seconds(m_cfg.metacache);
  if (rc != 0) {
    stamp.keyOk = false;
    if (m_cfg.metacache > 0 && err == ENOENT) {
      std::lock_guard<std::mutex> lock(m_mu);
      m_keys[akid] = stamp;
    }
    return -1;
  }
  out.accessKey = akid;
  out.secret = props["secret"];
  out.user = props["user"];
  out.status = props.count("status") ? props["status"] : "active";
  out.expires = props.count("expires") ? std::atoll(props["expires"].c_str()) : 0;
  if (out.secret.empty() || !ValidUser(out.user)) {
    err = EINVAL;
    m_log.Emsg("S3", "access key record is incomplete", akid.c_str());
    return -1;
  }
  stamp.keyOk = true;
  stamp.key = out;
  if (m_cfg.metacache > 0) {
    std::lock_guard<std::mutex> lock(m_mu);
    m_keys[akid] = stamp;
  }
  return 0;
}

int Meta::LoadBucket(const std::string &name, BucketRecord &out, int &err) {
  err = 0;
  if (!ValidBucket(name)) {
    err = EINVAL;
    return -1;
  }
  if (m_cfg.metacache > 0) {
    std::lock_guard<std::mutex> lock(m_mu);
    auto it = m_buckets.find(name);
    if (it != m_buckets.end() && std::chrono::steady_clock::now() < it->second.exp) {
      if (!it->second.bucketOk) {
        err = ENOENT;
        return -1;
      }
      out = it->second.bucket;
      return 0;
    }
  }
  std::map<std::string, std::string> props;
  int rc = ReadProps(BucketIndex(m_cfg, name), MetaWho(), props, err);
  Stamp stamp;
  stamp.exp = std::chrono::steady_clock::now() + std::chrono::seconds(m_cfg.metacache);
  if (rc != 0) {
    stamp.bucketOk = false;
    if (m_cfg.metacache > 0 && err == ENOENT) {
      std::lock_guard<std::mutex> lock(m_mu);
      m_buckets[name] = stamp;
    }
    return -1;
  }
  out.name = name;
  out.owner = props["owner"];
  out.created = props.count("created") ? std::atoll(props["created"].c_str()) : 0;
  out.acl = props.count("acl") && !props["acl"].empty() ? props["acl"] : "private";
  out.region = props.count("region") && !props["region"].empty() ? props["region"] : m_cfg.region;
  if (!ValidUser(out.owner)) {
    err = EINVAL;
    return -1;
  }
  stamp.bucketOk = true;
  stamp.bucket = out;
  if (m_cfg.metacache > 0) {
    std::lock_guard<std::mutex> lock(m_mu);
    m_buckets[name] = stamp;
  }
  return 0;
}

int Meta::StoreBucket(const BucketRecord &rec, int &err) {
  std::map<std::string, std::string> props;
  props["owner"] = rec.owner;
  props["created"] = std::to_string(rec.created);
  props["acl"] = rec.acl.empty() ? "private" : rec.acl;
  props["region"] = rec.region.empty() ? m_cfg.region : rec.region;
  if (WriteProps(BucketIndex(m_cfg, rec.name), MetaWho(), props, 0644, err) != 0)
    return -1;
  if (WriteProps(UserBucketFile(m_cfg, rec.owner, rec.name), MetaWho(), props, 0644, err) != 0)
    return -1;
  if (m_fs->Mkdir(BucketData(m_cfg, rec.owner, rec.name), MetaWho(), err) != 0)
    return -1;
  if (m_cfg.metacache > 0) {
    std::lock_guard<std::mutex> lock(m_mu);
    m_buckets.erase(rec.name);
  }
  err = 0;
  return 0;
}

int Meta::RemoveBucket(const std::string &name, const std::string &owner, int &err) {
  int uerr = 0;
  m_fs->Unlink(BucketIndex(m_cfg, name), MetaWho(), err);
  m_fs->Unlink(UserBucketFile(m_cfg, owner, name), MetaWho(), uerr);
  m_fs->Rmdir(JoinPath(JoinPath(UserMeta(m_cfg, owner), "objmeta"), name), MetaWho(), uerr);
  if (m_cfg.metacache > 0) {
    std::lock_guard<std::mutex> lock(m_mu);
    m_buckets.erase(name);
  }
  err = 0;
  return 0;
}

int Meta::ListUserBuckets(const std::string &user, std::vector<BucketRecord> &out, int &err) {
  out.clear();
  std::vector<std::string> names;
  std::string dir = JoinPath(UserMeta(m_cfg, user), "buckets");
  if (m_fs->List(dir, MetaWho(), names, err) != 0) {
    if (err == ENOENT) {
      err = 0;
      return 0;
    }
    return -1;
  }
  for (const std::string &name : names) {
    if (!ValidBucket(name))
      continue;
    BucketRecord rec;
    int berr = 0;
    if (LoadBucket(name, rec, berr) != 0)
      continue;
    if (rec.owner == user)
      out.push_back(rec);
  }
  err = 0;
  return 0;
}

int Meta::ReadObjectMeta(const std::string &user, const std::string &bucket,
                         const std::string &key, ObjectMeta &out) {
  out = ObjectMeta();
  int err = 0;
  std::map<std::string, std::string> props;
  if (ReadProps(ObjectRecord(m_cfg, user, bucket, key), MetaWho(), props, err) == 0) {
    if (!props["etag"].empty())
      out.etag = props["etag"];
    if (!props["content-type"].empty())
      out.contentType = props["content-type"];
    for (const auto &kv : props) {
      if (kv.first.compare(0, 5, "user.") == 0 && kv.first.size() > 5)
        out.user[kv.first.substr(5)] = kv.second;
    }
    return 0;
  }
  std::string etag;
  std::string ctype;
  if (m_fs->GetXattr(ObjectFile(m_cfg, user, bucket, key), MetaWho(), "user.s3.etag", etag,
                     err) == 0 && !etag.empty())
    out.etag = etag;
  if (m_fs->GetXattr(ObjectFile(m_cfg, user, bucket, key), MetaWho(), "user.s3.content-type",
                     ctype, err) == 0 && !ctype.empty())
    out.contentType = ctype;
  return out.etag.empty() ? -1 : 0;
}

int Meta::WriteObjectMeta(const std::string &user, const std::string &bucket,
                          const std::string &key, const ObjectMeta &meta, int &err) {
  std::map<std::string, std::string> props;
  props["etag"] = meta.etag;
  props["content-type"] = meta.contentType;
  for (const auto &kv : meta.user) {
    if (kv.second.find('\n') != std::string::npos || kv.second.find('\r') != std::string::npos)
      continue;
    props["user." + kv.first] = kv.second;
  }
  if (WriteProps(ObjectRecord(m_cfg, user, bucket, key), MetaWho(), props, 0644, err) != 0)
    return -1;
  int xerr = 0;
  m_fs->SetXattr(ObjectFile(m_cfg, user, bucket, key), MetaWho(), "user.s3.etag", meta.etag,
                 xerr);
  m_fs->SetXattr(ObjectFile(m_cfg, user, bucket, key), MetaWho(), "user.s3.content-type",
                 meta.contentType, xerr);
  err = 0;
  return 0;
}

int Meta::RemoveObjectMeta(const std::string &user, const std::string &bucket,
                           const std::string &key) {
  int err = 0;
  m_fs->Unlink(ObjectRecord(m_cfg, user, bucket, key), MetaWho(), err);
  return 0;
}

}
