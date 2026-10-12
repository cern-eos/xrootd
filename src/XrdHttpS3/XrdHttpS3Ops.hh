#ifndef XRDHTTPS3_OPS_HH
#define XRDHTTPS3_OPS_HH

#include "XrdHttpS3Meta.hh"

#include <string>

class XrdHttpExtReq;
class XrdSysError;

namespace XrdHttpS3 {

class Ops {
public:
  Ops(Meta &meta, const Config &cfg, XrdSysError &log);

  int Fail(XrdHttpExtReq &req, int http, const char *code, const std::string &message);
  int Dispatch(XrdHttpExtReq &req, const KeyRecord &key);

private:
  int ListBuckets(XrdHttpExtReq &req, const KeyRecord &key);
  int CreateBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket);
  int DeleteBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket);
  int HeadBucket(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket);
  int ListObjects(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket);
  int PutObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                const std::string &objKey);
  int GetObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                const std::string &objKey, bool head);
  int DeleteObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                   const std::string &objKey);
  int CopyObject(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                 const std::string &objKey);
  int DeleteObjects(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket);
  int CreateMultipart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                      const std::string &objKey);
  int UploadPart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                 const std::string &objKey);
  int UploadPartCopy(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                     const std::string &objKey);
  int CompleteMultipart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                        const std::string &objKey);
  int AbortMultipart(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                     const std::string &objKey);
  int ListParts(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                const std::string &objKey);
  int ListMultipartUploads(XrdHttpExtReq &req, const KeyRecord &key,
                           const std::string &bucket);
  int GetBucketLocation(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket);
  // 1 on success. 0 or -1 when a response was already sent.
  int WriteObjectBytes(XrdHttpExtReq &req, const KeyRecord &key, const std::string &path,
                       const XrdSecEntity *who, std::string &etag);

  bool Owned(const KeyRecord &key, const BucketRecord &bucket) const;
  int LoadOwned(XrdHttpExtReq &req, const KeyRecord &key, const std::string &bucket,
                BucketRecord &out);

  Meta &m_meta;
  const Config &m_cfg;
  XrdSysError &m_log;
};

}

#endif
