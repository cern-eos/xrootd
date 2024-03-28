//
// Created by segransm on 11/9/23.
//

#ifndef XROOTD_XRDS3AUTH_HH
#define XROOTD_XRDS3AUTH_HH

#include <set>
#include <shared_mutex>

#include "XrdS3Action.hh"
#include "XrdS3Crypt.hh"
#include "XrdS3Req.hh"

namespace S3 {

enum class AuthType {
  Unknown,
  Anonymous,
  Presigned,
  PostPolicy,
  Signed,
  StreamingSigned,
  StreamingSignedTrailer,
  StreamingUnsignedTrailer,
};

const std::string STREAMING_SHA256_PAYLOAD =
    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";
const std::string STREAMING_SHA256_TRAILER =
    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER";
const std::string SHA256_PAYLOAD = "AWS4-HMAC-SHA256-PAYLOAD";
const std::string SHA256_TRAILER = "AWS4-HMAC-SHA256-TRAILER";
const std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
const std::string STREAMING_UNSIGNED_TRAILER =
    "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
const std::string AWS4_ALGORITHM = "AWS4-HMAC-SHA256";
const std::string X_AMZ_CONTENT_SHA256 = "x-amz-content-sha256";

class S3Auth {
 public:
  S3Auth() = default;

  explicit S3Auth(const std::filesystem::path &path, std::string region,
                  std::string service);

  ~S3Auth() = default;

  static AuthType GetRequestAuthType(const XrdS3Req &req);

  S3Error AuthenticateRequest(XrdS3Req &req);

  struct Owner {
    std::string id;
    std::string display_name;
  };

  struct Bucket {
    std::string name;
    Owner owner;
    std::filesystem::path path;
  };

  std::pair<S3Error, Bucket> AuthorizeRequest(const XrdS3Req &req,
                                              const Action &action,
                                              const std::string &bucket,
                                              const std::string &object);

  std::pair<S3Error, Bucket> ValidateRequest(XrdS3Req &req,
                                             const Action &action,
                                             const std::string &bucket,
                                             const std::string &object);
  S3Error VerifySigV4(XrdS3Req &req);
  static std::string GetCanonicalQueryString(
      S3Utils *utils, const std::map<std::string, std::string> &query_params);
  static std::tuple<std::string, std::string> GetCanonicalHeaders(
      const Headers &headers, const std::set<std::string> &signed_headers);

  struct SigV4 {
    std::string signature;
    std::set<std::string> signed_headers;
    struct Scope {
      std::string access_key;
      std::string date;
      std::string region;
      std::string service;
      std::string request;
    } credentials;
  };
  SigV4 ParseSigV4(const XrdS3Req &req);
  static std::string GetCanonicalRequestHash(
      const std::string &method, const std::string &canonical_uri,
      const std::string &canonical_query_string,
      const std::string &canonical_headers, const std::string &signed_headers,
      const std::string &hashed_payload);
  static std::string GetStringToSign(const std::string &algorithm,
                                     const struct tm &date,
                                     const std::string &canonical_request_hash,
                                     const SigV4::Scope &scope);
  static std::string GetSignature(const std::string &secret_key,
                                  const SigV4::Scope &scope,
                                  const std::string &string_to_sign);
  static sha256_digest GetSigningKey(const std::string &secret_key,
                                     const SigV4::Scope &scope);

  void DeleteBucketInfo(const Bucket &bucket);

  S3Error CreateBucketInfo(const Bucket &bucket);

 private:
  // Map of user access key id to access key secret and userid
  std::map<std::string, std::pair<std::string, std::string>> keyMap;

  std::filesystem::path bucketInfoPath;

  std::string region;
  std::string service;

  std::pair<S3Error, Bucket> GetBucket(const std::string &name) const;
};

}  // namespace S3

#endif  // XROOTD_XRDS3AUTH_HH
