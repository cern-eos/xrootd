#ifndef XRDHTTPS3_SIGV4_HH
#define XRDHTTPS3_SIGV4_HH

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace XrdHttpS3 {

std::string AwsUriEncode(const std::string &in, bool encodeSlash);
std::string PercentDecode(const std::string &in);
std::string HexSha256(const std::string &data);
std::string HexMd5(const std::string &data);

std::map<std::string, std::string> ParseQuery(const std::string &raw);
std::string CanonicalQuery(const std::string &raw);
// Same encoding as CanonicalQuery, omitting one decoded parameter name.
std::string CanonicalQueryExcept(const std::string &raw, const std::string &drop);
std::string CanonicalUri(const std::string &resource);

struct ParsedAuth {
  bool ok{false};
  std::string accessKey;
  std::string date;
  std::string region;
  std::string service;
  std::string signedHeaders;
  std::string signature;
};

ParsedAuth ParseAuthorization(const std::string &header);

std::string CanonicalRequest(const std::string &method, const std::string &uri,
                             const std::string &query,
                             const std::vector<std::pair<std::string, std::string>> &headers,
                             const std::string &signedHeaders,
                             const std::string &payloadHash);

std::string SignatureHex(const std::string &secret, const std::string &amzDate,
                         const std::string &region, const std::string &service,
                         const std::string &canonical);

enum class AuthFail {
  Ok = 0,
  Malformed,
  Skew,
  Signature,
  MissingHash,
  Expired
};

struct VerifyInput {
  std::string method;
  std::string resource;
  std::string query;
  std::map<std::string, std::string> headers;
  std::string authorization;
  std::string region;
  int skew{900};
  long long now{0};
};

AuthFail VerifySigV4(const VerifyInput &in, const std::string &secret,
                     std::string &detail);

// Query-string SigV4. On success, auth carries the access key and signature.
AuthFail VerifyPresign(const VerifyInput &in, const std::string &secret,
                       ParsedAuth &auth, std::string &detail);

// Chunk string-to-sign for STREAMING-AWS4-HMAC-SHA256-PAYLOAD.
std::string ChunkSignature(const std::string &secret, const std::string &amzDate,
                           const std::string &region, const std::string &previous,
                           const std::string &chunk);

struct SignInput {
  std::string method;
  std::string uri;
  std::string query;
  std::string host;
  std::string amzDate;
  std::string payloadHash;
  std::string secret;
  std::string accessKey;
  std::string region;
  std::vector<std::pair<std::string, std::string>> extra;
};

struct SignOutput {
  std::string authorization;
  std::string signature;
  std::string canonical;
  std::string signedHeaders;
  // SignQuery stores the encoded query string here, including the signature.
  std::string query;
};

SignOutput SignHeader(const SignInput &in);

// Presigned query. in.amzDate and in.payloadHash are used; payloadHash empty
// means UNSIGNED-PAYLOAD. expires is seconds, 1..604800.
SignOutput SignQuery(const SignInput &in, long long expires);

bool ConstantTimeEqual(const std::string &a, const std::string &b);

}

#endif
