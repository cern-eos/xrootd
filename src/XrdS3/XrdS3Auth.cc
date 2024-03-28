//
// Created by segransm on 11/9/23.
//

#include "XrdS3Auth.hh"

#include <fcntl.h>
#include <sys/xattr.h>

#include <algorithm>
#include <utility>

#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucTUtils.hh"
#include "XrdOuc/XrdOucUtils.hh"
#include "XrdPosix/XrdPosixExtern.hh"

namespace S3 {

AuthType S3Auth::GetRequestAuthType(const XrdS3Req &req) {
  if (req.method == Put) {
    if (S3Utils::MapHasEntry(req.lowercase_headers, X_AMZ_CONTENT_SHA256,
                             STREAMING_SHA256_PAYLOAD)) {
      return AuthType::StreamingSigned;
    }
    if (S3Utils::MapHasEntry(req.lowercase_headers, X_AMZ_CONTENT_SHA256,
                             STREAMING_SHA256_TRAILER)) {
      return AuthType::StreamingSignedTrailer;
    }
    if (S3Utils::MapHasEntry(req.lowercase_headers, X_AMZ_CONTENT_SHA256,
                             STREAMING_UNSIGNED_TRAILER)) {
      return AuthType::StreamingUnsignedTrailer;
    }
  }

  if (S3Utils::MapEntryStartsWith(req.lowercase_headers, "authorization",
                                  AWS4_ALGORITHM)) {
    return AuthType::Signed;
  }
  if (S3Utils::MapHasEntry(req.query, "X-Amz-Algorithm", AWS4_ALGORITHM)) {
    return AuthType::Presigned;
  }
  // TODO: Detect other authentication types.
  return AuthType::Unknown;
}
S3Error S3Auth::AuthenticateRequest(XrdS3Req &req) {
  switch (GetRequestAuthType(req)) {
    case AuthType::Signed: {
      return VerifySigV4(req);
    }
    // TODO: Implement verification of other request type
    default: {
      return S3Error::NotImplemented;
    }
  }
}

/// See
/// <https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>
/// for documentation on the AWS SigV4 format.

/// Parse the `Credentials`, `SignedHeaders` and `Signature` in the
/// authorization header.
/// Header example: `AWS4-HMAC-SHA256
/// Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7`
S3Auth::SigV4 S3Auth::ParseSigV4(const XrdS3Req &req) {
  SigV4 sig;

  auto it = req.lowercase_headers.find("authorization");
  if (it == req.lowercase_headers.end()) {
    return {};
  }
  auto authorization = it->second;

  size_t loc = authorization.find(' ');
  if (loc == std::string::npos) {
    return {};
  }

  // Check that the authorization header starts with `AWS4-HMAC-SHA256`
  if (authorization.substr(0, loc) != AWS4_ALGORITHM) {
    return {};
  }

  authorization.erase(0, loc);

  std::vector<std::string> components;
  XrdOucTUtils::splitString(components, authorization, ",");

  if (components.size() != 3) {
    return {};
  }

  // Find each component, trimming its value
  for (const auto &component : components) {
    loc = component.find('=');
    if (loc == std::string::npos) {
      return {};
    }
    std::string key = component.substr(0, loc);
    XrdOucUtils::trim(key);
    std::string value = component.substr(loc + 1);

    if (key == "Credential") {
      std::vector<std::string> credentials;
      credentials.reserve(5);
      XrdOucTUtils::splitString(credentials, value, "/");

      if (credentials.size() < 5) {
        return {};
      }
      if (credentials[credentials.size() - 1] != "aws4_request") {
        return {};
      }
      if (credentials[credentials.size() - 2] != service) {
        return {};
      }
      if (credentials[credentials.size() - 3] != region) {
        return {};
      }

      sig.credentials.request = "aws4_request";
      sig.credentials.service = service;
      sig.credentials.region = region;
      sig.credentials.date = credentials[credentials.size() - 4];
      // access key can contain '/', reconstruct it back after split
      for (size_t i = 0; i < credentials.size() - 4; ++i) {
        sig.credentials.access_key += credentials[i];
      }
    } else if (key == "SignedHeaders") {
      std::vector<std::string> headers;
      headers.reserve(3);
      XrdOucTUtils::splitString(headers, value, ";");
      sig.signed_headers.insert(headers.begin(), headers.end());
    } else if (key == "Signature") {
      sig.signature = value;
    } else {
      return {};
    }
  }

  return sig;
}

std::string S3Auth::GetCanonicalRequestHash(
    const std::string &method, const std::string &canonical_uri,
    const std::string &canonical_query_string,
    const std::string &canonical_headers, const std::string &signed_headers,
    const std::string &hashed_payload) {
  const std::string canonical_request =
      S3Utils::stringJoin('\n', method, canonical_uri, canonical_query_string,
                          canonical_headers, signed_headers, hashed_payload);

  const auto hashed_request = S3Crypt::SHA256_OS(canonical_request);

  return S3Utils::HexEncode(hashed_request);
}

std::string S3Auth::GetStringToSign(const std::string &algorithm,
                                    const struct tm &date,
                                    const std::string &canonical_request_hash,
                                    const SigV4::Scope &scope) {
  const std::string scope_str =
      S3Utils::stringJoin('/', scope.date, scope.region, scope.service,
                          std::string("aws4_request"));

  std::string date_iso8601 = S3Utils::timestampToIso8016(&date);

  std::string string_to_sign =
      S3Utils::stringJoin('\n', algorithm, std::string(date_iso8601), scope_str,
                          canonical_request_hash);

  return string_to_sign;
}

sha256_digest S3Auth::GetSigningKey(const std::string &secret_key,
                                    const SigV4::Scope &scope) {
  std::string key = "AWS4" + secret_key;
  auto dateKey = S3Crypt::HMAC_SHA256(scope.date, key);

  auto dateRegionKey = S3Crypt::HMAC_SHA256(scope.region, dateKey);
  auto dateRegionServiceKey =
      S3Crypt::HMAC_SHA256(scope.service, dateRegionKey);
  return S3Crypt::HMAC_SHA256(std::string("aws4_request"),
                              dateRegionServiceKey);
}

std::string S3Auth::GetSignature(const std::string &secret_key,
                                 const SigV4::Scope &scope,
                                 const std::string &string_to_sign) {
  const auto signing_key = GetSigningKey(secret_key, scope);

  const auto digest = S3Crypt::HMAC_SHA256(string_to_sign, signing_key);

  return S3Utils::HexEncode(digest);
}
S3Error S3Auth::VerifySigV4(XrdS3Req &req) {
  auto sig = ParseSigV4(req);

  if (sig.credentials.access_key.empty()) {
    return S3Error::InvalidAccessKeyId;
  }

  Context *ctx = req.ctx;

  auto it = keyMap.find(sig.credentials.access_key);
  if (it == keyMap.end()) {
    return S3Error::InvalidAccessKeyId;
  }
  req.id = it->second.first;
  auto key = it->second.second;

  auto header_it = req.lowercase_headers.find(X_AMZ_CONTENT_SHA256);
  if (header_it == req.lowercase_headers.end()) {
    return S3Error::InvalidRequest;
  }

  // TODO: Compare this hash with the actual hash of the request body once we
  //  read it.
  auto hashed_payload = header_it->second;

  auto canonical_uri = ctx->utils.ObjectUriEncode(req.uri_path);
  auto canonical_query_string = GetCanonicalQueryString(&ctx->utils, req.query);

  std::string canonical_headers, signed_headers;
  std::tie(canonical_headers, signed_headers) =
      GetCanonicalHeaders(req.lowercase_headers, sig.signed_headers);

  auto canonical_request_hash = GetCanonicalRequestHash(
      HttpMethodMap.find(req.method)->second, canonical_uri,
      canonical_query_string, canonical_headers, signed_headers,
      hashed_payload);

  auto string_to_sign = GetStringToSign(
      AWS4_ALGORITHM, req.date, canonical_request_hash, sig.credentials);

  const auto signature = GetSignature(key, sig.credentials, string_to_sign);
  ctx->log->Emsg("VerifySignature", "calculated signature:", signature.c_str());
  ctx->log->Emsg("VerifySignature",
                 "  received signature:", sig.signature.c_str());

  if (signature == sig.signature) {
    return S3Error::None;
  }
  return S3Error::SignatureDoesNotMatch;
}

std::string S3Auth::GetCanonicalQueryString(
    S3Utils *utils, const std::map<std::string, std::string> &query_params) {
  std::vector<std::pair<std::string, std::string>> query_params_map;
  std::string canonical_query_string;
  query_params_map.reserve(query_params.size());

  for (const auto &p : query_params) {
    query_params_map.emplace_back(utils->UriEncode(p.first),
                                  utils->UriEncode(p.second));
  }
  std::sort(query_params_map.begin(), query_params_map.end());

  for (const auto &param : query_params_map) {
    canonical_query_string.append(param.first + "=" + param.second + '&');
  }
  if (!canonical_query_string.empty()) {
    canonical_query_string.pop_back();
  }

  return canonical_query_string;
}

std::tuple<std::string, std::string> S3Auth::GetCanonicalHeaders(
    const Headers &headers, const std::set<std::string> &signed_headers) {
  std::string canonical_headers, canonical_signed_headers;
  std::vector<std::pair<std::string, std::string>> canonical_headers_map;

  for (const auto &hd : headers) {
    if (signed_headers.count(hd.first)) {
      std::string value(hd.second);

      // TODO: If a header contains multiple values, separate them using commas.
      S3Utils::TrimAll(value);
      canonical_headers_map.emplace_back(hd.first, value);
    } else if (hd.first.substr(0, 6) == "x-amz-" ||
               hd.first == "content-type" || hd.first == "host") {
      // signed headers must include all x-amz-* headers, host and, if present,
      // content-type headers.
      return {};
    }
  }
  std::sort(canonical_headers_map.begin(), canonical_headers_map.end());

  for (const auto &hd : canonical_headers_map) {
    canonical_headers.append(hd.first + ':' + hd.second + '\n');
    canonical_signed_headers.append(hd.first + ';');
  }
  if (!signed_headers.empty()) {
    canonical_signed_headers.pop_back();
  }

  return std::make_tuple(canonical_headers, canonical_signed_headers);
}

std::pair<S3Error, S3Auth::Bucket> S3Auth::ValidateRequest(
    XrdS3Req &req, const Action &action, const std::string &bucket,
    const std::string &object) {
  auto err = AuthenticateRequest(req);
  if (err != S3Error::None) {
    return {err, {}};
  }

  return AuthorizeRequest(req, action, bucket, object);
}

std::pair<S3Error, S3Auth::Bucket> S3Auth::GetBucket(
    const std::string &name) const {
  auto path = bucketInfoPath / name;

  Bucket b{};
  struct stat buf;
  if (XrdPosix_Stat(path.c_str(), &buf)) return {S3Error::NoSuchBucket, b};

  b.owner.id = S3Utils::GetXattr(path, "owner");
  if (b.owner.id.empty()) return {S3Error::InternalError, b};

  b.path = S3Utils::GetXattr(path, "path");
  if (b.path.empty()) return {S3Error::InternalError, b};

  b.name = name;

  return {S3Error::None, b};
}

std::pair<S3Error, S3Auth::Bucket> S3Auth::AuthorizeRequest(
    const XrdS3Req &req, const Action &action, const std::string &bucket_name,
    const std::string &object) {
  if (action == Action::ListBuckets) {
    return {S3Error::None, {}};
  }
  // TODO: head bucket might not need to be owner

  auto [err, bucket] = GetBucket(bucket_name);

  if (action == Action::CreateBucket) {
    if (err == S3Error::None) {
      if (bucket.owner.id == req.id) {
        return {S3Error::BucketAlreadyOwnedByYou, bucket};
      }
      return {S3Error::BucketAlreadyExists, bucket};
    }
    if (err == S3Error::NoSuchBucket) {
      return {S3Error::None, bucket};
    }
    return {err, bucket};
  }

  if (err != S3Error::None) {
    return {err, bucket};
  }

  // TODO: At the moment only the bucket owner can execute actions in the
  //  bucket, delegate this authorization to EOS/XrootD
  if (bucket.owner.id == req.id) {
    return {S3Error::None, bucket};
  }
  return {S3Error::AccessDenied, bucket};
}

void S3Auth::DeleteBucketInfo(const S3Auth::Bucket &bucket) {
  XrdPosix_Unlink((bucketInfoPath / bucket.name).c_str());
}

S3Error S3Auth::CreateBucketInfo(const S3Auth::Bucket &bucket) {
  auto path = bucketInfoPath / bucket.name;
  if (XrdPosix_Mkdir(path.c_str(), S_IRWXU | S_IRWXG)) {
    return S3Error::InternalError;
  }

  if (S3Utils::SetXattr(path, "path", bucket.path, XATTR_CREATE)) {
    XrdPosix_Rmdir(path.c_str());
    return S3Error::InternalError;
  }
  if (S3Utils::SetXattr(path, "owner", bucket.owner.id, XATTR_CREATE)) {
    XrdPosix_Rmdir(path.c_str());
    return S3Error::InternalError;
  }

  return S3Error::None;
}

/// Parse the auth data on startup, at the moment this data is never updated if
/// the on disk data changes.
S3Auth::S3Auth(const std::filesystem::path &path, std::string region,
               std::string service)
    : region(std::move(region)), service(std::move(service)) {
  auto keystore = path / "keystore";
  bucketInfoPath = path / "buckets";

  XrdPosix_Mkdir(keystore.c_str(), S_IRWXU | S_IRWXG);
  XrdPosix_Mkdir(bucketInfoPath.c_str(), S_IRWXU | S_IRWXG);

  auto dir = XrdPosix_Opendir(keystore.c_str());
  if (dir == nullptr) {
    std::string error = "Unable to open keystore: ";
    throw std::runtime_error(error + keystore.string());
  }

  dirent *entry = nullptr;
  while ((entry = XrdPosix_Readdir(dir)) != nullptr) {
    if (entry->d_name[0] == '.') {
      continue;
    }

    std::string access_key_id = entry->d_name;

    auto filepath = keystore / access_key_id;

    auto user_id = S3Utils::GetXattr(filepath, "user");
    if (user_id.empty()) {
      continue;
    }

    struct stat buff;
    if (XrdPosix_Stat(filepath.c_str(), &buff)) {
      continue;
    }

    auto fd = XrdPosix_Open(filepath.c_str(), O_RDONLY);
    if (fd <= 0) {
      continue;
    }

    std::string access_key_secret;
    access_key_secret.resize(buff.st_size);

    auto ret =
        XrdPosix_Read(fd, access_key_secret.data(), access_key_secret.size());
    if (!ret) {
      continue;
    }

    XrdPosix_Close(fd);
    keyMap.insert({access_key_id, {user_id, access_key_secret}});
  }

  XrdPosix_Closedir(dir);
}

}  // namespace S3
