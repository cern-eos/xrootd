//
// Created by segransm on 11/9/23.
//

#include "XrdS3Auth.hh"

#include <fcntl.h>

#include <algorithm>

#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucTUtils.hh"
#include "XrdOuc/XrdOucUtils.hh"
#include "XrdS3ObjectStore.hh"

namespace S3 {

AuthType S3Auth::GetRequestAuthType(const XrdS3Req &req) {
  if (req.method == Put) {
    if (S3Utils::HeaderEq(req.lowercase_headers, X_AMZ_CONTENT_SHA256,
                          STREAMING_SHA256_PAYLOAD)) {
      return AuthType::StreamingSigned;
    }
    if (S3Utils::HeaderEq(req.lowercase_headers, X_AMZ_CONTENT_SHA256,
                          STREAMING_SHA256_TRAILER)) {
      return AuthType::StreamingSignedTrailer;
    }
    if (S3Utils::HeaderEq(req.lowercase_headers, X_AMZ_CONTENT_SHA256,
                          STREAMING_UNSIGNED_TRAILER)) {
      return AuthType::StreamingUnsignedTrailer;
    }
  }

  if (S3Utils::HeaderStartsWith(req.lowercase_headers, "authorization",
                                AWS4_ALGORITHM)) {
    return AuthType::Signed;
  }
  if (S3Utils::HeaderEq(req.query, "X-Amz-Algorithm", AWS4_ALGORITHM)) {
    return AuthType::Presigned;
  }
  // todo: parse other auth types
  return AuthType::Unknown;
}
S3Error S3Auth::AuthenticateRequest(XrdS3Req &req) {
  switch (GetRequestAuthType(req)) {
    case AuthType::Signed: {
      // todo: not hardcode
      return VerifySigV4(req, "us-east-1", "s3");
    }
    // todo:  invalidate all other requests for now
    default: {
      return S3Error::NotImplemented;
    }
  }
}

// todo: handle errors
S3Auth::SigV4 S3Auth::ParseSigV4(const XrdS3Req &req, const std::string &region,
                                 const std::string &service) {
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

  if (authorization.substr(0, loc) != AWS4_ALGORITHM) {
    return {};
  }

  authorization.erase(0, loc);

  std::vector<std::string> components;
  XrdOucTUtils::splitString(components, authorization, ",");

  if (components.size() != 3) {
    return {};
  }

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
      // todo: validate date

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
    Context *ctx, const std::string &method, const std::string &canonical_uri,
    const std::string &canonical_query_string,
    const std::string &canonical_headers, const std::string &signed_headers,
    const std::string &hashed_payload) {
  const std::string canonical_request =
      S3Utils::stringJoin('\n', method, canonical_uri, canonical_query_string,
                          canonical_headers, signed_headers, hashed_payload);

  fprintf(stderr, "Canonical request:\n%s\n", canonical_request.c_str());
  const auto hashed_request = ctx->crypt.mSha256.calculate(canonical_request);

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

sha256_digest S3Auth::GetSigningKey(Context *ctx, const std::string &secret_key,
                                    const SigV4::Scope &scope) {
  std::string key = "AWS4" + secret_key;
  auto dateKey = ctx->crypt.mHmac.calculate(scope.date, key);

  auto dateRegionKey = ctx->crypt.mHmac.calculate(scope.region, dateKey);
  auto dateRegionServiceKey =
      ctx->crypt.mHmac.calculate(scope.service, dateRegionKey);
  return ctx->crypt.mHmac.calculate(std::string("aws4_request"),
                                    dateRegionServiceKey);
}

std::string S3Auth::GetSignature(Context *ctx, const std::string &secret_key,
                                 const SigV4::Scope &scope,
                                 const std::string &string_to_sign) {
  const auto signing_key = GetSigningKey(ctx, secret_key, scope);

  const auto digest = ctx->crypt.mHmac.calculate(string_to_sign, signing_key);

  return S3Utils::HexEncode(digest);
}
S3Error S3Auth::VerifySigV4(XrdS3Req &req, const std::string &region,
                            const std::string &service) {
  fprintf(stderr, "Verifying sigv4...\n");
  auto sig = ParseSigV4(req, region, service);

  req.id = sig.credentials.access_key;
  if (req.id.empty()) {
    fprintf(stderr, "REQ ID IS EMPTY!\n");
    return S3Error::InvalidAccessKeyId;
  }
  Context *ctx = req.ctx;

  // todo: global store?
  if (keyMap.find(sig.credentials.access_key) == keyMap.end()) {
    fprintf(stderr, "KEY NOT IN STORE!\n");
    return S3Error::InvalidAccessKeyId;
  }
  auto key = keyMap.find(sig.credentials.access_key)->second;

  // todo: not segfault
  auto hashed_payload =
      req.lowercase_headers.find(X_AMZ_CONTENT_SHA256)->second;

  auto canonical_uri = ctx->utils.ObjectUriEncode(req.uri_path);
  auto canonical_query_string = GetCanonicalQueryString(ctx, req.query);

  std::string canonical_headers, signed_headers;
  std::tie(canonical_headers, signed_headers) =
      GetCanonicalHeaders(req.lowercase_headers, sig.signed_headers);

  // todo: compare hash on stream close if type == xs and body is not empty
  auto canonical_request_hash = GetCanonicalRequestHash(
      ctx, HttpMethodMap.find(req.method)->second, canonical_uri,
      canonical_query_string, canonical_headers, signed_headers,
      hashed_payload);
  // todo: limit to 7 days

  // todo: sanitize if we log

  // todo: not hardcode algorithm

  auto string_to_sign = GetStringToSign(
      AWS4_ALGORITHM, req.date, canonical_request_hash, sig.credentials);
  fprintf(stderr, "String to sign:\n%s\n", string_to_sign.c_str());

  const auto signature =
      GetSignature(ctx, key, sig.credentials, string_to_sign);
  ctx->log->Emsg("VerifySignature", "calculated signature:", signature.c_str());
  ctx->log->Emsg("VerifySignature",
                 "  received signature:", sig.signature.c_str());

  // todo: secure compare?
  if (signature == sig.signature) {
    return S3Error::None;
  }
  return S3Error::SignatureDoesNotMatch;
}

std::string S3Auth::GetCanonicalQueryString(
    Context *ctx, const std::map<std::string, std::string> &query_params) {
  std::vector<std::pair<std::string, std::string>> query_params_map;
  std::string canonical_query_string;
  query_params_map.reserve(query_params.size());

  for (const auto &p : query_params) {
    query_params_map.emplace_back(ctx->utils.UriEncode(p.first),
                                  ctx->utils.UriEncode(p.second));
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

      // todo: separate the values for a multi-value header using commas.
      S3Utils::TrimAll(value);
      canonical_headers_map.emplace_back(hd.first, value);
    } else if (hd.first.substr(0, 6) == "x-amz-" ||
               hd.first == "content-type" || hd.first == "host") {
      // signed headers must include all x-amz-* headers, host and content-type
      // (if present) headers.
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

// todo: do we need object here?
S3Error S3Auth::ValidateRequest(XrdS3Req &req, const S3ObjectStore &objectStore,
                                const Action &action, const std::string &bucket,
                                const std::string &object) {
  auto err = AuthenticateRequest(req);
  if (err != S3Error::None) {
    return err;
  }

  return AuthorizeRequest(req, objectStore, action, bucket, object);
}

S3Error S3Auth::AuthorizeRequest(const XrdS3Req &req,
                                 const S3ObjectStore &objectStore,
                                 const Action &action,
                                 const std::string &bucket,
                                 const std::string &object) {
  // todo:
  if (action == Action::ListBuckets || action == Action::CreateBucket) {
    return S3Error::None;
  }
  // todo: head bucket might not need to be owner

  auto owner = objectStore.GetBucketOwner(bucket);
  if (owner.empty()) {
    return S3Error::NoSuchBucket;
  }
  if (owner == req.id) {
    return S3Error::None;
  } else {
    return S3Error::AccessDenied;
  }
  // todo: iam instead of manually
}
S3Auth::S3Auth(const std::string &path) {
  XrdOucStream stream;

  auto fd =
      open((path + "/users").c_str(), O_RDONLY | O_CREAT, S_IREAD | S_IWRITE);

  if (fd < 0) {
    throw std::runtime_error("Unable to open auth file");
  }

  stream.Attach(fd);
  const char *line;
  while ((line = stream.GetLine())) {
    const std::string l(line);

    auto pos = l.find(':');
    const std::string id = l.substr(0, pos);
    const std::string key = l.substr(pos + 1);

    keyMap.insert({id, key});
  }
}

}  // namespace S3
