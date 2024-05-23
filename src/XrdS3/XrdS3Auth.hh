//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Mano Segransan / CERN EOS Project <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#pragma once

//------------------------------------------------------------------------------
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>
#include <sys/types.h>
#include <pwd.h>
//------------------------------------------------------------------------------
#include "XrdS3ErrorResponse.hh"
#include "XrdS3Action.hh"
#include "XrdS3Crypt.hh"
#include "XrdS3Req.hh"

//------------------------------------------------------------------------------
// S3Auth
//------------------------------------------------------------------------------
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
    uid_t uid;
    gid_t gid;

    void resolve() {
      // translate username
      struct passwd *pwd = getpwnam(id.c_str());
      if (pwd == nullptr) {
	uid=99;
	gid=99;
      } else {
	uid = pwd->pw_uid;
	gid = pwd->pw_gid;
      }
    }
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


