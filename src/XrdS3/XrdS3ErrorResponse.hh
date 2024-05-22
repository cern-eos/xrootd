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

#include <map>
#include <string>
#include <utility>

namespace S3 {

//------------------------------------------------------------------------------
//! \brief S3 error code 
//------------------------------------------------------------------------------
struct S3ErrorCode {
  std::string code;
  std::string description;
  int httpCode;
};

//------------------------------------------------------------------------------
//! \brief S3 error enums
//------------------------------------------------------------------------------
enum class S3Error {
  None,
  AccessControlListNotSupported,
  AccessDenied,
  AccessPointAlreadyOwnedByYou,
  AccountProblem,
  AllAccessDisabled,
  AmbiguousGrantByEmailAddress,
  AuthorizationHeaderMalformed,
  BadDigest,
  BucketAlreadyExists,
  BucketAlreadyOwnedByYou,
  BucketNotEmpty,
  ClientTokenConflict,
  CredentialsNotSupported,
  CrossLocationLoggingProhibited,
  EntityTooSmall,
  EntityTooLarge,
  ExpiredToken,
  IllegalLocationConstraintException,
  IllegalVersioningConfigurationException,
  IncompleteBody,
  IncorrectNumberOfFilesInPostRequest,
  InlineDataTooLarge,
  InternalError,
  InvalidAccessKeyId,
  InvalidAccessPoint,
  InvalidAccessPointAliasError,
  InvalidAddressingHeader,
  InvalidArgument,
  InvalidBucketAclWithObjectOwnership,
  InvalidBucketName,
  InvalidBucketState,
  InvalidDigest,
  InvalidEncryptionAlgorithmError,
  InvalidLocationConstraint,
  InvalidObjectState,
  InvalidPart,
  InvalidPartOrder,
  InvalidPayer,
  InvalidPolicyDocument,
  InvalidRange,
  InvalidRequest,
  InvalidSecurity,
  InvalidSOAPRequest,
  InvalidStorageClass,
  InvalidTargetBucketForLogging,
  InvalidToken,
  InvalidURI,
  KeyTooLongError,
  MalformedACLError,
  MalformedPOSTRequest,
  MalformedXML,
  MaxMessageLengthExceeded,
  MaxPostPreDataLengthExceededError,
  MetadataTooLarge,
  MethodNotAllowed,
  MissingAttachment,
  MissingContentLength,
  MissingRequestBodyError,
  MissingSecurityElement,
  MissingSecurityHeader,
  NoLoggingStatusForKey,
  NoSuchBucket,
  NoSuchBucketPolicy,
  NoSuchCORSConfiguration,
  NoSuchKey,
  NoSuchLifecycleConfiguration,
  NoSuchMultiRegionAccessPoint,
  NoSuchWebsiteConfiguration,
  NoSuchTagSet,
  NoSuchUpload,
  NoSuchVersion,
  NotImplemented,
  NotModified,
  NotSignedUp,
  OwnershipControlsNotFoundError,
  OperationAborted,
  PermanentRedirect,
  PreconditionFailed,
  Redirect,
  RequestHeaderSectionTooLarge,
  RequestIsNotMultiPartContent,
  RequestTimeout,
  RequestTimeTooSkewed,
  RequestTorrentOfBucketError,
  RestoreAlreadyInProgress,
  ServerSideEncryptionConfigurationNotFoundError,
  ServiceUnavailable,
  SignatureDoesNotMatch,
  SlowDown,
  TemporaryRedirect,
  TokenRefreshRequired,
  TooManyAccessPoints,
  TooManyBuckets,
  TooManyMultiRegionAccessPointregionsError,
  TooManyMultiRegionAccessPoints,
  UnexpectedContent,
  UnresolvableGrantByEmailAddress,
  UserKeyMustBeSpecified,
  NoSuchAccessPoint,
  InvalidTag,
  MalformedPolicy,
  // S3 Error

  XAmzContentSHA256Mismatch,
  // Custom errors
  InvalidObjectName,
  ObjectExistAsDir,
  ObjectExistInObjectPath,
};

//------------------------------------------------------------------------------
//! \brief S3 error codes and descriptions
//------------------------------------------------------------------------------
const std::map<S3Error, S3ErrorCode> S3ErrorMap = {
    {S3Error::NotImplemented,
     {.code = "NotImplemented",
      .description = "Operation not implemented",
      .httpCode = 501}},
    {S3Error::MissingContentLength,
     {.code = "MissingContentLength",
      .description = "Request is missing content length",
      .httpCode = 411}},
    {S3Error::IncompleteBody,
     {.code = "IncompleteBody",
      .description = "Request has an incomplete body",
      .httpCode = 400}},
    {S3Error::InternalError,
     {.code = "InternalError",
      .description = "Server internal error",
      .httpCode = 500}},
    {S3Error::BucketNotEmpty,
     {.code = "BucketNotEmpty",
      .description = "Bucket is not empty",
      .httpCode = 409}},
    {S3Error::BadDigest,
     {.code = "BadDigest", .description = "Bad digest", .httpCode = 400}},
    {S3Error::AccessDenied,
     {.code = "AccessDenied", .description = "Access denied", .httpCode = 403}},
    {S3Error::InvalidDigest,
     {.code = "InvalidDigest",
      .description = "Invalid digest",
      .httpCode = 400}},
    {S3Error::InvalidRequest,
     {.code = "InvalidRequest",
      .description = "Request is invalid",
      .httpCode = 400}},
    {S3Error::BucketAlreadyOwnedByYou,
     {.code = "BucketAlreadyOwnedByYou",
      .description = "You already own this bucket",
      .httpCode = 409}},
    {S3Error::InvalidURI,
     {.code = "InvalidURI", .description = "URI is invalid", .httpCode = 400}},
    {S3Error::InvalidObjectName,
     {.code = "InvalidObjectName",
      .description = "Object name is not valid",
      .httpCode = 400}},
    {S3Error::ObjectExistAsDir,
     {.code = "ObjectExistAsDir",
      .description = "A directory already exist with this path",
      .httpCode = 400}},
    {S3Error::ObjectExistInObjectPath,
     {.code = "ObjectExistInObjectPath",
      .description = "An object already exist in the object path",
      .httpCode = 400}},
    {S3Error::NoSuchKey,
     {.code = "NoSuchKey",
      .description = "The specified key does not exist",
      .httpCode = 404}},
    {S3Error::InvalidBucketName,
     {.code = "InvalidBucketName",
      .description = "Bucket name is not valid",
      .httpCode = 400}},
    {S3Error::InvalidArgument,
     {.code = "InvalidArgument",
      .description = "An argument is invalid",
      .httpCode = 400}},
    {S3Error::NoSuchBucket,
     {.code = "NoSuchBucket",
      .description = "The specified bucket does not exist",
      .httpCode = 404}},
    {S3Error::OperationAborted,
     {.code = "OperationAborted",
      .description = "Operation was aborted",
      .httpCode = 404}},
    {S3Error::BucketAlreadyExists,
     {.code = "BucketAlreadyExists",
      .description = "This bucket already exists",
      .httpCode = 409}},
    {S3Error::MalformedXML,
     {.code = "MalformedXML", .description = "Malformed XML", .httpCode = 400}},
    {S3Error::PreconditionFailed,
     {.code = "PreconditionFailed",
      .description = "Precondition failed",
      .httpCode = 412}},
    {S3Error::NotModified,
     {.code = "NotModified", .description = "Not modified", .httpCode = 304}},
    {S3Error::SignatureDoesNotMatch,
     {.code = "SignatureDoesNotMatch",
      .description = "Signature does not match",
      .httpCode = 403}},
    {S3Error::InvalidAccessKeyId,
     {.code = "InvalidAccessKeyId",
      .description = "Access key id is invalid",
      .httpCode = 403}},
    {S3Error::NoSuchAccessPoint,
     {.code = "NoSuchAccessPoint",
      .description = "No such access point",
      .httpCode = 404}},
    {S3Error::XAmzContentSHA256Mismatch,
     {.code = "XAmzContentSHA256Mismatch",
      .description = "X-Amz-Content-Sha256 mismatch",
      .httpCode = 400}},
    {S3Error::NoSuchUpload,
     {.code = "NoSuchUpload",
      .description = "No such upload",
      .httpCode = 404}},
    {S3Error::InvalidPart,
     {.code = "InvalidPart",
      .description = "Part is invalid",
      .httpCode = 400}},
    {S3Error::InvalidPartOrder,
     {.code = "InvalidPartOrder",
      .description = "Part order is invalid",
      .httpCode = 400}},
    {S3Error::InvalidRange,
     {.code = "InvalidRange",
      .description = "Range is invalid",
      .httpCode = 416}},
    {S3Error::AccessControlListNotSupported,
     {.code = "AccessControlListNotSupported",
      .description = "",
      .httpCode = 400}},
};

}  // namespace S3
