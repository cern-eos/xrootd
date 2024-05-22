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

namespace S3 {

//------------------------------------------------------------------------------
//! Action to perform on S3
//------------------------------------------------------------------------------
enum class Action {
  Unknown,
  AbortMultipartUpload,
  CompleteMultipartUpload,
  CopyObject,
  CreateBucket,
  CreateMultipartUpload,
  DeleteBucket,
  DeleteBucketAnalyticsConfiguration,
  DeleteBucketCors,
  DeleteBucketEncryption,
  DeleteBucketIntelligentTieringConfiguration,
  DeleteBucketInventoryConfiguration,
  DeleteBucketLifecycle,
  DeleteBucketMetricsConfiguration,
  DeleteBucketOwnershipControls,
  DeleteBucketPolicy,
  DeleteBucketReplication,
  DeleteBucketTagging,
  DeleteBucketWebsite,
  DeleteObject,
  DeleteObjects,
  DeleteObjectTagging,
  DeletePublicAccessBlock,
  GetBucketAccelerateConfiguration,
  GetBucketAcl,
  GetBucketAnalyticsConfiguration,
  GetBucketCors,
  GetBucketEncryption,
  GetBucketIntelligentTieringConfiguration,
  GetBucketInventoryConfiguration,
  GetBucketLifecycleConfiguration,
  GetBucketLocation,
  GetBucketLogging,
  GetBucketMetricsConfiguration,
  GetBucketNotificationConfiguration,
  GetBucketOwnershipControls,
  GetBucketPolicy,
  GetBucketPolicyStatus,
  GetBucketReplication,
  GetBucketRequestPayment,
  GetBucketTagging,
  GetBucketVersioning,
  GetBucketWebsite,
  GetObject,
  GetObjectAcl,
  GetObjectAttributes,
  GetObjectLegalHold,
  GetObjectLockConfiguration,
  GetObjectRetention,
  GetObjectTagging,
  GetObjectTorrent,
  GetPublicAccessBlock,
  HeadBucket,
  HeadObject,
  ListBucketAnalyticsConfigurations,
  ListBucketIntelligentTieringConfigurations,
  ListBucketInventoryConfigurations,
  ListBucketMetricsConfigurations,
  ListBuckets,
  ListMultipartUploads,
  ListObjects,
  ListObjectsV2,
  ListObjectVersions,
  ListParts,
  PutBucketAccelerateConfiguration,
  PutBucketAcl,
  PutBucketAnalyticsConfiguration,
  PutBucketCors,
  PutBucketEncryption,
  PutBucketIntelligentTieringConfiguration,
  PutBucketInventoryConfiguration,
  PutBucketLifecycle,
  PutBucketLifecycleConfiguration,
  PutBucketLogging,
  PutBucketMetricsConfiguration,
  PutBucketNotification,
  PutBucketNotificationConfiguration,
  PutBucketOwnershipControls,
  PutBucketPolicy,
  PutBucketReplication,
  PutBucketRequestPayment,
  PutBucketTagging,
  PutBucketVersioning,
  PutBucketWebsite,
  PutObject,
  PutObjectAcl,
  PutObjectLegalHold,
  PutObjectLockConfiguration,
  PutObjectRetention,
  PutObjectTagging,
  PutPublicAccessBlock,
  RestoreObject,
  SelectObjectContent,
  UploadPart,
  UploadPartCopy,
  WriteGetObjectResponse,
};

}  // namespace S

