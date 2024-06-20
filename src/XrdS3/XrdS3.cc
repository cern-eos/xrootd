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

//------------------------------------------------------------------------------
#include "XrdS3.hh"
//------------------------------------------------------------------------------
#include <fcntl.h>

#include <algorithm>
#include <sstream>
//------------------------------------------------------------------------------
#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdS3ErrorResponse.hh"
#include "XrdS3Log.hh"
#include "XrdS3ScopedFsId.hh"
#include "XrdVersion.hh"
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
//! XRootD S3 plug-in implementation
//------------------------------------------------------------------------------

namespace S3 {
XrdVERSIONINFO(XrdHttpGetExtHandler, HttpS3);
S3Handler *S3Handler::sInstance =
    nullptr;  // for convenience to get access to our logger
//------------------------------------------------------------------------------
//! This is the default handler for requests that are not handled by the
//! router. It returns a 404 error.
//------------------------------------------------------------------------------
int NotFoundHandler(XrdS3Req &req) {
  return req.S3ErrorResponse(S3Error::NoSuchAccessPoint);
}

//------------------------------------------------------------------------------
//! S3Handler constructor.
//!
//! \param log The logger to use.
//! \param config The configuration string.
//! \param myEnv The environment to use.
//!
//------------------------------------------------------------------------------
S3Handler::S3Handler(XrdSysError *log, const char *config, XrdOucEnv *myEnv)
    : mErr(log->logger(), "S3_"), mApi(), mRouter(NotFoundHandler) {
  S3Handler::sInstance = this;
  mLog.Init(&mErr);

  S3::ScopedFsId::Validate();  // verify we can switch filesystem IDs !

  if (!ParseConfig(config, *myEnv)) {
    throw std::runtime_error("Failed to configure the HTTP S3 handler.");
  }

  ctx.log = &mErr;

  mApi = S3Api(mConfig.config_dir, mConfig.region, mConfig.service,
               mConfig.multipart_upload_dir);

  ConfigureRouter();

  mLog.Log(S3::ALL, "Handler", "finished configuring S3 Handler");
}

//------------------------------------------------------------------------------
//! S3Handler ConfigureRouter.
//!
//! \param log The logger to use.
//! \param config The configuration string.
//! \param myEnv The environment to use.
//!
//------------------------------------------------------------------------------
void S3Handler::ConfigureRouter() {
#define HANDLER(f) #f, [this](XrdS3Req &req) { return mApi.f##Handler(req); }
  // The router needs to be initialized in the right order, with the most
  // restrictive matcher first.

  /* -------------------------------------------------------------------------*/
  /*                                   HEAD                                   */
  /* -------------------------------------------------------------------------*/

  // HeadObject
  mRouter.AddRoute(S3Route(HANDLER(HeadObject))
                       .Method(HttpMethod::Head)
                       .Path(PathMatch::MatchObject));
  // HeadBucket
  mRouter.AddRoute(S3Route(HANDLER(HeadBucket))
                       .Method(HttpMethod::Head)
                       .Path(PathMatch::MatchBucket));

  /* -------------------------------------------------------------------------*/
  /*                                   GET                                    */
  /* -------------------------------------------------------------------------*/

  /*
   * MatchObject
   * */

  mRouter.AddRoute(S3Route(HANDLER(GetObjectAcl))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"acl", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetObjectAttributes))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"attributes", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetObjectLegalHold))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"legal-hold", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetObjectLockConfiguration))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"object-lock", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetObjectRetention))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"retention", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetObjectTagging))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"tagging", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetObjectTorrent))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"torrent", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(ListParts))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"uploadId", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(GetObject))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchObject));

  /*
   * MatchBucket
   * */
  mRouter.AddRoute(S3Route(HANDLER(ListObjectsV2))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"list-type", "2"}}));
  mRouter.AddRoute(S3Route(HANDLER(ListObjectVersions))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"versions", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketAccelerateConfiguration))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"accelerate", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketAcl))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"acl", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketAnalyticsConfiguration))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"analytics", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(ListBucketAnalyticsConfigurations))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"analytics", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketCors))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"cors", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketEncryption))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"encryption", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketIntelligentTieringConfiguration))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"intelligent-tiering", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(ListBucketIntelligentTieringConfigurations))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"inteligent-tiering", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketInventoryConfiguration))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"inventory", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(ListBucketInventoryConfigurations))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"inventory", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketLifecycleConfiguration))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"lifecycle", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketLocation))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"location", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketLogging))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"logging", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketMetricsConfiguration))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"metrics", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(ListBucketMetricsConfigurations))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"metrics", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketNotificationConfiguration))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"notification", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketOwnershipControls))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"ownershipControls", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketPolicy))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"policy", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketPolicyStatus))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"policyStatus", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketReplication))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"replication", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketRequestPayment))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"requestPayment", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketTagging))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"tagging", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketVersioning))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"versioning", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetBucketWebsite))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"website", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(GetPublicAccessBlock))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"publicAccessBlock", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(ListMultipartUploads))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"uploads", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(ListObjects))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchBucket));

  /*
   * MatchNoBucket
   * */
  mRouter.AddRoute(S3Route(HANDLER(ListBuckets))
                       .Method(HttpMethod::Get)
                       .Path(PathMatch::MatchNoBucket));

  /* -------------------------------------------------------------------------*/
  /*                                   PUT                                    */
  /* -------------------------------------------------------------------------*/

  /*
   * MatchObject
   * */

  mRouter.AddRoute(S3Route(HANDLER(PutObjectAcl))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"acl", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutObjectLegalHold))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"legal-hold", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutObjectLockConfiguration))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"object-lock", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutObjectRetention))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"retention", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutObjectTagging))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"tagging", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(UploadPartCopy))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"partNumber", "+"}, {"uploadId", "+"}})
                       .Headers({{"x-amz-copy-source", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(UploadPart))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"partNumber", "+"}, {"uploadId", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(CopyObject))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject)
                       .Headers({{"x-amz-copy-source", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(PutObject))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchObject));

  /*
   * MatchBucket
   * */
  mRouter.AddRoute(S3Route(HANDLER(PutBucketAccelerateConfiguration))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"accelerate", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketAcl))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"acl", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketAnalyticsConfiguration))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"analytics", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketCors))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"cors", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketEncryption))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"encryption", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketIntelligentTieringConfiguration))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"intelligent-tiering", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketInventoryConfiguration))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"inventory", ""}, {"id", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketLifecycleConfiguration))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"lifecycle", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketLogging))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"logging", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketMetricsConfiguration))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"metrics", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketNotificationConfiguration))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"notification", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketOwnershipControls))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"ownershipControls", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketPolicy))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"policy", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketReplication))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"replication", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketRequestPayment))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"requestPayment", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketTagging))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"tagging", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketVersioning))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"versioning", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutBucketWebsite))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"website", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(PutPublicAccessBlock))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"publicAccessBlock", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(CreateBucket))
                       .Method(HttpMethod::Put)
                       .Path(PathMatch::MatchBucket));

  /* -------------------------------------------------------------------------*/
  /*                                  DELETE                                  */
  /* -------------------------------------------------------------------------*/

  /*
   * MatchObject
   * */
  mRouter.AddRoute(S3Route(HANDLER(AbortMultipartUpload))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"uploadId", "+"}}));

  mRouter.AddRoute(S3Route(HANDLER(DeleteObjectTagging))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"tagging", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteObject))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchObject));

  /*
   * MatchBucket
   * */
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketAnalyticsConfiguration))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"analytics", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketCors))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"cors", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketEncryption))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"encryption", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketIntelligentTieringConfiguration))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"intelligent-tiering", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketInventoryConfiguration))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"inventory", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketLifecycle))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"lifecycle", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketMetricsConfiguration))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"metrics", ""}, {"id", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketOwnershipControls))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"ownershipControls", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketPolicy))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"policy", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketReplication))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"replication", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketTagging))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"tagging", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucketWebsite))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"website", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeletePublicAccessBlock))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"publicAcccessBlock", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteBucket))
                       .Method(HttpMethod::Delete)
                       .Path(PathMatch::MatchBucket));

  /* -------------------------------------------------------------------------*/
  /*                                   POST                                   */
  /* -------------------------------------------------------------------------*/
  mRouter.AddRoute(S3Route(HANDLER(CreateMultipartUpload))
                       .Method(HttpMethod::Post)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"uploads", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(RestoreObject))
                       .Method(HttpMethod::Post)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"restore", ""}}));
  mRouter.AddRoute(S3Route(HANDLER(SelectObjectContent))
                       .Method(HttpMethod::Post)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"select", ""}, {"select-type", "2"}}));

  mRouter.AddRoute(S3Route(HANDLER(CompleteMultipartUpload))
                       .Method(HttpMethod::Post)
                       .Path(PathMatch::MatchObject)
                       .Queries({{"uploadId", "+"}}));
  mRouter.AddRoute(S3Route(HANDLER(DeleteObjects))
                       .Method(HttpMethod::Post)
                       .Path(PathMatch::MatchBucket)
                       .Queries({{"delete", ""}}));

#undef HANDLER
}

//------------------------------------------------------------------------------
//! Parse the configuration file and populate the environment
//!
//! @param config  The configuration file to parse
//! @param env     The environment to populate
//!
//! @return true if the configuration file was parsed successfully
//!
//------------------------------------------------------------------------------
bool S3Handler::ParseConfig(const char *config, XrdOucEnv &env) {
  XrdOucStream Config(&mErr, getenv("XRDINSTANCE"), &env, "=====> ");

  auto fd = open(config, O_RDONLY);

  if (fd < 0) {
    return false;
  }

  Config.Attach(fd);

  const char *val;

  mErr.setMsgMask(LogMask::ERROR |
                  LogMask::WARN);  // by default don't log so much

  while ((val = Config.GetMyFirstWord())) {
    if (!strcmp("s3.vmp", val)) {
      if (!(val = Config.GetWord())) {
	Config.Close();
	std::cerr << "error: s3.vmp value not defined" << std::endl;
      }
      mConfig.vmp = val;
      // export the XrdPosix VMP configuration
      setenv("XROOTD_VMP", mConfig.vmp.c_str(), 1);
    } else if (!strcmp("s3.config", val)) {
      if (!(val = Config.GetWord())) {
        Config.Close();
        std::cerr << "error: s3.config value not defined" << std::endl;
        return false;
      }
      mConfig.config_dir = val;
    } else if (!strcmp("s3.region", val)) {
      if (!(val = Config.GetWord())) {
        Config.Close();
        std::cerr << "error: s3.region value not defined" << std::endl;
        return false;
      }
      mConfig.region = val;
    } else if (!strcmp("s3.service", val)) {
      if (!(val = Config.GetWord())) {
        Config.Close();
        std::cerr << "error: s3.service value not defined" << std::endl;
        return false;
      }
      mConfig.service = val;
    } else if (!strcmp("s3.multipart", val)) {
      if (!(val = Config.GetWord())) {
        Config.Close();
        std::cerr << "error: s3.multipart value not defined" << std::endl;
        return false;
      }
      mConfig.multipart_upload_dir = val;
    } else if (!strcmp("s3.trace", val)) {
      if (!(val = Config.GetWord())) {
        Config.Close();
        std::cerr << "error: s3.trace value not defined" << std::endl;
        return false;
      }
      mConfig.trace = val;
      mErr.setMsgMask(0);
      if (!strcmp(val, "all")) {
        mErr.setMsgMask(LogMask::ALL);
      } else if (!strcmp(val, "error")) {
        mErr.setMsgMask(LogMask::ERROR);
      } else if (!strcmp(val, "warning")) {
        mErr.setMsgMask(LogMask::ERROR | LogMask::WARN);
      } else if (!strcmp(val, "info")) {
        mErr.setMsgMask(LogMask::ERROR | LogMask::WARN | LogMask::INFO);
      } else if (!strcmp(val, "debug")) {
        mErr.setMsgMask(LogMask::ERROR | LogMask::WARN | LogMask::INFO |
                        LogMask::DEBUG);
      } else if (!strcmp(val, "none")) {
        mErr.setMsgMask(0);
      } else {
        std::cerr << "error: s3.trace encountered an unknown directive: " << val
                  << std::endl;
        return false;
      }
    }
  }
  Config.Close();

  if (mConfig.config_dir.empty()) {
    std::cerr << "error: s3.config not defined in configuration file"
              << std::endl;
  }
  if (mConfig.service.empty()) {
    std::cerr << "error: s3.service not defined in configuration file"
              << std::endl;
  }
  if (mConfig.region.empty()) {
    std::cerr << "error: s3.region not defined in configuration file"
              << std::endl;
  }
  if (mConfig.multipart_upload_dir.empty()) {
    std::cerr << "error: s3.multipar not defined in configuration file"
              << std::endl;
  }

  return (!mConfig.config_dir.empty() && !mConfig.service.empty() &&
          !mConfig.region.empty() && !mConfig.multipart_upload_dir.empty());
}

S3Handler::~S3Handler() = default;

//------------------------------------------------------------------------------
//! Match the request against the handler
//!
//! @param verb  The HTTP verb
//! @param path  The HTTP path
//!
//! @return true if the request matches the handler
//!
//------------------------------------------------------------------------------
bool S3Handler::MatchesPath(const char *verb, const char *path) {
  // match all paths for now, as we do not have access to request headers here.
  return true;
}

int S3Handler::ProcessReq(XrdHttpExtReq &req) {
  XrdS3Req s3req(&ctx, req);

  if (!s3req.isValid()) {
    return s3req.S3ErrorResponse(S3Error::InvalidRequest);
  }

  return mRouter.ProcessReq(s3req);
}

extern "C" {

//------------------------------------------------------------------------------
//! Get external handler instance
//!
//! @param log    The system log
//! @param config The configuration file
//! @param parms  The parameters
//! @param myEnv  The environment
//!
//! @return external handler instance
//------------------------------------------------------------------------------
XrdHttpExtHandler *XrdHttpGetExtHandler(XrdSysError *log, const char *config,
                                        const char *parms, XrdOucEnv *myEnv) {
  return new S3Handler(log, config, myEnv);
}
}
};  // namespace S3
