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
#include <functional>
#include <vector>
//------------------------------------------------------------------------------
#include "XrdHttp/XrdHttpChecksumHandler.hh"
#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdS3Api.hh"
#include "XrdS3Auth.hh"
#include "XrdS3Crypt.hh"
#include "XrdS3Router.hh"
#include "XrdS3Utils.hh"
#include "XrdS3Log.hh"
#include "XrdSys/XrdSysError.hh"
//------------------------------------------------------------------------------


namespace S3 {
//------------------------------------------------------------------------------
//! \brief S3Handler is a class that implements the XRootD HTTP extension
//! handler for S3.
//------------------------------------------------------------------------------
class S3Handler : public XrdHttpExtHandler {
 public:
  S3Handler(XrdSysError *log, const char *config, XrdOucEnv *myEnv);

  ~S3Handler() override;

  bool MatchesPath(const char *verb, const char *path) override;

  int ProcessReq(XrdHttpExtReq &req) override;

  // Abstract method in the base class, but does not seem to be used
  int Init(const char *cfgfile) override { return 0; }

  static S3Handler* sInstance;
  static S3Log* Logger() { return sInstance->GetLogger(); }

  S3Log* GetLogger() { return &mLog; }

  Context ctx;

 private:
  struct {
    std::string config_dir;
    std::string region;
    std::string service;
    std::string multipart_upload_dir;
    std::string trace;
  } mConfig;

  XrdSysError mErr;
  S3Log mLog;
  S3Api mApi;
  S3Router mRouter;

  bool ParseConfig(const char *config, XrdOucEnv &env);

  void ConfigureRouter();
};

}  // namespace S3
