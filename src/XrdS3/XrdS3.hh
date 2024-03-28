
#ifndef XROOTD_XRDS3_HH
#define XROOTD_XRDS3_HH

#include <functional>
#include <vector>

#include "XrdHttp/XrdHttpChecksumHandler.hh"
#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdPosix/XrdPosixXrootd.hh"
#include "XrdS3Api.hh"
#include "XrdS3Auth.hh"
#include "XrdS3Crypt.hh"
#include "XrdS3Router.hh"
#include "XrdS3Utils.hh"
#include "XrdSys/XrdSysError.hh"

namespace S3 {

class S3Handler : public XrdHttpExtHandler {
 public:
  S3Handler(XrdSysError *log, const char *config, XrdOucEnv *myEnv);

  ~S3Handler() override;

  bool MatchesPath(const char *verb, const char *path) override;

  int ProcessReq(XrdHttpExtReq &req) override;

  // Abstract method in the base class, but does not seem to be used
  int Init(const char *cfgfile) override { return 0; }

 public:
  Context ctx;

 private:
  struct {
    std::string config_dir;
    std::string region;
    std::string service;
    std::string multipart_upload_dir;
  } mConfig;

  XrdSysError mLog;

  S3Api mApi;
  S3Router mRouter;

  bool ParseConfig(const char *config, XrdOucEnv &env);

  void ConfigureRouter();
};

}  // namespace S3

#endif
