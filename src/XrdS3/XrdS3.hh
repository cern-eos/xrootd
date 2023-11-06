
#ifndef XROOTD_XRDS3_HH
#define XROOTD_XRDS3_HH

#include <functional>
#include <vector>

#include "XrdHttp/XrdHttpChecksumHandler.hh"
#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdS3Api.hh"
#include "XrdS3Auth.hh"
#include "XrdS3Crypt.hh"
#include "XrdS3Router.hh"
#include "XrdS3Utils.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdXrootd/XrdXrootdProtocol.hh"

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
  // todo: global s3crypt etc.
  Context ctx;

 private:
  struct {
    std::string data_dir;
    std::string auth_dir;
    std::string region;
    std::string service;
  } mConfig;

  XrdSysError mLog;

  S3Api mApi;
  S3Router mRouter;

  // todo: dont calculate empty sha
  const std::string EMPTY_SHA =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
  // todo: size of biggest algorithm
  // Algorithm + Date + Scope + CanonicalRequestHash
  //  const size_t STRING_TO_SIGN_LENGTH =
  //      16 + 1 + 16 + 1 +
  //      (8 + 1 + mRegion.size() + 1 + mService.size() + 1 + 12) + 1 + 64;

  bool ParseConfig(const char *config, XrdOucEnv &env);

  void ConfigureRouter();
};

}  // namespace S3

#endif
