#ifndef XRDHTTPS3_HANDLER_HH
#define XRDHTTPS3_HANDLER_HH

#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdHttpS3Config.hh"
#include "XrdHttpS3Meta.hh"
#include "XrdHttpS3Ops.hh"

#include <memory>

class XrdHttpReq;
class XrdSfsFileSystem;
class XrdSysError;

namespace XrdHttpS3 {

class Handler : public XrdHttpExtHandler {
public:
  Handler(XrdSysError *log, const char *config, XrdOucEnv *env);
  virtual ~Handler();

  bool MatchesPath(const char *verb, const char *path) override;
  int ProcessReq(XrdHttpExtReq &req) override;
  int Init(const char *cfgfile) override;
  bool Claim(const XrdHttpReq &req) const;

private:
  XrdSysError &m_log;
  Config m_cfg;
  XrdSfsFileSystem *m_sfs{nullptr};
  std::unique_ptr<Meta> m_meta;
  std::unique_ptr<Ops> m_ops;
};

}

#endif
