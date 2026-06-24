#pragma once

#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

namespace XrdCl {

class JournalCacheSystem;

class JournalCacheStatHandler : public XrdCl::ResponseHandler {
public:
  JournalCacheStatHandler() = default;
  JournalCacheStatHandler(XrdCl::ResponseHandler *handler,
                          JournalCacheSystem *system, const std::string &path)
      : mHandler(handler), pSystem(system), mPath(path) {}

  virtual ~JournalCacheStatHandler() = default;

  void HandleResponse(XrdCl::XRootDStatus *pStatus,
                      XrdCl::AnyObject *pResponse) override;

private:
  XrdCl::ResponseHandler *mHandler;
  JournalCacheSystem *pSystem;
  std::string mPath;
};

} // namespace XrdCl
