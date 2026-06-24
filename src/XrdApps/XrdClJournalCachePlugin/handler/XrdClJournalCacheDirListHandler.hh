#pragma once

#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

namespace XrdCl {

class JournalCacheSystem;

class JournalCacheDirListHandler : public XrdCl::ResponseHandler {
public:
  JournalCacheDirListHandler() = default;
  JournalCacheDirListHandler(XrdCl::ResponseHandler *handler,
                             JournalCacheSystem *system, const std::string &path,
                             DirListFlags::Flags flags)
      : mHandler(handler), pSystem(system), mPath(path), mFlags(flags) {}

  virtual ~JournalCacheDirListHandler() = default;

  void HandleResponse(XrdCl::XRootDStatus *pStatus,
                      XrdCl::AnyObject *pResponse) override;

private:
  XrdCl::ResponseHandler *mHandler;
  JournalCacheSystem *pSystem;
  std::string mPath;
  DirListFlags::Flags mFlags;
};

} // namespace XrdCl
