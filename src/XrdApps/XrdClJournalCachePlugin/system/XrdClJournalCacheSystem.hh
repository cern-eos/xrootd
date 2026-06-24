#pragma once

#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClPlugInInterface.hh"
#include <cstdint>
#include <string>

namespace XrdCl {

class JournalCacheSystem : public XrdCl::FileSystemPlugIn {
public:
  static uint64_t sListTtl;
  static bool sEnableListStat;

  JournalCacheSystem(const URL &url);
  virtual ~JournalCacheSystem();

  virtual XRootDStatus Locate(const std::string &path, OpenFlags::Flags flags,
                              ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus DeepLocate(const std::string &path,
                                  OpenFlags::Flags flags,
                                  ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus Mv(const std::string &source, const std::string &dest,
                          ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus Query(QueryCode::Code queryCode, const Buffer &arg,
                             ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus Truncate(const std::string &path, uint64_t size,
                                ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus Rm(const std::string &path, ResponseHandler *handler,
                          time_t timeout) override;
  virtual XRootDStatus MkDir(const std::string &path, MkDirFlags::Flags flags,
                             Access::Mode mode, ResponseHandler *handler,
                             time_t timeout) override;
  virtual XRootDStatus RmDir(const std::string &path, ResponseHandler *handler,
                             time_t timeout) override;
  virtual XRootDStatus ChMod(const std::string &path, Access::Mode mode,
                             ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus Ping(ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus Stat(const std::string &path, ResponseHandler *handler,
                            time_t timeout) override;
  virtual XRootDStatus StatVFS(const std::string &path,
                               ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus Protocol(ResponseHandler *handler,
                                time_t timeout = 0) override;
  virtual XRootDStatus DirList(const std::string &path,
                               DirListFlags::Flags flags,
                               ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus SendInfo(const std::string &info,
                                ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus Prepare(const std::vector<std::string> &fileList,
                               PrepareFlags::Flags flags, uint8_t priority,
                               ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus SetXAttr(const std::string &path,
                                const std::vector<xattr_t> &attrs,
                                ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus GetXAttr(const std::string &path,
                                const std::vector<std::string> &attrs,
                                ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus DelXAttr(const std::string &path,
                                const std::vector<std::string> &attrs,
                                ResponseHandler *handler, time_t timeout) override;
  virtual XRootDStatus ListXAttr(const std::string &path,
                                 ResponseHandler *handler, time_t timeout) override;
  virtual bool SetProperty(const std::string &name, const std::string &value) override;
  virtual bool GetProperty(const std::string &name, std::string &value) const override;

  XrdCl::FileSystem *System() { return pSystem; }
  const std::string &Url() const { return mUrl; }

  bool SaveDirList(const std::string &path, DirectoryList *dirList,
                   DirListFlags::Flags flags);
  bool SaveStat(const std::string &path, const StatInfo *statInfo);

private:
  bool listingCacheEnabled() const;
  bool statCacheEnabled() const;
  void invalidateForMutation(const std::string &path);

  XrdCl::FileSystem *pSystem;
  std::string mUrl;
  Log *mLog;
};

} // namespace XrdCl
