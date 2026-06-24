#include "handler/XrdClJournalCacheStatHandler.hh"
#include "system/XrdClJournalCacheSystem.hh"

#include "file/CachePath.hh"
#include "file/Hierarchy.hh"
#include "file/XrdClJournalCacheFile.hh"
#include "handler/XrdClJournalCacheDirListHandler.hh"
#include "system/ListCache.hh"

#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClMessageUtils.hh"

#include <sys/stat.h>

namespace XrdCl {

uint64_t JournalCacheSystem::sListTtl = 0;
bool JournalCacheSystem::sEnableListStat = true;

JournalCacheSystem::JournalCacheSystem(const URL &url) {
  pSystem = new XrdCl::FileSystem(url, false);
  mUrl = url.GetURL();
  mLog = DefaultEnv::GetLog();
}

JournalCacheSystem::~JournalCacheSystem() { delete pSystem; }

bool JournalCacheSystem::listingCacheEnabled() const {
  return !JournalCacheFile::policyBypass() && !JournalCacheFile::sCachePath.empty();
}

bool JournalCacheSystem::statCacheEnabled() const {
  return sEnableListStat && listingCacheEnabled();
}

void JournalCacheSystem::invalidateForMutation(const std::string &path) {
  JournalCache::invalidateCachesForMutation(mUrl, path);
}

XRootDStatus JournalCacheSystem::Stat(const std::string &path,
                                      ResponseHandler *handler,
                                      time_t timeout) {
  if (!statCacheEnabled()) {
    return pSystem->Stat(path, handler, timeout);
  }

  const std::string statPath =
      JournalCache::statCachePath(mUrl, path);
  XrdCl::StatInfo *cachedStat = nullptr;
  if (JournalCache::loadStatInfo(statPath, sListTtl, cachedStat)) {
    mLog->Debug(1, "JournalCache : stat cache hit for %s", path.c_str());
    auto *obj = new AnyObject();
    obj->Set(cachedStat);
    handler->HandleResponse(new XRootDStatus(stOK, 0), obj);
    return XRootDStatus(stOK, 0);
  }

  const std::string cacheDir = JournalCache::resolveCacheDir(mUrl, path);
  if (!JournalCache::makeHierarchy(cacheDir + "/.journalcache_stat")) {
    mLog->Error(1, "JournalCache : unable to create stat cache directory for %s",
                path.c_str());
    return pSystem->Stat(path, handler, timeout);
  }

  auto *shandler =
      new JournalCacheStatHandler(handler, this, statPath);
  return pSystem->Stat(path, shandler, timeout);
}

XRootDStatus JournalCacheSystem::DirList(const std::string &path,
                                         DirListFlags::Flags flags,
                                         ResponseHandler *handler,
                                         time_t timeout) {
  flags |= DirListFlags::Stat;
  const auto cacheFlags = JournalCache::dirListCacheKeyFlags(flags);

  if (!listingCacheEnabled() ||
      !JournalCache::isDirListCacheable(flags)) {
    return pSystem->DirList(path, flags, handler, timeout);
  }

  const std::string listingPath =
      JournalCache::listingCachePath(mUrl, path, cacheFlags);
  struct stat buf;
  if (!::stat(listingPath.c_str(), &buf)) {
    XrdCl::DirectoryList *response =
        JournalCache::loadDirectoryList(listingPath, cacheFlags, sListTtl);
    if (response) {
      mLog->Debug(1, "JournalCache : dirlist cache hit for %s", path.c_str());
      auto *obj = new AnyObject();
      obj->Set(response);
      handler->HandleResponse(new XRootDStatus(stOK, 0), obj);
      return XRootDStatus(stOK, 0);
    }
    mLog->Warning(1, "JournalCache : invalid dirlist cache for %s, refreshing",
                  path.c_str());
  }

  if (!JournalCache::makeHierarchy(listingPath)) {
    mLog->Error(1, "JournalCache : unable to create listing cache directory for %s",
                path.c_str());
    return XRootDStatus(stError, errOSError);
  }

  auto *lhandler =
      new JournalCacheDirListHandler(handler, this, listingPath, cacheFlags);
  const XRootDStatus st = pSystem->DirList(path, flags, lhandler, timeout);
  if (!st.IsOK()) {
    mLog->Error(1, "JournalCache : unable to get listing for %s", path.c_str());
  }
  return st;
}

bool JournalCacheSystem::SaveDirList(const std::string &path,
                                     DirectoryList *dirList,
                                     DirListFlags::Flags flags) {
  if (!JournalCache::saveDirectoryList(path, dirList, flags)) {
    mLog->Warning(1, "JournalCache : failed to save dirlist cache at %s",
                  path.c_str());
    return false;
  }
  mLog->Debug(1, "JournalCache : saved dirlist cache at %s", path.c_str());
  return true;
}

bool JournalCacheSystem::SaveStat(const std::string &path,
                                  const StatInfo *statInfo) {
  if (!JournalCache::saveStatInfo(path, statInfo)) {
    mLog->Warning(1, "JournalCache : failed to save stat cache at %s",
                  path.c_str());
    return false;
  }
  mLog->Debug(1, "JournalCache : saved stat cache at %s", path.c_str());
  return true;
}

XRootDStatus JournalCacheSystem::Rm(const std::string &path,
                                    ResponseHandler *handler,
                                    time_t timeout) {
  invalidateForMutation(path);
  return pSystem->Rm(path, handler, timeout);
}

XRootDStatus JournalCacheSystem::MkDir(const std::string &path,
                                       MkDirFlags::Flags flags,
                                       Access::Mode mode,
                                       ResponseHandler *handler,
                                       time_t timeout) {
  invalidateForMutation(path);
  return pSystem->MkDir(path, flags, mode, handler, timeout);
}

XRootDStatus JournalCacheSystem::RmDir(const std::string &path,
                                       ResponseHandler *handler,
                                       time_t timeout) {
  invalidateForMutation(path);
  return pSystem->RmDir(path, handler, timeout);
}

XRootDStatus JournalCacheSystem::Mv(const std::string &source,
                                    const std::string &dest,
                                    ResponseHandler *handler,
                                    time_t timeout) {
  invalidateForMutation(source);
  invalidateForMutation(dest);
  return pSystem->Mv(source, dest, handler, timeout);
}

XRootDStatus JournalCacheSystem::Truncate(const std::string &path, uint64_t size,
                                          ResponseHandler *handler,
                                          time_t timeout) {
  invalidateForMutation(path);
  return pSystem->Truncate(path, size, handler, timeout);
}

#define JOURNALCACHE_DELEGATE1(method)                                         \
  XRootDStatus JournalCacheSystem::method(ResponseHandler *handler,            \
                                          time_t timeout) {                  \
    return pSystem->method(handler, timeout);                                  \
  }

#define JOURNALCACHE_DELEGATE2(method)                                         \
  XRootDStatus JournalCacheSystem::method(                                     \
      const std::string &path, ResponseHandler *handler, time_t timeout) {   \
    return pSystem->method(path, handler, timeout);                            \
  }

#define JOURNALCACHE_DELEGATE3(method)                                         \
  XRootDStatus JournalCacheSystem::method(                                     \
      const std::string &path, OpenFlags::Flags flags,                         \
      ResponseHandler *handler, time_t timeout) {                          \
    return pSystem->method(path, flags, handler, timeout);                     \
  }

XRootDStatus JournalCacheSystem::Locate(const std::string &path,
                                        OpenFlags::Flags flags,
                                        ResponseHandler *handler,
                                        time_t timeout) {
  return pSystem->Locate(path, flags, handler, timeout);
}

XRootDStatus JournalCacheSystem::DeepLocate(const std::string &path,
                                            OpenFlags::Flags flags,
                                            ResponseHandler *handler,
                                            time_t timeout) {
  return pSystem->DeepLocate(path, flags, handler, timeout);
}

XRootDStatus JournalCacheSystem::Query(QueryCode::Code queryCode,
                                       const Buffer &arg,
                                       ResponseHandler *handler,
                                       time_t timeout) {
  return pSystem->Query(queryCode, arg, handler, timeout);
}

JOURNALCACHE_DELEGATE1(Ping)
JOURNALCACHE_DELEGATE2(StatVFS)
JOURNALCACHE_DELEGATE1(Protocol)

XRootDStatus JournalCacheSystem::SendInfo(const std::string &info,
                                          ResponseHandler *handler,
                                          time_t timeout) {
  return pSystem->SendInfo(info, handler, timeout);
}

XRootDStatus JournalCacheSystem::Prepare(const std::vector<std::string> &fileList,
                                         PrepareFlags::Flags flags,
                                         uint8_t priority,
                                         ResponseHandler *handler,
                                         time_t timeout) {
  return pSystem->Prepare(fileList, flags, priority, handler, timeout);
}

XRootDStatus JournalCacheSystem::SetXAttr(const std::string &path,
                                          const std::vector<xattr_t> &attrs,
                                          ResponseHandler *handler,
                                          time_t timeout) {
  invalidateForMutation(path);
  return pSystem->SetXAttr(path, attrs, handler, timeout);
}

XRootDStatus JournalCacheSystem::GetXAttr(const std::string &path,
                                          const std::vector<std::string> &attrs,
                                          ResponseHandler *handler,
                                          time_t timeout) {
  return pSystem->GetXAttr(path, attrs, handler, timeout);
}

XRootDStatus JournalCacheSystem::DelXAttr(const std::string &path,
                                          const std::vector<std::string> &attrs,
                                          ResponseHandler *handler,
                                          time_t timeout) {
  invalidateForMutation(path);
  return pSystem->DelXAttr(path, attrs, handler, timeout);
}

XRootDStatus JournalCacheSystem::ListXAttr(const std::string &path,
                                           ResponseHandler *handler,
                                           time_t timeout) {
  return pSystem->ListXAttr(path, handler, timeout);
}

XRootDStatus JournalCacheSystem::ChMod(const std::string &path,
                                       Access::Mode mode,
                                       ResponseHandler *handler,
                                       time_t timeout) {
  invalidateForMutation(path);
  return pSystem->ChMod(path, mode, handler, timeout);
}

bool JournalCacheSystem::SetProperty(const std::string &name,
                                     const std::string &value) {
  return pSystem->SetProperty(name, value);
}

bool JournalCacheSystem::GetProperty(const std::string &name,
                                     std::string &value) const {
  return pSystem->GetProperty(name, value);
}

} // namespace XrdCl
