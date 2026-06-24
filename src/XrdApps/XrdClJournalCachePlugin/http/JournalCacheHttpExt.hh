#pragma once

#include "XrdHttp/XrdHttpExtHandler.hh"
#include "XrdSys/XrdSysError.hh"
#include "file/CacheHeaders.hh"
#include "file/OriginAllowlist.hh"

#include <map>
#include <string>
#include <vector>

namespace JournalCache {

class JournalCacheHttpExtHandler : public XrdHttpExtHandler {
public:
  explicit JournalCacheHttpExtHandler(XrdSysError *log);

  bool MatchesPath(const char *verb, const char *path) override;
  int ProcessReq(XrdHttpExtReq &req) override;
  int Init(const char *cfgfile) override;

private:
  bool loadConfig(const char *cfgfile);
  bool pathMatches(const char *path) const;
  std::vector<std::pair<std::string, std::string>>
  collectCgiParams(const std::string &path,
                   const std::map<std::string, std::string> &requestHeaders);
  bool fetchXAttrParams(const std::string &path,
                        std::vector<std::pair<std::string, std::string>> &params);
  bool fetchHttpOriginParams(const std::string &path,
                             std::vector<std::pair<std::string, std::string>> &params);
  std::string resolveJournalPath(const std::string &path) const;
  void setResponseHeadersFromCache(XrdHttpExtReq &req,
                                   const CacheHeaders &headers) const;
  CacheValidators extractValidators(
      const std::map<std::string, std::string> &requestHeaders) const;

  XrdSysError *mLog;
  std::string mServerUrl;
  std::string mCacheRoot;
  std::string mBasePath;
  std::string mPathPrefix;
  std::string mExcludePrefix;
  std::string mHttpOrigin;
  std::string mHttpOriginStrip;
  bool mForwarding = false;
  bool mFlatHierarchy = false;
  OriginAllowlist mOriginAllowlist;
  std::vector<std::pair<std::string, std::string>> mXAttrMappings;
};

} // namespace JournalCache
