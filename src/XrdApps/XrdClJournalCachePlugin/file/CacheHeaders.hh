#pragma once

#include "XrdCl/XrdClURL.hh"
#include "XrdCl/XrdClLog.hh"
#include "XrdCl/XrdClXRootDResponses.hh"

#include <cstdint>
#include <string>

namespace JournalCache {

//! Parsed Cache-Control directives relevant to journal caching.
struct CachePolicy {
  bool noStore = false;
  bool noCache = false;
  bool isPrivate = false;
  int64_t maxAge = -1;  //!< seconds; -1 if unset
  int64_t sMaxAge = -1; //!< shared-cache max age; -1 if unset
};

//! Cache header values persisted on a journal file.
struct CacheHeaders {
  std::string cacheControl;
  std::string expires;
  std::string etag;
  std::string lastModified;
  uint64_t cachedAt = 0; //!< unix seconds when headers were stored

  bool empty() const {
    return cacheControl.empty() && expires.empty() && etag.empty() &&
           lastModified.empty();
  }
};

//! Client validation headers supplied as CGI on the file URL.
struct CacheValidators {
  std::string ifNoneMatch;
  std::string ifModifiedSince;

  bool empty() const {
    return ifNoneMatch.empty() && ifModifiedSince.empty();
  }
};

//! CGI keys for setter cache headers (raw HTTP header values).
constexpr const char *CACHE_CONTROL_CGI = "xrd.journalcache.cache-control";
constexpr const char *EXPIRES_CGI = "xrd.journalcache.expires";
constexpr const char *ETAG_CGI = "xrd.journalcache.etag";
constexpr const char *LAST_MODIFIED_CGI = "xrd.journalcache.last-modified";

//! CGI keys for request validation headers.
constexpr const char *IF_NONE_MATCH_CGI = "xrd.journalcache.if-none-match";
constexpr const char *IF_MODIFIED_SINCE_CGI = "xrd.journalcache.if-modified-since";

//! Per-file operational CGI toggles.
constexpr const char *BYPASS_CGI = "xrd.journalcache.bypass";
constexpr const char *FORCE_CLEAN_CGI = "xrd.journalcache.clean";

CachePolicy parseCacheControl(const std::string &cacheControl);

bool extractCacheHeadersFromParams(const XrdCl::URL::ParamsMap &params,
                                   CacheHeaders &out);

bool extractCacheValidatorsFromParams(const XrdCl::URL::ParamsMap &params,
                                      CacheValidators &out);

bool storeCacheHeaders(const std::string &journalPath,
                       const CacheHeaders &headers, XrdCl::Log *log);

bool loadCacheHeaders(const std::string &journalPath, CacheHeaders &out);

bool applyCacheHeadersFromParams(const std::string &journalPath,
                                 const XrdCl::URL::ParamsMap &params,
                                 XrdCl::Log *log);

//! Fill missing getter headers from a remote Stat response.
void enrichCacheHeadersFromStat(const XrdCl::StatInfo *stat,
                                CacheHeaders &headers);

std::string formatHttpDate(uint64_t unixSeconds);

bool requiresRevalidation(const CacheHeaders &headers);

bool shouldUseJournalCache(const CacheHeaders &headers, uint64_t nowSeconds);

bool isCacheEntryStale(const CacheHeaders &headers, uint64_t nowSeconds);

//! Return true when request validators prove the cached entry is stale.
bool validationRequiresRefresh(const CacheHeaders &stored,
                               const CacheValidators &request);

//! Return true when the client sent validators that match stored getter headers.
bool canRespondNotModified(const CacheHeaders &stored,
                             const CacheValidators &request,
                             uint64_t nowSeconds);

//! Append HTTP response header lines (CRLF-separated, no trailing CRLF).
void appendGetterResponseHeaders(const CacheHeaders &headers, std::string &out);

bool paramEnabled(const XrdCl::URL::ParamsMap &params, const char *key);

} // namespace JournalCache
