#include "file/CacheHeaders.hh"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <string>
#include <sys/xattr.h>
#include <vector>

namespace JournalCache {
namespace {

constexpr const char *XATTR_CACHE_CONTROL = "user.journalcache.cache-control";
constexpr const char *XATTR_EXPIRES = "user.journalcache.expires";
constexpr const char *XATTR_ETAG = "user.journalcache.etag";
constexpr const char *XATTR_LAST_MODIFIED = "user.journalcache.last-modified";
constexpr const char *XATTR_CACHED_AT = "user.journalcache.cached-at";

#ifdef __APPLE__
constexpr int XATTR_OPTIONS = 0;

int jcSetXAttr(const char *path, const char *name, const void *value, size_t size) {
  return setxattr(path, name, value, size, 0, XATTR_OPTIONS);
}

ssize_t jcGetXAttr(const char *path, const char *name, void *value, size_t size) {
  return getxattr(path, name, value, size, 0, XATTR_OPTIONS);
}

int jcRemoveXAttr(const char *path, const char *name) {
  return removexattr(path, name, XATTR_OPTIONS);
}
#else
int jcSetXAttr(const char *path, const char *name, const void *value, size_t size) {
  return setxattr(path, name, value, size, 0);
}

ssize_t jcGetXAttr(const char *path, const char *name, void *value, size_t size) {
  return getxattr(path, name, value, size);
}

int jcRemoveXAttr(const char *path, const char *name) {
  return removexattr(path, name);
}
#endif

std::string trimCopy(const std::string &value) {
  size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start]))) {
    ++start;
  }
  size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1]))) {
    --end;
  }
  return value.substr(start, end - start);
}

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return value;
}

std::string unquoteEtag(std::string value) {
  value = trimCopy(value);
  if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
    return value.substr(1, value.size() - 2);
  }
  return value;
}

bool setXAttrString(const std::string &path, const char *name,
                    const std::string &value) {
  if (value.empty()) {
    jcRemoveXAttr(path.c_str(), name);
    return true;
  }
  return jcSetXAttr(path.c_str(), name, value.data(), value.size()) == 0;
}

bool getXAttrString(const std::string &path, const char *name,
                    std::string &value) {
  ssize_t size = jcGetXAttr(path.c_str(), name, nullptr, 0);
  if (size < 0) {
    return false;
  }
  std::vector<char> buffer(static_cast<size_t>(size));
  if (jcGetXAttr(path.c_str(), name, buffer.data(), buffer.size()) != size) {
    return false;
  }
  value.assign(buffer.data(), buffer.size());
  return true;
}

bool parseHttpDate(const std::string &value, uint64_t &unixTime) {
  struct tm tm = {};
  const char *formats[] = {"%a, %d %b %Y %H:%M:%S GMT",
                           "%A, %d-%b-%Y %H:%M:%S GMT", nullptr};
  for (int i = 0; formats[i]; ++i) {
    if (strptime(value.c_str(), formats[i], &tm) != nullptr) {
      unixTime = static_cast<uint64_t>(timegm(&tm));
      return true;
    }
  }
  return false;
}

int64_t parseAgeDirective(const std::string &directive, const char *name) {
  const size_t nameLen = std::strlen(name);
  if (directive.size() <= nameLen || directive.compare(0, nameLen, name) != 0) {
    return -1;
  }
  if (directive.size() == nameLen) {
    return 0;
  }
  if (directive[nameLen] != '=') {
    return -1;
  }
  try {
    return std::stoll(directive.substr(nameLen + 1));
  } catch (...) {
    return -1;
  }
}

bool etagMatches(const std::string &stored, const std::string &candidate) {
  const std::string storedValue = unquoteEtag(stored);
  const std::string candidateValue = unquoteEtag(candidate);
  if (candidateValue == "*") {
    return false;
  }
  return !storedValue.empty() && storedValue == candidateValue;
}

} // namespace

std::string formatHttpDate(uint64_t unixSeconds) {
  struct tm tm = {};
  const time_t seconds = static_cast<time_t>(unixSeconds);
  if (!gmtime_r(&seconds, &tm)) {
    return {};
  }
  char buffer[64];
  if (!strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S GMT", &tm)) {
    return {};
  }
  return buffer;
}

CachePolicy parseCacheControl(const std::string &cacheControl) {
  CachePolicy policy;
  if (cacheControl.empty()) {
    return policy;
  }

  size_t start = 0;
  while (start < cacheControl.size()) {
    size_t end = cacheControl.find(',', start);
    if (end == std::string::npos) {
      end = cacheControl.size();
    }
    const std::string directive = toLowerCopy(trimCopy(
        cacheControl.substr(start, end - start)));
    if (directive == "no-store") {
      policy.noStore = true;
    } else if (directive == "no-cache") {
      policy.noCache = true;
    } else if (directive == "private") {
      policy.isPrivate = true;
    } else {
      const int64_t maxAge = parseAgeDirective(directive, "max-age");
      if (maxAge >= 0) {
        policy.maxAge = maxAge;
      }
      const int64_t sMaxAge = parseAgeDirective(directive, "s-maxage");
      if (sMaxAge >= 0) {
        policy.sMaxAge = sMaxAge;
      }
    }
    start = end + 1;
  }

  return policy;
}

bool extractCacheHeadersFromParams(const XrdCl::URL::ParamsMap &params,
                                   CacheHeaders &out) {
  out = CacheHeaders{};
  bool found = false;

  auto cacheControl = params.find(CACHE_CONTROL_CGI);
  if (cacheControl != params.end()) {
    out.cacheControl = cacheControl->second;
    found = true;
  }

  auto expires = params.find(EXPIRES_CGI);
  if (expires != params.end()) {
    out.expires = expires->second;
    found = true;
  }

  auto etag = params.find(ETAG_CGI);
  if (etag != params.end()) {
    out.etag = etag->second;
    found = true;
  }

  auto lastModified = params.find(LAST_MODIFIED_CGI);
  if (lastModified != params.end()) {
    out.lastModified = lastModified->second;
    found = true;
  }

  return found;
}

bool extractCacheValidatorsFromParams(const XrdCl::URL::ParamsMap &params,
                                      CacheValidators &out) {
  out = CacheValidators{};
  bool found = false;

  auto ifNoneMatch = params.find(IF_NONE_MATCH_CGI);
  if (ifNoneMatch != params.end()) {
    out.ifNoneMatch = ifNoneMatch->second;
    found = true;
  }

  auto ifModifiedSince = params.find(IF_MODIFIED_SINCE_CGI);
  if (ifModifiedSince != params.end()) {
    out.ifModifiedSince = ifModifiedSince->second;
    found = true;
  }

  return found;
}

bool storeCacheHeaders(const std::string &journalPath,
                       const CacheHeaders &headers, XrdCl::Log *log) {
  if (journalPath.empty()) {
    return false;
  }

  if (!setXAttrString(journalPath, XATTR_CACHE_CONTROL, headers.cacheControl) ||
      !setXAttrString(journalPath, XATTR_EXPIRES, headers.expires) ||
      !setXAttrString(journalPath, XATTR_ETAG, headers.etag) ||
      !setXAttrString(journalPath, XATTR_LAST_MODIFIED, headers.lastModified)) {
    if (log) {
      log->Warning(1, "JournalCache : failed to store cache headers on %s: %s",
                   journalPath.c_str(), std::strerror(errno));
    }
    return false;
  }

  if (headers.cachedAt) {
    const std::string cachedAt = std::to_string(headers.cachedAt);
    if (!setXAttrString(journalPath, XATTR_CACHED_AT, cachedAt)) {
      if (log) {
        log->Warning(1,
                     "JournalCache : failed to store cached-at on %s: %s",
                     journalPath.c_str(), std::strerror(errno));
      }
      return false;
    }
  } else {
    jcRemoveXAttr(journalPath.c_str(), XATTR_CACHED_AT);
  }

  return true;
}

bool loadCacheHeaders(const std::string &journalPath, CacheHeaders &out) {
  out = CacheHeaders{};
  bool found = false;

  if (getXAttrString(journalPath, XATTR_CACHE_CONTROL, out.cacheControl)) {
    found = true;
  }
  if (getXAttrString(journalPath, XATTR_EXPIRES, out.expires)) {
    found = true;
  }
  if (getXAttrString(journalPath, XATTR_ETAG, out.etag)) {
    found = true;
  }
  if (getXAttrString(journalPath, XATTR_LAST_MODIFIED, out.lastModified)) {
    found = true;
  }

  std::string cachedAt;
  if (getXAttrString(journalPath, XATTR_CACHED_AT, cachedAt)) {
    try {
      out.cachedAt = std::stoull(cachedAt);
      found = true;
    } catch (...) {
      out.cachedAt = 0;
    }
  }

  return found;
}

bool applyCacheHeadersFromParams(const std::string &journalPath,
                                 const XrdCl::URL::ParamsMap &params,
                                 XrdCl::Log *log) {
  CacheHeaders headers;
  if (!extractCacheHeadersFromParams(params, headers)) {
    return false;
  }
  headers.cachedAt = static_cast<uint64_t>(std::time(nullptr));
  return storeCacheHeaders(journalPath, headers, log);
}

void enrichCacheHeadersFromStat(const XrdCl::StatInfo *stat,
                                CacheHeaders &headers) {
  if (!stat) {
    return;
  }

  if (headers.lastModified.empty()) {
    headers.lastModified = formatHttpDate(stat->GetModTime());
  }

  if (headers.etag.empty() && stat->HasChecksum()) {
    headers.etag = '"' + stat->GetChecksum() + '"';
  }
}

bool requiresRevalidation(const CacheHeaders &headers) {
  if (headers.empty()) {
    return false;
  }
  return parseCacheControl(headers.cacheControl).noCache;
}

bool shouldUseJournalCache(const CacheHeaders &headers, uint64_t nowSeconds) {
  if (headers.empty()) {
    return true;
  }

  const CachePolicy policy = parseCacheControl(headers.cacheControl);
  if (policy.noStore) {
    return false;
  }

  (void)nowSeconds;
  return true;
}

bool isCacheEntryStale(const CacheHeaders &headers, uint64_t nowSeconds) {
  if (headers.empty()) {
    return false;
  }

  const CachePolicy policy = parseCacheControl(headers.cacheControl);
  if (policy.noStore) {
    return true;
  }

  if (headers.cachedAt) {
    int64_t freshness = policy.sMaxAge;
    if (freshness < 0) {
      freshness = policy.maxAge;
    }
    if (freshness >= 0 &&
        nowSeconds > headers.cachedAt + static_cast<uint64_t>(freshness)) {
      return true;
    }
  }

  if (!headers.expires.empty()) {
    uint64_t expiresAt = 0;
    if (parseHttpDate(headers.expires, expiresAt) && nowSeconds >= expiresAt) {
      return true;
    }
  }

  return false;
}

bool validationRequiresRefresh(const CacheHeaders &stored,
                               const CacheValidators &request) {
  if (request.empty()) {
    return false;
  }

  if (!request.ifNoneMatch.empty()) {
    if (stored.etag.empty()) {
      return true;
    }
    if (!etagMatches(stored.etag, request.ifNoneMatch)) {
      return true;
    }
  }

  if (!request.ifModifiedSince.empty()) {
    uint64_t since = 0;
    if (!parseHttpDate(request.ifModifiedSince, since)) {
      return true;
    }

    uint64_t lastModified = 0;
    if (!stored.lastModified.empty() &&
        parseHttpDate(stored.lastModified, lastModified)) {
      if (lastModified > since) {
        return true;
      }
    } else if (stored.cachedAt && stored.cachedAt > since) {
      return true;
    }
  }

  return false;
}

bool canRespondNotModified(const CacheHeaders &stored,
                             const CacheValidators &request,
                             uint64_t nowSeconds) {
  if (request.empty() || stored.empty()) {
    return false;
  }
  if (validationRequiresRefresh(stored, request)) {
    return false;
  }
  if (isCacheEntryStale(stored, nowSeconds)) {
    return false;
  }
  return true;
}

void appendGetterResponseHeaders(const CacheHeaders &headers,
                                 std::string &out) {
  auto append = [&](const char *name, const std::string &value) {
    if (value.empty()) {
      return;
    }
    if (!out.empty()) {
      out += "\r\n";
    }
    out += name;
    out += ": ";
    out += value;
  };

  append("Cache-Control", headers.cacheControl);
  append("Expires", headers.expires);
  append("ETag", headers.etag);
  append("Last-Modified", headers.lastModified);
}

bool paramEnabled(const XrdCl::URL::ParamsMap &params, const char *key) {
  const auto it = params.find(key);
  if (it == params.end()) {
    return false;
  }
  const std::string &value = it->second;
  return value == "1" || value == "true" || value == "yes";
}

} // namespace JournalCache
