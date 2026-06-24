#include "http/JournalCacheHttpExt.hh"
#include "file/CacheHeaders.hh"
#include "http/ForwardingUrl.hh"
#include "http/HttpHeaderMap.hh"

#include "XrdCl/XrdClDefaultEnv.hh"
#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClURL.hh"
#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdVersion.hh"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>

#ifdef JOURNALCACHE_HTTP_EXT_HAVE_CURL
#include <curl/curl.h>
#endif

XrdVERSIONINFO(XrdHttpGetExtHandler, JournalCacheHttp);

namespace JournalCache {
namespace {

std::string trim(const std::string &value) {
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

std::string stripQuery(const std::string &path) {
  const auto pos = path.find('?');
  if (pos == std::string::npos) {
    return path;
  }
  return path.substr(0, pos);
}

bool startsWith(const std::string &value, const std::string &prefix) {
  return value.size() >= prefix.size() &&
         value.compare(0, prefix.size(), prefix) == 0;
}

bool parseBool(const std::string &value) {
  return value == "1" || value == "true" || value == "yes";
}

void appendUnique(std::vector<std::pair<std::string, std::string>> &params,
                  const std::pair<std::string, std::string> &entry) {
  for (const auto &existing : params) {
    if (existing.first == entry.first) {
      return;
    }
  }
  params.push_back(entry);
}

#ifdef JOURNALCACHE_HTTP_EXT_HAVE_CURL
size_t curlHeaderCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
  const size_t total = size * nitems;
  auto *headers = static_cast<std::map<std::string, std::string> *>(userdata);
  std::string line(buffer, total);
  while (!line.empty() &&
         (line.back() == '\r' || line.back() == '\n')) {
    line.pop_back();
  }
  const auto colon = line.find(':');
  if (colon == std::string::npos || colon == 0) {
    return total;
  }
  std::string name = line.substr(0, colon);
  std::string value = trim(line.substr(colon + 1));
  if (!value.empty()) {
    (*headers)[name] = value;
  }
  return total;
}

bool fetchHttpHeadParams(const std::string &url, XrdSysError *log,
                         std::vector<std::pair<std::string, std::string>> &params) {
  std::map<std::string, std::string> responseHeaders;

  CURL *curl = curl_easy_init();
  if (!curl) {
    return false;
  }

  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, curlHeaderCallback);
  curl_easy_setopt(curl, CURLOPT_HEADERDATA, &responseHeaders);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);

  const CURLcode rc = curl_easy_perform(curl);
  curl_easy_cleanup(curl);
  if (rc != CURLE_OK) {
    if (log) {
      log->Emsg("JournalCacheHttpExt", "HTTP HEAD failed for", url.c_str());
    }
    return false;
  }

  for (const auto &entry :
       HttpHeaderMap::mapHeaders(responseHeaders, HttpHeaderMap::setterMappings())) {
    appendUnique(params, entry);
  }
  return !params.empty();
}
#endif

} // namespace

JournalCacheHttpExtHandler::JournalCacheHttpExtHandler(XrdSysError *log)
    : mLog(log),
      mServerUrl("root://localhost:1094"),
      mPathPrefix("/"),
      mExcludePrefix("/static/") {
  mXAttrMappings = {
      {"http.cache-control", CACHE_CONTROL_CGI},
      {"http.expires", EXPIRES_CGI},
      {"http.etag", ETAG_CGI},
      {"http.last-modified", LAST_MODIFIED_CGI},
  };
}

bool JournalCacheHttpExtHandler::loadConfig(const char *cfgfile) {
  if (!cfgfile || !cfgfile[0]) {
    return true;
  }

  std::ifstream in(cfgfile);
  if (!in) {
    mLog->Emsg("Config", "Unable to open JournalCache HTTP ext config:", cfgfile);
    return false;
  }

  std::string line;
  while (std::getline(in, line)) {
    line = trim(line);
    if (line.empty() || line[0] == '#') {
      continue;
    }

    const auto eq = line.find('=');
    if (eq == std::string::npos) {
      continue;
    }

    const std::string key = trim(line.substr(0, eq));
    const std::string value = trim(line.substr(eq + 1));
    if (key.empty()) {
      continue;
    }

    if (key == "server") {
      mServerUrl = value;
    } else if (key == "cache") {
      mCacheRoot = value;
      if (!mCacheRoot.empty() && mCacheRoot.back() != '/') {
        mCacheRoot.push_back('/');
      }
    } else if (key == "flat") {
      mFlatHierarchy = parseBool(value);
    } else if (key == "basepath") {
      mBasePath = value;
    } else if (key == "prefix") {
      mPathPrefix = value;
    } else if (key == "exclude") {
      mExcludePrefix = value;
    } else if (key == "forwarding") {
      mForwarding = parseBool(value);
    } else if (key == "http_origin") {
      mHttpOrigin = value;
      if (!mHttpOrigin.empty() && mHttpOrigin.back() == '/') {
        mHttpOrigin.pop_back();
      }
    } else if (key == "http_origin_strip") {
      mHttpOriginStrip = value;
    } else if (key == "xattr") {
      const auto space = value.find(' ');
      if (space != std::string::npos) {
        mXAttrMappings.emplace_back(trim(value.substr(0, space)),
                                    trim(value.substr(space + 1)));
      }
    }
  }

  return true;
}

int JournalCacheHttpExtHandler::Init(const char *cfgfile) {
  if (!loadConfig(cfgfile)) {
    return 1;
  }
  mLog->Say("++++++ JournalCache HTTP ext handler initialized");
  if (mForwarding) {
    mLog->Say("Config mode=forwarding prefix=", mPathPrefix.c_str());
    if (mCacheRoot.empty()) {
      mLog->Say("Config warning: cache= unset; 304/getter headers need on-disk journals");
    }
    if (!mHttpOrigin.empty()) {
      mLog->Say("Config warning: http_origin ignored in forwarding mode");
    }
  } else {
    mLog->Say("Config server=", mServerUrl.c_str(),
              " prefix=", mPathPrefix.c_str());
  }
  if (!mCacheRoot.empty()) {
    mLog->Say("Config cache=", mCacheRoot.c_str(),
              " flat=", mFlatHierarchy ? "1" : "0");
  }
  if (!mForwarding && !mHttpOrigin.empty()) {
    mLog->Say("Config http_origin=", mHttpOrigin.c_str());
  }
  return 0;
}

bool JournalCacheHttpExtHandler::pathMatches(const char *path) const {
  if (!path || !path[0]) {
    return false;
  }
  if (!mPathPrefix.empty() && !startsWith(path, mPathPrefix)) {
    return false;
  }
  if (!mExcludePrefix.empty() && startsWith(path, mExcludePrefix)) {
    return false;
  }
  if (mForwarding && !parseEmbeddedFileUrl(path).valid) {
    return false;
  }
  return true;
}

bool JournalCacheHttpExtHandler::MatchesPath(const char *verb, const char *path) {
  if (!verb || !path) {
    return false;
  }
  if (strcmp(verb, "GET") && strcmp(verb, "HEAD")) {
    return false;
  }
  return pathMatches(path);
}

std::string JournalCacheHttpExtHandler::resolveJournalPath(
    const std::string &path) const {
  if (mCacheRoot.empty()) {
    return {};
  }

  const EmbeddedFileUrl embedded = parseEmbeddedFileUrl(path);
  if (embedded.valid) {
    return resolveJournalPathFromCacheKey(mCacheRoot, embedded.fileUrl,
                                          mFlatHierarchy, mBasePath);
  }

  return resolveJournalDirWithSettings(mCacheRoot, mServerUrl, path,
                                       mFlatHierarchy, mBasePath) +
         "/journal";
}

CacheValidators JournalCacheHttpExtHandler::extractValidators(
    const std::map<std::string, std::string> &requestHeaders) const {
  CacheValidators validators;
  for (const auto &entry :
       HttpHeaderMap::mapHeaders(requestHeaders,
                                 HttpHeaderMap::validatorMappings())) {
    if (entry.first == IF_NONE_MATCH_CGI) {
      validators.ifNoneMatch = entry.second;
    } else if (entry.first == IF_MODIFIED_SINCE_CGI) {
      validators.ifModifiedSince = entry.second;
    }
  }
  return validators;
}

void JournalCacheHttpExtHandler::setResponseHeadersFromCache(
    XrdHttpExtReq &req, const CacheHeaders &headers) const {
  if (!headers.cacheControl.empty()) {
    req.SetResponseHeader("Cache-Control", headers.cacheControl);
  }
  if (!headers.expires.empty()) {
    req.SetResponseHeader("Expires", headers.expires);
  }
  if (!headers.etag.empty()) {
    req.SetResponseHeader("ETag", headers.etag);
  }
  if (!headers.lastModified.empty()) {
    req.SetResponseHeader("Last-Modified", headers.lastModified);
  }
}

bool JournalCacheHttpExtHandler::fetchXAttrParams(
    const std::string &path,
    std::vector<std::pair<std::string, std::string>> &params) {
  if (mForwarding || mXAttrMappings.empty()) {
    return false;
  }

  const std::string fileUrl = HttpHeaderMap::buildFileUrl(mServerUrl, path);
  XrdCl::URL fileUrlObj(fileUrl);
  if (!fileUrlObj.IsValid()) {
    mLog->Emsg("JournalCacheHttpExt", "Invalid server URL:", fileUrl.c_str());
    return false;
  }

  XrdCl::URL serverUrl;
  serverUrl.SetProtocol(fileUrlObj.GetProtocol());
  serverUrl.SetHostName(fileUrlObj.GetHostName());
  serverUrl.SetPort(fileUrlObj.GetPort());
  serverUrl.SetUserName(fileUrlObj.GetUserName());

  XrdCl::FileSystem fs(serverUrl);
  std::vector<std::string> names;
  names.reserve(mXAttrMappings.size());
  for (const auto &mapping : mXAttrMappings) {
    names.push_back(mapping.first);
  }

  std::vector<XrdCl::XAttr> attrs;
  const XrdCl::XRootDStatus st =
      fs.GetXAttr(fileUrlObj.GetPath(), names, attrs);
  if (!st.IsOK()) {
    return false;
  }

  bool found = false;
  for (const auto &attr : attrs) {
    if (!attr.status.IsOK() || attr.value.empty()) {
      continue;
    }
    for (const auto &mapping : mXAttrMappings) {
      if (mapping.first == attr.name) {
        appendUnique(params, {mapping.second, attr.value});
        found = true;
        break;
      }
    }
  }
  return found;
}

bool JournalCacheHttpExtHandler::fetchHttpOriginParams(
    const std::string &path,
    std::vector<std::pair<std::string, std::string>> &params) {
#ifdef JOURNALCACHE_HTTP_EXT_HAVE_CURL
  const EmbeddedFileUrl embedded = parseEmbeddedFileUrl(path);
  if (embedded.valid) {
    return fetchHttpHeadParams(embedded.fileUrl, mLog, params);
  }

  if (mHttpOrigin.empty()) {
    return false;
  }

  std::string resourcePath = path;
  if (!mHttpOriginStrip.empty() &&
      startsWith(resourcePath, mHttpOriginStrip)) {
    resourcePath.erase(0, mHttpOriginStrip.size());
    if (resourcePath.empty() || resourcePath.front() != '/') {
      resourcePath.insert(resourcePath.begin(), '/');
    }
  }

  return fetchHttpHeadParams(mHttpOrigin + resourcePath, mLog, params);
#else
  (void)path;
  (void)params;
  return false;
#endif
}

std::vector<std::pair<std::string, std::string>>
JournalCacheHttpExtHandler::collectCgiParams(
    const std::string &path,
    const std::map<std::string, std::string> &requestHeaders) {
  std::vector<std::pair<std::string, std::string>> params;

  for (const auto &entry :
       HttpHeaderMap::mapHeaders(requestHeaders, HttpHeaderMap::validatorMappings())) {
    appendUnique(params, entry);
  }

  fetchXAttrParams(path, params);
  fetchHttpOriginParams(path, params);

  return params;
}

int JournalCacheHttpExtHandler::ProcessReq(XrdHttpExtReq &req) {
  const std::string path = stripQuery(req.resource);
  const CacheValidators validators = extractValidators(req.headers);

  CacheHeaders stored;
  const std::string journalPath = resolveJournalPath(path);
  if (!journalPath.empty() && std::filesystem::exists(journalPath)) {
    loadCacheHeaders(journalPath, stored);
  }

  const uint64_t now = static_cast<uint64_t>(std::time(nullptr));
  if (canRespondNotModified(stored, validators, now)) {
    std::string responseHeaders;
    appendGetterResponseHeaders(stored, responseHeaders);
    return req.SendSimpleResp(
        304, nullptr,
        responseHeaders.empty() ? nullptr : responseHeaders.c_str(), nullptr, 0);
  }

  const auto params = collectCgiParams(path, req.headers);
  for (const auto &param : params) {
    req.AppendOpaque(param.first, param.second);
  }

  if (stored.empty()) {
    XrdCl::URL::ParamsMap paramsMap;
    for (const auto &param : params) {
      paramsMap[param.first] = param.second;
    }
    extractCacheHeadersFromParams(paramsMap, stored);
  }

  if (!stored.empty()) {
    setResponseHeadersFromCache(req, stored);
  }

  return XrdHttpExtContinueProcessing;
}

} // namespace JournalCache

extern "C" XrdHttpExtHandler *
XrdHttpGetExtHandler(XrdSysError *eDest, const char *confg, const char *parms,
                     XrdOucEnv * /*myEnv*/) {
  auto *handler = new JournalCache::JournalCacheHttpExtHandler(eDest);
  if (handler->Init(parms && parms[0] ? parms : confg)) {
    delete handler;
    return nullptr;
  }
  return handler;
}
