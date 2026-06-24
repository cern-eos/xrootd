#pragma once

#include "file/CacheHeaders.hh"

#include <cctype>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace JournalCache {
namespace HttpHeaderMap {

struct Mapping {
  const char *httpName;
  const char *cgiName;
};

inline std::string headerKeyLower(const std::string &name) {
  std::string lower = name;
  for (char &ch : lower) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return lower;
}

inline const std::string *findHeader(const std::map<std::string, std::string> &headers,
                                     const char *name) {
  const std::string key = headerKeyLower(name);
  for (const auto &entry : headers) {
    if (headerKeyLower(entry.first) == key && !entry.second.empty()) {
      return &entry.second;
    }
  }
  return nullptr;
}

inline std::vector<Mapping> setterMappings() {
  return {
      {"Cache-Control", CACHE_CONTROL_CGI},
      {"Expires", EXPIRES_CGI},
      {"ETag", ETAG_CGI},
      {"Last-Modified", LAST_MODIFIED_CGI},
  };
}

inline std::vector<Mapping> validatorMappings() {
  return {
      {"If-None-Match", IF_NONE_MATCH_CGI},
      {"If-Modified-Since", IF_MODIFIED_SINCE_CGI},
  };
}

inline std::vector<std::pair<std::string, std::string>>
mapHeaders(const std::map<std::string, std::string> &headers,
           const std::vector<Mapping> &mappings) {
  std::vector<std::pair<std::string, std::string>> params;
  for (const auto &mapping : mappings) {
    const std::string *value = findHeader(headers, mapping.httpName);
    if (value) {
      params.emplace_back(mapping.cgiName, *value);
    }
  }
  return params;
}

inline std::string buildFileUrl(const std::string &serverUrl,
                                const std::string &path) {
  if (path.empty()) {
    return serverUrl;
  }
  std::string url = serverUrl;
  if (!url.empty() && url.back() == '/') {
    url.pop_back();
  }
  if (path.front() == '/') {
    url.push_back('/');
  }
  url += path;
  return url;
}

} // namespace HttpHeaderMap
} // namespace JournalCache
