#include "http/ForwardingUrl.hh"
#include "file/Digest.hh"

#include "XrdCl/XrdClURL.hh"

namespace JournalCache {
namespace {

bool startsWith(const std::string &value, const std::string &prefix) {
  return value.size() >= prefix.size() &&
         value.compare(0, prefix.size(), prefix) == 0;
}

std::string stripQuery(const std::string &path) {
  const auto pos = path.find('?');
  if (pos == std::string::npos) {
    return path;
  }
  return path.substr(0, pos);
}

bool isEmbeddedProtocolPrefix(const std::string &value) {
  static const char *prefixes[] = {"https://",  "http://",   "roots://",
                                     "root://",   "xroots://", "xroot://"};
  for (const char *prefix : prefixes) {
    if (startsWith(value, prefix)) {
      return true;
    }
  }
  return false;
}

std::string normalizeRemotePath(const std::string &path) {
  if (path.empty()) {
    return "/";
  }
  if (path[0] == '/') {
    return path;
  }
  return "/" + path;
}

EmbeddedFileUrl canonicalizeFileUrl(const std::string &url) {
  EmbeddedFileUrl result;
  XrdCl::URL parsed(url);
  if (!parsed.IsValid()) {
    return result;
  }

  XrdCl::URL clean;
  clean.SetProtocol(parsed.GetProtocol());
  clean.SetHostName(parsed.GetHostName());
  clean.SetPort(parsed.GetPort());
  clean.SetPath(parsed.GetPath());
  clean.SetParams(parsed.GetParams());
  result.fileUrl = clean.GetURL();
  result.valid = !result.fileUrl.empty();
  return result;
}

EmbeddedFileUrl parseEmbeddedFromRest(std::string rest) {
  EmbeddedFileUrl result;
  while (!rest.empty() && rest.front() == '/') {
    rest.erase(0, 1);
  }
  if (!isEmbeddedProtocolPrefix(rest)) {
    return result;
  }
  return canonicalizeFileUrl(rest);
}

} // namespace

EmbeddedFileUrl parseEmbeddedFileUrl(const std::string &path) {
  return parseEmbeddedFromRest(stripQuery(path));
}

EmbeddedFileUrl parseChainedFileUrl(const std::string &url) {
  // Path-only forwarding: /https://host/path or /root://host//path
  if (!url.empty() && url.front() == '/') {
    EmbeddedFileUrl embedded = parseEmbeddedFileUrl(url);
    if (embedded.valid) {
      return embedded;
    }
  }

  XrdCl::URL outer(url);
  if (!outer.IsValid()) {
    return {};
  }

  EmbeddedFileUrl embedded = parseEmbeddedFileUrl(outer.GetPath());
  if (embedded.valid) {
    return embedded;
  }

  return canonicalizeFileUrl(url);
}

std::string resolveJournalDirWithSettings(const std::string &cacheRoot,
                                          const std::string &serverUrl,
                                          const std::string &remotePath,
                                          bool flatHierarchy,
                                          const std::string &basePath) {
  const std::string normPath = normalizeRemotePath(remotePath);
  if (flatHierarchy) {
    return cacheRoot + computeSHA256(serverUrl + normPath);
  }

  if (!basePath.empty()) {
    const size_t pos = normPath.find(basePath);
    if (pos != std::string::npos) {
      return cacheRoot + normPath.substr(pos);
    }
    XrdCl::URL url(serverUrl);
    const size_t urlPos = url.GetPath().find(basePath);
    if (urlPos != std::string::npos) {
      return cacheRoot + normPath;
    }
  }

  XrdCl::URL url(serverUrl);
  const std::string host =
      url.GetHostName() + ":" + std::to_string(url.GetPort());
  return cacheRoot + host + normPath;
}

std::string resolveJournalPathFromCacheKey(const std::string &cacheRoot,
                                           const std::string &cacheKeyUrl,
                                           bool flatHierarchy,
                                           const std::string &basePath) {
  if (cacheRoot.empty() || cacheKeyUrl.empty()) {
    return {};
  }

  std::string journalDir;
  if (flatHierarchy) {
    journalDir = cacheRoot + computeSHA256(cacheKeyUrl);
  } else {
    XrdCl::URL url(cacheKeyUrl);
    journalDir = resolveJournalDirWithSettings(cacheRoot, cacheKeyUrl,
                                               url.GetPath(), flatHierarchy,
                                               basePath);
  }
  return journalDir + "/journal";
}

} // namespace JournalCache
