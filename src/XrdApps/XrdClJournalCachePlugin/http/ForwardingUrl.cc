#include "http/ForwardingUrl.hh"
#include "file/Digest.hh"

#include "XrdCl/XrdClURL.hh"

#include <filesystem>
#include <vector>

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

std::string safeNormalizeRemotePath(const std::string &path) {
  std::string in = path.empty() ? "/" : path;
  if (in.front() != '/') {
    in.insert(in.begin(), '/');
  }
  std::vector<std::string> parts;
  size_t i = 1;
  while (i <= in.size()) {
    const size_t j = (i >= in.size()) ? in.size() : in.find('/', i);
    const size_t end = (j == std::string::npos) ? in.size() : j;
    const std::string seg = in.substr(i, end - i);
    if (seg == ".") {
    } else if (seg == "..") {
      if (!parts.empty()) {
        parts.pop_back();
      }
    } else {
      parts.push_back(seg);
    }
    if (end >= in.size()) {
      break;
    }
    i = end + 1;
  }
  if (parts.empty()) {
    return "/";
  }
  std::string out;
  for (const auto &part : parts) {
    out += '/';
    out += part;
  }
  return out;
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

EmbeddedFileUrl unwrapFullyChained(const EmbeddedFileUrl &first) {
  EmbeddedFileUrl result = first;
  if (!result.valid) {
    return result;
  }

  while (true) {
    XrdCl::URL current(result.fileUrl);
    if (!current.IsValid()) {
      break;
    }
    const EmbeddedFileUrl inner = parseEmbeddedFileUrl(current.GetPath());
    if (!inner.valid) {
      break;
    }
    result = inner;
  }
  return result;
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
      return unwrapFullyChained(embedded);
    }
  }

  XrdCl::URL outer(url);
  if (!outer.IsValid()) {
    return {};
  }

  EmbeddedFileUrl embedded = parseEmbeddedFileUrl(outer.GetPath());
  if (embedded.valid) {
    return unwrapFullyChained(embedded);
  }

  return canonicalizeFileUrl(url);
}

std::string resolveJournalDirWithSettings(const std::string &cacheRoot,
                                          const std::string &serverUrl,
                                          const std::string &remotePath,
                                          bool flatHierarchy,
                                          const std::string &basePath) {
  const std::string normPath = safeNormalizeRemotePath(remotePath);
  const std::string key = serverUrl + normPath;
  std::string resolved;
  if (flatHierarchy) {
    resolved = cacheRoot + computeSHA256(key);
  } else if (!basePath.empty() &&
             normPath.find(basePath) != std::string::npos) {
    resolved = cacheRoot + normPath.substr(normPath.find(basePath));
  } else {
    XrdCl::URL url(serverUrl);
    const std::string host =
        url.GetHostName() + ":" + std::to_string(url.GetPort());
    resolved = cacheRoot + host + normPath;
  }

  if (cacheRoot.empty()) {
    return resolved;
  }
  const auto normalPath = std::filesystem::path(resolved).lexically_normal();
  const auto normalRoot = std::filesystem::path(cacheRoot).lexically_normal();
  const auto relative = normalPath.lexically_relative(normalRoot);
  const std::string rel = relative.generic_string();
  if (!rel.empty() && rel.rfind("..", 0) != 0 &&
      rel.find("/../") == std::string::npos) {
    return resolved;
  }
  return cacheRoot + computeSHA256(key);
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
