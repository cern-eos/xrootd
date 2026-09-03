#include "file/OriginAllowlist.hh"

#include "XrdCl/XrdClURL.hh"

#include <cctype>
#include <sstream>

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

bool looksLikeUrlPattern(const std::string &pattern) {
  return pattern.find("://") != std::string::npos ||
         (!pattern.empty() && pattern.front() == '^');
}

} // namespace

void OriginAllowlist::clear() {
  mPatterns.clear();
  mCompiledPatterns.clear();
  mCompiled = false;
}

bool OriginAllowlist::addPattern(const std::string &pattern) {
  const std::string trimmed = trim(pattern);
  if (trimmed.empty()) {
    return false;
  }
  try {
    (void)std::regex(trimmed, std::regex::ECMAScript);
  } catch (const std::regex_error &) {
    return false;
  }
  mPatterns.push_back(trimmed);
  mCompiled = false;
  return true;
}

void OriginAllowlist::addPatternsFromCsv(const std::string &csv) {
  std::string item;
  std::stringstream ss(csv);
  while (std::getline(ss, item, ',')) {
    addPattern(item);
  }
}

void OriginAllowlist::ensureCompiled() const {
  if (mCompiled) {
    return;
  }

  mCompiledPatterns.clear();
  for (const auto &pattern : mPatterns) {
    try {
      CompiledPattern compiled;
      compiled.pattern = pattern;
      compiled.regex = std::regex(pattern, std::regex::ECMAScript);
      compiled.urlPattern = looksLikeUrlPattern(pattern);
      mCompiledPatterns.push_back(std::move(compiled));
    } catch (const std::regex_error &) {
      continue;
    }
  }
  mCompiled = true;
}

bool OriginAllowlist::isAllowed(const std::string &fileUrl) const {
  if (mPatterns.empty()) {
    return false;
  }

  ensureCompiled();
  if (mCompiledPatterns.empty()) {
    return false;
  }

  XrdCl::URL url(fileUrl);
  const std::string hostId = url.IsValid() ? url.GetHostId() : std::string{};
  const std::string host = url.IsValid() ? url.GetHostName() : std::string{};

  for (const auto &compiled : mCompiledPatterns) {
    if (compiled.urlPattern) {
      if (std::regex_search(fileUrl, compiled.regex)) {
        return true;
      }
      continue;
    }

    if (!hostId.empty() && std::regex_match(hostId, compiled.regex)) {
      return true;
    }
    if (!host.empty() && std::regex_match(host, compiled.regex)) {
      return true;
    }
  }
  return false;
}

} // namespace JournalCache
