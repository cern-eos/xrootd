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

} // namespace

void OriginAllowlist::clear() {
  mPatterns.clear();
  mRegexes.clear();
  mCompiled = false;
}

void OriginAllowlist::addPattern(const std::string &pattern) {
  const std::string trimmed = trim(pattern);
  if (trimmed.empty()) {
    return;
  }
  mPatterns.push_back(trimmed);
  mCompiled = false;
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

  mRegexes.clear();
  for (const auto &pattern : mPatterns) {
    try {
      mRegexes.emplace_back(pattern, std::regex::ECMAScript);
    } catch (const std::regex_error &) {
      continue;
    }
  }
  mCompiled = true;
}

bool OriginAllowlist::isAllowed(const std::string &fileUrl) const {
  if (mPatterns.empty()) {
    return true;
  }

  ensureCompiled();
  if (mRegexes.empty()) {
    return false;
  }

  XrdCl::URL url(fileUrl);
  const std::string hostId = url.IsValid() ? url.GetHostId() : std::string{};
  const std::string location = url.IsValid() ? url.GetLocation() : fileUrl;

  for (const auto &regex : mRegexes) {
    if (std::regex_search(fileUrl, regex) || std::regex_search(location, regex)) {
      return true;
    }
    if (!hostId.empty() &&
        (std::regex_match(hostId, regex) || std::regex_search(hostId, regex))) {
      return true;
    }
  }
  return false;
}

} // namespace JournalCache
