#include "file/ExternalRedirect.hh"

#include <algorithm>
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

std::string suffixAfterPrefix(const std::string &path,
                              const std::string &prefix) {
  if (path.size() <= prefix.size()) {
    return {};
  }
  if (path.compare(0, prefix.size(), prefix) != 0) {
    return {};
  }
  return path.substr(prefix.size());
}

} // namespace

void ExternalRedirect::clear() { mRules.clear(); }

void ExternalRedirect::addRule(const std::string &prefix,
                               const std::string &target) {
  const std::string normalizedPrefix = normalizePrefix(prefix);
  const std::string normalizedTarget = trim(target);
  if (normalizedPrefix.empty() || normalizedTarget.empty()) {
    return;
  }

  for (auto &rule : mRules) {
    if (rule.prefix == normalizedPrefix) {
      rule.target = normalizedTarget;
      return;
    }
  }
  mRules.push_back({normalizedPrefix, normalizedTarget});
}

void ExternalRedirect::addRuleFromSpec(const std::string &spec) {
  const std::string trimmed = trim(spec);
  if (trimmed.empty()) {
    return;
  }

  const auto pipe = trimmed.find('|');
  if (pipe != std::string::npos) {
    addRule(trim(trimmed.substr(0, pipe)), trim(trimmed.substr(pipe + 1)));
    return;
  }

  const auto space = trimmed.find(' ');
  if (space == std::string::npos) {
    return;
  }
  addRule(trim(trimmed.substr(0, space)), trim(trimmed.substr(space + 1)));
}

void ExternalRedirect::addRulesFromCsv(const std::string &csv) {
  std::string item;
  std::stringstream ss(csv);
  while (std::getline(ss, item, ',')) {
    addRuleFromSpec(item);
  }
}

std::string ExternalRedirect::normalizePrefix(std::string prefix) {
  prefix = trim(prefix);
  if (prefix.empty()) {
    return {};
  }
  if (prefix.front() != '/') {
    prefix.insert(prefix.begin(), '/');
  }
  return prefix;
}

bool ExternalRedirect::pathMatchesPrefix(const std::string &path,
                                         const std::string &prefix) {
  if (prefix.empty()) {
    return false;
  }
  if (path == prefix) {
    return true;
  }
  if (path.size() < prefix.size()) {
    if (prefix.back() == '/' && path.size() + 1 == prefix.size() &&
        path == prefix.substr(0, path.size())) {
      return true;
    }
    return false;
  }
  if (path.compare(0, prefix.size(), prefix) != 0) {
    return false;
  }
  if (prefix.back() == '/') {
    return true;
  }
  return path[prefix.size()] == '/';
}

std::string ExternalRedirect::joinTarget(const std::string &targetBase,
                                         const std::string &suffixPath) {
  if (suffixPath.empty()) {
    return targetBase;
  }

  std::string result = targetBase;
  if (result.empty()) {
    return suffixPath;
  }

  if (result.back() == '/' && !suffixPath.empty() && suffixPath.front() == '/') {
    result.pop_back();
  } else if (result.back() != '/' && !suffixPath.empty() &&
             suffixPath.front() != '/') {
    result.push_back('/');
  }
  result += suffixPath;
  return result;
}

std::string ExternalRedirect::resolve(const std::string &path,
                                      const std::string &querySuffix) const {
  if (mRules.empty() || path.empty()) {
    return {};
  }

  std::vector<const Rule *> matches;
  matches.reserve(mRules.size());
  for (const auto &rule : mRules) {
    if (pathMatchesPrefix(path, rule.prefix)) {
      matches.push_back(&rule);
    }
  }
  if (matches.empty()) {
    return {};
  }

  std::sort(matches.begin(), matches.end(),
            [](const Rule *a, const Rule *b) {
              return a->prefix.size() > b->prefix.size();
            });

  const Rule &rule = *matches.front();
  std::string redirect = joinTarget(rule.target, suffixAfterPrefix(path, rule.prefix));
  if (!querySuffix.empty()) {
    if (redirect.find('?') == std::string::npos) {
      redirect += querySuffix;
    } else if (querySuffix.front() == '?') {
      redirect += '&';
      redirect += querySuffix.substr(1);
    } else {
      redirect += querySuffix;
    }
  }
  return redirect;
}

} // namespace JournalCache
