#pragma once

#include <regex>
#include <string>
#include <vector>

namespace JournalCache {

//! Regex allowlist for forwarded/chained upstream URLs.
class OriginAllowlist {
public:
  void clear();
  void addPattern(const std::string &pattern);
  void addPatternsFromCsv(const std::string &csv);

  bool empty() const { return mPatterns.empty(); }
  const std::vector<std::string> &patterns() const { return mPatterns; }
  bool isAllowed(const std::string &fileUrl) const;

private:
  void ensureCompiled() const;

  std::vector<std::string> mPatterns;
  mutable std::vector<std::regex> mRegexes;
  mutable bool mCompiled = false;
};

} // namespace JournalCache
