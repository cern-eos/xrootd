#pragma once

#include <regex>
#include <string>
#include <vector>

namespace JournalCache {

//! Regex allowlist for forwarded/chained upstream URLs.
class OriginAllowlist {
public:
  void clear();
  //! @return false if the pattern is empty or not a valid ECMAScript regex.
  bool addPattern(const std::string &pattern);
  void addPatternsFromCsv(const std::string &csv);

  bool empty() const { return mPatterns.empty(); }
  const std::vector<std::string> &patterns() const { return mPatterns; }
  //! Empty allowlist denies (closed proxy). Non-chained opens do not consult this.
  bool isAllowed(const std::string &fileUrl) const;

private:
  struct CompiledPattern {
    std::string pattern;
    std::regex regex;
    bool urlPattern = false;
  };

  void ensureCompiled() const;

  std::vector<std::string> mPatterns;
  mutable std::vector<CompiledPattern> mCompiledPatterns;
  mutable bool mCompiled = false;
};

} // namespace JournalCache
