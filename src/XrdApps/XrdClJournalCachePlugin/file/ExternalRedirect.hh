#pragma once

#include <string>
#include <vector>

namespace JournalCache {

//! Path-prefix rules that bypass cache and redirect to an external URL.
class ExternalRedirect {
public:
  struct Rule {
    std::string prefix;
    std::string target;
  };

  void clear();
  void addRule(const std::string &prefix, const std::string &target);
  void addRuleFromSpec(const std::string &spec);
  void addRulesFromCsv(const std::string &csv);

  bool empty() const { return mRules.empty(); }
  const std::vector<Rule> &rules() const { return mRules; }

  //! Return redirect URL for path (and optional query suffix, e.g. "?a=1"), or empty.
  std::string resolve(const std::string &path,
                      const std::string &querySuffix = "") const;

private:
  static std::string normalizePrefix(std::string prefix);
  static bool pathMatchesPrefix(const std::string &path,
                                const std::string &prefix);
  static std::string joinTarget(const std::string &targetBase,
                                const std::string &suffixPath);

  std::vector<Rule> mRules;
};

} // namespace JournalCache
