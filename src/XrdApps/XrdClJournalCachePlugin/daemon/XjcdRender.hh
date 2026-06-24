#pragma once

#include "daemon/XjcdState.hh"

#include <string>
#include <vector>

namespace JournalCache {

struct XjcdValidationIssue {
  bool error = false;
  std::string message;
};

std::string substituteTemplate(const std::string &tmpl,
                               const XjcdState &state,
                               const std::string &hostname);

bool writeTextFile(const std::string &path, const std::string &content);
bool ensureInitialPolicy(const std::string &policyPath);
bool renderXjcdConfigs(const XjcdState &state, const std::string &hostname);
std::vector<XjcdValidationIssue> validateXjcdState(const XjcdState &state);

std::string defaultSystemdUnitName();
std::string defaultCleanerSystemdUnitName();
std::string systemdUnitInstallPath(const std::string &unitName);
bool isValidSystemdUnitName(const std::string &unitName);
bool installSystemdUnitFile(const std::string &sourceUnitPath,
                            const std::string &unitName, std::string &error);
bool installXjcdSystemdUnit(const XjcdState &state, const std::string &unitName,
                            bool enable, std::string &error);
bool installXjcdSystemdUnits(const XjcdState &state,
                             const std::string &proxyUnitName,
                             const std::string &cleanerUnitName, bool enable,
                             std::string &error);

} // namespace JournalCache
