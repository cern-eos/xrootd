#pragma once

#include <string>

namespace JournalCache {

//! Persistent xjcd bootstrap parameters (stored under $journal/.xjc/state.conf).
struct XjcdState {
  std::string journal;
  unsigned xrootPort = 1094;
  unsigned httpsPort = 8443;
  std::string tlsCert;
  std::string tlsKey;
  std::string libDir = "/usr/lib64";
  std::string pluginSuffix = "5";

  std::string etcDir() const;
  std::string policyPath() const;
  std::string statePath() const;
  std::string clientPluginDir() const;
  std::string xrootdConfigPath() const;
  std::string httpExtConfigPath() const;
  std::string clientPluginConfigPath() const;
  std::string systemdEnvPath() const;
  std::string systemdUnitPath() const;
  std::string cleanerPath() const;
  std::string cleanerSystemdEnvPath() const;
  std::string cleanerSystemdUnitPath() const;

  bool load(const std::string &journalRoot);
  bool save() const;
  bool isComplete() const;
};

bool parseXjcdStateText(const std::string &text, XjcdState &out);
std::string formatXjcdState(const XjcdState &state);

std::string normalizeJournalRoot(std::string path);
std::string defaultLibDir();

} // namespace JournalCache
