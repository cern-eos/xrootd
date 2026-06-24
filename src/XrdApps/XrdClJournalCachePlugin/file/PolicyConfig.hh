#pragma once

#include "file/ExternalRedirect.hh"
#include "file/OriginAllowlist.hh"

#include <string>

namespace JournalCache {

//! Hot-reloadable JournalCache policy knobs.
struct PolicySettings {
  bool bypass = false;
  bool multiOriginUnwrap = false;
  OriginAllowlist originAllowlist;
  ExternalRedirect externalRedirect;
};

bool parseBoolPolicy(const std::string &value);

//! Parse policy file contents (key = value lines, repeatable keys).
bool parsePolicyText(const std::string &text, PolicySettings &out);

//! Read policy file; returns false if missing or unreadable.
bool loadPolicyFile(const std::string &path, PolicySettings &out);

//! Write policy file atomically.
bool savePolicyFile(const std::string &path, const PolicySettings &settings);

std::string formatPolicyFile(const PolicySettings &settings);

//! Default runtime policy path under a cache root.
std::string defaultPolicyPath(const std::string &cacheRoot);

} // namespace JournalCache
