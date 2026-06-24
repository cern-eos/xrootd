#include "daemon/XjcdState.hh"

#include "file/CleanerConfig.hh"
#include "file/PolicyConfig.hh"

#include <cctype>
#include <filesystem>
#include <fstream>
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

std::string joinPath(const std::string &root, const std::string &suffix) {
  if (root.empty()) {
    return suffix;
  }
  if (suffix.empty()) {
    return root;
  }
  if (root.back() == '/') {
    return root + suffix;
  }
  return root + "/" + suffix;
}

} // namespace

std::string normalizeJournalRoot(std::string path) {
  path = trim(path);
  while (path.size() > 1 && path.back() == '/') {
    path.pop_back();
  }
  return path;
}

std::string defaultLibDir() {
#if defined(__APPLE__)
  return "/usr/local/lib";
#else
  return "/usr/lib64";
#endif
}

std::string XjcdState::etcDir() const {
  return joinPath(journal, ".xjc/etc");
}

std::string XjcdState::policyPath() const {
  return defaultPolicyPath(journal + "/");
}

std::string XjcdState::statePath() const {
  return joinPath(journal, ".xjc/state.conf");
}

std::string XjcdState::clientPluginDir() const {
  return joinPath(etcDir(), "client.plugins.d");
}

std::string XjcdState::xrootdConfigPath() const {
  return joinPath(etcDir(), "xrootd.cf");
}

std::string XjcdState::httpExtConfigPath() const {
  return joinPath(etcDir(), "journalcache-http.ext.conf");
}

std::string XjcdState::clientPluginConfigPath() const {
  return joinPath(clientPluginDir(), "journalcache.conf");
}

std::string XjcdState::systemdEnvPath() const {
  return joinPath(etcDir(), "xjcd.env");
}

std::string XjcdState::systemdUnitPath() const {
  return joinPath(etcDir(), "xjcd.service");
}

std::string XjcdState::cleanerPath() const {
  return defaultCleanerPath(journal + "/");
}

std::string XjcdState::cleanerSystemdEnvPath() const {
  return joinPath(etcDir(), "xjccleand.env");
}

std::string XjcdState::cleanerSystemdUnitPath() const {
  return joinPath(etcDir(), "xjccleand.service");
}

bool XjcdState::isComplete() const {
  return !journal.empty() && xrootPort > 0 && httpsPort > 0 && !tlsCert.empty() &&
         !tlsKey.empty();
}

bool parseXjcdStateText(const std::string &text, XjcdState &out) {
  out = XjcdState{};
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) {
    line = trim(line);
    if (line.empty() || line[0] == '#') {
      continue;
    }
    const auto eq = line.find('=');
    if (eq == std::string::npos) {
      continue;
    }
    const std::string key = trim(line.substr(0, eq));
    const std::string value = trim(line.substr(eq + 1));
    if (key == "journal") {
      out.journal = normalizeJournalRoot(value);
    } else if (key == "xroot_port") {
      out.xrootPort = static_cast<unsigned>(std::stoul(value));
    } else if (key == "https_port") {
      out.httpsPort = static_cast<unsigned>(std::stoul(value));
    } else if (key == "tls_cert") {
      out.tlsCert = value;
    } else if (key == "tls_key") {
      out.tlsKey = value;
    } else if (key == "lib_dir") {
      out.libDir = value;
    } else if (key == "plugin_suffix") {
      out.pluginSuffix = value;
    }
  }
  return out.isComplete();
}

std::string formatXjcdState(const XjcdState &state) {
  std::ostringstream out;
  out << "# xjcd bootstrap state\n";
  out << "journal = " << state.journal << "\n";
  out << "xroot_port = " << state.xrootPort << "\n";
  out << "https_port = " << state.httpsPort << "\n";
  out << "tls_cert = " << state.tlsCert << "\n";
  out << "tls_key = " << state.tlsKey << "\n";
  out << "lib_dir = " << state.libDir << "\n";
  out << "plugin_suffix = " << state.pluginSuffix << "\n";
  return out.str();
}

bool XjcdState::load(const std::string &journalRoot) {
  journal = normalizeJournalRoot(journalRoot);
  if (journal.empty()) {
    return false;
  }
  std::ifstream in(statePath());
  if (!in) {
    return false;
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  return parseXjcdStateText(ss.str(), *this);
}

bool XjcdState::save() const {
  if (!isComplete()) {
    return false;
  }
  std::error_code ec;
  std::filesystem::create_directories(joinPath(journal, ".xjc"), ec);
  const std::string path = statePath();
  const std::string tmp = path + ".tmp";
  {
    std::ofstream out(tmp, std::ios::trunc);
    if (!out) {
      return false;
    }
    out << formatXjcdState(*this);
  }
  std::filesystem::rename(tmp, path, ec);
  if (ec) {
    std::filesystem::remove(tmp, ec);
    return false;
  }
  return true;
}

} // namespace JournalCache
