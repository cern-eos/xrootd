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

void applyPolicyLine(const std::string &key, const std::string &value,
                     PolicySettings &out) {
  if (key == "bypass") {
    out.bypass = parseBoolPolicy(value);
  } else if (key == "multi_origin") {
    out.multiOriginUnwrap = parseBoolPolicy(value);
  } else if (key == "allow_origin") {
    out.originAllowlist.addPattern(value);
  } else if (key == "external_redirect") {
    out.externalRedirect.addRuleFromSpec(value);
  }
}

} // namespace

bool parseBoolPolicy(const std::string &value) {
  return value == "1" || value == "true" || value == "yes";
}

bool parsePolicyText(const std::string &text, PolicySettings &out) {
  out = PolicySettings{};
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
    applyPolicyLine(trim(line.substr(0, eq)), trim(line.substr(eq + 1)), out);
  }
  return true;
}

bool loadPolicyFile(const std::string &path, PolicySettings &out) {
  if (path.empty()) {
    return false;
  }
  std::ifstream in(path);
  if (!in) {
    return false;
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  return parsePolicyText(ss.str(), out);
}

std::string formatPolicyFile(const PolicySettings &settings) {
  std::ostringstream out;
  out << "# JournalCache runtime policy (edit with xjc; reloaded on mtime change)\n";
  out << "bypass = " << (settings.bypass ? "1" : "0") << "\n";
  out << "multi_origin = " << (settings.multiOriginUnwrap ? "1" : "0") << "\n";
  for (const auto &pattern : settings.originAllowlist.patterns()) {
    out << "allow_origin = " << pattern << "\n";
  }
  for (const auto &rule : settings.externalRedirect.rules()) {
    out << "external_redirect = " << rule.prefix << " " << rule.target << "\n";
  }
  return out.str();
}

bool savePolicyFile(const std::string &path, const PolicySettings &settings) {
  if (path.empty()) {
    return false;
  }

  std::error_code ec;
  const std::filesystem::path filePath(path);
  const std::filesystem::path dir = filePath.parent_path();
  if (!dir.empty()) {
    std::filesystem::create_directories(dir, ec);
  }

  const std::filesystem::path tmpPath = path + ".tmp";
  {
    std::ofstream out(tmpPath, std::ios::trunc);
    if (!out) {
      return false;
    }
    out << formatPolicyFile(settings);
    if (!out) {
      return false;
    }
  }

  std::filesystem::rename(tmpPath, filePath, ec);
  if (ec) {
    std::filesystem::remove(tmpPath, ec);
    return false;
  }
  return true;
}

std::string defaultPolicyPath(const std::string &cacheRoot) {
  if (cacheRoot.empty()) {
    return {};
  }
  std::string root = cacheRoot;
  if (root.back() != '/') {
    root.push_back('/');
  }
  return root + ".xjc/policy.conf";
}

} // namespace JournalCache
