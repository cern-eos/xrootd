#include "file/CleanerConfig.hh"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace JournalCache {
namespace {

constexpr uint64_t kMinHighWatermark = 1024ull * 1024ull * 1024ull;

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

void applyCleanerLine(const std::string &key, const std::string &value,
                      CleanerSettings &out) {
  if (key == "enabled") {
    out.enabled = value == "1" || value == "true" || value == "yes";
  } else if (key == "journal") {
    out.journal = normalizeCacheRoot(value);
  } else if (key == "high_watermark") {
    out.highWatermark = std::stoull(value);
  } else if (key == "low_watermark") {
    out.lowWatermark = std::stoull(value);
  } else if (key == "interval") {
    out.interval = static_cast<unsigned>(std::stoul(value));
  } else if (key == "config_poll") {
    out.configPoll = static_cast<unsigned>(std::stoul(value));
  }
}

} // namespace

std::string normalizeCacheRoot(std::string path) {
  path = trim(path);
  while (path.size() > 1 && path.back() == '/') {
    path.pop_back();
  }
  return path;
}

bool parseCleanerText(const std::string &text, CleanerSettings &out) {
  out = CleanerSettings{};
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
    applyCleanerLine(trim(line.substr(0, eq)), trim(line.substr(eq + 1)), out);
  }
  return true;
}

bool loadCleanerFile(const std::string &path, CleanerSettings &out) {
  if (path.empty()) {
    return false;
  }
  std::ifstream in(path);
  if (!in) {
    return false;
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  return parseCleanerText(ss.str(), out);
}

std::string formatCleanerFile(const CleanerSettings &settings) {
  std::ostringstream out;
  out << "# JournalCache cleaner config (edit with xjc cleaner; reloaded by xjccleand)\n";
  out << "enabled = " << (settings.enabled ? "1" : "0") << "\n";
  out << "journal = " << settings.journal << "\n";
  out << "high_watermark = " << settings.highWatermark << "\n";
  out << "low_watermark = " << settings.lowWatermark << "\n";
  out << "interval = " << settings.interval << "\n";
  out << "config_poll = " << settings.configPoll << "\n";
  return out.str();
}

bool saveCleanerFile(const std::string &path, const CleanerSettings &settings) {
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
    out << formatCleanerFile(settings);
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

std::string defaultCleanerPath(const std::string &cacheRoot) {
  if (cacheRoot.empty()) {
    return {};
  }
  std::string root = cacheRoot;
  if (root.back() != '/') {
    root.push_back('/');
  }
  return root + ".xjc/cleaner.conf";
}

uint64_t effectiveLowWatermark(const CleanerSettings &settings) {
  if (settings.lowWatermark > 0) {
    return settings.lowWatermark;
  }
  return settings.highWatermark * 90 / 100;
}

bool validateCleanerSettings(const CleanerSettings &settings, std::string &error) {
  if (!settings.enabled) {
    return true;
  }
  if (settings.journal.empty()) {
    error = "journal is not set";
    return false;
  }
  if (settings.highWatermark < kMinHighWatermark) {
    error = "high_watermark must be at least 1GB when enabled";
    return false;
  }
  const uint64_t low = effectiveLowWatermark(settings);
  if (low >= settings.highWatermark) {
    error = "low_watermark must be below high_watermark";
    return false;
  }
  if (settings.interval == 0) {
    error = "interval must be greater than 0";
    return false;
  }
  if (settings.configPoll == 0) {
    error = "config_poll must be greater than 0";
    return false;
  }
  return true;
}

} // namespace JournalCache
