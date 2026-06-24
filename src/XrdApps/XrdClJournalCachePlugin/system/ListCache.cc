#include "system/ListCache.hh"

#include <cerrno>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <time.h>

namespace JournalCache {

namespace {

std::string urlEncode(const std::string &value) {
  std::ostringstream escaped;
  escaped.fill('0');
  escaped << std::hex;

  for (unsigned char c : value) {
    if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      escaped << c;
    } else {
      escaped << '%' << std::setw(2) << std::uppercase << static_cast<int>(c);
    }
  }
  return escaped.str();
}

std::string urlDecode(const std::string &value) {
  auto decodePercent = [](const std::string &hex) -> char {
    int hexValue = 0;
    std::istringstream hexStream(hex);
    if (!(hexStream >> std::hex >> hexValue)) {
      throw std::invalid_argument("invalid URL encoding");
    }
    return static_cast<char>(hexValue);
  };

  std::ostringstream decoded;
  for (size_t i = 0; i < value.length(); ++i) {
    if (value[i] == '%') {
      if (i + 2 >= value.length()) {
        throw std::invalid_argument("invalid URL encoding");
      }
      decoded << decodePercent(value.substr(i + 1, 2));
      i += 2;
    } else if (value[i] == '+') {
      decoded << ' ';
    } else {
      decoded << value[i];
    }
  }
  return decoded.str();
}

std::string statEncode(const XrdCl::StatInfo *stat) {
  if (!stat) {
    return "0";
  }

  std::ostringstream oss;
  oss << (stat->ExtendedFormat() ? 1 : 0) << '\t' << stat->GetId() << '\t'
      << stat->GetSize() << '\t' << stat->GetFlags() << '\t'
      << stat->GetModTime() << '\t' << stat->GetAccessTime() << '\t'
      << stat->GetChangeTime();

  if (stat->ExtendedFormat()) {
    oss << '\t' << stat->GetModeAsString() << '\t' << stat->GetOwner() << '\t'
        << stat->GetGroup() << '\t' << (stat->HasChecksum() ? 1 : 0);
    if (stat->HasChecksum()) {
      oss << '\t' << stat->GetChecksum();
    }
  }
  return oss.str();
}

XrdCl::StatInfo *statDecode(const std::string &statStr) {
  std::istringstream iss(statStr);
  std::string token;
  if (!std::getline(iss, token, '\t')) {
    return nullptr;
  }

  if (token != "0" && token != "1") {
    return nullptr;
  }

  const bool extended = token == "1";
  std::string id;
  uint64_t size = 0;
  uint32_t flags = 0;
  uint64_t modTime = 0;
  uint64_t accessTime = 0;
  uint64_t changeTime = 0;

  if (!std::getline(iss, id, '\t')) {
    return nullptr;
  }

  try {
    if (!std::getline(iss, token, '\t')) {
      return nullptr;
    }
    size = std::stoull(token);
    if (!std::getline(iss, token, '\t')) {
      return nullptr;
    }
    flags = std::stoul(token);
    if (!std::getline(iss, token, '\t')) {
      return nullptr;
    }
    modTime = std::stoull(token);
    if (!std::getline(iss, token, '\t')) {
      return nullptr;
    }
    accessTime = std::stoull(token);
    if (!std::getline(iss, token, '\t')) {
      return nullptr;
    }
    changeTime = std::stoull(token);
  } catch (const std::exception &) {
    return nullptr;
  }

  if (!extended) {
    return new XrdCl::StatInfo(id, size, flags, modTime);
  }

  std::string mode;
  std::string owner;
  std::string group;
  std::string checksum;

  if (!std::getline(iss, mode, '\t') || !std::getline(iss, owner, '\t') ||
      !std::getline(iss, group, '\t') || !std::getline(iss, token, '\t')) {
    return nullptr;
  }

  const bool hasChecksum = token == "1";
  if (hasChecksum) {
    if (!std::getline(iss, checksum)) {
      return nullptr;
    }
    while (!checksum.empty() &&
           (checksum.back() == '\n' || checksum.back() == '\r')) {
      checksum.pop_back();
    }
  }

  return new XrdCl::StatInfo(id, size, flags, modTime, changeTime, accessTime,
                             mode, owner, group, checksum);
}

bool parseLegacyStatLine(const std::string &statStr, XrdCl::StatInfo *&stat) {
  std::istringstream iss(statStr);
  std::string token;
  std::string id;
  uint64_t size = 0;
  uint32_t flags = 0;
  uint64_t modTime = 0;
  uint64_t accessTime = 0;
  uint64_t changeTime = 0;
  bool extended = false;
  std::string mode;
  std::string owner;
  std::string group;
  std::string checksum;

  if (!std::getline(iss, id, '\t')) {
    return false;
  }
  try {
    if (!std::getline(iss, token, '\t')) {
      return false;
    }
    size = std::stoull(token);
    if (!std::getline(iss, token, '\t')) {
      return false;
    }
    flags = std::stoul(token);
    if (!std::getline(iss, token, '\t')) {
      return false;
    }
    modTime = std::stoull(token);
    if (!std::getline(iss, token, '\t')) {
      return false;
    }
    accessTime = std::stoull(token);
    if (!std::getline(iss, token, '\t')) {
      return false;
    }
    changeTime = std::stoull(token);
    if (std::getline(iss, token, '\t') && !token.empty()) {
      extended = true;
      mode = token;
      if (!std::getline(iss, owner, '\t') || !std::getline(iss, group, '\t') ||
          !std::getline(iss, token, '\t')) {
        return false;
      }
      if (token == "1" && std::getline(iss, checksum, '\t')) {
        // legacy only stored checksum when HasChecksum was true
      } else if (token != "0" && token != "1") {
        checksum = token;
      }
    }
  } catch (const std::exception &) {
    return false;
  }

  if (extended) {
    stat = new XrdCl::StatInfo(id, size, flags, modTime, changeTime, accessTime,
                               mode, owner, group, checksum);
  } else {
    stat = new XrdCl::StatInfo(id, size, flags, modTime);
  }
  return true;
}

bool parseLegacyListLine(const std::string &line, std::string &hostAddress,
                         std::string &name, XrdCl::StatInfo *&stat) {
  const auto idx1 = line.find('\t');
  const auto idx2 = line.find('\t', idx1 + 1);
  if (idx1 == std::string::npos || idx2 == std::string::npos) {
    return false;
  }

  try {
    hostAddress = urlDecode(line.substr(0, idx1));
    name = urlDecode(line.substr(idx1 + 1, idx2 - idx1 - 1));
  } catch (const std::exception &) {
    return false;
  }

  return parseLegacyStatLine(line.substr(idx2 + 1), stat);
}

} // namespace

bool isCacheEntryExpired(uint64_t created, uint64_t ttlSeconds) {
  if (ttlSeconds == 0 || created == 0) {
    return false;
  }
  const uint64_t now = static_cast<uint64_t>(time(nullptr));
  return now > created && (now - created) >= ttlSeconds;
}

std::string serializeListEntry(const std::string &hostAddress,
                               const std::string &name,
                               const XrdCl::StatInfo *stat) {
  return urlEncode(hostAddress) + '\t' + urlEncode(name) + '\t' +
         statEncode(stat) + '\n';
}

std::tuple<std::string, std::string, XrdCl::StatInfo *>
deserializeListEntry(const std::string &line) {
  const auto idx1 = line.find('\t');
  const auto idx2 = line.find('\t', idx1 + 1);
  if (idx1 == std::string::npos || idx2 == std::string::npos) {
    return {std::string(), std::string(), nullptr};
  }

  try {
    std::string hostAddress = urlDecode(line.substr(0, idx1));
    std::string name = urlDecode(line.substr(idx1 + 1, idx2 - idx1 - 1));
    XrdCl::StatInfo *stat = statDecode(line.substr(idx2 + 1));
    if (!stat &&
        !parseLegacyListLine(line, hostAddress, name, stat)) {
      return {std::string(), std::string(), nullptr};
    }
    return {hostAddress, name, stat};
  } catch (const std::exception &) {
    return {std::string(), std::string(), nullptr};
  }
}

std::string formatListHeader(XrdCl::DirListFlags::Flags flags, uint64_t created) {
  std::ostringstream oss;
  oss << "# journalcache-list-v2 flags="
      << static_cast<uint32_t>(flags) << " created=" << created << '\n';
  return oss.str();
}

ListCacheHeader parseListHeader(const std::string &line) {
  ListCacheHeader header;
  if (line.rfind("# journalcache-list-v2 ", 0) != 0) {
    return header;
  }

  header.version = 2;
  header.valid = true;

  std::istringstream iss(line.substr(23));
  std::string token;
  while (iss >> token) {
    const auto eq = token.find('=');
    if (eq == std::string::npos) {
      continue;
    }
    const std::string key = token.substr(0, eq);
    const std::string value = token.substr(eq + 1);
    try {
      if (key == "flags") {
        header.flags =
            static_cast<XrdCl::DirListFlags::Flags>(std::stoul(value));
      } else if (key == "created") {
        header.created = std::stoull(value);
      }
    } catch (const std::exception &) {
      header.valid = false;
      return header;
    }
  }
  return header;
}

XrdCl::DirectoryList *loadDirectoryList(const std::string &path,
                                        XrdCl::DirListFlags::Flags expectedFlags,
                                        uint64_t ttlSeconds) {
  std::ifstream file(path);
  if (!file) {
    return nullptr;
  }

  ListCacheHeader header;
  std::string line;
  bool legacy = true;

  if (std::getline(file, line)) {
    header = parseListHeader(line);
    if (header.valid) {
      legacy = false;
      if (header.flags != expectedFlags) {
        return nullptr;
      }
      if (isCacheEntryExpired(header.created, ttlSeconds)) {
        return nullptr;
      }
    } else if (!line.empty() && line[0] != '#') {
      file.clear();
      file.seekg(0);
    } else if (!line.empty()) {
      return nullptr;
    }
  }

  auto *list = new XrdCl::DirectoryList();
  while (std::getline(file, line)) {
    if (line.empty() || line[0] == '#') {
      continue;
    }

    if (legacy && list->GetSize() == 0 &&
        line.rfind("# journalcache-list-v2", 0) == 0) {
      header = parseListHeader(line);
      if (!header.valid || header.flags != expectedFlags ||
          isCacheEntryExpired(header.created, ttlSeconds)) {
        delete list;
        return nullptr;
      }
      legacy = false;
      continue;
    }

    auto [addr, name, statinfo] = deserializeListEntry(line);
    if (!statinfo) {
      continue;
    }
    list->Add(new XrdCl::DirectoryList::ListEntry(addr, name, statinfo));
  }

  if (file.bad() || list->GetSize() == 0) {
    delete list;
    return nullptr;
  }

  if (legacy && expectedFlags != XrdCl::DirListFlags::Stat) {
    delete list;
    return nullptr;
  }

  return list;
}

bool saveDirectoryList(const std::string &path, XrdCl::DirectoryList *dirList,
                       XrdCl::DirListFlags::Flags flags) {
  const std::string tmpPath = path + ".tmp";
  std::error_code ec;
  std::filesystem::create_directories(std::filesystem::path(path).parent_path(),
                                      ec);
  std::ofstream file(tmpPath, std::ios::trunc);
  if (!file) {
    return false;
  }

  const uint64_t created = static_cast<uint64_t>(time(nullptr));
  file << formatListHeader(flags, created);

  for (auto entry = dirList->Begin(); entry != dirList->End(); ++entry) {
    file << serializeListEntry((*entry)->GetHostAddress(), (*entry)->GetName(),
                               (*entry)->GetStatInfo());
  }

  file.close();
  if (file.fail()) {
    std::error_code ec;
    std::filesystem::remove(tmpPath, ec);
    return false;
  }

  try {
    std::filesystem::rename(tmpPath, path);
    return true;
  } catch (const std::filesystem::filesystem_error &) {
    std::error_code ec;
    std::filesystem::remove(tmpPath, ec);
    return false;
  }
}

bool loadStatInfo(const std::string &path, uint64_t ttlSeconds,
                  XrdCl::StatInfo *&stat) {
  stat = nullptr;
  std::ifstream file(path);
  if (!file) {
    return false;
  }

  std::string line;
  if (!std::getline(file, line)) {
    return false;
  }

  uint64_t created = 0;
  if (line.rfind("# journalcache-stat-v1 ", 0) == 0) {
    const auto pos = line.find("created=");
    if (pos != std::string::npos) {
      try {
        created = std::stoull(line.substr(pos + 8));
      } catch (const std::exception &) {
        return false;
      }
    }
    if (isCacheEntryExpired(created, ttlSeconds)) {
      return false;
    }
    if (!std::getline(file, line)) {
      return false;
    }
  }

  stat = statDecode(line);
  if (!stat) {
    XrdCl::StatInfo *legacyStat = nullptr;
    if (!parseLegacyStatLine(line, legacyStat)) {
      return false;
    }
    stat = legacyStat;
  }
  return true;
}

bool saveStatInfo(const std::string &path, const XrdCl::StatInfo *statInfo) {
  const std::string tmpPath = path + ".tmp";
  std::error_code ec;
  std::filesystem::create_directories(std::filesystem::path(path).parent_path(),
                                      ec);
  std::ofstream file(tmpPath, std::ios::trunc);
  if (!file) {
    return false;
  }

  const uint64_t created = static_cast<uint64_t>(time(nullptr));
  file << "# journalcache-stat-v1 created=" << created << '\n';
  file << statEncode(statInfo) << '\n';
  file.close();

  if (file.fail()) {
    std::error_code ec;
    std::filesystem::remove(tmpPath, ec);
    return false;
  }

  try {
    std::filesystem::rename(tmpPath, path);
    return true;
  } catch (const std::filesystem::filesystem_error &) {
    std::error_code ec;
    std::filesystem::remove(tmpPath, ec);
    return false;
  }
}

} // namespace JournalCache
