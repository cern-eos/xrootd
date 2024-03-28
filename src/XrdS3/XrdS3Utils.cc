//
// Created by segransm on 11/3/23.
//

#include "XrdS3Utils.hh"

#include <sys/stat.h>

#include <cstring>
#include <iomanip>
#include <sstream>

#include "XrdPosix/XrdPosixExtern.hh"

namespace S3 {

std::string S3Utils::UriEncode(const std::bitset<256> &encoder,
                               const std::string &str) {
  std::ostringstream res;

  for (const auto &c : str) {
    if (encoder[(unsigned char)c]) {
      res << c;
    } else {
      res << "%" << std::uppercase << std::hex << std::setw(2)
          << std::setfill('0') << (unsigned int)(unsigned char)c;
    }
  }

  return res.str();
}

std::string S3Utils::UriDecode(const std::string &str) {
  size_t i = 0;
  std::string res;
  res.reserve(str.size());
  unsigned char v1, v2;

  while (i < str.size()) {
    if (str[i] == '%' && i + 2 < str.size()) {
      v1 = mDecoder[(unsigned char)str[i + 1]];
      v2 = mDecoder[(unsigned char)str[i + 2]];
      if ((v1 | v2) == 0xFF) {
        res += str[i++];
      } else {
        res += (char)((v1 << 4) | v2);
        i += 3;
      }
    } else
      res += str[i++];
  }

  return res;
}

S3Utils::S3Utils() {
  for (int i = 0; i < 256; ++i) {
    mEncoder[i] = isalnum(i) || i == '-' || i == '.' || i == '_' || i == '~';
    mObjectEncoder[i] = mEncoder[i];

    if (isxdigit(i)) {
      mDecoder[i] = (i < 'A') ? (i - '0') : ((i & ~0x20) - 'A' + 10);
    } else {
      mDecoder[i] = 0xFF;
    }
  }
  mObjectEncoder['/'] = true;
}

// AWS uses a different uri encoding for objects during canonical signature
// calculation.
std::string S3Utils::UriEncode(const std::string &str) {
  return UriEncode(mEncoder, str);
}

std::string S3Utils::ObjectUriEncode(const std::string &str) {
  return UriEncode(mObjectEncoder, str);
}

void S3Utils::TrimAll(std::string &str) {
  // Trim leading non-letters
  size_t n = 0;
  while (n < str.size() && isspace(str[n])) {
    n++;
  }
  str.erase(0, n);

  if (str.empty()) {
    return;
  }

  // Trim trailing non-letters
  n = str.size() - 1;
  while (n > 0 && isspace(str[n])) {
    n--;
  }
  str.erase(n + 1);

  // Squash sequential spaces
  n = 0;
  while (n < str.size()) {
    size_t x = 0;
    while (n + x < str.size() && isspace(str[n + x])) {
      x++;
    }
    if (x > 1) {
      str.erase(n, x - 1);
    }
    n++;
  }
}

bool S3Utils::MapHasKey(const std::map<std::string, std::string> &map,
                        const std::string &key) {
  return (map.find(key) != map.end());
}

bool S3Utils::MapHasEntry(const std::map<std::string, std::string> &map,
                          const std::string &key, const std::string &val) {
  auto it = map.find(key);

  return (it != map.end() && it->second == val);
}

bool S3Utils::MapEntryStartsWith(const std::map<std::string, std::string> &map,
                                 const std::string &key,
                                 const std::string &val) {
  auto it = map.find(key);

  return (it != map.end() && it->second.substr(0, val.length()) == val);
}

std::string S3Utils::timestampToIso8016(const std::string &t) {
  try {
    return timestampToIso8016(std::stol(t));
  } catch (std::exception &e) {
    return "";
  }
}

std::string S3Utils::timestampToIso8016(const time_t &t) {
  struct tm *date = gmtime(&t);
  return timestampToIso8016(date);
}

std::string S3Utils::timestampToIso8016(const struct tm *t) {
  char date_iso8601[17]{};
  if (!t || strftime(date_iso8601, 17, "%Y%m%dT%H%M%SZ", t) != 16) {
    return "";
  }
  return date_iso8601;
}

int S3Utils::makePath(char *path, mode_t mode) {
  char *next_path = path + 1;
  struct stat buf;

  // Typically, the path exists. So, do a quick check before launching into it
  //
  if (!XrdPosix_Stat(path, &buf)) {
    if (S_ISDIR(buf.st_mode)) {
      return 0;
    }
    return ENOTDIR;
  }

  // Start creating directories starting with the root
  //
  while ((next_path = index(next_path, int('/')))) {
    *next_path = '\0';
    if (XrdPosix_Mkdir(path, mode))
      if (errno != EEXIST) return errno;
    *next_path = '/';
    next_path = next_path + 1;
  }
  if (XrdPosix_Mkdir(path, mode))
    if (errno != EEXIST) return errno;
  // All done
  //
  return 0;
}

void S3Utils::RmPath(std::filesystem::path path,
                    const std::filesystem::path &stop) {
  while (path != stop && !XrdPosix_Rmdir(path.c_str())) {
    path = path.parent_path();
  }
}

std::string S3Utils::GetXattr(const std::filesystem::path &path,
                              const std::string &key) {
  std::vector<char> res;

  auto ret =
      XrdPosix_Getxattr(path.c_str(), ("user.s3." + key).c_str(), nullptr, 0);

  if (ret == -1) {
    return {};
  }

  // Add a terminating '\0'
  res.resize(ret + 1);
  XrdPosix_Getxattr(path.c_str(), ("user.s3." + key).c_str(), res.data(),
                    res.size() - 1);

  return {res.data()};
}

#include <sys/xattr.h>
#define XrdPosix_Setxattr setxattr
// TODO: Replace by XrdPosix_Setxattr once implemented

int S3Utils::SetXattr(const std::filesystem::path &path, const std::string &key,
                      const std::string &value, int flags) {
  return XrdPosix_Setxattr(path.c_str(), ("user.s3." + key).c_str(),
                           value.c_str(), value.size(), flags);
}

#undef XrdPosix_Setxattr

bool S3Utils::IsDirEmpty(const std::filesystem::path &path) {
  auto dir = XrdPosix_Opendir(path.c_str());

  if (dir == nullptr) {
    return false;
  }

  int n = 0;
  while (XrdPosix_Readdir(dir) != nullptr) {
    if (++n > 2) {
      break;
    }
  }

  XrdPosix_Closedir(dir);
  return n <= 2;
}

int S3Utils::DirIterator(const std::filesystem::path &path,
                         const std::function<void(dirent *)> &f) {
  auto dir = XrdPosix_Opendir(path.c_str());

  if (dir == nullptr) {
    return 1;
  }

  dirent *entry;
  while ((entry = XrdPosix_Readdir(dir)) != nullptr) {
    f(entry);
  }

  XrdPosix_Closedir(dir);
  return 0;
}

}  // namespace S3
