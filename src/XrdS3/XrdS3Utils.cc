//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Mano Segransan / CERN EOS Project <andreas.joachim.peters@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

//------------------------------------------------------------------------------
#include "XrdS3Utils.hh"
//------------------------------------------------------------------------------
#include <sys/stat.h>
#include <sys/xattr.h>
#include <cstring>
#include <iomanip>
#include <sstream>
//------------------------------------------------------------------------------
#include "XrdPosix/XrdPosixExtern.hh"
//------------------------------------------------------------------------------

namespace S3 {

//------------------------------------------------------------------------------
//! Encode a string according to RFC 3986
//!
//! @param str The string to encode
//! @return The encoded string
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! Decode a string according to RFC 3986
//!
//! @param str The string to decode
//! @return The decoded string
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! Constructor
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! Encode a string according to the AWS URI encoding rules
//!
//! @param str The string to encode
//! @return The encoded string
//------------------------------------------------------------------------------
std::string S3Utils::UriEncode(const std::string &str) {
  return UriEncode(mEncoder, str);
}

//------------------------------------------------------------------------------
//! Encode a string for objects according to the AWS URI encoding rules
//!
//! @param str The string to encode
//! @return The encoded string
//------------------------------------------------------------------------------
std::string S3Utils::ObjectUriEncode(const std::string &str) {
  return UriEncode(mObjectEncoder, str);
}

//------------------------------------------------------------------------------
//! trim all whitespace from a string
//!
//! @param str The string to trim
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! Check if a map has a key
//!
//! @param map The map to check
//! @param key The key to check for
//! @return True if the map has the key, false otherwise
//------------------------------------------------------------------------------
bool S3Utils::MapHasKey(const std::map<std::string, std::string> &map,
                        const std::string &key) {
  return (map.find(key) != map.end());
}

//------------------------------------------------------------------------------
//! Check if a map has a key and a value
//!
//! @param map The map to check
//! @param key The key to check for
//! @param val The value to check for
//! @return True if the map has the key and the value, false otherwise
//------------------------------------------------------------------------------
bool S3Utils::MapHasEntry(const std::map<std::string, std::string> &map,
                          const std::string &key, const std::string &val) {
  auto it = map.find(key);

  return (it != map.end() && it->second == val);
}

//------------------------------------------------------------------------------
//! Check if a map has a key and a value that starts with a given value
//!
//! @param map The map to check
//! @param key The key to check for
//! @param val The value to check for
//! @return True if the map has the key and the value, false otherwise
//------------------------------------------------------------------------------
bool S3Utils::MapEntryStartsWith(const std::map<std::string, std::string> &map,
                                 const std::string &key,
                                 const std::string &val) {
  auto it = map.find(key);

  return (it != map.end() && it->second.substr(0, val.length()) == val);
}

//------------------------------------------------------------------------------
//! string timestamp to iso8016 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToIso8016(const std::string &t) {
  try {
    return timestampToIso8016(std::stol(t));
  } catch (std::exception &e) {
    return "";
  }
}

//------------------------------------------------------------------------------
//! timestamp to iso8016 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToIso8016(const time_t &t) {
  struct tm *date = gmtime(&t);
  return timestampToIso8016(date);
}

//------------------------------------------------------------------------------
//! timestamp to iso8016 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToIso8016(const struct tm *t) {
  char date_iso8601[17]{};
  if (!t || strftime(date_iso8601, 17, "%Y%m%dT%H%M%SZ", t) != 16) {
    return "";
  }
  return date_iso8601;
}

//------------------------------------------------------------------------------
//! make a path
//!
//! @param path The path to create
//! @param mode The mode to use
//! @return 0 on success, an error code otherwise
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! delete a path
//!
//! @param path The path to delete
//! @param stop The path to stop at
//------------------------------------------------------------------------------
void S3Utils::RmPath(std::filesystem::path path,
                    const std::filesystem::path &stop) {
  while (path != stop && !XrdPosix_Rmdir(path.c_str())) {
    path = path.parent_path();
  }
}

//------------------------------------------------------------------------------
//! get an xattr
//!
//! @param path The path to get the xattr from
//! @param key The key to get
//! @return The xattr value
//------------------------------------------------------------------------------
std::string S3Utils::GetXattr(const std::filesystem::path &path,
                              const std::string &key) {
  std::vector<char> res;

// TODO: Replace with the real XrdPosix_Getxattr once implemented.
#include "XrdS3XAttr.hh"
#define XrdPosix_Getxattr getxattr
  
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
#include "XrdS3XAttr.hh"
#define XrdPosix_Setxattr setxattr
// TODO: Replace by XrdPosix_Setxattr once implemented

int S3Utils::SetXattr(const std::filesystem::path &path, const std::string &key,
                      const std::string &value, int flags) {
  return XrdPosix_Setxattr(path.c_str(), ("user.s3." + key).c_str(),
                           value.c_str(), value.size(), flags);
}

#undef XrdPosix_Setxattr

//------------------------------------------------------------------------------
//! check if a directory is empty
//!
//! @param path The path to check
//! @return True if the directory is empty, false otherwise
//------------------------------------------------------------------------------
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

//------------------------------------------------------------------------------
//! iterate over a directory
//!
//! @param path The path to iterate over
//! @param f The function to call for each entry
//! @return 0 on success, 1 on error
//------------------------------------------------------------------------------
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
