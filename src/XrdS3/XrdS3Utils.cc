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
#include <sys/types.h>
#include <fcntl.h>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
//------------------------------------------------------------------------------
#include "XrdPosix/XrdPosixExtern.hh"
//------------------------------------------------------------------------------

namespace S3 {

std::atomic<bool> S3Utils::sFileAttributes=false; // this indicates if we use native xattr (false) or xattr in hidden files

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
//! string timestamp to iso8601 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToIso8601(const std::string &t) {
  try {
    return timestampToIso8601(std::stol(t));
  } catch (std::exception &e) {
    return "";
  }
}

//------------------------------------------------------------------------------
//! timestamp to iso8601 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToIso8601(const time_t &t) {
  struct tm *date = gmtime(&t);
  return timestampToIso8601(date);
}

//------------------------------------------------------------------------------
//! timestamp to iso8016 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToIso8601(const struct tm *t) {
  char date_iso8601[17]{};
  if (!t || strftime(date_iso8601, 17, "%Y%m%dT%H%M%SZ", t) != 16) {
    return "";
  }
  return date_iso8601;
}

//------------------------------------------------------------------------------
//! string timestamp to RFC7231 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToRFC7231(const std::string &t) {
  try {
    return timestampToRFC7231(std::stol(t));
  } catch (std::exception &e) {
    return "";
  }
}

//------------------------------------------------------------------------------
//! timestamp to RFC7231 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToRFC7231(const time_t &t) {
  struct tm *date = gmtime(&t);
  return timestampToRFC7231(date);
}

//------------------------------------------------------------------------------
//! timestamp to iso8016 format
//!
//! @param t The timestamp to convert
//! @return The converted timestamp
//------------------------------------------------------------------------------
std::string S3Utils::timestampToRFC7231(const struct tm *gmt) {

  // Create a buffer to hold the formatted date string
  char buffer[30];
  // Format tm structure to RFC 7231 format
  std::strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S GMT", gmt);
  return std::string(buffer);
}


//------------------------------------------------------------------------------
//! extract bucket name out of virtual host
//! @param FQHN
//! @return the name of the bucket or an empty string
//------------------------------------------------------------------------------
std::string S3Utils::getBucketName(std::string host)
{
  auto ndot = std::count(host.begin(), host.end(), '.');
  if ( ndot > 2)  {
    // return bucket name
    size_t pos=host.find('.');
    for ( auto i=0; i< (ndot-3); i++) {
      host.erase(0,pos);
      pos = host.find('.');
    }
    return host.substr(0, pos);
  } else {
    return "";
  }
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

//------------------------------------------------------------------------------
//! Scandir using a DirIterator
//!
//! @param path the full path to scan
//! @param the basepath to store into entries
//! @param the vector with BasicPath entries to return
//! @return number of items in directory
//------------------------------------------------------------------------------
int
S3Utils::ScanDir(const std::filesystem::path& fullpath, const std::filesystem::path& basepath, std::vector<S3Utils::BasicPath>& entries) {
  std::map<std::string, BasicPath> sentries;
  // the filler function
  auto get_entry = [&sentries, &basepath](dirent *entry) {
		     sentries[entry->d_name].name = entry->d_name;
		     sentries[entry->d_name].base = basepath.string();
		     sentries[entry->d_name].d_type = entry->d_type;
		   };
  S3Utils::DirIterator(fullpath, get_entry);

  for ( auto i:sentries ) {
    entries.push_back(i.second);
  }
  return sentries.size();
}

S3Utils::FileAttributes::FileAttributes(const std::string& changelogFile) : changelogFile(changelogFile) {
  loadChangelog();
}

std::string
S3Utils::FileAttributes::getattr(const std::string& name) {
  auto it = attributes.find(name);
  if (it != attributes.end()) {
    return it->second;
  }
  return "";
}

void
S3Utils::FileAttributes::setattr(const std::string& name, const std::string& value, bool persist) {
  attributes[name] = value;
  if (persist) {
    saveChangelog("set", name, value);
  }
}

std::vector<std::string>
S3Utils::FileAttributes::listattr() {
  std::vector<std::string> keys;
  for (const auto& pair : attributes) {
    keys.push_back(pair.first);
  }
  return keys;
}

void
S3Utils::FileAttributes::rmattr(const std::string& name) {
  attributes.erase(name);
  saveChangelog("remove", name);
}

void
S3Utils::FileAttributes::trimChangelog() {
  int tempFileDescriptor = open("temp_changelog.txt", O_CREAT | O_WRONLY | O_TRUNC, 0666);
  if (tempFileDescriptor == -1) {
    std::cerr << "Failed to open temporary changelog file." << std::endl;
    return;
  }
  for (const auto& pair : attributes) {
    saveToTempFile(tempFileDescriptor, "set", pair.first, pair.second);
  }
  close(tempFileDescriptor);
  if (unlink(changelogFile.c_str()) == -1) {
    std::cerr << "Failed to unlink original changelog file." << std::endl;
    return;
  }
  if (rename("temp_changelog.txt", changelogFile.c_str()) == -1) {
      std::cerr << "Failed to rename temporary changelog file." << std::endl;
      return;
  }
}

void
S3Utils::FileAttributes::loadChangelog() {
  int changelogFileDescriptor = open(changelogFile.c_str(), O_RDONLY);
  if (changelogFileDescriptor == -1) {
    std::cerr << "Failed to open changelog file." << std::endl;
    return;
  }

  char buffer[4096];
  ssize_t bytesRead;
  std::string jsonBuffer;
  while ((bytesRead = read(changelogFileDescriptor, buffer, sizeof(buffer) - 1)) > 0) {
    buffer[bytesRead] = '\0';  // Null-terminate the buffer
    jsonBuffer += buffer;
  }

  close(changelogFileDescriptor);

  Json::CharReaderBuilder readerBuilder;
  Json::CharReader* reader = readerBuilder.newCharReader();
  Json::Value root;
  std::string errs;
  if (!reader->parse(jsonBuffer.c_str(), jsonBuffer.c_str() + jsonBuffer.length(), &root, &errs)) {
    std::cerr << "Failed to parse changelog file: " << errs << std::endl;
    delete reader;
    return;
  }

  delete reader;

  // Process parsed JSON data
  for (const auto& entry : root) {
    std::string action = entry["action"].asString();
    std::string name = entry["name"].asString();
    if (action == "set") {
      std::string value = entry["value"].asString();
      attributes[name] = value;
    } else if (action == "remove") {
      attributes.erase(name);
    }
  }
}

void
S3Utils::FileAttributes::saveChangelog(const std::string& action, const std::string& name, const std::string& value) {
  Json::Value entry;
  entry["action"] = action;
  entry["name"] = name;
  if (!value.empty()) {
    entry["value"] = value;
  }
  std::string entryStr = entry.toStyledString() + "\n";

  int changelogFileDescriptor = open(changelogFile.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0666);
  if (changelogFileDescriptor == -1) {
    std::cerr << "Failed to open changelog file." << std::endl;
    return;
  }

  if (write(changelogFileDescriptor, entryStr.c_str(), entryStr.size()) == -1) {
    std::cerr << "Failed to write to changelog file." << std::endl;
  }

  close(changelogFileDescriptor);
}

void
S3Utils::FileAttributes::saveToTempFile(int tempFileDescriptor, const std::string& action, const std::string& name, const std::string& value) {
  Json::Value entry;
  entry["action"] = action;
  entry["name"] = name;
  if (!value.empty()) {
    entry["value"] = value;
  }
  std::string entryStr = entry.toStyledString() + "\n";

  if (write(tempFileDescriptor, entryStr.c_str(), entryStr.size()) == -1) {
    std::cerr << "Failed to write to temporary changelog file." << std::endl;
  }
}

}  // namespace S3
