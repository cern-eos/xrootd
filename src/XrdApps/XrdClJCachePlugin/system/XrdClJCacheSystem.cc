//------------------------------------------------------------------------------
// Copyright (c) 2024 by European Organization for Nuclear Research (CERN)
// Author: Andreas-Joachim Peters <andreas.joachim.peters@cern.ch>
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

#include "system/XrdClJCacheSystem.hh"
#include "file/Hierarchy.hh"
#include "file/XrdClJCacheFile.hh"
#include "handler/XrdClJCacheDirListHandler.hh"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "XrdCl/XrdClMessageUtils.hh"

namespace XrdCl {

//------------------------------------------------------------------------
//! @see XrdCl::FileSystem::Stat
//------------------------------------------------------------------------
XRootDStatus JCacheSystem::Stat(const std::string &path,
                                ResponseHandler *handler, uint16_t timeout) {
  return pSystem->Stat(path, handler, timeout);
}

//------------------------------------------------------------------------
//! @see XrdCl::FileSystem::DirlList
//------------------------------------------------------------------------
XRootDStatus JCacheSystem::DirList(const std::string &path,
                                   DirListFlags::Flags flags,
                                   ResponseHandler *handler, uint16_t timeout) {
  XrdCl::URL url(mUrl);
  std::string HostAddress =
      url.GetHostName() + ":" + std::to_string(url.GetPort());

  std::string ListingDir =
      XrdCl::JCacheFile::sCachePath + HostAddress + "/" + path;
  std::string ListingPath = ListingDir + "/.jcache_list";
  struct stat buf;

  // always require stats when listing
  flags |= DirListFlags::Stat;

  if (!::stat(ListingPath.c_str(), &buf)) {
    // load the listing from the cache
    XrdCl::DirectoryList *response = LoadDirList(ListingPath);
    if (response) {
      AnyObject *obj = new AnyObject();
      obj->Set(response);
      XRootDStatus *ret_st = new XRootDStatus(XRootDStatus(stOK, 0));
      handler->HandleResponse(ret_st, obj);
      return XRootDStatus(stOK, 0);
    }
    // fall back to the remote listing
  }

  if (!JCache::makeHierarchy(ListingPath)) {
    auto st = XRootDStatus(stError, errOSError);
    std::cerr << "error: unable to create cache directory: " << ListingDir
              << std::endl;
    return st;
  }
  // get the listing

  auto lhandler = new JCacheDirListHandler(handler, this, ListingPath);

  XRootDStatus st = pSystem->DirList(path, flags, lhandler, timeout);
  if (!st.IsOK()) {
    std::cerr << "error: unable to get listing: " << path << std::endl;
  }

  return st;
}

//------------------------------------------------------------------------
//! load a persisted listing from the cache
//------------------------------------------------------------------------
DirectoryList *JCacheSystem::LoadDirList(const std::string &path) {
  std::ifstream file(path);

  // Check if the file was opened successfully
  if (!file) {
    std::cerr << "Error opening file: " << path << std::endl;
    return nullptr;
  }

  DirectoryList *list = new DirectoryList();

  std::string line;
  while (std::getline(file, line)) {
    // Process the line (for now, just print it)
    auto [addr, name, statinfo] = Deserialize(line);
    DirectoryList::ListEntry *e =
        new DirectoryList::ListEntry(addr, name, statinfo);
    list->Add(e);
  }

  // Close the file
  file.close();

  // Optional: Check for errors during reading
  if (file.bad()) {
    std::cerr << "Error reading from file: " << path << std::endl;
    delete list;
    return nullptr;
  }

  return list;
}

//------------------------------------------------------------------------
//! save a listing to the cache
//------------------------------------------------------------------------
bool JCacheSystem::SaveDirList(const std::string &path,
                               DirectoryList *dirList) {
  // std::cerr << "saving .. " << std::endl;

  std::string tmppath = path + ".tmp";

  std::ofstream file(tmppath, std::ios::trunc);

  // Check if the file was opened successfully
  if (!file) {
    std::cerr << "Error opening file: " << tmppath << std::endl;
    std::cerr << "Error message: " << std::strerror(errno) << std::endl;
    return false;
  }

  for (auto entry = dirList->Begin(); entry != dirList->End(); ++entry) {
    // std::cout << (*entry)->GetName() << "host:" << (*entry)->GetHostAddress()
    //           << std::endl;
    file << Serialize((*entry)->GetHostAddress(), (*entry)->GetName(),
                      (*entry)->GetStatInfo());
  }

  // Close the file
  file.close();

  // Optional: Check for write errors
  if (file.fail()) {
    std::cerr << "Error writing to file: " << tmppath << std::endl;
    return false;
  }

  // atomic replcae
  try {
    // Rename (or move) the file
    std::filesystem::rename(tmppath, path);
    return true;
  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Error renaming file: " << e.what() << std::endl;
    return false;
  }
}

//------------------------------------------------------------------------
//! Serialize a StatInfo object to a string
//------------------------------------------------------------------------
std::string JCacheSystem::Serialize(const std::string &hostaddress,
                                    const std::string &name,
                                    XrdCl::StatInfo *stat) {

  auto url_encode = [](const std::string &value) -> std::string {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (char c : value) {
      // Keep alphanumeric and other accepted characters intact
      if (isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' ||
          c == '.' || c == '~') {
        escaped << c;
      } else {
        // Any other characters are percent-encoded
        escaped << '%' << std::setw(2) << std::uppercase
                << static_cast<int>(static_cast<unsigned char>(c));
      }
    }
    return escaped.str();
  };

  auto stat_encode = [](const XrdCl::StatInfo *stat) -> std::string {
    if (!stat)
      return "";
    std::ostringstream oss;
    oss << stat->GetId() << '\t' << stat->GetSize() << '\t' << stat->GetFlags()
        << '\t' << stat->GetModTime() << '\t' << stat->GetAccessTime() << '\t'
        << stat->GetChangeTime() << '\t';

    if (stat->ExtendedFormat()) {
      oss << stat->GetModeAsString() << '\t' << stat->GetOwner() << '\t'
          << stat->GetGroup() << '\t' << stat->HasChecksum() << '\t'
          << stat->GetChecksum() << '\t';
    }
    return oss.str();
  };

  std::string encoded = url_encode(hostaddress) + "\t" + url_encode(name) +
                        "\t" + stat_encode(stat) + "\n";
  return encoded;
}

//------------------------------------------------------------------------
//! Deserialize a string to a StatInfo object
//------------------------------------------------------------------------
std::tuple<std::string, std::string, XrdCl::StatInfo *>
JCacheSystem::Deserialize(const std::string &data) {
  // Define the URL decode lambda function
  auto url_decode = [](const std::string &value) -> std::string {
    auto decode_percent = [](const std::string &hex) -> char {
      int hex_value;
      std::istringstream hex_stream(hex);
      if (!(hex_stream >> std::hex >> hex_value)) {
        throw std::invalid_argument("Invalid URL encoding");
      }
      return static_cast<char>(hex_value);
    };

    std::ostringstream decoded;
    for (size_t i = 0; i < value.length(); ++i) {
      if (value[i] == '%') {
        if (i + 2 >= value.length()) {
          throw std::invalid_argument("Invalid URL encoding");
        }
        decoded << decode_percent(value.substr(i + 1, 2));
        i += 2;
      } else if (value[i] == '+') {
        decoded << ' ';
      } else {
        decoded << value[i];
      }
    }
    return decoded.str();
  };

  auto stat_decode = [](const std::string &stat_str) -> XrdCl::StatInfo * {
    std::istringstream iss(stat_str);
    std::string token;
    std::string id;
    uint64_t size;
    uint32_t flags;
    uint64_t modTime;
    uint64_t changeTime;
    uint64_t accessTime;
    bool extended;
    std::string mode;
    std::string owner;
    std::string group;
    std::string checksum;

    std::getline(iss, id, '\t');
    std::getline(iss, token, '\t');
    size = std::stoull(token);
    std::getline(iss, token, '\t');
    flags = std::stoul(token);
    std::getline(iss, token, '\t');
    modTime = std::stoull(token);
    std::getline(iss, token, '\t');
    accessTime = std::stoull(token);
    std::getline(iss, token, '\t');
    changeTime = std::stoull(token);
    std::getline(iss, token, '\t');
    if (token.length()) {
      extended = true;
      mode = token;
      std::getline(iss, token, '\t');
      owner = token;
      std::getline(iss, token, '\t');
      group = token;
      std::getline(iss, token, '\t');
      if (token.length()) {
        checksum = token;
      }
    }
    //    std::cerr << "id:" << id <<  " size:" << size << " flags:" << flags <<
    //    " modtime:" << modTime << std::endl;

    XrdCl::StatInfo *stat;
    if (extended) {
      stat = new XrdCl::StatInfo(id, size, flags, modTime, changeTime,
                                 accessTime, mode, owner, group, checksum);
    } else {
      stat = new XrdCl::StatInfo(id, size, flags, modTime);
    }

    return stat;
  };
  auto idx1 = data.find('\t');
  auto idx2 = data.find('\t', idx1 + 1);
  std::string hostaddress = data.substr(0, idx1);
  std::string name = data.substr(idx1 + 1, idx2 - idx1 - 1);
  std::string stat_str = data.substr(idx2 + 1);
  //  std::cerr << stat_str << std::endl;
  return std::make_tuple(url_decode(hostaddress), url_decode(name),
                         stat_decode(stat_str));
}

} // namespace XrdCl