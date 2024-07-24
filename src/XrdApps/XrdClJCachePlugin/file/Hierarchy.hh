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

#pragma once

#include <filesystem>
#include <string>
#include <sys/stat.h>

namespace JCache {

bool makeHierarchy(const std::string &path) {
  std::filesystem::path dirPath(path);

  try {
    // Create the directories with 755 permissions
    if (!std::filesystem::exists(dirPath)) {
      std::filesystem::create_directories(dirPath.parent_path());

      for (auto &p : std::filesystem::recursive_directory_iterator(
               dirPath.parent_path())) {
        if (std::filesystem::is_directory(p)) {
          chmod(p.path().c_str(), 0755);
        }
      }
    }
    return true;
  } catch (const std::filesystem::filesystem_error &e) {
    // Handle error
    std::cerr << "Filesystem error: " << e.what() << std::endl;
    return false;
  } catch (const std::exception &e) {
    // Handle other errors
    std::cerr << "Error: " << e.what() << std::endl;
    return false;
  }
}

} // namespace JCache
