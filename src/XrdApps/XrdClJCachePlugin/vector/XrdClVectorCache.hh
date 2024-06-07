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

/*----------------------------------------------------------------------------*/
#include <iostream>
#include <vector>
#include <utility>
#include <openssl/sha.h>  // For SHA-256
#include <sstream>
#include <iomanip>
#include <cstring>        // For std::memcpy
#include <string>
#include <fstream>        // For file operations
#include <sys/stat.h>     // For checking if file exists
/*----------------------------------------------------------------------------*/
#include "XrdCl/XrdClXRootDResponses.hh"
//----------------------------------------------------------------------------
//! VectorCache class caching readv buffers on a filesystem
//----------------------------------------------------------------------------

namespace XrdCl {

class VectorCache {
public:
    VectorCache(const XrdCl::ChunkList chunks, const std::string& name, const char* data, const std::string& prefix, bool verbose=false)
        : chunks(chunks), name(name), data(data), prefix(prefix), verbose(verbose) {}
    
    std::pair<std::string, std::string> computeHash() const;
    bool store() const;
    bool retrieve() const;

    static std::string computeSHA256(const std::vector<unsigned char>& data);
    static std::string computeSHA256(const std::string& data);
    static bool ensureLastSubdirectoryExists(const std::string& dirName);

private:
    XrdCl::ChunkList chunks;
    std::string name;
    const char* data;
    std::string prefix;
    bool verbose;

    std::vector<unsigned char> serializeVector() const;
};

} // namespace XrdCl