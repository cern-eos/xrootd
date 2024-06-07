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

/*----------------------------------------------------------------------------*/
#include "vector/XrdClVectorCache.hh"
#include <filesystem>
/*----------------------------------------------------------------------------*/

namespace fs = std::filesystem;

namespace XrdCl {

//----------------------------------------------------------------------------
//! serialize a vector into a buffer
//----------------------------------------------------------------------------
std::vector<unsigned char> VectorCache::serializeVector() const {
    std::vector<unsigned char> serializedData;
    for (const auto& i : chunks) {
        uint64_t o = i.GetOffset();
        uint64_t n = i.GetLength();
        unsigned char buffer[sizeof(uint64_t) + sizeof(size_t)];
        std::memcpy(buffer, &o, sizeof(uint64_t));
        std::memcpy(buffer + sizeof(uint64_t), &n, sizeof(size_t));
        serializedData.insert(serializedData.end(), buffer, buffer + sizeof(buffer));
    }
    return serializedData;
}

//----------------------------------------------------------------------------
//! compute SHA256 signature for a given vector read
//----------------------------------------------------------------------------
std::string VectorCache::computeSHA256(const std::vector<unsigned char>& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, data.data(), data.size());
    SHA256_Final(hash, &sha256);
    
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

//----------------------------------------------------------------------------
//! compute SHA256 signature for a string
//----------------------------------------------------------------------------
std::string VectorCache::computeSHA256(const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, data.c_str(), data.size());
    SHA256_Final(hash, &sha256);
    
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
    }
    return ss.str();
}

//----------------------------------------------------------------------------
//! compute SHA256 for vector read and name
//----------------------------------------------------------------------------
std::pair<std::string, std::string> VectorCache::computeHash() const {
    std::vector<unsigned char> serializedData = serializeVector();
    std::string vectorHash = computeSHA256(serializedData);
    std::string nameHash = computeSHA256(name);
    return {vectorHash, nameHash};
}

//----------------------------------------------------------------------------
//! ensure that the last subdirectory directory exists
//----------------------------------------------------------------------------
bool VectorCache::ensureLastSubdirectoryExists(const std::string& dirName) {
    fs::path dirPath(dirName);
    
    if (fs::exists(dirPath) && fs::is_directory(dirPath)) {
        return true;
    }
    
    // Extract the parent path
    fs::path parentPath = dirPath.parent_path();
    
    if (!fs::exists(parentPath)) {
        std::cerr << "error: parent directory does not exist. Cannot create subdirectory.\n";
        return false;
    }
    
    if (fs::create_directory(dirPath)) {
        return true;
    } else {
        std::cerr << "error: failed to create subdirectory.\n";
        return false;
    }

    return false;
}

//----------------------------------------------------------------------------
//! store a vector read in the cache
//----------------------------------------------------------------------------
bool VectorCache::store() const {
    // Compute hashes
    auto [vectorHash, nameHash] = computeHash();
    
    // Compute the total expected length from the input vector
    size_t expectedLen = 0;
    for (const auto& chunk : chunks) {
        expectedLen += chunk.GetLength();
    }

    // Try to have a cache toplevel directory 
    if (!ensureLastSubdirectoryExists(prefix)) {
        return false;
    }

    // Generate the dir name using the prefix and the hash of the name
    std::string dirName = prefix + nameHash;
    std::string fileName = dirName + "/" + vectorHash;
    std::string tmpName = fileName + ".tmp";

    // Try to have a cache subdirectory for this file
    if (!ensureLastSubdirectoryExists(dirName)) {
        return false;
    }

    // Open the file for writing (binary mode
    // Write specified segments of data to the file
    std::ofstream outFile(tmpName, std::ios::binary);
    if (outFile.is_open()) {
        std::error_code ec;
        outFile.write(data, expectedLen);
        if (outFile.fail()) {
                std::cerr << "error: failed writing to file: " << tmpName << std::endl;
                outFile.close();
                fs::remove(tmpName, ec);
                if (ec) {
                    std::cerr << "error: failed cleanup of temporary file: " << tmpName << std::endl;
                }
                return false;
        }
        outFile.close();
        fs::rename(tmpName, fileName, ec);
        if (ec) {
            outFile.close();
            std::cerr << "error: failed atomic rename to file: " << fileName << std::endl;
            fs::remove(tmpName, ec);
            if (ec) {
                    std::cerr << "error: failed cleanup of temporary file: " << tmpName << std::endl;
            }
            return false;
        }
        return true;
    } else {
        std::cerr << "error: failed to open file: " << tmpName << std::endl;
        return false;
    }
}

//----------------------------------------------------------------------------
//! retrieve a vector read from the cache
//----------------------------------------------------------------------------
bool VectorCache::retrieve() const {
    // Compute the total expected length from the input vector
    size_t expectedLen = 0;
    for (const auto& chunk : chunks) {
        expectedLen += chunk.GetLength();
    }

    // Compute hashes
    auto [vectorHash, nameHash] = computeHash();

    // Generate the dir name using the prefix and the hash of the name
    std::string dirName = prefix + nameHash;
    std::string fileName = dirName + "/" + vectorHash;

    // Check if the file exists
    struct stat fileInfo;
    if (stat(fileName.c_str(), &fileInfo) != 0) {
        if (verbose) {
            std::cerr << "error: file does not exist: " << fileName << std::endl;
        }
        return false;
    }

    // Check if the file size matches the expected length
    if ((size_t)fileInfo.st_size != expectedLen) {
        if (verbose) {
            std::cerr << "error: file size mismatch. Expected: " << expectedLen << ", Actual: " << fileInfo.st_size << std::endl;
        }
        return false;
    }

    // Open the file for reading
    std::ifstream inFile(fileName, std::ios::binary);
    if (inFile.is_open()) {
        inFile.read((char*)data, fileInfo.st_size);
        inFile.close();
        return true;
    } else {
        if (verbose) {
            std::cerr << "error: failed to open file: " << fileName << std::endl;
        }
        return false;
    }
}

} // namespace XrdCl