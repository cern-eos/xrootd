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

#include <iostream>
#include <filesystem>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>
#include <iomanip>

namespace fs = std::filesystem;

void printCurrentTime() {
    auto now = std::chrono::system_clock::now();
    auto now_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    auto now_ns_since_epoch = now_ns.time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(now_ns_since_epoch);
    auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(now_ns_since_epoch - seconds);

    auto now_t = std::chrono::system_clock::to_time_t(now);
    struct tm* tm = std::localtime(&now_t);
    
    int year = tm->tm_year - 100; // tm_year represents years since 1900

    std::cout << std::setfill('0') << std::setw(2) << year << std::setfill('0') << std::setw(4) << (tm->tm_mon + 1) * 100 + tm->tm_mday << " ";
    std::cout << std::setw(2) << std::setfill('0') << tm->tm_hour << ":" << std::setw(2) << tm->tm_min << ":" << std::setw(2) << tm->tm_sec << " ";
    std::cout << "time=" << seconds.count() << "." << std::setw(9) << std::setfill('0') << nanoseconds.count() << " ";
}

time_t getLastAccessTime(const fs::path& filePath) {
    struct stat fileInfo;
    if (stat(filePath.c_str(), &fileInfo) != 0) {
        return -1; // Error occurred
    }
    return fileInfo.st_atime;
}

long long getDirectorySize(const fs::path& directory) {
    long long totalSize = 0;
    for (const auto& entry : fs::recursive_directory_iterator(directory)) {
        if (fs::is_regular_file(entry)) {
            totalSize += fs::file_size(entry);
        }
    }
    return totalSize;
}

std::vector<std::pair<long long, fs::path>> getFilesByAccessTime(const fs::path& directory) {
    std::vector<std::pair<long long, fs::path>> fileList;
    for (const auto& entry : fs::recursive_directory_iterator(directory)) {
        if (fs::is_regular_file(entry)) {
            auto accessTime = getLastAccessTime(entry.path());
            fileList.emplace_back(accessTime, entry.path());
        }
    }
    std::sort(fileList.begin(), fileList.end());
    return fileList;
}

void cleanDirectory(const fs::path& directory, long long highWatermark, long long lowWatermark) {
    long long currentSize = getDirectorySize(directory);
    if (currentSize <= highWatermark) {
        printCurrentTime();
        std::cout << "Directory size is within the limit. No action needed." << std::endl;
        return;
    }

    auto files = getFilesByAccessTime(directory);

    for (const auto& [accessTime, filePath] : files) {
        if (currentSize <= lowWatermark) {
            break;
        }
        long long fileSize = fs::file_size(filePath);
        try {
            fs::remove(filePath);
            currentSize -= fileSize;
            printCurrentTime();
            std::cout << "Deleted: " << filePath << " (Size: " << fileSize << " bytes)" << std::endl;
        } catch (const std::exception& e) {
            printCurrentTime();
            std::cerr << "Error deleting " << filePath << ": " << e.what() << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <directory> <highwatermark> <lowwatermark> <interval>" << std::endl;
        return 1;
    }

    fs::path directory = argv[1];
    long long highWatermark = std::stoll(argv[2]);
    long long lowWatermark = std::stoll(argv[3]);
    int interval = std::stoi(argv[4]);

    while (true) {
        cleanDirectory(directory, highWatermark, lowWatermark);
        std::this_thread::sleep_for(std::chrono::seconds(interval));
    }

    return 0;
}
