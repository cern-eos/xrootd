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

#include <iostream>
#include <vector>
#include <chrono>
#include <mutex>

class TimeBench {
private:
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = std::chrono::time_point<Clock>;
    using Duration = std::chrono::microseconds;

    std::vector<std::pair<TimePoint,uint64_t>> measurements;
    std::vector<uint64_t> bins;
    TimePoint start;
    TimePoint end;
    uint64_t totalBytes;
    std::mutex mtx;
  
public:
    TimeBench() : totalBytes(0) {}

    void AddMeasurement(uint64_t bytes) {
        std::lock_guard<std::mutex> guard(mtx);
        auto now = Clock::now();
        if (measurements.empty()) {
            start = now;
        }
        measurements.push_back(std::make_pair(now,bytes));
        totalBytes += bytes;
        end = now;
    }

    std::vector<uint64_t> GetBins() {
        std::lock_guard<std::mutex> guard(mtx);
        Duration totalTime = std::chrono::duration_cast<Duration>(end - start);
        Duration binSize = totalTime / 10;
        bins.clear();
        bins.resize(10, 0);

        TimePoint binStart = start;
        size_t binIndex = 0;

        for (auto i : measurements) {
            TimePoint measurementTime = binStart + binSize;

	    binIndex = (i.first - start)/ binSize;
            if (Clock::now() >= measurementTime) {
	      bins[binIndex] += i.second;
            } else {
                break; // Don't process future measurements
            }
        }

        return bins;
    }
  
    Duration GetTimePerBin() {
        Duration totalTime = std::chrono::duration_cast<Duration>(end - start);
	Duration binSize = totalTime / 10;
	return binSize;
    }
};
