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
#include <stdarg.h>
#include <stdio.h>

#include <mutex>
//------------------------------------------------------------------------------
#include "XrdSys/XrdSysError.hh"
//------------------------------------------------------------------------------

#pragma once
namespace S3 {
//------------------------------------------------------------------------------
//! \brief enum defining logging levels
//------------------------------------------------------------------------------
enum LogMask {
  DEBUG = 0x01,
  INFO = 0x02,
  WARN = 0x04,
  ERROR = 0x08,
  ALL = 0xff
};

//------------------------------------------------------------------------------
//! \brief class to log from S3 plug-in
//------------------------------------------------------------------------------
class S3Log {
 public:
  S3Log(XrdSysError& mErr) : mLog(&mErr), traceId(0) {}
  S3Log() {}
  virtual ~S3Log() {}

  std::string LogString(int c) {
    switch (c) {
      case DEBUG:
        return "| DEBUG |";
      case INFO:
        return "| INFO  |";
      case WARN:
        return "| REQU  |";
      case ERROR:
        return "| ERROR |";
      default:
        return "| INIT  |";
    }
  };

  std::string newTrace() {
    std::lock_guard<std::mutex> guard(logMutex);
    traceId++;
    std::stringstream ss;
    ss << "[req:" << std::setw(8) << std::setfill('0') << std::hex << traceId
       << "]";
    return ss.str();
  }

  //! \brief initialize logging
  void Init(XrdSysError* log) { mLog = log; }

  //! \brief log message
  std::string Log(S3::LogMask mask, const char* unit, const char* msg, ...) {
    std::lock_guard<std::mutex> guard(logMutex);
    va_list args;
    va_start(args, msg);
    vsnprintf(logBuffer, sizeof(logBuffer), msg, args);
    va_end(args);
    std::string tag =
        std::string("X") + LogString(mask) + std::string(" ") + unit;
    int l = 48 - tag.size();
    for (auto i = 0; i < l; ++i) {
      tag += " ";
    }

    mLog->Log((int)mask, tag.c_str(), logBuffer);
    return std::string(logBuffer);
  }

 private:
  XrdSysError* mLog;
  char logBuffer[65535];
  std::mutex logMutex;
  uint64_t traceId;
};
}  // namespace S3
