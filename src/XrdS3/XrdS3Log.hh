

#include <mutex>
#include <stdio.h>
#include <stdarg.h>

#include "XrdSys/XrdSysError.hh"

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
  
  class S3Log {
  public:
    S3Log(XrdSysError& mErr) : mLog(&mErr) {}
    S3Log() {}
    virtual ~S3Log() {}
    
    void Init(XrdSysError* log) { mLog = log; }
    
    std::string
    Log(S3::LogMask mask, const char* unit, const char* msg, ...)
    {
      std::lock_guard<std::mutex> guard(logMutex); 
      
      va_list args;
      va_start(args, msg);
      vsnprintf(logBuffer, sizeof(logBuffer), msg, args);
      va_end(args);
      mLog->Log( (int) mask, unit, logBuffer );
      return std::string(logBuffer);
    }

  private:
    XrdSysError* mLog;
    char logBuffer[65535]; 
    std::mutex logMutex; 
  };
}
