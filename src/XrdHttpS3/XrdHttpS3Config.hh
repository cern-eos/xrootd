#ifndef XRDHTTPS3_CONFIG_HH
#define XRDHTTPS3_CONFIG_HH

#include <string>

class XrdSysError;

namespace XrdHttpS3 {

struct Config {
  bool enabled{false};
  std::string region{"us-east-1"};
  std::string sysmeta{"/s3meta"};
  std::string userprefix{"/prefix"};
  bool userhash{true};
  int skew{900};
  int metacache{30};
  std::string metaentity;
  bool traceInfo{false};
  // DNS suffix for virtual-hosted buckets, such as s3.example.com.
  // Empty keeps path-style URLs only.
  std::string vhost;
};

bool LoadConfig(const char *configfn, XrdSysError &log, Config &cfg);

}

#endif
