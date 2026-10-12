#include "XrdHttpS3Config.hh"

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOuca2x.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdSys/XrdSysError.hh"

#include <cctype>
#include <cstring>
#include <fcntl.h>
#include <strings.h>

namespace XrdHttpS3 {
namespace {

bool OnOff(const char *val, bool &out) {
  if (!val || !val[0])
    return false;
  if (!strcmp(val, "1") || !strcasecmp(val, "on") || !strcasecmp(val, "yes") ||
      !strcasecmp(val, "true")) {
    out = true;
    return true;
  }
  if (!strcmp(val, "0") || !strcasecmp(val, "off") || !strcasecmp(val, "no") ||
      !strcasecmp(val, "false")) {
    out = false;
    return true;
  }
  return false;
}

bool AbsPath(std::string &path) {
  if (path.empty() || path[0] != '/' || path.find("..") != std::string::npos)
    return false;
  while (path.size() > 1 && path.back() == '/')
    path.pop_back();
  return true;
}

}

bool LoadConfig(const char *configfn, XrdSysError &log, Config &cfg) {
  if (!configfn || !configfn[0]) {
    log.Emsg("Config", "S3 handler requires the xrootd configuration file");
    return false;
  }

  XrdOucEnv cfgEnv;
  XrdOucStream config(&log, getenv("XRDINSTANCE"), &cfgEnv, "=====> ");
  int fd = open(configfn, O_RDONLY, 0);
  if (fd < 0) {
    log.Emsg("Config", errno, "open config file", configfn);
    return false;
  }
  config.Attach(fd);
  static const char *cvec[] = {"*** http s3 plugin config:", 0};
  config.Capture(cvec);

  const char *val;
  while ((val = config.GetMyFirstWord())) {
    if (!strcmp(val, "s3.switch")) {
      val = config.GetWord();
      if (!OnOff(val, cfg.enabled)) {
        log.Emsg("Config", "s3.switch requires on or off");
        config.Close();
        return false;
      }
    } else if (!strcmp(val, "s3.region")) {
      val = config.GetWord();
      if (!val || !val[0]) {
        log.Emsg("Config", "s3.region value not specified");
        config.Close();
        return false;
      }
      cfg.region = val;
    } else if (!strcmp(val, "s3.sysmeta")) {
      val = config.GetWord();
      if (!val || !val[0]) {
        log.Emsg("Config", "s3.sysmeta value not specified");
        config.Close();
        return false;
      }
      cfg.sysmeta = val;
      if (!AbsPath(cfg.sysmeta)) {
        log.Emsg("Config", "s3.sysmeta must be an absolute path", cfg.sysmeta.c_str());
        config.Close();
        return false;
      }
    } else if (!strcmp(val, "s3.userprefix")) {
      val = config.GetWord();
      if (!val || !val[0]) {
        log.Emsg("Config", "s3.userprefix value not specified");
        config.Close();
        return false;
      }
      cfg.userprefix = val;
      if (!AbsPath(cfg.userprefix)) {
        log.Emsg("Config", "s3.userprefix must be an absolute path",
                 cfg.userprefix.c_str());
        config.Close();
        return false;
      }
    } else if (!strcmp(val, "s3.userhash")) {
      val = config.GetWord();
      if (!val || !val[0]) {
        log.Emsg("Config", "s3.userhash requires first or none");
        config.Close();
        return false;
      }
      if (!strcmp(val, "first"))
        cfg.userhash = true;
      else if (!strcmp(val, "none"))
        cfg.userhash = false;
      else {
        log.Emsg("Config", "s3.userhash value is invalid", val);
        config.Close();
        return false;
      }
    } else if (!strcmp(val, "s3.skew")) {
      val = config.GetWord();
      if (!val || XrdOuca2x::a2tm(log, "s3.skew", val, &cfg.skew, 0)) {
        config.Close();
        return false;
      }
    } else if (!strcmp(val, "s3.metacache")) {
      val = config.GetWord();
      if (!val || XrdOuca2x::a2i(log, "s3.metacache", val, &cfg.metacache, 0)) {
        config.Close();
        return false;
      }
    } else if (!strcmp(val, "s3.metaentity")) {
      val = config.GetWord();
      if (!val || !val[0]) {
        log.Emsg("Config", "s3.metaentity value not specified");
        config.Close();
        return false;
      }
      cfg.metaentity = val;
    } else if (!strcmp(val, "s3.vhost")) {
      val = config.GetWord();
      if (!val || !val[0]) {
        log.Emsg("Config", "s3.vhost value not specified");
        config.Close();
        return false;
      }
      cfg.vhost = val;
      for (char &c : cfg.vhost)
        c = static_cast<char>(tolower(static_cast<unsigned char>(c)));
      if (cfg.vhost.find('/') != std::string::npos || cfg.vhost.front() == '.' ||
          cfg.vhost.find("..") != std::string::npos) {
        log.Emsg("Config", "s3.vhost must be a DNS suffix", cfg.vhost.c_str());
        config.Close();
        return false;
      }
    } else if (!strcmp(val, "s3.trace")) {
      val = config.GetWord();
      if (!val || !val[0]) {
        log.Emsg("Config", "s3.trace requires a level");
        config.Close();
        return false;
      }
      cfg.traceInfo = !strcasecmp(val, "all") || !strcasecmp(val, "info") ||
                      !strcasecmp(val, "debug");
    }
  }
  config.Close();

  if (!AbsPath(cfg.sysmeta) || !AbsPath(cfg.userprefix)) {
    log.Emsg("Config", "s3.sysmeta and s3.userprefix must be absolute paths");
    return false;
  }
  if (cfg.region.empty()) {
    log.Emsg("Config", "s3.region is empty");
    return false;
  }
  std::string state = cfg.enabled ? "protocol enabled" : "protocol loaded but off";
  log.Emsg("S3", state.c_str(), cfg.sysmeta.c_str());
  return true;
}

}
