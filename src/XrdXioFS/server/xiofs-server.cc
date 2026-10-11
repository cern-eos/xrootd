//------------------------------------------------------------------------------
// xiofs-server — stand-alone XrdHttp2 POSIX export for XIOFS clients.
//
// Reads a small key=value file (export path + TLS/auth) and execs xrootd
// with FsOss + ofs.posix + setfsuid (via xrootd-fsuid on Linux).
//
//   xiofs-server [-c FILE] [--print] [--] [xrootd-args]
//
// Default config: /etc/xrootd/xiofs-server.conf
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

constexpr const char *kDefaultConf = "/etc/xrootd/xiofs-server.conf";
constexpr const char *kRunDir = "/run/xiofs-server";
constexpr const char *kLogFile = "/var/log/xrootd/xiofs-server.log";
constexpr const char *kSpool = "/var/spool/xrootd";

struct Cfg {
  std::string export_path;
  int port{1094};
  std::string tls_cert;
  std::string tls_key;
  std::string tls_ca{"/etc/grid-security/certificates"};
  std::string auth{"none"};
  std::string krb5_keytab{"/etc/krb5.keytab"};
  std::string krb5_principal;
  std::string jwt_config{"/etc/xrootd/scitokens.cfg"};
  std::string cont;
  std::string user{"xrootd"};
  bool readonly{false};
  bool h2{true};
};

void usage(const char *argv0)
{
  std::cerr
      << "Usage: " << argv0 << " [-c FILE] [--print] [--] [xrootd-args]\n"
      << "\n"
      << "  Generate an XrdHttp2 + FsOss (setfsuid) config from FILE\n"
      << "  (default " << kDefaultConf << ") and exec xrootd.\n"
      << "  --print writes the xrootd config to stdout and exits.\n";
}

std::string trim(std::string s)
{
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front())))
    s.erase(s.begin());
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back())))
    s.pop_back();
  if (s.size() >= 2 && ((s.front() == '"' && s.back() == '"') ||
                        (s.front() == '\'' && s.back() == '\'')))
    s = s.substr(1, s.size() - 2);
  return s;
}

bool parseBool(const std::string &v, bool &out, std::string &err)
{
  std::string s = v;
  for (char &c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (s == "1" || s == "yes" || s == "true" || s == "on") {
    out = true;
    return true;
  }
  if (s == "0" || s == "no" || s == "false" || s == "off") {
    out = false;
    return true;
  }
  err = "invalid boolean: " + v;
  return false;
}

bool parseFile(const std::string &path, Cfg &cfg, std::string &err)
{
  std::ifstream in(path);
  if (!in) {
    err = "cannot read " + path + ": " + std::strerror(errno);
    return false;
  }
  std::string line;
  int n = 0;
  while (std::getline(in, line)) {
    n++;
    auto hash = line.find('#');
    if (hash != std::string::npos)
      line = line.substr(0, hash);
    line = trim(line);
    if (line.empty())
      continue;
    auto eq = line.find('=');
    if (eq == std::string::npos) {
      err = path + ":" + std::to_string(n) + ": missing '='";
      return false;
    }
    std::string key = trim(line.substr(0, eq));
    std::string val = trim(line.substr(eq + 1));
    if (key == "export")
      cfg.export_path = val;
    else if (key == "port")
      cfg.port = std::atoi(val.c_str());
    else if (key == "tls.cert" || key == "cert")
      cfg.tls_cert = val;
    else if (key == "tls.key" || key == "key")
      cfg.tls_key = val;
    else if (key == "tls.ca" || key == "ca")
      cfg.tls_ca = val;
    else if (key == "auth")
      cfg.auth = val;
    else if (key == "krb5.keytab")
      cfg.krb5_keytab = val;
    else if (key == "krb5.principal")
      cfg.krb5_principal = val;
    else if (key == "jwt.config")
      cfg.jwt_config = val;
    else if (key == "continue")
      cfg.cont = val;
    else if (key == "user")
      cfg.user = val;
    else if (key == "readonly") {
      if (!parseBool(val, cfg.readonly, err))
        return false;
    } else if (key == "h2") {
      if (!parseBool(val, cfg.h2, err))
        return false;
    } else {
      err = path + ":" + std::to_string(n) + ": unknown key '" + key + "'";
      return false;
    }
  }
  return true;
}

bool isDir(const std::string &p)
{
  struct stat st {};
  return !p.empty() && ::stat(p.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool validate(const Cfg &cfg, std::string &err)
{
  if (cfg.export_path.empty()) {
    err = "export= is required (POSIX directory to serve)";
    return false;
  }
  if (cfg.export_path.front() != '/') {
    err = "export= must be an absolute path";
    return false;
  }
  if (!isDir(cfg.export_path)) {
    err = "export path is not a directory: " + cfg.export_path;
    return false;
  }
  if (cfg.port <= 0 || cfg.port > 65535) {
    err = "invalid port";
    return false;
  }
  if (cfg.auth != "none" && cfg.auth != "krb5" && cfg.auth != "jwt") {
    err = "auth= must be none, krb5, or jwt";
    return false;
  }
  if (cfg.auth != "none" && (cfg.tls_cert.empty() || cfg.tls_key.empty())) {
    err = "auth=" + cfg.auth + " requires tls.cert and tls.key (HTTPS)";
    return false;
  }
  if (!cfg.tls_cert.empty() && cfg.tls_key.empty()) {
    err = "tls.key is required when tls.cert is set";
    return false;
  }
  if (!cfg.tls_cert.empty()) {
    struct stat st {};
    if (::stat(cfg.tls_cert.c_str(), &st) < 0) {
      err = "tls.cert not found: " + cfg.tls_cert;
      return false;
    }
    if (::stat(cfg.tls_key.c_str(), &st) < 0) {
      err = "tls.key not found: " + cfg.tls_key;
      return false;
    }
  }
  if (cfg.auth == "krb5") {
    if (cfg.krb5_keytab.empty() || cfg.krb5_principal.empty()) {
      err = "auth=krb5 needs krb5.keytab and krb5.principal "
            "(e.g. HTTP/<host>@REALM)";
      return false;
    }
    struct stat st {};
    if (::stat(cfg.krb5_keytab.c_str(), &st) < 0) {
      err = "krb5.keytab not found: " + cfg.krb5_keytab;
      return false;
    }
  }
  if (cfg.auth == "jwt") {
    struct stat st {};
    if (::stat(cfg.jwt_config.c_str(), &st) < 0) {
      err = "jwt.config not found: " + cfg.jwt_config;
      return false;
    }
  }
  return true;
}

std::string emitXrd(const Cfg &cfg)
{
  std::ostringstream os;
  os << "# Generated by xiofs-server. Do not edit.\n"
     << "all.role server\n"
     << "all.adminpath " << kSpool << "\n"
     << "all.pidpath " << kRunDir << "\n"
     << "all.export /" << (cfg.readonly ? " r/o" : "") << "\n"
     << "oss.localroot " << cfg.export_path << "\n"
     << "ofs.osslib libXrdFsOss.so\n"
     << "ofs.posix\n"
     << "oss.fsuid require\n"
     << "\n";

  if (!cfg.tls_cert.empty()) {
    os << "xrd.protocol https:" << cfg.port << " libXrdHttp.so\n"
       << "xrd.tls " << cfg.tls_cert << " " << cfg.tls_key << "\n";
    if (!cfg.tls_ca.empty()) {
      struct stat st {};
      if (::stat(cfg.tls_ca.c_str(), &st) == 0) {
        if (S_ISDIR(st.st_mode))
          os << "xrd.tlsca certdir " << cfg.tls_ca << "\n";
        else
          os << "xrd.tlsca certfile " << cfg.tls_ca << "\n";
      }
    }
    os << "http.tlsclientauth off\n";
    if (cfg.h2)
      os << "http.h2 on\n";
    else
      os << "http.h2 off\n";
  } else {
    os << "xrd.protocol XrdHttp:" << cfg.port << " libXrdHttp.so\n";
    if (cfg.h2)
      os << "http.h2 on\n";
  }

  if (cfg.auth == "krb5") {
    os << "http.auth krb5 " << cfg.krb5_keytab << " " << cfg.krb5_principal
       << "\n";
  } else if (cfg.auth == "jwt") {
    os << "ofs.authorize\n"
       << "ofs.authlib libXrdAccSciTokens.so config=" << cfg.jwt_config << "\n"
       << "http.header2cgi Authorization authz\n";
  }

  if (!cfg.cont.empty())
    os << "continue " << cfg.cont << "\n";
  return os.str();
}

bool mkdirp(const std::string &dir)
{
  if (dir.empty() || isDir(dir))
    return true;
  auto slash = dir.rfind('/');
  if (slash != std::string::npos && slash > 0 &&
      !mkdirp(dir.substr(0, slash)))
    return false;
  if (::mkdir(dir.c_str(), 0755) < 0 && errno != EEXIST)
    return false;
  return true;
}

std::string sibling(const char *argv0, const char *name)
{
  std::string a = argv0 ? argv0 : "";
  auto s = a.rfind('/');
  if (s != std::string::npos) {
    std::string p = a.substr(0, s + 1) + name;
    if (::access(p.c_str(), X_OK) == 0)
      return p;
  }
  return name;
}

int fail(const std::string &msg)
{
  std::cerr << "xiofs-server: " << msg << "\n";
  return 1;
}

} // namespace

int main(int argc, char **argv)
{
  std::string conf = kDefaultConf;
  bool print_only = false;
  std::vector<char *> extra;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "-h" || a == "--help") {
      usage(argv[0]);
      return 0;
    }
    if ((a == "-c" || a == "--config") && i + 1 < argc)
      conf = argv[++i];
    else if (a == "--print")
      print_only = true;
    else if (a == "--") {
      for (int j = i + 1; j < argc; ++j)
        extra.push_back(argv[j]);
      break;
    } else if (!a.empty() && a[0] == '-')
      return fail("unknown option " + a);
    else
      extra.push_back(argv[i]);
  }

  Cfg cfg;
  std::string err;
  if (!parseFile(conf, cfg, err))
    return fail(err);
  if (!validate(cfg, err))
    return fail(err);

  std::string body = emitXrd(cfg);
  if (print_only) {
    std::cout << body;
    return 0;
  }

  if (!mkdirp(kRunDir))
    return fail(std::string("mkdir ") + kRunDir + ": " + std::strerror(errno));
  std::string out = std::string(kRunDir) + "/xrootd.cfg";
  {
    std::ofstream os(out);
    if (!os)
      return fail("cannot write " + out + ": " + std::strerror(errno));
    os << body;
  }

  std::string xrd = sibling(argv[0], "xrootd");
  std::string logdir = "/var/log/xrootd";
  mkdirp(logdir);
  mkdirp(kSpool);

  std::vector<std::string> args;
#ifdef __linux__
  std::string fsuid = sibling(argv[0], "xrootd-fsuid");
  if (::access(fsuid.c_str(), X_OK) == 0 ||
      ::access("/usr/bin/xrootd-fsuid", X_OK) == 0) {
    if (::access(fsuid.c_str(), X_OK) != 0)
      fsuid = "/usr/bin/xrootd-fsuid";
    args.push_back(fsuid);
    args.push_back("-u");
    args.push_back(cfg.user);
    args.push_back("--");
  }
#endif
  args.push_back(xrd);
  args.push_back("-c");
  args.push_back(out);
  args.push_back("-n");
  args.push_back("xiofs");
  args.push_back("-k");
  args.push_back("fifo");
  args.push_back("-a");
  args.push_back(kRunDir);
  args.push_back("-w");
  args.push_back(kRunDir);
  args.push_back("-s");
  args.push_back(std::string(kRunDir) + "/xrootd.pid");
  if (isDir(logdir) || mkdirp(logdir)) {
    args.push_back("-l");
    args.push_back(kLogFile);
  }
  for (char *e : extra)
    args.push_back(e);

  std::vector<char *> av;
  for (auto &s : args)
    av.push_back(const_cast<char *>(s.c_str()));
  av.push_back(nullptr);
  execvp(av[0], av.data());
  return fail(std::string("exec ") + av[0] + ": " + std::strerror(errno));
}
