#include "daemon/XjcdRender.hh"
#include "daemon/XjcdState.hh"

#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>

namespace {

void usage() {
  std::cerr
      << "Usage: xjcd [--journal PATH] <command> [args...]\n"
      << "\n"
      << "Commands:\n"
      << "  init [--journal PATH] --xroot-port N --https-port N "
         "--tls-cert PATH --tls-key PATH\n"
      << "  render\n"
      << "  show\n"
      << "  validate\n"
      << "\n"
      << "Generates XRootD + JournalCache configs under $journal/.xjc/etc/.\n"
      << "Runtime policy is written to $journal/.xjc/policy.conf (edit with xjc).\n"
      << "Start xrootd via systemd using the generated xrootd.cf and xjcd.env.\n";
}

std::string getHostname() {
  char host[256];
  if (gethostname(host, sizeof(host)) != 0) {
    return "localhost";
  }
  host[sizeof(host) - 1] = '\0';
  return host;
}

std::string resolveJournal(int argc, char **argv, int &argi) {
  std::string journal;
  if (const char *env = getenv("XRD_JOURNALCACHE_CACHE")) {
    journal = JournalCache::normalizeJournalRoot(env);
  }

  for (; argi < argc; ++argi) {
    const std::string arg = argv[argi];
    if (arg == "--journal" && argi + 1 < argc) {
      journal = JournalCache::normalizeJournalRoot(argv[++argi]);
    } else if (arg == "--help" || arg == "-h") {
      usage();
      std::exit(0);
    } else {
      break;
    }
  }
  return journal;
}

bool requireArg(int argc, char **argv, int &argi, const char *name,
                std::string &out) {
  if (argi >= argc) {
    std::cerr << "xjcd: missing " << name << "\n";
    return false;
  }
  out = argv[argi++];
  return true;
}

bool parsePort(const std::string &value, unsigned &out) {
  try {
    const unsigned port = static_cast<unsigned>(std::stoul(value));
    if (port == 0 || port > 65535) {
      return false;
    }
    out = port;
    return true;
  } catch (...) {
    return false;
  }
}

bool loadStateOrExit(const std::string &journal, JournalCache::XjcdState &state) {
  if (journal.empty()) {
    std::cerr << "xjcd: journal path not set; use --journal or "
                 "XRD_JOURNALCACHE_CACHE\n";
    return false;
  }
  if (!state.load(journal)) {
    std::cerr << "xjcd: no state at " << state.statePath()
              << "; run xjcd init first\n";
    return false;
  }
  return true;
}

} // namespace

int main(int argc, char **argv) {
  int argi = 1;
  const std::string journalArg = resolveJournal(argc, argv, argi);
  if (argi >= argc) {
    usage();
    return 1;
  }

  const std::string command = argv[argi++];

  if (command == "init") {
    JournalCache::XjcdState state;
    state.journal = journalArg;
    state.libDir = JournalCache::defaultLibDir();

    std::string value;
    while (argi < argc) {
      const std::string flag = argv[argi++];
      if (flag == "--journal" && requireArg(argc, argv, argi, "journal path", value)) {
        state.journal = JournalCache::normalizeJournalRoot(value);
      } else if (flag == "--xroot-port" && requireArg(argc, argv, argi, "xroot port", value)) {
        if (!parsePort(value, state.xrootPort)) {
          std::cerr << "xjcd: invalid xroot port\n";
          return 1;
        }
      } else if (flag == "--https-port" &&
                 requireArg(argc, argv, argi, "https port", value)) {
        if (!parsePort(value, state.httpsPort)) {
          std::cerr << "xjcd: invalid https port\n";
          return 1;
        }
      } else if (flag == "--tls-cert" &&
                 requireArg(argc, argv, argi, "tls cert path", value)) {
        state.tlsCert = value;
      } else if (flag == "--tls-key" &&
                 requireArg(argc, argv, argi, "tls key path", value)) {
        state.tlsKey = value;
      } else if (flag == "--lib-dir" &&
                 requireArg(argc, argv, argi, "lib dir", value)) {
        state.libDir = value;
      } else if (flag == "--plugin-suffix" &&
                 requireArg(argc, argv, argi, "plugin suffix", value)) {
        state.pluginSuffix = value;
      } else {
        std::cerr << "xjcd: unknown init option: " << flag << "\n";
        usage();
        return 1;
      }
    }

    if (state.journal.empty()) {
      std::cerr << "xjcd: --journal PATH is required for init\n";
      return 1;
    }
    if (!state.isComplete()) {
      std::cerr << "xjcd: init requires --xroot-port, --https-port, "
                   "--tls-cert, and --tls-key\n";
      return 1;
    }

    if (!state.save()) {
      std::cerr << "xjcd: failed to write " << state.statePath() << "\n";
      return 1;
    }
    if (!JournalCache::renderXjcdConfigs(state, getHostname())) {
      std::cerr << "xjcd: failed to render configs\n";
      return 1;
    }
    std::cout << "xjcd: initialized " << state.journal << "\n";
    std::cout << "xjcd: xrootd config " << state.xrootdConfigPath() << "\n";
    std::cout << "xjcd: policy file " << state.policyPath() << "\n";
    return 0;
  }

  JournalCache::XjcdState state;
  if (!loadStateOrExit(journalArg, state)) {
    return 1;
  }

  if (command == "render") {
    if (!JournalCache::renderXjcdConfigs(state, getHostname())) {
      std::cerr << "xjcd: failed to render configs\n";
      return 1;
    }
    std::cout << "xjcd: rendered configs under " << state.etcDir() << "\n";
    return 0;
  }

  if (command == "show") {
    std::cout << "journal       = " << state.journal << "\n";
    std::cout << "xroot_port    = " << state.xrootPort << "\n";
    std::cout << "https_port    = " << state.httpsPort << "\n";
    std::cout << "tls_cert      = " << state.tlsCert << "\n";
    std::cout << "tls_key       = " << state.tlsKey << "\n";
    std::cout << "lib_dir       = " << state.libDir << "\n";
    std::cout << "state         = " << state.statePath() << "\n";
    std::cout << "xrootd.cf     = " << state.xrootdConfigPath() << "\n";
    std::cout << "http ext      = " << state.httpExtConfigPath() << "\n";
    std::cout << "client plugin = " << state.clientPluginConfigPath() << "\n";
    std::cout << "policy        = " << state.policyPath() << "\n";
    std::cout << "systemd env   = " << state.systemdEnvPath() << "\n";
    std::cout << "\n# systemd example:\n";
    std::cout << "EnvironmentFile=" << state.systemdEnvPath() << "\n";
    std::cout << "ExecStart=/usr/bin/xrootd -c " << state.xrootdConfigPath()
              << " -R daemon\n";
    return 0;
  }

  if (command == "validate") {
    const auto issues = JournalCache::validateXjcdState(state);
    bool failed = false;
    for (const auto &issue : issues) {
      std::cout << (issue.error ? "ERROR: " : "WARN:  ") << issue.message << "\n";
      failed = failed || issue.error;
    }
    if (issues.empty()) {
      std::cout << "OK\n";
    }
    return failed ? 1 : 0;
  }

  usage();
  return 1;
}
