#include "file/PolicyConfig.hh"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

void usage() {
  std::cerr
      << "Usage: xjc [--policy PATH] <command> [args...]\n"
      << "\n"
      << "Commands:\n"
      << "  show\n"
      << "  bypass on|off\n"
      << "  multi-origin on|off\n"
      << "  allow-origin list\n"
      << "  allow-origin add <regex>\n"
      << "  allow-origin remove <regex>\n"
      << "  redirect list\n"
      << "  redirect add <prefix> <url>\n"
      << "  redirect remove <prefix>\n"
      << "\n"
      << "Policy file defaults to $XRD_JOURNALCACHE_POLICY or\n"
      << "  $XRD_JOURNALCACHE_CACHE/.xjc/policy.conf\n"
      << "Running xrootd processes reload when the file mtime changes.\n";
}

std::string resolvePolicyPath(int argc, char **argv, int &argi) {
  std::string policyPath;
  if (const char *env = getenv("XRD_JOURNALCACHE_POLICY")) {
    policyPath = env;
  }
  if (const char *cache = getenv("XRD_JOURNALCACHE_CACHE")) {
    const std::string derived = JournalCache::defaultPolicyPath(cache);
    if (policyPath.empty()) {
      policyPath = derived;
    }
  }

  for (; argi < argc; ++argi) {
    const std::string arg = argv[argi];
    if (arg == "--policy" && argi + 1 < argc) {
      policyPath = argv[++argi];
    } else if (arg == "--help" || arg == "-h") {
      usage();
      std::exit(0);
    } else {
      break;
    }
  }
  return policyPath;
}

void printPolicy(const JournalCache::PolicySettings &settings) {
  std::cout << JournalCache::formatPolicyFile(settings);
}

bool loadOrCreate(const std::string &path, JournalCache::PolicySettings &settings) {
  if (JournalCache::loadPolicyFile(path, settings)) {
    return true;
  }
  return JournalCache::savePolicyFile(path, settings);
}

bool savePolicy(const std::string &path,
                const JournalCache::PolicySettings &settings) {
  if (!JournalCache::savePolicyFile(path, settings)) {
    std::cerr << "xjc: failed to write policy file: " << path << "\n";
    return false;
  }
  std::cout << "xjc: updated " << path << "\n";
  return true;
}

bool parseOnOff(const std::string &value, bool &out) {
  if (value == "on" || value == "1" || value == "true" || value == "yes") {
    out = true;
    return true;
  }
  if (value == "off" || value == "0" || value == "false" || value == "no") {
    out = false;
    return true;
  }
  return false;
}

} // namespace

int main(int argc, char **argv) {
  int argi = 1;
  const std::string policyPath = resolvePolicyPath(argc, argv, argi);
  if (policyPath.empty()) {
    std::cerr << "xjc: policy path not set; use --policy or XRD_JOURNALCACHE_POLICY\n";
    return 1;
  }
  if (argi >= argc) {
    usage();
    return 1;
  }

  const std::string command = argv[argi++];
  JournalCache::PolicySettings settings;
  if (!loadOrCreate(policyPath, settings)) {
    std::cerr << "xjc: unable to read or create policy file: " << policyPath
              << "\n";
    return 1;
  }

  if (command == "show") {
    printPolicy(settings);
    return 0;
  }

  if (command == "bypass") {
    if (argi >= argc) {
      usage();
      return 1;
    }
    if (!parseOnOff(argv[argi], settings.bypass)) {
      std::cerr << "xjc: expected on|off\n";
      return 1;
    }
    return savePolicy(policyPath, settings) ? 0 : 1;
  }

  if (command == "multi-origin") {
    if (argi >= argc) {
      usage();
      return 1;
    }
    if (!parseOnOff(argv[argi], settings.multiOriginUnwrap)) {
      std::cerr << "xjc: expected on|off\n";
      return 1;
    }
    return savePolicy(policyPath, settings) ? 0 : 1;
  }

  if (command == "allow-origin") {
    if (argi >= argc) {
      usage();
      return 1;
    }
    const std::string sub = argv[argi++];
    if (sub == "list") {
      for (const auto &pattern : settings.originAllowlist.patterns()) {
        std::cout << pattern << "\n";
      }
      return 0;
    }
    if (sub == "add") {
      if (argi >= argc) {
        std::cerr << "xjc: missing regex\n";
        return 1;
      }
      settings.originAllowlist.addPattern(argv[argi]);
      return savePolicy(policyPath, settings) ? 0 : 1;
    }
    if (sub == "remove") {
      if (argi >= argc) {
        std::cerr << "xjc: missing regex\n";
        return 1;
      }
      const std::string target = argv[argi];
      JournalCache::PolicySettings updated;
      updated.bypass = settings.bypass;
      updated.multiOriginUnwrap = settings.multiOriginUnwrap;
      updated.externalRedirect = settings.externalRedirect;
      for (const auto &pattern : settings.originAllowlist.patterns()) {
        if (pattern != target) {
          updated.originAllowlist.addPattern(pattern);
        }
      }
      return savePolicy(policyPath, updated) ? 0 : 1;
    }
    usage();
    return 1;
  }

  if (command == "redirect") {
    if (argi >= argc) {
      usage();
      return 1;
    }
    const std::string sub = argv[argi++];
    if (sub == "list") {
      for (const auto &rule : settings.externalRedirect.rules()) {
        std::cout << rule.prefix << " " << rule.target << "\n";
      }
      return 0;
    }
    if (sub == "add") {
      if (argi + 1 >= argc) {
        std::cerr << "xjc: redirect add requires <prefix> <url>\n";
        return 1;
      }
      settings.externalRedirect.addRule(argv[argi], argv[argi + 1]);
      return savePolicy(policyPath, settings) ? 0 : 1;
    }
    if (sub == "remove") {
      if (argi >= argc) {
        std::cerr << "xjc: missing prefix\n";
        return 1;
      }
      const std::string targetPrefix = argv[argi];
      JournalCache::PolicySettings updated;
      updated.bypass = settings.bypass;
      updated.multiOriginUnwrap = settings.multiOriginUnwrap;
      updated.originAllowlist = settings.originAllowlist;
      for (const auto &rule : settings.externalRedirect.rules()) {
        if (rule.prefix != targetPrefix) {
          updated.externalRedirect.addRule(rule.prefix, rule.target);
        }
      }
      return savePolicy(policyPath, updated) ? 0 : 1;
    }
    usage();
    return 1;
  }

  usage();
  return 1;
}
