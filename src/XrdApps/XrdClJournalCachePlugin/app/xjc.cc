#include "file/CleanerConfig.hh"
#include "file/PolicyConfig.hh"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

void usage() {
  std::cerr
      << "Usage: xjc [--policy PATH] [--cleaner PATH] [--journal PATH] "
         "<command> [args...]\n"
      << "\n"
      << "Commands:\n"
      << "  show\n"
      << "  bypass on|off\n"
      << "  multi-origin on|off\n"
      << "  allow-origin list|add|remove ...\n"
      << "  redirect list|add|remove ...\n"
      << "  cleaner show\n"
      << "  cleaner enable on|off\n"
      << "  cleaner set journal PATH\n"
      << "  cleaner set high BYTES\n"
      << "  cleaner set low BYTES\n"
      << "  cleaner set interval SECONDS\n"
      << "  cleaner set config-poll SECONDS\n"
      << "\n"
      << "Policy defaults to $XRD_JOURNALCACHE_POLICY or\n"
      << "  $XRD_JOURNALCACHE_CACHE/.xjc/policy.conf\n"
      << "Cleaner defaults to $XRD_JOURNALCACHE_CLEANER or\n"
      << "  $XRD_JOURNALCACHE_CACHE/.xjc/cleaner.conf\n";
}

std::string resolveJournalRoot(int argc, char **argv, int &argi) {
  std::string journal;
  if (const char *cache = getenv("XRD_JOURNALCACHE_CACHE")) {
    journal = JournalCache::normalizeCacheRoot(cache);
  }
  for (; argi < argc; ++argi) {
    const std::string arg = argv[argi];
    if (arg == "--journal" && argi + 1 < argc) {
      journal = JournalCache::normalizeCacheRoot(argv[++argi]);
    } else if (arg == "--help" || arg == "-h") {
      usage();
      std::exit(0);
    } else {
      break;
    }
  }
  return journal;
}

std::string resolvePolicyPath(const std::string &journal, int argc, char **argv,
                              int &argi) {
  std::string policyPath;
  if (const char *env = getenv("XRD_JOURNALCACHE_POLICY")) {
    policyPath = env;
  }
  if (!journal.empty() && policyPath.empty()) {
    policyPath = JournalCache::defaultPolicyPath(journal + "/");
  }

  for (; argi < argc; ++argi) {
    const std::string arg = argv[argi];
    if (arg == "--policy" && argi + 1 < argc) {
      policyPath = argv[++argi];
    } else if (arg == "--cleaner") {
      ++argi;
    } else if (arg == "--journal") {
      ++argi;
    } else if (arg == "--help" || arg == "-h") {
      usage();
      std::exit(0);
    } else {
      break;
    }
  }
  return policyPath;
}

std::string resolveCleanerPath(const std::string &journal, int argc,
                               char **argv, int &argi) {
  std::string cleanerPath;
  if (const char *env = getenv("XRD_JOURNALCACHE_CLEANER")) {
    cleanerPath = env;
  }
  if (!journal.empty() && cleanerPath.empty()) {
    cleanerPath = JournalCache::defaultCleanerPath(journal + "/");
  }

  int scan = 1;
  for (; scan < argc; ++scan) {
    const std::string arg = argv[scan];
    if (arg == "--cleaner" && scan + 1 < argc) {
      cleanerPath = argv[scan + 1];
    }
  }
  return cleanerPath;
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

bool saveCleaner(const std::string &path,
                 const JournalCache::CleanerSettings &settings) {
  if (!JournalCache::saveCleanerFile(path, settings)) {
    std::cerr << "xjc: failed to write cleaner config: " << path << "\n";
    return false;
  }
  std::cout << "xjc: updated " << path << "\n";
  return true;
}

bool loadOrCreatePolicy(const std::string &path,
                        JournalCache::PolicySettings &settings) {
  if (JournalCache::loadPolicyFile(path, settings)) {
    return true;
  }
  return JournalCache::savePolicyFile(path, settings);
}

bool loadOrCreateCleaner(const std::string &path,
                         JournalCache::CleanerSettings &settings) {
  if (JournalCache::loadCleanerFile(path, settings)) {
    return true;
  }
  return JournalCache::saveCleanerFile(path, settings);
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

bool parseU64(const std::string &value, uint64_t &out) {
  try {
    out = std::stoull(value);
    return true;
  } catch (...) {
    return false;
  }
}

bool parseU32(const std::string &value, unsigned &out) {
  try {
    out = static_cast<unsigned>(std::stoul(value));
    return true;
  } catch (...) {
    return false;
  }
}

int handleCleaner(int argc, char **argv, int argi,
                  const std::string &cleanerPath) {
  if (cleanerPath.empty()) {
    std::cerr << "xjc: cleaner path not set; use --cleaner, --journal, or "
                 "XRD_JOURNALCACHE_CACHE\n";
    return 1;
  }
  if (argi >= argc) {
    usage();
    return 1;
  }

  JournalCache::CleanerSettings settings;
  if (!loadOrCreateCleaner(cleanerPath, settings)) {
    std::cerr << "xjc: unable to read or create cleaner config: " << cleanerPath
              << "\n";
    return 1;
  }

  const std::string sub = argv[argi++];
  if (sub == "show") {
    std::cout << JournalCache::formatCleanerFile(settings);
    return 0;
  }

  if (sub == "enable") {
    if (argi >= argc || !parseOnOff(argv[argi], settings.enabled)) {
      std::cerr << "xjc: expected on|off\n";
      return 1;
    }
    return saveCleaner(cleanerPath, settings) ? 0 : 1;
  }

  if (sub == "set") {
    if (argi + 1 >= argc) {
      usage();
      return 1;
    }
    const std::string key = argv[argi++];
    const std::string value = argv[argi];
    if (key == "journal") {
      settings.journal = JournalCache::normalizeCacheRoot(value);
    } else if (key == "high") {
      if (!parseU64(value, settings.highWatermark)) {
        std::cerr << "xjc: invalid high watermark\n";
        return 1;
      }
    } else if (key == "low") {
      if (!parseU64(value, settings.lowWatermark)) {
        std::cerr << "xjc: invalid low watermark\n";
        return 1;
      }
    } else if (key == "interval") {
      if (!parseU32(value, settings.interval)) {
        std::cerr << "xjc: invalid interval\n";
        return 1;
      }
    } else if (key == "config-poll") {
      if (!parseU32(value, settings.configPoll)) {
        std::cerr << "xjc: invalid config-poll value\n";
        return 1;
      }
    } else {
      usage();
      return 1;
    }
    return saveCleaner(cleanerPath, settings) ? 0 : 1;
  }

  usage();
  return 1;
}

} // namespace

int main(int argc, char **argv) {
  int argi = 1;
  const std::string journal = resolveJournalRoot(argc, argv, argi);
  int pathArgi = argi;
  const std::string cleanerPath =
      resolveCleanerPath(journal, argc, argv, pathArgi);
  pathArgi = argi;
  const std::string policyPath =
      resolvePolicyPath(journal, argc, argv, pathArgi);

  if (pathArgi >= argc) {
    usage();
    return 1;
  }

  const std::string command = argv[pathArgi++];
  if (command == "cleaner") {
    return handleCleaner(argc, argv, pathArgi, cleanerPath);
  }

  if (policyPath.empty()) {
    std::cerr << "xjc: policy path not set; use --policy or --journal\n";
    return 1;
  }

  JournalCache::PolicySettings settings;
  if (!loadOrCreatePolicy(policyPath, settings)) {
    std::cerr << "xjc: unable to read or create policy file: " << policyPath
              << "\n";
    return 1;
  }

  if (command == "show") {
    std::cout << JournalCache::formatPolicyFile(settings);
    return 0;
  }

  if (command == "bypass") {
    if (pathArgi >= argc || !parseOnOff(argv[pathArgi], settings.bypass)) {
      std::cerr << "xjc: expected on|off\n";
      return 1;
    }
    return savePolicy(policyPath, settings) ? 0 : 1;
  }

  if (command == "multi-origin") {
    if (pathArgi >= argc ||
        !parseOnOff(argv[pathArgi], settings.multiOriginUnwrap)) {
      std::cerr << "xjc: expected on|off\n";
      return 1;
    }
    return savePolicy(policyPath, settings) ? 0 : 1;
  }

  if (command == "allow-origin") {
    if (pathArgi >= argc) {
      usage();
      return 1;
    }
    const std::string sub = argv[pathArgi++];
    if (sub == "list") {
      for (const auto &pattern : settings.originAllowlist.patterns()) {
        std::cout << pattern << "\n";
      }
      return 0;
    }
    if (sub == "add") {
      if (pathArgi >= argc) {
        std::cerr << "xjc: missing regex\n";
        return 1;
      }
      settings.originAllowlist.addPattern(argv[pathArgi]);
      return savePolicy(policyPath, settings) ? 0 : 1;
    }
    if (sub == "remove") {
      if (pathArgi >= argc) {
        std::cerr << "xjc: missing regex\n";
        return 1;
      }
      const std::string target = argv[pathArgi];
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
    if (pathArgi >= argc) {
      usage();
      return 1;
    }
    const std::string sub = argv[pathArgi++];
    if (sub == "list") {
      for (const auto &rule : settings.externalRedirect.rules()) {
        std::cout << rule.prefix << " " << rule.target << "\n";
      }
      return 0;
    }
    if (sub == "add") {
      if (pathArgi + 1 >= argc) {
        std::cerr << "xjc: redirect add requires <prefix> <url>\n";
        return 1;
      }
      settings.externalRedirect.addRule(argv[pathArgi], argv[pathArgi + 1]);
      return savePolicy(policyPath, settings) ? 0 : 1;
    }
    if (sub == "remove") {
      if (pathArgi >= argc) {
        std::cerr << "xjc: missing prefix\n";
        return 1;
      }
      const std::string targetPrefix = argv[pathArgi];
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
