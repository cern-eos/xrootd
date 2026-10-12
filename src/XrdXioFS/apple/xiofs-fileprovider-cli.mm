//------------------------------------------------------------------------------
// Command-line domain management tool for XIOFS Apple FileProvider.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#import <Foundation/Foundation.h>
#import <FileProvider/FileProvider.h>

#import "XioFSFileProviderConfig.h"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace {

void usage(const char *prog) {
  std::cerr << "Usage:\n"
            << "  " << prog << " add <domain> --url <URL> [--token <TOK>] [--tokenfile <FILE>] "
            << "[--cacert <FILE>] [--krb5] [--principal <USER>] [--insecure]\n"
            << "  " << prog << " remove <domain>\n"
            << "  " << prog << " list\n"
            << "  " << prog << " status <domain>\n";
}

std::string readTokenFile(const std::string &path) {
  std::ifstream f(path);
  if (!f) return {};
  std::string tok;
  std::getline(f, tok);
  while (!tok.empty() && (tok.back() == '\r' || tok.back() == '\n' ||
                          tok.back() == ' ' || tok.back() == '\t')) {
    tok.pop_back();
  }
  return tok;
}

} // namespace

int main(int argc, const char *argv[]) {
  @autoreleasepool {
    if (argc < 2) {
      usage(argv[0]);
      return 1;
    }

    std::string cmd = argv[1];

    if (cmd == "add") {
      if (argc < 4) {
        usage(argv[0]);
        return 1;
      }
      NSString *domainName = [NSString stringWithUTF8String:argv[2]];
      XioFS::DomainConfig cfg;

      for (int i = 3; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--url" && i + 1 < argc) {
          cfg.url = argv[++i];
        } else if (a == "--token" && i + 1 < argc) {
          cfg.token = argv[++i];
        } else if (a == "--tokenfile" && i + 1 < argc) {
          cfg.token = readTokenFile(argv[++i]);
        } else if (a == "--cacert" && i + 1 < argc) {
          cfg.cacert = argv[++i];
        } else if (a == "--krb5") {
          cfg.krb5 = true;
        } else if (a == "--principal" && i + 1 < argc) {
          cfg.principal = argv[++i];
        } else if (a == "--insecure") {
          cfg.insecure = true;
        }
      }

      if (cfg.url.empty()) {
        std::cerr << "Error: --url is required for 'add'\n";
        return 1;
      }

      if (!XioFS::saveDomainConfig(domainName, cfg)) {
        std::cerr << "Error: Failed to save domain configuration\n";
        return 1;
      }

      NSFileProviderDomainIdentifier domainIdent = domainName;
      NSFileProviderDomain *domain = [[NSFileProviderDomain alloc] initWithIdentifier:domainIdent
                                                                          displayName:domainName];

      dispatch_semaphore_t sem = dispatch_semaphore_create(0);
      __block int exitCode = 0;

      [NSFileProviderManager addDomain:domain completionHandler:^(NSError * _Nullable error) {
        if (error) {
          std::cerr << "Error adding FileProvider domain: "
                    << [[error localizedDescription] UTF8String] << "\n";
          XioFS::removeDomainConfig(domainName);
          exitCode = 1;
        } else {
          std::cout << "Successfully added FileProvider domain '" << [domainName UTF8String] << "'\n"
                    << "Mounted at: ~/Library/CloudStorage/" << [domainName UTF8String] << "\n";
        }
        dispatch_semaphore_signal(sem);
      }];

      dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
      return exitCode;

    } else if (cmd == "remove") {
      if (argc < 3) {
        usage(argv[0]);
        return 1;
      }
      NSString *domainName = [NSString stringWithUTF8String:argv[2]];
      NSFileProviderDomainIdentifier domainIdent = domainName;
      NSFileProviderDomain *domain = [[NSFileProviderDomain alloc] initWithIdentifier:domainIdent
                                                                          displayName:domainName];

      dispatch_semaphore_t sem = dispatch_semaphore_create(0);
      __block int exitCode = 0;

      [NSFileProviderManager removeDomain:domain completionHandler:^(NSError * _Nullable error) {
        if (error) {
          std::cerr << "Error removing FileProvider domain: "
                    << [[error localizedDescription] UTF8String] << "\n";
          exitCode = 1;
        } else {
          XioFS::removeDomainConfig(domainName);
          std::cout << "Successfully removed FileProvider domain '" << [domainName UTF8String] << "'\n";
        }
        dispatch_semaphore_signal(sem);
      }];

      dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
      return exitCode;

    } else if (cmd == "list") {
      dispatch_semaphore_t sem = dispatch_semaphore_create(0);
      __block int exitCode = 0;

      [NSFileProviderManager getDomainsWithCompletionHandler:^(NSArray<NSFileProviderDomain *> * _Nonnull domains,
                                                               NSError * _Nullable error) {
        if (error) {
          std::cerr << "Error retrieving domains: "
                    << [[error localizedDescription] UTF8String] << "\n";
          exitCode = 1;
        } else {
          std::cout << "Registered XIOFS FileProvider domains (" << [domains count] << "):\n";
          for (NSFileProviderDomain *d in domains) {
            std::cout << "  - " << [d.displayName UTF8String]
                      << " (identifier: " << [d.identifier UTF8String] << ")\n";
          }
        }
        dispatch_semaphore_signal(sem);
      }];

      dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
      return exitCode;

    } else if (cmd == "status") {
      if (argc < 3) {
        usage(argv[0]);
        return 1;
      }
      NSString *domainName = [NSString stringWithUTF8String:argv[2]];
      XioFS::DomainConfig cfg;
      if (XioFS::loadDomainConfig(domainName, cfg)) {
        std::cout << "Domain: " << [domainName UTF8String] << "\n"
                  << "  URL: " << cfg.url << "\n"
                  << "  Auth: " << (cfg.krb5 ? "Kerberos (SPNEGO)" : (!cfg.token.empty() ? "OIDC/Bearer Token" : "None")) << "\n"
                  << "  TLS Verification: " << (cfg.insecure ? "Disabled" : "Enabled") << "\n";
        return 0;
      } else {
        std::cerr << "Domain '" << [domainName UTF8String] << "' not found in configuration\n";
        return 1;
      }
    } else {
      usage(argv[0]);
      return 1;
    }
  }
}
