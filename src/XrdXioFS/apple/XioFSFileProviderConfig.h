//------------------------------------------------------------------------------
// Apple FileProvider configuration persistence for XIOFS domains.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_FILEPROVIDER_CONFIG_H
#define XIOFS_FILEPROVIDER_CONFIG_H

#import <Foundation/Foundation.h>
#include <string>

namespace XioFS {

struct DomainConfig {
  std::string url;
  std::string cacert;
  std::string token;
  std::string principal;
  bool krb5{false};
  bool insecure{false};
};

bool saveDomainConfig(NSString *domainId, const DomainConfig &cfg);
bool loadDomainConfig(NSString *domainId, DomainConfig &cfg);
void removeDomainConfig(NSString *domainId);

} // namespace XioFS

#endif // XIOFS_FILEPROVIDER_CONFIG_H
