//------------------------------------------------------------------------------
// Apple FileProvider configuration persistence for XIOFS domains.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#import "XioFSFileProviderConfig.h"

namespace XioFS {

static NSString *suiteName(NSString *domainId) {
  return [NSString stringWithFormat:@"org.xrootd.xiofs.fileprovider.%@", domainId];
}

bool saveDomainConfig(NSString *domainId, const DomainConfig &cfg) {
  NSUserDefaults *defaults = [[NSUserDefaults alloc] initWithSuiteName:suiteName(domainId)];
  if (!defaults) return false;

  [defaults setObject:[NSString stringWithUTF8String:cfg.url.c_str()] forKey:@"url"];
  [defaults setObject:[NSString stringWithUTF8String:cfg.cacert.c_str()] forKey:@"cacert"];
  [defaults setObject:[NSString stringWithUTF8String:cfg.token.c_str()] forKey:@"token"];
  [defaults setObject:[NSString stringWithUTF8String:cfg.principal.c_str()] forKey:@"principal"];
  [defaults setBool:cfg.krb5 forKey:@"krb5"];
  [defaults setBool:cfg.insecure forKey:@"insecure"];
  [defaults synchronize];
  return true;
}

bool loadDomainConfig(NSString *domainId, DomainConfig &cfg) {
  NSUserDefaults *defaults = [[NSUserDefaults alloc] initWithSuiteName:suiteName(domainId)];
  if (!defaults) return false;

  NSString *url = [defaults stringForKey:@"url"];
  if (!url || [url length] == 0) return false;

  cfg.url = [url UTF8String];
  cfg.cacert = [[defaults stringForKey:@"cacert"] ?: @"" UTF8String];
  cfg.token = [[defaults stringForKey:@"token"] ?: @"" UTF8String];
  cfg.principal = [[defaults stringForKey:@"principal"] ?: @"" UTF8String];
  cfg.krb5 = [defaults boolForKey:@"krb5"];
  cfg.insecure = [defaults boolForKey:@"insecure"];
  return true;
}

void removeDomainConfig(NSString *domainId) {
  NSUserDefaults *defaults = [[NSUserDefaults alloc] initWithSuiteName:suiteName(domainId)];
  if (defaults) {
    [defaults removePersistentDomainForName:suiteName(domainId)];
    [defaults synchronize];
  }
}

} // namespace XioFS
