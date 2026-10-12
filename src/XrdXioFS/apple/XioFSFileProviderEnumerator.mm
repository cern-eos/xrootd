//------------------------------------------------------------------------------
// NSFileProviderEnumerator implementation for XIOFS directory listings.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#import "XioFSFileProviderEnumerator.h"
#import "XioFSFileProviderItem.h"

@implementation XioFSFileProviderEnumerator {
  NSFileProviderItemIdentifier _containerIdentifier;
  XioFS::Client *_client;
}

- (instancetype)initWithContainerItemIdentifier:(NSFileProviderItemIdentifier)identifier
                                         client:(XioFS::Client *)client {
  if ((self = [super init])) {
    _containerIdentifier = [identifier copy];
    _client = client;
  }
  return self;
}

- (void)invalidate {
  _client = nullptr;
}

- (void)enumerateItemsForObserver:(id<NSFileProviderEnumerationObserver>)observer
                     startingAtPage:(NSFileProviderPage)page {
  if (!_client) {
    NSError *error = [NSError errorWithDomain:NSFileProviderErrorDomain
                                         code:NSFileProviderErrorServerUnreachable
                                     userInfo:nil];
    [observer finishEnumeratingWithError:error];
    return;
  }

  std::string relPath;
  if (![_containerIdentifier isEqualToString:NSFileProviderRootContainerItemIdentifier]) {
    relPath = [_containerIdentifier UTF8String];
  }

  std::vector<XioFS::DavEntry> entries;
  std::string err;
  int rc = _client->readdir(relPath, entries, err);
  if (rc != 0) {
    NSError *error = [NSError errorWithDomain:NSPOSIXErrorDomain
                                         code:abs(rc)
                                     userInfo:@{NSLocalizedDescriptionKey: [NSString stringWithUTF8String:err.c_str()]}];
    [observer finishEnumeratingWithError:error];
    return;
  }

  NSMutableArray<id<NSFileProviderItem>> *items = [NSMutableArray array];
  for (const auto &entry : entries) {
    std::string itemRelPath;
    if (relPath.empty()) {
      itemRelPath = entry.name;
    } else {
      itemRelPath = relPath + "/" + entry.name;
    }

    XioFS::Attr attr;
    std::string getattrErr;
    if (_client->getattr(itemRelPath, attr, getattrErr) == 0) {
      attr.path = itemRelPath;
      XioFSFileProviderItem *item = [[XioFSFileProviderItem alloc] initWithAttr:attr
                                                               parentIdentifier:_containerIdentifier];
      [items addObject:item];
    }
  }

  [observer didEnumerateItems:items];
  [observer finishEnumeratingUpToPage:nil];
}

- (void)enumerateChangesForObserver:(id<NSFileProviderChangeObserver>)observer
                     fromSyncAnchor:(NSFileProviderSyncAnchor)syncAnchor {
  [observer finishEnumeratingChangesUpToSyncAnchor:syncAnchor moreComing:NO];
}

- (void)currentSyncAnchorWithCompletionHandler:(void (^)(NSFileProviderSyncAnchor _Nullable))completionHandler {
  NSData *anchor = [@"1" dataUsingEncoding:NSUTF8StringEncoding];
  completionHandler(anchor);
}

@end
