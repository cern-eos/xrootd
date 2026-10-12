//------------------------------------------------------------------------------
// NSFileProviderReplicatedExtension implementation for XIOFS on macOS.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#import "XioFSFileProviderExtension.h"
#import "XioFSFileProviderConfig.h"
#import "XioFSFileProviderEnumerator.h"
#import "XioFSFileProviderItem.h"

#include "XioClient.hh"

#include <memory>
#include <string>

@implementation XioFSFileProviderExtension {
  NSFileProviderDomain *_domain;
  std::unique_ptr<XioFS::Client> _client;
}

- (instancetype)initWithDomain:(NSFileProviderDomain *)domain {
  if ((self = [super init])) {
    _domain = domain;
    _client = std::make_unique<XioFS::Client>();

    XioFS::DomainConfig cfg;
    if (!XioFS::loadDomainConfig(domain.identifier, cfg)) {
      return self;
    }

    XioFS::Http2Session::Options opt;
    opt.cacert = cfg.cacert;
    opt.bearer = cfg.token;
    opt.krb5 = cfg.krb5;
    opt.principal = cfg.principal;
    opt.verify_peer = !cfg.insecure;

    std::string err;
    _client->open(cfg.url, opt, err);
  }
  return self;
}

- (void)invalidate {
  if (_client) {
    _client->close();
    _client.reset();
  }
}

static std::string relPathFromIdentifier(NSFileProviderItemIdentifier ident) {
  if ([ident isEqualToString:NSFileProviderRootContainerItemIdentifier]) {
    return "";
  }
  return [ident UTF8String];
}

static NSFileProviderItemIdentifier parentFromIdentifier(NSFileProviderItemIdentifier ident) {
  if ([ident isEqualToString:NSFileProviderRootContainerItemIdentifier]) {
    return NSFileProviderRootContainerItemIdentifier;
  }
  NSString *parent = [ident stringByDeletingLastPathComponent];
  if ([parent isEqualToString:@"."] || [parent isEqualToString:@"/"] || parent.length == 0) {
    return NSFileProviderRootContainerItemIdentifier;
  }
  return parent;
}

- (id<NSFileProviderItem>)itemForIdentifier:(NSFileProviderItemIdentifier)itemIdentifier
                                    request:(NSFileProviderRequest *)request
                          completionHandler:(void (^)(id<NSFileProviderItem> _Nullable, NSError * _Nullable))completionHandler {
  if ([itemIdentifier isEqualToString:NSFileProviderRootContainerItemIdentifier]) {
    XioFSFileProviderItem *root = [[XioFSFileProviderItem alloc] initRoot];
    completionHandler(root, nil);
    return root;
  }

  std::string relPath = relPathFromIdentifier(itemIdentifier);
  XioFS::Attr attr;
  std::string err;
  int rc = _client ? _client->getattr(relPath, attr, err) : -ENOTCONN;
  if (rc != 0) {
    NSError *error = [NSError errorWithDomain:NSFileProviderErrorDomain
                                         code:NSFileProviderErrorNoSuchItem
                                     userInfo:@{NSLocalizedDescriptionKey: [NSString stringWithUTF8String:err.c_str()]}];
    completionHandler(nil, error);
    return nil;
  }

  attr.path = relPath;
  NSFileProviderItemIdentifier parent = parentFromIdentifier(itemIdentifier);
  XioFSFileProviderItem *item = [[XioFSFileProviderItem alloc] initWithAttr:attr parentIdentifier:parent];
  completionHandler(item, nil);
  return item;
}

- (id<NSFileProviderEnumerator>)enumeratorForContainerItemIdentifier:(NSFileProviderItemIdentifier)containerItemIdentifier
                                                             request:(NSFileProviderRequest *)request
                                                               error:(NSError **)error {
  if (!_client) {
    if (error) {
      *error = [NSError errorWithDomain:NSFileProviderErrorDomain
                                   code:NSFileProviderErrorServerUnreachable
                               userInfo:nil];
    }
    return nil;
  }
  return [[XioFSFileProviderEnumerator alloc] initWithContainerItemIdentifier:containerItemIdentifier
                                                                       client:_client.get()];
}

- (NSProgress *)fetchContentsForItemWithIdentifier:(NSFileProviderItemIdentifier)itemIdentifier
                                           version:(NSFileProviderItemVersion *)requestedVersion
                                           request:(NSFileProviderRequest *)request
                                 completionHandler:(void (^)(NSURL * _Nullable, id<NSFileProviderItem> _Nullable, NSError * _Nullable))completionHandler {
  NSProgress *progress = [NSProgress progressWithTotalUnitCount:1];
  std::string relPath = relPathFromIdentifier(itemIdentifier);

  XioFS::Attr attr;
  std::string err;
  int rc = _client ? _client->getattr(relPath, attr, err) : -ENOTCONN;
  if (rc != 0) {
    NSError *error = [NSError errorWithDomain:NSFileProviderErrorDomain
                                         code:NSFileProviderErrorNoSuchItem
                                     userInfo:@{NSLocalizedDescriptionKey: [NSString stringWithUTF8String:err.c_str()]}];
    completionHandler(nil, nil, error);
    return progress;
  }

  std::string body;
  uint64_t sz = attr.size < 0 ? 0 : static_cast<uint64_t>(attr.size);
  if (sz > 0) {
    rc = _client->read(relPath, 0, sz, body, err);
    if (rc != 0) {
      NSError *error = [NSError errorWithDomain:NSPOSIXErrorDomain
                                           code:abs(rc)
                                       userInfo:@{NSLocalizedDescriptionKey: [NSString stringWithUTF8String:err.c_str()]}];
      completionHandler(nil, nil, error);
      return progress;
    }
  }

  NSString *tempPath = [NSTemporaryDirectory() stringByAppendingPathComponent:[[NSUUID UUID] UUIDString]];
  NSData *data = [NSData dataWithBytes:body.data() length:body.size()];
  if (![data writeToFile:tempPath atomically:YES]) {
    NSError *error = [NSError errorWithDomain:NSPOSIXErrorDomain code:EIO userInfo:nil];
    completionHandler(nil, nil, error);
    return progress;
  }

  attr.path = relPath;
  NSFileProviderItemIdentifier parent = parentFromIdentifier(itemIdentifier);
  XioFSFileProviderItem *item = [[XioFSFileProviderItem alloc] initWithAttr:attr parentIdentifier:parent];
  NSURL *tempUrl = [NSURL fileURLWithPath:tempPath];
  progress.completedUnitCount = 1;
  completionHandler(tempUrl, item, nil);
  return progress;
}

- (NSProgress *)createItemBasedOnTemplate:(id<NSFileProviderItem>)itemTemplate
                                   fields:(NSFileProviderItemFields)fields
                                 contents:(NSURL *)url
                                  options:(NSFileProviderCreateItemOptions)options
                                  request:(NSFileProviderRequest *)request
                        completionHandler:(void (^)(id<NSFileProviderItem> _Nullable, NSFileProviderItemFields, BOOL, NSError * _Nullable))completionHandler {
  NSProgress *progress = [NSProgress progressWithTotalUnitCount:1];
  std::string parentRel = relPathFromIdentifier(itemTemplate.parentItemIdentifier);
  std::string itemName = [itemTemplate.filename UTF8String];
  std::string relPath = parentRel.empty() ? itemName : parentRel + "/" + itemName;

  std::string err;
  int rc = 0;
  if ([itemTemplate.contentType conformsToType:UTTypeFolder]) {
    rc = _client ? _client->mkdir(relPath, err) : -ENOTCONN;
  } else {
    std::string body;
    if (url) {
      NSData *data = [NSData dataWithContentsOfURL:url];
      if (data) {
        body.assign(reinterpret_cast<const char *>(data.bytes), data.length);
      }
    }
    rc = _client ? _client->put(relPath, body, err) : -ENOTCONN;
  }

  if (rc != 0) {
    NSError *error = [NSError errorWithDomain:NSPOSIXErrorDomain
                                         code:abs(rc)
                                     userInfo:@{NSLocalizedDescriptionKey: [NSString stringWithUTF8String:err.c_str()]}];
    completionHandler(nil, 0, NO, error);
    return progress;
  }

  XioFS::Attr attr;
  _client->getattr(relPath, attr, err);
  attr.path = relPath;
  XioFSFileProviderItem *item = [[XioFSFileProviderItem alloc] initWithAttr:attr
                                                           parentIdentifier:itemTemplate.parentItemIdentifier];
  progress.completedUnitCount = 1;
  completionHandler(item, fields, NO, nil);
  return progress;
}

- (NSProgress *)modifyItem:(id<NSFileProviderItem>)item
               baseVersion:(NSFileProviderItemVersion *)version
             changedFields:(NSFileProviderItemFields)changedFields
                  contents:(NSURL *)newContents
                   options:(NSFileProviderModifyItemOptions)options
                   request:(NSFileProviderRequest *)request
         completionHandler:(void (^)(id<NSFileProviderItem> _Nullable, NSFileProviderItemFields, BOOL, NSError * _Nullable))completionHandler {
  NSProgress *progress = [NSProgress progressWithTotalUnitCount:1];
  std::string relPath = relPathFromIdentifier(item.itemIdentifier);
  std::string err;

  if (newContents) {
    NSData *data = [NSData dataWithContentsOfURL:newContents];
    if (data) {
      std::string body(reinterpret_cast<const char *>(data.bytes), data.length);
      _client->write(relPath, 0, body, err);
    }
  }

  XioFS::Attr attr;
  _client->getattr(relPath, attr, err);
  attr.path = relPath;
  XioFSFileProviderItem *updatedItem = [[XioFSFileProviderItem alloc] initWithAttr:attr
                                                                  parentIdentifier:item.parentItemIdentifier];
  progress.completedUnitCount = 1;
  completionHandler(updatedItem, changedFields, NO, nil);
  return progress;
}

- (NSProgress *)deleteItemWithIdentifier:(NSFileProviderItemIdentifier)itemIdentifier
                             baseVersion:(NSFileProviderItemVersion *)version
                                 options:(NSFileProviderDeleteItemOptions)options
                                 request:(NSFileProviderRequest *)request
                       completionHandler:(void (^)(NSError * _Nullable))completionHandler {
  NSProgress *progress = [NSProgress progressWithTotalUnitCount:1];
  std::string relPath = relPathFromIdentifier(itemIdentifier);
  std::string err;
  int rc = _client ? _client->unlink(relPath, err) : -ENOTCONN;
  if (rc != 0) {
    NSError *error = [NSError errorWithDomain:NSPOSIXErrorDomain
                                         code:abs(rc)
                                     userInfo:@{NSLocalizedDescriptionKey: [NSString stringWithUTF8String:err.c_str()]}];
    completionHandler(error);
    return progress;
  }
  progress.completedUnitCount = 1;
  completionHandler(nil);
  return progress;
}

@end
