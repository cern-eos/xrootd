//------------------------------------------------------------------------------
// NSFileProviderItem implementation for XIOFS files and directories.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#import "XioFSFileProviderItem.h"

@implementation XioFSFileProviderItem {
  NSString *_etag;
}

- (instancetype)initRoot {
  if ((self = [super init])) {
    _itemIdentifier = NSFileProviderRootContainerItemIdentifier;
    _parentItemIdentifier = NSFileProviderRootContainerItemIdentifier;
    _filename = @"";
    _contentType = UTTypeFolder;
    _documentSize = @(0);
    _creationDate = [NSDate dateWithTimeIntervalSince1970:0];
    _contentModificationDate = [NSDate dateWithTimeIntervalSince1970:0];
    _etag = @"root";
  }
  return self;
}

- (instancetype)initWithAttr:(const XioFS::Attr &)attr
            parentIdentifier:(NSFileProviderItemIdentifier)parent {
  if ((self = [super init])) {
    NSString *pathStr = [NSString stringWithUTF8String:attr.path.c_str()];
    _itemIdentifier = [pathStr copy];
    _parentItemIdentifier = [parent copy];
    _filename = [[pathStr lastPathComponent] copy];

    if (attr.is_dir) {
      _contentType = UTTypeFolder;
      _documentSize = @(0);
    } else {
      NSString *ext = [_filename pathExtension];
      UTType *type = ext.length ? [UTType typeWithFilenameExtension:ext] : nil;
      _contentType = type ?: UTTypeItem;
      _documentSize = @(attr.size < 0 ? 0 : attr.size);
    }

    _creationDate = [NSDate dateWithTimeIntervalSince1970:attr.atime ? attr.atime : attr.mtime];
    _contentModificationDate = [NSDate dateWithTimeIntervalSince1970:attr.mtime];
    _etag = [NSString stringWithUTF8String:attr.etag.c_str()];
  }
  return self;
}

- (NSFileProviderItemVersion *)itemVersion {
  NSData *verData = [_etag dataUsingEncoding:NSUTF8StringEncoding];
  if (!verData) {
    verData = [NSData data];
  }
  return [[NSFileProviderItemVersion alloc] initWithContentVersion:verData metadataVersion:verData];
}

- (NSFileProviderItemCapabilities)capabilities {
  return NSFileProviderItemCapabilitiesAllowsReading |
         NSFileProviderItemCapabilitiesAllowsWriting |
         NSFileProviderItemCapabilitiesAllowsDeleting |
         NSFileProviderItemCapabilitiesAllowsRenaming |
         NSFileProviderItemCapabilitiesAllowsReparenting;
}

@end
