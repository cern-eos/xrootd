//------------------------------------------------------------------------------
// NSFileProviderItem implementation for XIOFS files and directories.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_FILEPROVIDER_ITEM_H
#define XIOFS_FILEPROVIDER_ITEM_H

#import <Foundation/Foundation.h>
#import <FileProvider/FileProvider.h>
#import <UniformTypeIdentifiers/UniformTypeIdentifiers.h>
#include "XioClient.hh"

NS_ASSUME_NONNULL_BEGIN

@interface XioFSFileProviderItem : NSObject <NSFileProviderItem>

@property (nonatomic, readonly, copy) NSFileProviderItemIdentifier itemIdentifier;
@property (nonatomic, readonly, copy) NSFileProviderItemIdentifier parentItemIdentifier;
@property (nonatomic, readonly, copy) NSString *filename;
@property (nonatomic, readonly, copy) UTType *contentType;
@property (nonatomic, readonly, copy, nullable) NSNumber *documentSize;
@property (nonatomic, readonly, copy, nullable) NSDate *creationDate;
@property (nonatomic, readonly, copy, nullable) NSDate *contentModificationDate;
@property (nonatomic, readonly, strong) NSFileProviderItemVersion *itemVersion;
@property (nonatomic, readonly) NSFileProviderItemCapabilities capabilities;

- (instancetype)initWithAttr:(const XioFS::Attr &)attr
            parentIdentifier:(NSFileProviderItemIdentifier)parent;

- (instancetype)initRoot;

@end

NS_ASSUME_NONNULL_END

#endif // XIOFS_FILEPROVIDER_ITEM_H
