//------------------------------------------------------------------------------
// NSFileProviderReplicatedExtension implementation for XIOFS on macOS.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_FILEPROVIDER_EXTENSION_H
#define XIOFS_FILEPROVIDER_EXTENSION_H

#import <Foundation/Foundation.h>
#import <FileProvider/FileProvider.h>

NS_ASSUME_NONNULL_BEGIN

@interface XioFSFileProviderExtension : NSObject <NSFileProviderReplicatedExtension>

- (instancetype)initWithDomain:(NSFileProviderDomain *)domain;

@end

NS_ASSUME_NONNULL_END

#endif // XIOFS_FILEPROVIDER_EXTENSION_H
