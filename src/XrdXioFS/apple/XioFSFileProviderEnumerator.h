//------------------------------------------------------------------------------
// NSFileProviderEnumerator implementation for XIOFS directory listings.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_FILEPROVIDER_ENUMERATOR_H
#define XIOFS_FILEPROVIDER_ENUMERATOR_H

#import <Foundation/Foundation.h>
#import <FileProvider/FileProvider.h>
#include "XioClient.hh"

NS_ASSUME_NONNULL_BEGIN

@interface XioFSFileProviderEnumerator : NSObject <NSFileProviderEnumerator>

- (instancetype)initWithContainerItemIdentifier:(NSFileProviderItemIdentifier)identifier
                                         client:(nullable XioFS::Client *)client;

@end

NS_ASSUME_NONNULL_END

#endif // XIOFS_FILEPROVIDER_ENUMERATOR_H
