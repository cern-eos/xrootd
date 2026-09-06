//------------------------------------------------------------------------------
// Shared KernelFS transport and I/O vocabulary.
//
// Userspace (FUSE / kfscli) and the Linux module use the same operation
// names. The HTTP/2 FUSE client and the future HTTP/1.1+kTLS kernel
// transport both map these onto XrdHttp verbs.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef KERNELFS_OPS_H
#define KERNELFS_OPS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum kernelfs_memory_type {
  KFS_MEM_PAGECACHE = 0,
  KFS_MEM_USER,
  KFS_MEM_DMABUF,
  KFS_MEM_GPU
};

struct kernelfs_attr {
  uint64_t ino;
  uint64_t size;
  uint64_t mtime_sec;
  uint32_t mode;
  uint32_t is_dir;
  char     etag[128];
};

struct kernelfs_dirent {
  char     name[256];
  uint64_t size;
  uint64_t mtime_sec;
  uint32_t is_dir;
};

struct kernelfs_io {
  uint64_t object_ino;
  uint64_t offset;
  uint64_t length;
  enum kernelfs_memory_type memory_type;
  void    *buf;
};

/*
 * Filesystem operations, independent of HTTPS vs RDMA.
 *
 * XrdHttp mapping used by the HTTP transports:
 *   lookup/getattr -> PROPFIND Depth 0 (HEAD as a size-only hint)
 *   readdir        -> PROPFIND Depth 1
 *   read           -> GET Range
 *   write          -> PUT (whole object replace)
 *   mkdir          -> MKCOL
 *   unlink         -> DELETE
 *   rename         -> MOVE
 */
struct kernelfs_transport_ops {
  int (*connect)(void *ctx);
  void (*disconnect)(void *ctx);

  int (*lookup)(void *ctx, const char *parent, const char *name,
                struct kernelfs_attr *out);
  int (*getattr)(void *ctx, const char *path, struct kernelfs_attr *out);
  int (*readdir)(void *ctx, const char *path, struct kernelfs_dirent *ents,
                 size_t cap, size_t *nents);

  int (*submit_read_cpu)(void *ctx, const char *path, uint64_t offset,
                         uint64_t length, void *buf, size_t *nread);
  int (*submit_write_cpu)(void *ctx, const char *path, const void *buf,
                          size_t length);

  int (*mkdir)(void *ctx, const char *path);
  int (*unlink)(void *ctx, const char *path);
  int (*rename)(void *ctx, const char *from, const char *to);

  int (*cancel)(void *ctx, uint64_t request_id);
};

#ifdef __cplusplus
}
#endif

#endif
