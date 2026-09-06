//------------------------------------------------------------------------------
// Shared XIOFS transport and I/O vocabulary.
//
// Userspace (FUSE / xiofscli) and the Linux module use the same operation
// names. The HTTP/2 FUSE client and the kernel HTTP/1.1+kTLS transport
// both map these onto XrdHttp verbs.
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_OPS_H
#define XIOFS_OPS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum xiofs_memory_type {
  XIOFS_MEM_PAGECACHE = 0,
  XIOFS_MEM_USER,
  XIOFS_MEM_DMABUF,
  XIOFS_MEM_GPU
};

struct xiofs_attr {
  uint64_t ino;
  uint64_t size;
  uint64_t mtime_sec;
  uint32_t mode;
  uint32_t is_dir;
  char     etag[128];
};

struct xiofs_dirent {
  char     name[256];
  uint64_t size;
  uint64_t mtime_sec;
  uint32_t is_dir;
};

struct xiofs_io {
  uint64_t object_ino;
  uint64_t offset;
  uint64_t length;
  enum xiofs_memory_type memory_type;
  void    *buf;
};

/*
 * Filesystem operations, independent of HTTPS vs RDMA.
 *
 * XrdHttp mapping used by the HTTP transports:
 *   lookup/getattr -> PROPFIND Depth 0 (HEAD as a size-only hint)
 *   readdir        -> PROPFIND Depth 1
 *   read           -> GET Range
 *   write          -> PATCH with Content-Range (pwrite)
 *   create/trunc 0 -> PUT (whole object replace)
 *   mkdir          -> MKCOL
 *   unlink         -> DELETE
 *   rename         -> MOVE
 *   chmod          -> PROPPATCH (X:mode / Z:executable)
 *   link           -> LINK (Destination, POSIX hard link)
 */
struct xiofs_transport_ops {
  int (*connect)(void *ctx);
  void (*disconnect)(void *ctx);

  int (*lookup)(void *ctx, const char *parent, const char *name,
                struct xiofs_attr *out);
  int (*getattr)(void *ctx, const char *path, struct xiofs_attr *out);
  int (*readdir)(void *ctx, const char *path, struct xiofs_dirent *ents,
                 size_t cap, size_t *nents);

  int (*submit_read_cpu)(void *ctx, const char *path, uint64_t offset,
                         uint64_t length, void *buf, size_t *nread);
  int (*submit_write_cpu)(void *ctx, const char *path, uint64_t offset,
                          uint64_t length, const void *buf, size_t *nwritten);

  int (*mkdir)(void *ctx, const char *path);
  int (*unlink)(void *ctx, const char *path);
  int (*rename)(void *ctx, const char *from, const char *to);

  int (*cancel)(void *ctx, uint64_t request_id);
};

#ifdef __cplusplus
}
#endif

#endif
