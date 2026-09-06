/* SPDX-License-Identifier: GPL-2.0 */
#ifndef XIOFS_UAPI_H
#define XIOFS_UAPI_H

/*
 * Userspace TLS handshake agent <-> xiofs.ko.
 *
 * After OpenSSL finishes the handshake, install kTLS with
 * SSL_OP_ENABLE_KTLS (see xiofsagent) and pass the socket here.
 * Do not extract keys and roll a private TLS record layer.
 */
#include <linux/ioctl.h>
#include <linux/types.h>

#define XIOFS_IOC_MAGIC		0x58

#define XIOFS_IMPORT_TLS		(1u << 0)
#define XIOFS_IMPORT_BEARER		(1u << 1)

struct xiofs_import_sock {
	__s32	sockfd;
	__u32	flags;
	__u16	port;
	__u16	reserved;
	char	host[256];
	char	export_path[256];
	char	bearer[512];
};

#define XIOFS_IOC_IMPORT_SOCK \
	_IOW(XIOFS_IOC_MAGIC, 1, struct xiofs_import_sock)

/* GPU-direct (RDMA transport only). HTTP/kTLS returns -EOPNOTSUPP. */
struct xiofs_gpu_io {
	__u64	file_offset;
	__u64	length;
	__s32	dmabuf_fd;
	__u32	flags;
	__u64	buffer_offset;
};

#define XIOFS_IOC_GPU_READ \
	_IOW(XIOFS_IOC_MAGIC, 2, struct xiofs_gpu_io)
#define XIOFS_IOC_GPU_WRITE \
	_IOW(XIOFS_IOC_MAGIC, 3, struct xiofs_gpu_io)

#endif
