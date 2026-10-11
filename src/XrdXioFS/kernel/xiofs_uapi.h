/* SPDX-License-Identifier: GPL-2.0 */
#ifndef XIOFS_UAPI_H
#define XIOFS_UAPI_H

/*
 * Userspace TLS handshake agent <-> xiofs.ko.
 *
 * After OpenSSL finishes the handshake, install kTLS with
 * SSL_OP_ENABLE_KTLS (see xiofsagent) and pass the socket here.
 * Do not extract keys and roll a private TLS record layer.
 *
 * Kerberos (SPNEGO) and JWT bearer files cannot live in-kernel.
 * The agent finishes Negotiate or copies a uid-owned token file, then
 * imports the socket bound to that uid. The kernel keeps one
 * already-authenticated HTTP channel per uid. kdestroy / token
 * replacement does not touch that channel; xiofsagent --drop (this
 * ioctl) throws the uid's sockets away so the next I/O re-auths.
 */
#include <linux/ioctl.h>
#include <linux/types.h>

#define XIOFS_IOC_MAGIC		0x58

#define XIOFS_IMPORT_TLS		(1u << 0)
#define XIOFS_IMPORT_BEARER		(1u << 1)
#define XIOFS_IMPORT_H2			(1u << 2)
#define XIOFS_IMPORT_KRB5		(1u << 3)
#define XIOFS_IMPORT_JWT		(1u << 4)

/* WLCG / XrdSecztn default max. OIDC JWTs do not fit in 512 bytes. */
#ifndef XIOFS_BEARER_MAX
#define XIOFS_BEARER_MAX		4096
#endif

struct xiofs_import_sock {
	__s32	sockfd;
	__u32	flags;
	__u16	port;
	__u16	reserved;
	__u32	uid;
	__u32	pad;
	char	host[256];
	char	export_path[256];
	char	bearer[XIOFS_BEARER_MAX];
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

/*
 * Agent upcall: wait until some uid on a krb5/jwt/shard/hard (or
 * reconnecting) mount needs a socket. One WAIT_NEED dequeues one (sbi, uid) so several
 * xiofsagent processes can run in parallel. host/port are the target for
 * this channel. err is 0 on first connect, or the kernel connection errno
 * (timeout/reset) so the agent can take that host out of the live list.
 */
struct xiofs_need_conn {
	__u32	uid;
	__s32	err;
	__u16	port;
	__u16	flags;
	char	host[256];
	char	export_path[256];
};

#define XIOFS_IOC_WAIT_NEED \
	_IOWR(XIOFS_IOC_MAGIC, 4, struct xiofs_need_conn)
#define XIOFS_IOC_NEED_FAIL \
	_IOW(XIOFS_IOC_MAGIC, 5, struct xiofs_need_conn)
/*
 * Drop imported sockets for uc.uid. On /dev/xiofsctl (root) also match
 * host/port/path. On a file or directory of the mount, the superblock
 * selects the mount; a non-admin caller may only drop current_fsuid().
 */
#define XIOFS_IOC_DROP_CONN \
	_IOW(XIOFS_IOC_MAGIC, 6, struct xiofs_need_conn)

#ifndef XIOFS_MAX_TARGETS
#define XIOFS_MAX_TARGETS		32
#endif

struct xiofs_target_ent {
	char	host[256];
	__u16	port;
	__u16	reserved;
};

struct xiofs_targets {
	__u32			n;
	__u16			port;
	__u16			reserved;
	char			host[256];
	char			export_path[256];
	struct xiofs_target_ent	ent[XIOFS_MAX_TARGETS];
};

/*
 * Replace the live HTTP target list for a mount. The agent expands
 * aliases and removes hosts in cooldown; the kernel hashes fsuid onto
 * this list. host/port/path identify the mount (original alias or
 * first server).
 */
#define XIOFS_IOC_SET_TARGETS \
	_IOW(XIOFS_IOC_MAGIC, 7, struct xiofs_targets)

#endif
