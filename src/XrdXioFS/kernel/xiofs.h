/* SPDX-License-Identifier: GPL-2.0 */
#ifndef XIOFS_LINUX_H
#define XIOFS_LINUX_H

#include <linux/errno.h>
#include <linux/fs.h>
#include <linux/list.h>
#include <linux/mutex.h>
#include <linux/net.h>
#include <linux/types.h>
#include <linux/wait.h>
#include <linux/xattr.h>

#include "xiofs_uapi.h"

struct fs_context;

#define XIOFS_MAGIC		0x58494F31u /* "XIO1" */
#define XIOFS_RA_BYTES	(4u * 1024u * 1024u)
/* Largest coalesced PATCH built by writepages. */
#define XIOFS_WB_BYTES	(4u * 1024u * 1024u)
#define XIOFS_MAX_HDR	8192
/* Fixed-buffer DAV/xattr bodies (PROPFIND Depth 0, GET Xrd-Xattr). */
#define XIOFS_MAX_DAV	(1u * 1024u * 1024u)
/* Growable body cap for directory listings (PROPFIND Depth 1). */
#define XIOFS_MAX_LISTING	(256u * 1024u * 1024u)
#define XIOFS_MAX_DIRENTS	(4u * 1024u * 1024u)
#define XIOFS_PATH_MAX	1024
#define XIOFS_ETAG_MAX	96
#define XIOFS_DEF_ACTIMEO_SEC	30u
#define XIOFS_DEF_TIMEO_SEC	30u
#define XIOFS_DEF_CONNS		4u
#define XIOFS_MAX_CONNS		16u
#define XIOFS_H2_PREFACE	"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
#define XIOFS_H2_PREFACE_LEN	24

/* Positive return of the conditional fetchers: server said 304. */
#define XIOFS_NOT_MODIFIED	1

struct xiofs_attr {
	u64		id;		/* server dev:ino from the ETag, 0 = unknown */
	loff_t		size;
	time64_t	mtime;
	time64_t	ctime;
	time64_t	atime;
	umode_t		mode;
	u32		uid;
	u32		gid;
	u32		rdev;
	bool		have_uid;
	bool		have_gid;
	bool		is_dir;
	bool		is_lnk;
	bool		is_fifo;
	bool		is_chr;
	bool		is_blk;
	char		etag[XIOFS_ETAG_MAX];
};

struct xiofs_dirent {
	char		name[256];
	char		etag[XIOFS_ETAG_MAX];
	u64		id;
	loff_t		size;
	time64_t	mtime;
	time64_t	ctime;
	time64_t	atime;
	umode_t		mode;
	u32		uid;
	u32		gid;
	u32		rdev;
	bool		have_uid;
	bool		have_gid;
	bool		is_dir;
	bool		is_lnk;
	bool		is_fifo;
	bool		is_chr;
	bool		is_blk;
};

/*
 * Response metadata. alloc_max is an input: when non-zero and the caller
 * passes no output buffer, the transport kvmallocs the entity into body
 * (up to alloc_max bytes) and the caller must kvfree() it.
 */
struct xiofs_http_resp {
	size_t		alloc_max;
	int		status;
	char		etag[XIOFS_ETAG_MAX];
	long long	content_length;
	char		*body;
	size_t		body_len;
	size_t		body_cap;
};

struct xiofs_sb_info;

/*
 * One already-authenticated HTTP channel. Shared mounts use uid 0.
 * krb5/jwt mounts key by current_fsuid(); GSS and token files stay
 * in xiofsagent. A (mount, uid) may own up to sbi->nconns channels so
 * metadata is not queued behind a 4 MiB GET or PATCH.
 */
struct xiofs_conn {
	struct list_head	list;
	struct xiofs_sb_info	*sbi;
	u32			uid;
	struct mutex		io_lock;
	wait_queue_head_t	wait;
	struct socket		*sock;
	bool			tls;
	bool			http2;
	bool			h2_ready;
	bool			pending;
	int			last_err;
	u32			h2_next_sid;
	u32			h2_send_win;
	char			bearer[XIOFS_BEARER_MAX];
};

struct xiofs_sb_info {
	struct super_block	*sb;
	struct list_head	list;
	struct mutex		conns_lock;
	struct list_head	conns;
	wait_queue_head_t	conn_free;
	bool			krb5;
	bool			jwt;
	bool			http2;
	bool			defer_create;
	bool			shutting_down;
	unsigned int		actimeo_sec;
	unsigned int		timeo_sec;
	unsigned int		nconns;
	char			host[256];
	unsigned int		port;
	char			export_path[256];
	char			bearer[XIOFS_BEARER_MAX];
	char			hosthdr[288];
};

/* xiofs_inode_info.flags */
#define XIOFS_I_DEFERRED	0	/* create() not yet sent as PUT */
#define XIOFS_I_EXCL		1	/* O_EXCL: PUT carries If-None-Match: * */
#define XIOFS_I_GONE		2	/* unlinked before it reached the server */

/*
 * A child created locally but not yet PUT. Lives on the parent's
 * deferred list so readdir shows it before the first flush.
 */
struct xiofs_deferred {
	struct list_head	list;
	struct inode		*inode;
	char			name[256];
};

struct xiofs_inode_info {
	struct inode		vfs_inode;
	u64			remote_id;	/* server dev:ino, 0 = unknown */
	char			etag[XIOFS_ETAG_MAX];
	char			*link_target;
	unsigned long		attr_jiffies;
	unsigned long		flags;
	/* Deferred create: back pointer to the parent and its list entry. */
	struct inode		*defer_parent;
	struct xiofs_deferred	*defer_ent;
	/*
	 * dir_lock serialises the deferred PUT of a file and, on a
	 * directory, guards the listing cache and the deferred list.
	 */
	struct mutex		dir_lock;
	struct list_head	deferred;
	struct xiofs_dirent	*dir_ents;
	size_t			dir_nents;
	char			dir_etag[XIOFS_ETAG_MAX];
	unsigned long		dir_jiffies;
	bool			dir_valid;
};

static inline bool xiofs_connerr(int err)
{
	switch (err) {
	case -ECONNRESET:
	case -ECONNABORTED:
	case -ENOTCONN:
	case -EPIPE:
	case -ETIMEDOUT:
	case -ESHUTDOWN:
	case -EAGAIN:
		return true;
	default:
		return false;
	}
}

static inline struct xiofs_sb_info *XIOFS_SB(struct super_block *sb)
{
	return sb->s_fs_info;
}

static inline struct xiofs_inode_info *XIOFS_I(struct inode *inode)
{
	return container_of(inode, struct xiofs_inode_info, vfs_inode);
}

static inline bool xiofs_is_deferred(struct inode *inode)
{
	return test_bit(XIOFS_I_DEFERRED, &XIOFS_I(inode)->flags);
}

/* super.c */
int xiofs_fill_super(struct super_block *sb, struct fs_context *fc);
struct inode *xiofs_iget(struct super_block *sb, const struct xiofs_attr *attr);
struct inode *xiofs_new_inode(struct super_block *sb,
			      const struct xiofs_attr *attr);
void xiofs_inode_set_id(struct inode *inode, u64 id);
int xiofs_join_path(char *dst, size_t dstsz, const char *parent,
		       const char *name);
int xiofs_dentry_path(struct dentry *dentry, char *buf, size_t sz);
int xiofs_inode_path(struct inode *inode, char *buf, size_t sz);
u64 xiofs_etag_id(const char *etag);

/* inode.c */
void xiofs_refresh_inode(struct inode *inode, const struct xiofs_attr *attr);
void xiofs_dir_invalidate(struct inode *dir);
void xiofs_deferred_del(struct inode *inode);
int xiofs_create_now(struct inode *inode, const void *buf, size_t len);
int xiofs_ensure_created(struct inode *inode);
bool xiofs_attr_fresh(struct inode *inode);
unsigned long xiofs_actimeo_jiffies(struct xiofs_sb_info *sbi);

extern const struct inode_operations xiofs_dir_inode_ops;
extern const struct inode_operations xiofs_file_inode_ops;
extern const struct inode_operations xiofs_symlink_inode_ops;
extern const struct file_operations xiofs_file_ops;
extern const struct file_operations xiofs_dir_ops;
extern const struct address_space_operations xiofs_aops;
extern const struct dentry_operations xiofs_dops;
extern const struct xattr_handler * const xiofs_xattr_handlers[];

/* session.c */
int xiofs_session_init(void);
void xiofs_session_exit(void);
void xiofs_session_register(struct xiofs_sb_info *sbi);
void xiofs_session_unregister(struct xiofs_sb_info *sbi);
void xiofs_session_close(struct xiofs_sb_info *sbi);
int xiofs_conn_get(struct xiofs_sb_info *sbi, struct xiofs_conn **out);
void xiofs_conn_put(struct xiofs_conn *c);
int xiofs_conn_wait(struct xiofs_conn *c);
void xiofs_conn_drop(struct xiofs_conn *c);

/* http1.c */
int xiofs_http_status_to_errno(int status);
int xiofs_http_getattr_path(struct xiofs_sb_info *sbi, const char *path,
			    const char *if_none_match, struct xiofs_attr *attr);
int xiofs_http_getattr(struct inode *inode, const char *if_none_match,
		       struct xiofs_attr *attr);
int xiofs_http_readdir(struct inode *dir, const char *if_none_match,
		       struct xiofs_dirent **ents, size_t *nents,
		       char *etag, size_t etag_sz);
int xiofs_http_read(struct inode *inode, loff_t off, size_t len,
		       void *buf, size_t *nread);
int xiofs_http_write(struct inode *inode, loff_t off, size_t len,
			const void *buf, size_t *nwritten);
int xiofs_http_put(struct inode *inode, const void *buf, size_t len,
		   bool if_match, bool if_none_star);
int xiofs_http_create(struct inode *dir, const char *path, umode_t mode,
		      bool excl, struct xiofs_attr *attr);
int xiofs_http_mkdir(struct inode *dir, const char *path, umode_t mode,
		     struct xiofs_attr *attr);
int xiofs_http_unlink(struct inode *inode);
int xiofs_http_rename(struct inode *old_inode, const char *new_path);
int xiofs_http_truncate(struct inode *inode, loff_t size);
int xiofs_http_setattr(struct inode *inode, int mode, u32 uid, u32 gid,
		       time64_t atime, time64_t mtime);
int xiofs_http_link(struct inode *old_inode, const char *new_path);
int xiofs_http_symlink(struct inode *dir, const char *path, const char *target);
int xiofs_http_readlink(struct inode *inode, char *buf, size_t buflen);
int xiofs_http_mknod(struct inode *dir, const char *path, umode_t mode, dev_t rdev);
int xiofs_http_getxattr(struct inode *inode, const char *name, void *buf,
			size_t size);
int xiofs_http_setxattr(struct inode *inode, const char *name, const void *buf,
			size_t size);
int xiofs_http_listxattr(struct inode *inode, char *buf, size_t size);
int xiofs_http_removexattr(struct inode *inode, const char *name);
int xiofs_http_lock(struct inode *inode, int cmd, int type, int whence,
		    loff_t start, loff_t len);
int xiofs_http_flock(struct inode *inode, int op);

int xiofs_sock_send(struct socket *sock, const void *buf, size_t len);
int xiofs_sock_recv(struct socket *sock, void *buf, size_t len);
int xiofs_sock_recv_some(struct socket *sock, void *buf, size_t len);
int xiofs_resp_body_grow(struct xiofs_http_resp *meta, size_t need);

/* http2.c */
void xiofs_h2_reset(struct xiofs_conn *c);
int xiofs_h2_transact(struct xiofs_conn *c, const char *req, size_t reqlen,
		      const void *body, size_t bodylen, void *out, size_t outcap,
		      size_t *outlen, struct xiofs_http_resp *meta);

/* rdma.c */
int xiofs_rdma_gpu_io(struct file *file, struct xiofs_gpu_io *req,
			 bool writing);

#endif
