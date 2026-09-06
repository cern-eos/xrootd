/* SPDX-License-Identifier: GPL-2.0 */
#ifndef XIOFS_LINUX_H
#define XIOFS_LINUX_H

#include <linux/errno.h>
#include <linux/fs.h>
#include <linux/mutex.h>
#include <linux/net.h>
#include <linux/types.h>
#include <linux/wait.h>

#include "xiofs_uapi.h"

struct fs_context;

#define XIOFS_MAGIC		0x58494F31u /* "XIO1" */
#define XIOFS_RA_BYTES	(4u * 1024u * 1024u)
#define XIOFS_MAX_HDR	8192
#define XIOFS_MAX_DAV	(1u * 1024u * 1024u)
#define XIOFS_PATH_MAX	1024
#define XIOFS_DEF_ACTIMEO_SEC	30u
#define XIOFS_DEF_TIMEO_SEC	30u

struct xiofs_attr {
	loff_t		size;
	time64_t	mtime;
	bool		is_dir;
	char		etag[128];
};

struct xiofs_dirent {
	char		name[256];
	loff_t		size;
	time64_t	mtime;
	bool		is_dir;
};

struct xiofs_sb_info {
	struct super_block	*sb;
	struct list_head	list;
	struct mutex		io_lock;
	wait_queue_head_t	sock_wait;
	struct socket		*sock;
	bool			tls;
	bool			shutting_down;
	unsigned int		actimeo_sec;
	unsigned int		timeo_sec;
	char			host[256];
	unsigned int		port;
	char			export_path[256];
	char			bearer[512];
	char			hosthdr[288];
};

struct xiofs_inode_info {
	struct inode		vfs_inode;
	char			remote_path[XIOFS_PATH_MAX];
	char			etag[128];
	unsigned long		attr_jiffies;
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

int xiofs_fill_super(struct super_block *sb, struct fs_context *fc);
struct inode *xiofs_iget(struct super_block *sb, const char *path,
			    const struct xiofs_attr *attr);
int xiofs_join_path(char *dst, size_t dstsz, const char *parent,
		       const char *name);

extern const struct inode_operations xiofs_dir_inode_ops;
extern const struct inode_operations xiofs_file_inode_ops;
extern const struct file_operations xiofs_file_ops;
extern const struct file_operations xiofs_dir_ops;
extern const struct address_space_operations xiofs_aops;
extern const struct dentry_operations xiofs_dops;

int xiofs_session_init(void);
void xiofs_session_exit(void);
void xiofs_session_register(struct xiofs_sb_info *sbi);
void xiofs_session_unregister(struct xiofs_sb_info *sbi);
void xiofs_session_close(struct xiofs_sb_info *sbi);
int xiofs_session_wait(struct xiofs_sb_info *sbi);
void xiofs_session_drop(struct xiofs_sb_info *sbi);

int xiofs_http_status_to_errno(int status);
int xiofs_http_getattr_path(struct xiofs_sb_info *sbi, const char *path,
			       struct xiofs_attr *attr);
int xiofs_http_getattr(struct inode *inode, struct xiofs_attr *attr);
int xiofs_http_readdir(struct inode *dir, struct xiofs_dirent **ents,
			  size_t *nents);
int xiofs_http_read(struct inode *inode, loff_t off, size_t len,
		       void *buf, size_t *nread);
int xiofs_http_write(struct inode *inode, loff_t off, size_t len,
			const void *buf, size_t *nwritten);
int xiofs_http_create(struct inode *dir, const char *path,
			 struct xiofs_attr *attr);
int xiofs_http_mkdir(struct inode *dir, const char *path);
int xiofs_http_unlink(struct inode *inode);
int xiofs_http_rename(struct inode *old_inode, const char *new_path);
int xiofs_http_truncate(struct inode *inode, loff_t size);
int xiofs_http_chmod(struct inode *inode, umode_t mode);
int xiofs_http_link(struct inode *old_inode, const char *new_path);

int xiofs_rdma_gpu_io(struct file *file, struct xiofs_gpu_io *req,
			 bool writing);

#endif
