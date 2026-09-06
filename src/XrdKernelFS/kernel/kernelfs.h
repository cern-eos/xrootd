/* SPDX-License-Identifier: GPL-2.0 */
#ifndef KERNELFS_LINUX_H
#define KERNELFS_LINUX_H

#define KERNELFS_MAGIC 0x4B465331u /* "KFS1" */

struct super_block;
struct inode;
struct dentry;
struct file;

int kernelfs_fill_super(struct super_block *sb, void *data, int silent);

extern const struct inode_operations kernelfs_dir_inode_ops;
extern const struct inode_operations kernelfs_file_inode_ops;
extern const struct file_operations kernelfs_file_ops;

/* HTTP/1.1 transport stubs: implemented in http1.c */
int kernelfs_http1_getattr(const char *path, loff_t *size);
int kernelfs_http1_read(const char *path, loff_t off, size_t len,
			void *buf, size_t *nread);

#endif
