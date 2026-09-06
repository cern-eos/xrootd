// SPDX-License-Identifier: GPL-2.0
/*
 * kernelfs superblock: empty mount until the HTTP/1.1 + kTLS path is wired.
 */
#include <linux/fs.h>
#include <linux/module.h>
#include <linux/statfs.h>

#include "kernelfs.h"

static const struct super_operations kernelfs_sops = {
	.statfs = simple_statfs,
	.drop_inode = generic_delete_inode,
};

int kernelfs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct inode *root;

	sb->s_magic = KERNELFS_MAGIC;
	sb->s_op = &kernelfs_sops;
	sb->s_time_gran = 1;

	root = new_inode(sb);
	if (!root)
		return -ENOMEM;
	root->i_ino = 1;
	root->i_mode = S_IFDIR | 0555;
	root->i_op = &kernelfs_dir_inode_ops;
	root->i_fop = &simple_dir_operations;
	set_nlink(root, 2);

	sb->s_root = d_make_root(root);
	if (!sb->s_root)
		return -ENOMEM;
	return 0;
}

static struct dentry *kernelfs_mount(struct file_system_type *fs_type, int flags,
				     const char *dev_name, void *data)
{
	return mount_nodev(fs_type, flags, data, kernelfs_fill_super);
}

static struct file_system_type kernelfs_type = {
	.owner = THIS_MODULE,
	.name = "kernelfs",
	.mount = kernelfs_mount,
	.kill_sb = kill_anon_super,
};

static int __init kernelfs_init(void)
{
	return register_filesystem(&kernelfs_type);
}

static void __exit kernelfs_exit(void)
{
	unregister_filesystem(&kernelfs_type);
}

module_init(kernelfs_init);
module_exit(kernelfs_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("XRootD Collaboration");
MODULE_DESCRIPTION("KernelFS: VFS frontend for XrdHttp (HTTP/1.1 + kTLS)");
