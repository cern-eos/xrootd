// SPDX-License-Identifier: GPL-2.0
#include <linux/fs.h>
#include <linux/namei.h>

#include "kernelfs.h"

static struct dentry *kernelfs_lookup(struct inode *dir, struct dentry *dentry,
				      unsigned int flags)
{
	return ERR_PTR(-ENOENT);
}

const struct inode_operations kernelfs_dir_inode_ops = {
	.lookup = kernelfs_lookup,
};

const struct inode_operations kernelfs_file_inode_ops = {
	.getattr = simple_getattr,
};
