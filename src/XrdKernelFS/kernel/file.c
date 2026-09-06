// SPDX-License-Identifier: GPL-2.0
#include <linux/fs.h>
#include <linux/module.h>
#include <linux/uio.h>

#include "kernelfs.h"

static ssize_t kernelfs_read_iter(struct kiocb *iocb, struct iov_iter *to)
{
	return -ENOSYS;
}

const struct file_operations kernelfs_file_ops = {
	.owner = THIS_MODULE,
	.read_iter = kernelfs_read_iter,
	.llseek = generic_file_llseek,
};
