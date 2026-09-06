// SPDX-License-Identifier: GPL-2.0
/*
 * Placeholder HTTP/1.1 Range client. The FUSE prototype already speaks
 * the XrdHttp verb map over HTTP/2; this file is where persistent TCP,
 * a minimal HTTP/1.1 parser, and kTLS sockets will land.
 */
#include <linux/errno.h>
#include <linux/types.h>

#include "kernelfs.h"

int kernelfs_http1_getattr(const char *path, loff_t *size)
{
	return -ENOSYS;
}

int kernelfs_http1_read(const char *path, loff_t off, size_t len,
			void *buf, size_t *nread)
{
	return -ENOSYS;
}
