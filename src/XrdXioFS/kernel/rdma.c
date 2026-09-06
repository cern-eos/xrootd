// SPDX-License-Identifier: GPL-2.0
/*
 * RDMA / GPU-direct transport. Not implemented: HTTP/kTLS cannot DMA
 * into GPU VRAM, and XrdHttp has no RDMA endpoint yet.
 *
 * When this is filled in it will implement xiofs_transport_ops
 * submit_read_gpu / submit_write_gpu against an HFS/RDMA service,
 * using DMA-BUF as the memory target.
 */
#include <linux/errno.h>
#include <linux/fs.h>

#include "xiofs.h"

int xiofs_rdma_gpu_io(struct file *file, struct xiofs_gpu_io *req,
			 bool writing)
{
	if (!req->length || req->dmabuf_fd < 0)
		return -EINVAL;
	return -EOPNOTSUPP;
}
