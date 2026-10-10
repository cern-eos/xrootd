// SPDX-License-Identifier: GPL-2.0
/*
 * File I/O goes through the Linux page cache.
 *
 * AlmaLinux 9 (5.14): readpage / readahead_page / write_cache_pages
 * AlmaLinux 10 (6.12): read_folio / readahead_folio / writeback_iter
 *
 * Writeback coalesces runs of contiguous dirty pages into one PATCH of
 * up to XIOFS_WB_BYTES. A file whose create() was deferred and whose
 * first writeback covers the whole file is created with a single PUT
 * carrying the data.
 */
#include <linux/buffer_head.h>
#include <linux/fs.h>
#include <linux/highmem.h>
#include <linux/jiffies.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/timekeeping.h>
#include <linux/uaccess.h>
#include <linux/uio.h>
#include <linux/writeback.h>

#include "xiofs.h"
#include "xiofs_compat.h"
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 9, 0)
#include <linux/filelock.h>
#endif

/*                                   r e a d                                 */

static void xiofs_fill_page(struct page *page, const void *src,
			    loff_t src_pos, size_t src_len)
{
	size_t fsz = PAGE_SIZE;
	loff_t fpos = page_offset(page);
	void *kaddr = kmap_local_page(page);
	size_t copy = 0;

	if (src && fpos >= src_pos && (u64)(fpos - src_pos) < src_len) {
		size_t skip = (size_t)(fpos - src_pos);

		copy = min(fsz, src_len - skip);
		memcpy(kaddr, src + skip, copy);
	}
	if (copy < fsz)
		memset((char *)kaddr + copy, 0, fsz - copy);
	kunmap_local(kaddr);
	SetPageUptodate(page);
	unlock_page(page);
}

/*
 * A read that cannot return data from the server: the object is not
 * there yet (deferred create) or we know the range is past EOF.
 */
static bool xiofs_read_is_hole(struct inode *inode, loff_t pos)
{
	return xiofs_is_deferred(inode) ||
	       (xiofs_attr_fresh(inode) && pos >= i_size_read(inode));
}

static int xiofs_read_one(struct inode *inode, struct page *page)
{
	loff_t fpos = page_offset(page);
	void *buf;
	size_t nread = 0;
	int err;

	if (xiofs_read_is_hole(inode, fpos)) {
		xiofs_fill_page(page, NULL, fpos, 0);
		return 0;
	}
	buf = kvmalloc(PAGE_SIZE, GFP_KERNEL);
	if (!buf) {
		unlock_page(page);
		return -ENOMEM;
	}
	err = xiofs_http_read(inode, fpos, PAGE_SIZE, buf, &nread);
	if (err && err != -EINVAL) {
		kvfree(buf);
		unlock_page(page);
		return err;
	}
	xiofs_fill_page(page, buf, fpos, nread);
	kvfree(buf);
	return 0;
}

#ifdef XIOFS_HAS_READ_FOLIO
static int xiofs_read_folio(struct file *file, struct folio *folio)
{
	return xiofs_read_one(folio->mapping->host, folio_page(folio, 0));
}
#else
static int xiofs_readpage(struct file *file, struct page *page)
{
	return xiofs_read_one(page->mapping->host, page);
}
#endif

static void xiofs_readahead(struct readahead_control *rac)
{
	struct inode *inode = rac->mapping->host;
	loff_t pos = readahead_pos(rac);
	size_t len = readahead_length(rac);
	void *buf = NULL;
	size_t nread = 0;
	bool ok = true;
	int err;

	if (!len)
		return;
	if (len > XIOFS_RA_BYTES)
		len = XIOFS_RA_BYTES;

	if (!xiofs_read_is_hole(inode, pos)) {
		buf = kvmalloc(len, GFP_KERNEL);
		if (!buf) {
			ok = false;
		} else {
			err = xiofs_http_read(inode, pos, len, buf, &nread);
			if (err && err != -EINVAL)
				ok = false;
		}
	}

#ifdef XIOFS_HAS_READAHEAD_FOLIO
	{
		struct folio *folio;

		while ((folio = readahead_folio(rac)) != NULL) {
			if (ok)
				xiofs_fill_page(folio_page(folio, 0), buf, pos,
						nread);
			else
				folio_unlock(folio);
		}
	}
#else
	{
		struct page *page;

		while ((page = readahead_page(rac)) != NULL) {
			if (ok)
				xiofs_fill_page(page, buf, pos, nread);
			else
				unlock_page(page);
			put_page(page);
		}
	}
#endif
	kvfree(buf);
}

/*                                  w r i t e                                */

/*
 * Bring a page up to date before a partial overwrite. No round trip when
 * the server has nothing for it: a deferred file, or an offset at or
 * past an EOF we trust.
 */
static int xiofs_write_prepare_page(struct inode *inode, struct page *page,
				    loff_t pos, unsigned int len)
{
	loff_t fpos = page_offset(page);
	unsigned int offset = pos & (PAGE_SIZE - 1);
	void *buf;
	size_t nread = 0;
	int err;

	if (PageUptodate(page))
		return 0;
	if (!offset && len >= PAGE_SIZE)
		return 0;	/* fully overwritten in write_end */

	if (xiofs_read_is_hole(inode, fpos)) {
		zero_user(page, 0, PAGE_SIZE);
		SetPageUptodate(page);
		return 0;
	}
	buf = kvmalloc(PAGE_SIZE, GFP_KERNEL);
	if (!buf)
		return -ENOMEM;
	err = xiofs_http_read(inode, fpos, PAGE_SIZE, buf, &nread);
	if (err && err != -ENOENT && err != -EINVAL) {
		kvfree(buf);
		return err;
	}
	if (nread < PAGE_SIZE)
		memset((char *)buf + nread, 0, PAGE_SIZE - nread);
	xiofs_copy_to_page(page, buf);
	kvfree(buf);
	SetPageUptodate(page);
	return 0;
}

static unsigned int xiofs_write_commit_page(struct inode *inode,
					    struct page *page, loff_t pos,
					    unsigned int len,
					    unsigned int copied)
{
	if (copied < len && !PageUptodate(page)) {
		zero_user(page, 0, PAGE_SIZE);
		copied = 0;
	} else {
		SetPageUptodate(page);
	}
	if (pos + copied > i_size_read(inode))
		i_size_write(inode, pos + copied);
	/* Our own write: local size/mtime are authoritative for actimeo. */
	xiofs_set_times(inode, ktime_get_real_seconds());
	XIOFS_I(inode)->attr_jiffies = jiffies;
	set_page_dirty(page);
	return copied;
}

#ifdef XIOFS_HAS_WRITE_BEGIN_FOLIO
static int xiofs_write_begin(struct file *file, struct address_space *mapping,
			     loff_t pos, unsigned int len,
			     struct folio **foliop, void **fsdata)
{
	struct folio *folio;
	int err;

	folio = __filemap_get_folio(mapping, pos >> PAGE_SHIFT,
				    FGP_WRITEBEGIN, mapping_gfp_mask(mapping));
	if (IS_ERR(folio))
		return PTR_ERR(folio);
	err = xiofs_write_prepare_page(mapping->host, folio_page(folio, 0),
				       pos, len);
	if (err) {
		folio_unlock(folio);
		folio_put(folio);
		return err;
	}
	*foliop = folio;
	return 0;
}

static int xiofs_write_end(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned int len, unsigned int copied,
			   struct folio *folio, void *fsdata)
{
	copied = xiofs_write_commit_page(mapping->host, folio_page(folio, 0),
					 pos, len, copied);
	folio_unlock(folio);
	folio_put(folio);
	return copied;
}
#else
#ifdef XIOFS_HAS_WRITE_BEGIN_NOFLAGS
static int xiofs_write_begin(struct file *file, struct address_space *mapping,
			     loff_t pos, unsigned int len,
			     struct page **pagep, void **fsdata)
#else
static int xiofs_write_begin(struct file *file, struct address_space *mapping,
			     loff_t pos, unsigned int len, unsigned int flags,
			     struct page **pagep, void **fsdata)
#endif
{
	struct page *page;
	int err;

#ifdef XIOFS_HAS_WRITE_BEGIN_NOFLAGS
	page = grab_cache_page_write_begin(mapping, pos >> PAGE_SHIFT);
#else
	page = grab_cache_page_write_begin(mapping, pos >> PAGE_SHIFT, flags);
#endif
	if (!page)
		return -ENOMEM;
	err = xiofs_write_prepare_page(mapping->host, page, pos, len);
	if (err) {
		unlock_page(page);
		put_page(page);
		return err;
	}
	*pagep = page;
	return 0;
}

static int xiofs_write_end(struct file *file, struct address_space *mapping,
			   loff_t pos, unsigned int len, unsigned int copied,
			   struct page *page, void *fsdata)
{
	copied = xiofs_write_commit_page(mapping->host, page, pos, len, copied);
	unlock_page(page);
	put_page(page);
	return copied;
}
#endif

/*                              w r i t e b a c k                            */

struct xiofs_wb {
	struct inode			*inode;
	struct writeback_control	*wbc;
	char				*buf;
	struct page			**pages;
	unsigned int			npages;
	unsigned int			maxpages;
	loff_t				pos;	/* file offset of buf[0] */
	int				error;
};

static int xiofs_wb_init(struct xiofs_wb *wb, struct inode *inode,
			 struct writeback_control *wbc)
{
	loff_t isize = i_size_read(inode);
	size_t bytes = XIOFS_WB_BYTES;

	if (isize < (loff_t)bytes)
		bytes = max_t(size_t, PAGE_SIZE, round_up((size_t)isize, PAGE_SIZE));
	memset(wb, 0, sizeof(*wb));
	wb->inode = inode;
	wb->wbc = wbc;
	wb->maxpages = bytes / PAGE_SIZE;
	wb->buf = kvmalloc(bytes, GFP_NOFS);
	wb->pages = kvmalloc_array(wb->maxpages, sizeof(*wb->pages), GFP_NOFS);
	if (!wb->buf || !wb->pages) {
		kvfree(wb->buf);
		kvfree(wb->pages);
		return -ENOMEM;
	}
	return 0;
}

static void xiofs_wb_destroy(struct xiofs_wb *wb)
{
	kvfree(wb->buf);
	kvfree(wb->pages);
}

/* Send the batch as one request and finish writeback on its pages. */
static void xiofs_wb_flush(struct xiofs_wb *wb)
{
	struct inode *inode = wb->inode;
	loff_t isize = i_size_read(inode);
	size_t len = (size_t)wb->npages * PAGE_SIZE;
	size_t nwritten = 0;
	unsigned int i;
	int err = 0;

	if (!wb->npages)
		return;
	if (wb->pos >= isize)
		len = 0;
	else if (wb->pos + (loff_t)len > isize)
		len = (size_t)(isize - wb->pos);

	if (len) {
		bool patch = true;

		if (xiofs_is_deferred(inode) && wb->pos == 0 &&
		    (loff_t)len == isize) {
			/* Whole file in hand: create and write in one PUT. */
			err = xiofs_create_now(inode, wb->buf, len);
			patch = (err == -EALREADY);
			if (patch)
				err = 0;
		}
		if (!err && patch) {
			err = xiofs_ensure_created(inode);
			if (!err)
				err = xiofs_http_write(inode, wb->pos, len,
						       wb->buf, &nwritten);
		}
	} else if (xiofs_is_deferred(inode)) {
		err = xiofs_ensure_created(inode);
	}

	for (i = 0; i < wb->npages; i++) {
		struct page *page = wb->pages[i];

		if (xiofs_connerr(err))
			xiofs_page_redirty(wb->wbc, page);
		else if (err)
			mapping_set_error(inode->i_mapping, err);
		xiofs_page_end_wb(page);
		put_page(page);
	}
	if (err && !wb->error)
		wb->error = err;
	wb->npages = 0;
}

/* Take a locked, clean-for-io page into the batch. Unlocks it. */
static void xiofs_wb_add(struct xiofs_wb *wb, struct page *page)
{
	loff_t fpos = page_offset(page);
	void *kaddr;

	if (wb->npages &&
	    (fpos != wb->pos + (loff_t)wb->npages * PAGE_SIZE ||
	     wb->npages == wb->maxpages))
		xiofs_wb_flush(wb);
	if (!wb->npages)
		wb->pos = fpos;

	kaddr = kmap_local_page(page);
	memcpy(wb->buf + (size_t)wb->npages * PAGE_SIZE, kaddr, PAGE_SIZE);
	kunmap_local(kaddr);

	get_page(page);
	xiofs_page_start_wb(page);
	unlock_page(page);
	wb->pages[wb->npages++] = page;
}

#ifdef XIOFS_HAS_WRITEBACK_ITER
static int xiofs_writepages(struct address_space *mapping,
			    struct writeback_control *wbc)
{
	struct xiofs_wb wb;
	struct folio *folio = NULL;
	int error = 0;
	int err;

	err = xiofs_wb_init(&wb, mapping->host, wbc);
	if (err)
		return err;
	while ((folio = writeback_iter(mapping, wbc, folio, &error))) {
		xiofs_wb_add(&wb, folio_page(folio, 0));
		error = 0;
	}
	xiofs_wb_flush(&wb);
	err = wb.error;
	xiofs_wb_destroy(&wb);
	return err;
}
#else
static int xiofs_writepage_cb(struct page *page, struct writeback_control *wbc,
			      void *data)
{
	xiofs_wb_add(data, page);
	return 0;
}

static int xiofs_writepages(struct address_space *mapping,
			    struct writeback_control *wbc)
{
	struct xiofs_wb wb;
	int err;

	err = xiofs_wb_init(&wb, mapping->host, wbc);
	if (err)
		return err;
	err = write_cache_pages(mapping, wbc, xiofs_writepage_cb, &wb);
	xiofs_wb_flush(&wb);
	if (!err)
		err = wb.error;
	xiofs_wb_destroy(&wb);
	return err;
}
#endif

const struct address_space_operations xiofs_aops = {
#ifdef XIOFS_HAS_READ_FOLIO
	.read_folio	= xiofs_read_folio,
#else
	.readpage	= xiofs_readpage,
#endif
	.readahead	= xiofs_readahead,
	.write_begin	= xiofs_write_begin,
	.write_end	= xiofs_write_end,
	.writepages	= xiofs_writepages,
#ifdef XIOFS_HAS_DIRTY_FOLIO
	.dirty_folio	= filemap_dirty_folio,
#else
	.set_page_dirty	= __set_page_dirty_nobuffers,
#endif
};

/*                                f i l e   o p s                            */

static int xiofs_fsync(struct file *file, loff_t start, loff_t end,
		       int datasync)
{
	int err;

	err = file_write_and_wait_range(file, start, end);
	if (err)
		return err;
	/* An empty new file has no dirty pages; the PUT goes out here. */
	return xiofs_ensure_created(file_inode(file));
}

/*
 * close(2): push the data so the file is on the server when close
 * returns (close-to-open), and report writeback errors to the writer.
 */
static int xiofs_flush(struct file *file, fl_owner_t id)
{
	struct inode *inode = file_inode(file);
	int err;

	(void)id;
	if (!(file->f_mode & FMODE_WRITE))
		return 0;
	err = filemap_write_and_wait(inode->i_mapping);
	if (err)
		return err;
	return xiofs_ensure_created(inode);
}

static long xiofs_ioctl(struct file *file, unsigned int cmd,
			unsigned long arg)
{
	struct xiofs_gpu_io req;
	int err;

	if (cmd != XIOFS_IOC_GPU_READ && cmd != XIOFS_IOC_GPU_WRITE)
		return -ENOTTY;
	if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
		return -EFAULT;
	err = xiofs_ensure_created(file_inode(file));
	if (err)
		return err;
	return xiofs_rdma_gpu_io(file, &req, cmd == XIOFS_IOC_GPU_WRITE);
}

static int xiofs_file_lock(struct file *file, int cmd, struct file_lock *fl)
{
	loff_t len;
	int err;

	if (!fl)
		return -EINVAL;
	if (fl->fl_end == OFFSET_MAX)
		len = 0;
	else if (fl->fl_end < fl->fl_start)
		return -EINVAL;
	else
		len = fl->fl_end - fl->fl_start + 1;
	err = xiofs_ensure_created(file_inode(file));
	if (err)
		return err;
	return xiofs_http_lock(file_inode(file), cmd, xiofs_fl_type(fl), 0,
			       fl->fl_start, len);
}

static int xiofs_file_flock(struct file *file, int cmd, struct file_lock *fl)
{
	int err;

	(void)fl;
	err = xiofs_ensure_created(file_inode(file));
	if (err)
		return err;
	return xiofs_http_flock(file_inode(file), cmd);
}

const struct file_operations xiofs_file_ops = {
	.owner		= THIS_MODULE,
	.read_iter	= generic_file_read_iter,
	.write_iter	= generic_file_write_iter,
	.mmap		= generic_file_mmap,
	.fsync		= xiofs_fsync,
	.flush		= xiofs_flush,
	.llseek		= generic_file_llseek,
#ifdef XIOFS_HAS_FILEMAP_SPLICE_READ
	.splice_read	= filemap_splice_read,
#else
	.splice_read	= generic_file_splice_read,
#endif
	.unlocked_ioctl	= xiofs_ioctl,
	.lock		= xiofs_file_lock,
	.flock		= xiofs_file_flock,
};
