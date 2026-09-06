// SPDX-License-Identifier: GPL-2.0
/*
 * File I/O goes through the Linux page cache.
 *
 * AlmaLinux 9 (5.14): readpage / readahead_page / write_cache_pages
 * AlmaLinux 10 (6.12): read_folio / readahead_folio / writeback_iter
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
#include <linux/uaccess.h>
#include <linux/uio.h>
#include <linux/writeback.h>

#include "xiofs.h"
#include "xiofs_compat.h"

static int xiofs_fill_page(struct inode *inode, struct page *page,
			      const void *src, loff_t src_pos, size_t src_len)
{
	size_t fsz = PAGE_SIZE;
	loff_t fpos = page_offset(page);
	void *kaddr = kmap_local_page(page);
	size_t copy = 0;

	if (fpos >= src_pos && (u64)(fpos - src_pos) < src_len) {
		size_t skip = (size_t)(fpos - src_pos);

		copy = min(fsz, src_len - skip);
		memcpy(kaddr, src + skip, copy);
	}
	if (copy < fsz)
		memset((char *)kaddr + copy, 0, fsz - copy);
	kunmap_local(kaddr);
	SetPageUptodate(page);
	unlock_page(page);
	return 0;
}

#ifdef XIOFS_HAS_READ_FOLIO
static int xiofs_read_folio(struct file *file, struct folio *folio)
{
	struct page *page = folio_page(folio, 0);
	struct inode *inode = folio->mapping->host;
	size_t fsz = folio_size(folio);
	loff_t fpos = folio_pos(folio);
	void *buf;
	size_t nread = 0;
	int err;

	buf = kvmalloc(fsz, GFP_KERNEL);
	if (!buf) {
		folio_set_error(folio);
		folio_unlock(folio);
		return -ENOMEM;
	}
	err = xiofs_http_read(inode, fpos, fsz, buf, &nread);
	if (err && err != -EINVAL) {
		kvfree(buf);
		folio_set_error(folio);
		folio_unlock(folio);
		return err;
	}
	xiofs_fill_page(inode, page, buf, fpos, nread);
	kvfree(buf);
	return 0;
}
#else
static int xiofs_readpage(struct file *file, struct page *page)
{
	struct inode *inode = page->mapping->host;
	loff_t fpos = page_offset(page);
	void *buf;
	size_t nread = 0;
	int err;

	buf = kvmalloc(PAGE_SIZE, GFP_KERNEL);
	if (!buf) {
		SetPageError(page);
		unlock_page(page);
		return -ENOMEM;
	}
	err = xiofs_http_read(inode, fpos, PAGE_SIZE, buf, &nread);
	if (err && err != -EINVAL) {
		kvfree(buf);
		SetPageError(page);
		unlock_page(page);
		return err;
	}
	xiofs_fill_page(inode, page, buf, fpos, nread);
	kvfree(buf);
	return 0;
}
#endif

static void xiofs_readahead(struct readahead_control *rac)
{
	struct inode *inode = rac->mapping->host;
	loff_t pos = readahead_pos(rac);
	size_t len = readahead_length(rac);
	void *buf;
	size_t nread = 0;
	int err;

	if (!len)
		return;
	if (len > XIOFS_RA_BYTES)
		len = XIOFS_RA_BYTES;

	buf = kvmalloc(len, GFP_KERNEL);
	if (!buf)
		goto fallback;

	err = xiofs_http_read(inode, pos, len, buf, &nread);
	if (err) {
		kvfree(buf);
		buf = NULL;
		goto fallback;
	}

#ifdef XIOFS_HAS_READAHEAD_FOLIO
	{
		struct folio *folio;

		while ((folio = readahead_folio(rac)) != NULL)
			xiofs_fill_page(inode, folio_page(folio, 0),
					   buf, pos, nread);
	}
#else
	{
		struct page *page;

		while ((page = readahead_page(rac)) != NULL) {
			xiofs_fill_page(inode, page, buf, pos, nread);
			put_page(page);
		}
	}
#endif
	kvfree(buf);
	return;

fallback:
#ifdef XIOFS_HAS_READAHEAD_FOLIO
	{
		struct folio *folio;

		while ((folio = readahead_folio(rac)) != NULL) {
			folio_set_error(folio);
			folio_unlock(folio);
		}
	}
#else
	{
		struct page *page;

		while ((page = readahead_page(rac)) != NULL) {
			SetPageError(page);
			unlock_page(page);
			put_page(page);
		}
	}
#endif
	kvfree(buf);
}

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
	pgoff_t index = pos >> PAGE_SHIFT;
	unsigned int offset = pos & (PAGE_SIZE - 1);
	int err = 0;

#ifdef XIOFS_HAS_WRITE_BEGIN_NOFLAGS
	page = grab_cache_page_write_begin(mapping, index);
#else
	page = grab_cache_page_write_begin(mapping, index, flags);
#endif
	if (!page)
		return -ENOMEM;

	if (!PageUptodate(page) && (offset || len < PAGE_SIZE)) {
		void *buf = kvmalloc(PAGE_SIZE, GFP_KERNEL);
		size_t nread = 0;
		loff_t fpos = page_offset(page);

		if (!buf) {
			unlock_page(page);
			put_page(page);
			return -ENOMEM;
		}
		err = xiofs_http_read(mapping->host, fpos, PAGE_SIZE,
					 buf, &nread);
		if (err && err != -ENOENT && err != -EINVAL) {
			kvfree(buf);
			unlock_page(page);
			put_page(page);
			return err;
		}
		if (nread < PAGE_SIZE)
			memset((char *)buf + nread, 0, PAGE_SIZE - nread);
		xiofs_copy_to_page(page, buf);
		kvfree(buf);
		SetPageUptodate(page);
	}
	*pagep = page;
	return 0;
}

static int xiofs_write_end(struct file *file, struct address_space *mapping,
			      loff_t pos, unsigned int len, unsigned int copied,
			      struct page *page, void *fsdata)
{
	struct inode *inode = mapping->host;

	if (copied < len && !PageUptodate(page)) {
		zero_user(page, 0, PAGE_SIZE);
		copied = 0;
	} else {
		SetPageUptodate(page);
	}
	if (pos + copied > i_size_read(inode))
		i_size_write(inode, pos + copied);
	XIOFS_I(inode)->attr_jiffies = jiffies;
	set_page_dirty(page);
	unlock_page(page);
	put_page(page);
	return copied;
}

static int xiofs_writepage(struct page *page, struct writeback_control *wbc)
{
	struct inode *inode = page->mapping->host;
	void *kaddr;
	size_t nwritten = 0;
	size_t len = PAGE_SIZE;
	loff_t pos = page_offset(page);
	loff_t isize = i_size_read(inode);
	int err;

	if (pos >= isize) {
		unlock_page(page);
		return 0;
	}
	if (pos + (loff_t)len > isize)
		len = (size_t)(isize - pos);

	kaddr = kmap_local_page(page);
	err = xiofs_http_write(inode, pos, len, kaddr, &nwritten);
	kunmap_local(kaddr);
	if (xiofs_connerr(err)) {
		redirty_page_for_writepage(wbc, page);
		unlock_page(page);
		return err;
	}
	if (err) {
		SetPageError(page);
		mapping_set_error(page->mapping, err);
		unlock_page(page);
		return err;
	}
	ClearPageDirty(page);
	unlock_page(page);
	return 0;
}

static int xiofs_writepage_cb(struct page *page,
				 struct writeback_control *wbc, void *data)
{
	return xiofs_writepage(page, wbc);
}

#ifdef XIOFS_HAS_WRITEBACK_ITER
static int xiofs_writepages(struct address_space *mapping,
			       struct writeback_control *wbc)
{
	struct inode *inode = mapping->host;
	struct folio *folio = NULL;
	int error = 0;

	while ((folio = writeback_iter(mapping, wbc, folio, &error))) {
		loff_t pos = folio_pos(folio);
		loff_t isize = i_size_read(inode);
		size_t len = folio_size(folio);
		void *kaddr;
		size_t nwritten = 0;
		int err = 0;

		if (pos >= isize) {
			folio_unlock(folio);
			folio_end_writeback(folio);
			continue;
		}
		if (pos + (loff_t)len > isize)
			len = (size_t)(isize - pos);

		kaddr = kmap_local_folio(folio, 0);
		err = xiofs_http_write(inode, pos, len, kaddr, &nwritten);
		kunmap_local(kaddr);
		if (xiofs_connerr(err)) {
			folio_redirty_for_writepage(wbc, folio);
			folio_unlock(folio);
			folio_end_writeback(folio);
			if (!error)
				error = err;
			continue;
		}
		if (err) {
			folio_set_error(folio);
			mapping_set_error(mapping, err);
			folio_unlock(folio);
			folio_end_writeback(folio);
			if (!error)
				error = err;
			continue;
		}
		folio_unlock(folio);
		folio_end_writeback(folio);
	}
	return error;
}
#else
static int xiofs_writepages(struct address_space *mapping,
			       struct writeback_control *wbc)
{
	return write_cache_pages(mapping, wbc, xiofs_writepage_cb, NULL);
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
	.writepage	= xiofs_writepage,
	.writepages	= xiofs_writepages,
#ifdef XIOFS_HAS_DIRTY_FOLIO
	.dirty_folio	= filemap_dirty_folio,
#else
	.set_page_dirty	= __set_page_dirty_nobuffers,
#endif
};

static int xiofs_fsync(struct file *file, loff_t start, loff_t end,
			  int datasync)
{
	return file_write_and_wait_range(file, start, end);
}

static long xiofs_ioctl(struct file *file, unsigned int cmd,
			   unsigned long arg)
{
	struct xiofs_gpu_io req;

	if (cmd != XIOFS_IOC_GPU_READ && cmd != XIOFS_IOC_GPU_WRITE)
		return -ENOTTY;
	if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
		return -EFAULT;
	return xiofs_rdma_gpu_io(file, &req, cmd == XIOFS_IOC_GPU_WRITE);
}

const struct file_operations xiofs_file_ops = {
	.owner		= THIS_MODULE,
	.read_iter	= generic_file_read_iter,
	.write_iter	= generic_file_write_iter,
	.mmap		= generic_file_mmap,
	.fsync		= xiofs_fsync,
	.llseek		= generic_file_llseek,
#ifdef XIOFS_HAS_FILEMAP_SPLICE_READ
	.splice_read	= filemap_splice_read,
#else
	.splice_read	= generic_file_splice_read,
#endif
	.unlocked_ioctl	= xiofs_ioctl,
};
