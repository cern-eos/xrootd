/* SPDX-License-Identifier: GPL-2.0 */
/*
 * AlmaLinux 9  = RHEL 9 kernel 5.14.0-*.el9  (user_namespace, readpage)
 * AlmaLinux 10 = RHEL 10 kernel 6.12.0-*.el10 (mnt_idmap, read_folio)
 *
 * LINUX_VERSION_CODE stays 5.14 on EL9 even when APIs are backported,
 * so version checks (not RHEL_MAJOR alone) pick the published kABI.
 */
#ifndef XIOFS_COMPAT_H
#define XIOFS_COMPAT_H

#include <linux/fs.h>
#include <linux/pagemap.h>
#include <linux/version.h>
#include <linux/writeback.h>

#if LINUX_VERSION_CODE < KERNEL_VERSION(5, 14, 0)
#error "xiofs.ko needs Linux 5.14+ (AlmaLinux 9) or 6.12 (AlmaLinux 10)"
#endif

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 3, 0)
#include <linux/mnt_idmap.h>
typedef struct mnt_idmap *xiofs_idmap_t;
#else
typedef struct user_namespace *xiofs_idmap_t;
#endif

#if LINUX_VERSION_CODE >= KERNEL_VERSION(5, 19, 0)
#define XIOFS_HAS_READ_FOLIO		1
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(5, 18, 0)
#define XIOFS_HAS_READAHEAD_FOLIO		1
#define XIOFS_HAS_DIRTY_FOLIO		1
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(5, 19, 0)
#define XIOFS_HAS_WRITE_BEGIN_NOFLAGS	1
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 2, 0)
#define XIOFS_HAS_WRITEBACK_ITER		1
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 5, 0)
#define XIOFS_HAS_FILEMAP_SPLICE_READ	1
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 6, 0)
#define XIOFS_HAS_INODE_SET_MTIME_TO_TS	1
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 8, 0)
#define XIOFS_HAS_FILLATTR_MASK		1
#endif
/* 6.12: write_begin/write_end hand out a folio instead of a page. */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 12, 0)
#define XIOFS_HAS_WRITE_BEGIN_FOLIO	1
#endif
/* 6.9: struct file_lock common fields moved into file_lock_core. */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 9, 0)
#define xiofs_fl_type(fl)	((fl)->c.flc_type)
#else
#define xiofs_fl_type(fl)	((fl)->fl_type)
#endif

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 8, 0)
#define xiofs_fillattr(idmap, mask, inode, stat) \
	generic_fillattr((idmap), (mask), (inode), (stat))
#elif LINUX_VERSION_CODE >= KERNEL_VERSION(5, 12, 0)
#define xiofs_fillattr(idmap, mask, inode, stat) \
	generic_fillattr((idmap), (inode), (stat))
#else
#define xiofs_fillattr(idmap, mask, inode, stat) \
	generic_fillattr((inode), (stat))
#endif

static inline void xiofs_set_times2(struct inode *inode, time64_t mtime,
				    time64_t atime)
{
	struct timespec64 mt = { .tv_sec = mtime, .tv_nsec = 0 };
	struct timespec64 at = { .tv_sec = atime ? atime : mtime, .tv_nsec = 0 };

#ifdef XIOFS_HAS_INODE_SET_MTIME_TO_TS
	inode_set_mtime_to_ts(inode, mt);
	inode_set_ctime_to_ts(inode, mt);
	inode_set_atime_to_ts(inode, at);
#else
	inode->i_mtime = mt;
	inode->i_ctime = mt;
	inode->i_atime = at;
#endif
}

static inline void xiofs_set_times(struct inode *inode, time64_t sec)
{
	xiofs_set_times2(inode, sec, sec);
}

static inline void xiofs_set_ctime(struct inode *inode, time64_t sec)
{
	struct timespec64 ct = { .tv_sec = sec, .tv_nsec = 0 };

#ifdef XIOFS_HAS_INODE_SET_MTIME_TO_TS
	inode_set_ctime_to_ts(inode, ct);
#else
	inode->i_ctime = ct;
#endif
}

static inline time64_t xiofs_ctime_sec(struct inode *inode)
{
#ifdef XIOFS_HAS_INODE_SET_MTIME_TO_TS
	return inode_get_ctime(inode).tv_sec;
#else
	return inode->i_ctime.tv_sec;
#endif
}

/*
 * Writeback bookkeeping on a page that writepages has copied into a
 * coalesced PATCH. 5.14 has page APIs only; 5.18+ route through folios.
 */
#ifdef XIOFS_HAS_READAHEAD_FOLIO
#define xiofs_page_start_wb(p)		folio_start_writeback(page_folio(p))
#define xiofs_page_end_wb(p)		folio_end_writeback(page_folio(p))
#define xiofs_page_redirty(wbc, p)	folio_redirty_for_writepage((wbc), page_folio(p))
#else
#define xiofs_page_start_wb(p)		set_page_writeback(p)
#define xiofs_page_end_wb(p)		end_page_writeback(p)
#define xiofs_page_redirty(wbc, p)	redirty_page_for_writepage((wbc), (p))
#endif

static inline time64_t xiofs_mtime_sec(struct inode *inode)
{
#ifdef XIOFS_HAS_INODE_SET_MTIME_TO_TS
	return inode_get_mtime(inode).tv_sec;
#else
	return inode->i_mtime.tv_sec;
#endif
}

static inline void xiofs_copy_to_page(struct page *page, const void *src)
{
#ifdef XIOFS_HAS_WRITE_BEGIN_NOFLAGS
	memcpy_to_page(page, 0, src, PAGE_SIZE);
#else
	void *kaddr = kmap_local_page(page);

	memcpy(kaddr, src, PAGE_SIZE);
	kunmap_local(kaddr);
#endif
}

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 3, 0)
#define XIOFS_XATTR_SET_IDMAP struct mnt_idmap *idmap
#else
#define XIOFS_XATTR_SET_IDMAP struct user_namespace *idmap
#endif

typedef int (*xiofs_writepage_cb_t)(struct page *, struct writeback_control *,
				  void *);

#endif
