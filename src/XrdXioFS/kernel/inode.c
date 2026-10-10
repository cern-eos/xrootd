// SPDX-License-Identifier: GPL-2.0
/*
 * Inode and dentry ops.
 *
 * Metadata model:
 *  - attributes live on the inode for actimeo; after that they are
 *    revalidated with a conditional PROPFIND (If-None-Match: <ETag>) that
 *    costs one round trip and no body when nothing changed;
 *  - a directory keeps its last listing plus the listing's ETag; readdir
 *    serves getdents(2) continuations from it, revalidates it the same way
 *    (304 = keep), and primes the dcache with every entry so `ls -l` is one
 *    round trip instead of one per name;
 *  - create() is deferred: the inode exists locally at once and the PUT
 *    is sent by the first writeback (with the data when the file is small
 *    and fully dirty), by close(), or by the first operation that needs
 *    the object on the server.
 */
#include <linux/fs.h>
#include <linux/jiffies.h>
#include <linux/module.h>
#include <linux/namei.h>
#include <linux/pagemap.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/timekeeping.h>
#include <linux/uidgid.h>
#include <linux/user_namespace.h>
#include <linux/xattr.h>
#include <linux/kdev_t.h>

#include "xiofs.h"
#include "xiofs_compat.h"

unsigned long xiofs_actimeo_jiffies(struct xiofs_sb_info *sbi)
{
	if (!sbi->actimeo_sec)
		return 0;
	return msecs_to_jiffies(sbi->actimeo_sec * 1000u);
}

bool xiofs_attr_fresh(struct inode *inode)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);
	unsigned long timeout = xiofs_actimeo_jiffies(XIOFS_SB(inode->i_sb));

	if (xiofs_is_deferred(inode))
		return true;
	return timeout && ki->attr_jiffies &&
	       time_before(jiffies, ki->attr_jiffies + timeout);
}

static bool xiofs_has_local_data(struct inode *inode)
{
	struct address_space *mapping = inode->i_mapping;

	return mapping_tagged(mapping, PAGECACHE_TAG_DIRTY) ||
	       mapping_tagged(mapping, PAGECACHE_TAG_WRITEBACK);
}

void xiofs_refresh_inode(struct inode *inode, const struct xiofs_attr *attr)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);

	if (attr->etag[0])
		strscpy(ki->etag, attr->etag, sizeof(ki->etag));
	xiofs_inode_set_id(inode, attr->id ? attr->id : xiofs_etag_id(attr->etag));
	/* Do not shrink below data we still owe the server. */
	if (!xiofs_has_local_data(inode) || attr->size > i_size_read(inode))
		i_size_write(inode, attr->size);
	xiofs_set_times2(inode, attr->mtime, attr->atime);
	xiofs_set_ctime(inode, attr->ctime ? attr->ctime : attr->mtime);
	if (attr->mode)
		inode->i_mode = (inode->i_mode & S_IFMT) | (attr->mode & 07777);
	if (attr->have_uid)
		inode->i_uid = make_kuid(&init_user_ns, attr->uid);
	if (attr->have_gid)
		inode->i_gid = make_kgid(&init_user_ns, attr->gid);
	ki->attr_jiffies = jiffies;
}

static void xiofs_dirent_to_attr(const struct xiofs_dirent *de,
				 struct xiofs_attr *attr)
{
	memset(attr, 0, sizeof(*attr));
	attr->id = de->id;
	attr->size = de->size;
	attr->mtime = de->mtime;
	attr->ctime = de->ctime;
	attr->atime = de->atime;
	attr->mode = de->mode;
	attr->uid = de->uid;
	attr->gid = de->gid;
	attr->rdev = de->rdev;
	attr->have_uid = de->have_uid;
	attr->have_gid = de->have_gid;
	attr->is_dir = de->is_dir;
	attr->is_lnk = de->is_lnk;
	attr->is_fifo = de->is_fifo;
	attr->is_chr = de->is_chr;
	attr->is_blk = de->is_blk;
	strscpy(attr->etag, de->etag, sizeof(attr->etag));
}

/*
 * Conditional revalidation of one inode. Returns 0 (attributes current,
 * possibly refreshed), -ENOENT/-ESTALE when the name no longer resolves
 * to this object, or a transport error.
 */
static int xiofs_revalidate_inode(struct inode *inode)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);
	struct xiofs_attr attr = {};
	int err;

	if (xiofs_attr_fresh(inode))
		return 0;
	err = xiofs_http_getattr(inode, ki->etag[0] ? ki->etag : NULL, &attr);
	if (err == XIOFS_NOT_MODIFIED) {
		ki->attr_jiffies = jiffies;
		return 0;
	}
	if (err)
		return err;
	if (attr.id && ki->remote_id && attr.id != ki->remote_id)
		return -ESTALE;
	xiofs_refresh_inode(inode, &attr);
	return 0;
}

/*                         d i r e c t o r y   c a c h e                     */

/* Caller holds dir->dir_lock or the directory's i_rwsem exclusively. */
static void __xiofs_dir_invalidate(struct inode *dir)
{
	struct xiofs_inode_info *di = XIOFS_I(dir);
	time64_t now = ktime_get_real_seconds();

	di->dir_valid = false;
	/* Our own mutation: show it in stat() without a round trip. */
	xiofs_set_times2(dir, now, now);
	xiofs_set_ctime(dir, now);
}

void xiofs_dir_invalidate(struct inode *dir)
{
	struct xiofs_inode_info *di = XIOFS_I(dir);

	mutex_lock(&di->dir_lock);
	__xiofs_dir_invalidate(dir);
	mutex_unlock(&di->dir_lock);
}

/*                           d e f e r r e d   c r e a t e                   */

static int xiofs_deferred_add(struct inode *dir, struct inode *inode,
			      const struct qstr *name)
{
	struct xiofs_inode_info *di = XIOFS_I(dir);
	struct xiofs_inode_info *ki = XIOFS_I(inode);
	struct xiofs_deferred *d;

	if (name->len >= sizeof(d->name))
		return -ENAMETOOLONG;
	d = kzalloc(sizeof(*d), GFP_KERNEL);
	if (!d)
		return -ENOMEM;
	d->inode = inode;
	memcpy(d->name, name->name, name->len);
	d->name[name->len] = 0;
	mutex_lock(&di->dir_lock);
	list_add_tail(&d->list, &di->deferred);
	ki->defer_parent = igrab(dir);
	ki->defer_ent = d;
	mutex_unlock(&di->dir_lock);
	return 0;
}

void xiofs_deferred_del(struct inode *inode)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);
	struct inode *dir = ki->defer_parent;
	struct xiofs_inode_info *di;

	if (!dir)
		return;
	di = XIOFS_I(dir);
	mutex_lock(&di->dir_lock);
	if (ki->defer_ent) {
		list_del(&ki->defer_ent->list);
		kfree(ki->defer_ent);
		ki->defer_ent = NULL;
	}
	mutex_unlock(&di->dir_lock);
	ki->defer_parent = NULL;
	iput(dir);
}

/*
 * Send the PUT for a deferred inode, with buf/len as the whole entity
 * (len 0 creates it empty). Returns -EALREADY when it was created in the
 * meantime so the caller falls back to PATCH.
 */
int xiofs_create_now(struct inode *inode, const void *buf, size_t len)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);
	struct inode *dir;
	int err;

	mutex_lock(&ki->dir_lock);
	if (!test_bit(XIOFS_I_DEFERRED, &ki->flags)) {
		mutex_unlock(&ki->dir_lock);
		return -EALREADY;
	}
	if (test_bit(XIOFS_I_GONE, &ki->flags)) {
		mutex_unlock(&ki->dir_lock);
		return 0;
	}
	err = xiofs_http_put(inode, buf, len, false,
			     test_bit(XIOFS_I_EXCL, &ki->flags));
	if (!err) {
		clear_bit(XIOFS_I_DEFERRED, &ki->flags);
		ki->attr_jiffies = jiffies;
	}
	dir = err ? NULL : ki->defer_parent;
	mutex_unlock(&ki->dir_lock);
	if (!err) {
		if (dir)
			xiofs_dir_invalidate(dir);
		xiofs_deferred_del(inode);
	}
	return err;
}

int xiofs_ensure_created(struct inode *inode)
{
	int err;

	if (!xiofs_is_deferred(inode))
		return 0;
	err = xiofs_create_now(inode, NULL, 0);
	return err == -EALREADY ? 0 : err;
}

/*                                 d c a c h e                               */

static int xiofs_d_revalidate(struct dentry *dentry, unsigned int flags)
{
	struct inode *inode = d_inode(dentry);
	struct xiofs_sb_info *sbi;
	unsigned long timeout;
	int err;

	if (flags & LOOKUP_RCU)
		return -ECHILD;

	sbi = XIOFS_SB(dentry->d_sb);
	timeout = xiofs_actimeo_jiffies(sbi);

	if (d_really_is_negative(dentry)) {
		if (timeout && time_before(jiffies, dentry->d_time + timeout))
			return 1;
		return 0;
	}

	if (xiofs_is_deferred(inode))
		return 1;

	err = xiofs_revalidate_inode(inode);
	if (err == -ENOENT || err == -ESTALE)
		return 0;
	if (err)
		return err;
	dentry->d_time = jiffies;
	return 1;
}

const struct dentry_operations xiofs_dops = {
	.d_revalidate	= xiofs_d_revalidate,
};

static struct dentry *xiofs_lookup(struct inode *dir, struct dentry *dentry,
				      unsigned int flags)
{
	struct xiofs_attr attr = {};
	struct inode *inode;
	char *path;
	int err;

	if (dentry->d_name.len >= 256)
		return ERR_PTR(-ENAMETOOLONG);

	path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	if (!path)
		return ERR_PTR(-ENOMEM);
	err = xiofs_dentry_path(dentry, path, XIOFS_PATH_MAX);
	if (!err)
		err = xiofs_http_getattr_path(XIOFS_SB(dir->i_sb), path, NULL,
					      &attr);
	kfree(path);
	if (err == -ENOENT) {
		dentry->d_time = jiffies;
		d_add(dentry, NULL);
		return NULL;
	}
	if (err)
		return ERR_PTR(err);

	inode = xiofs_iget(dir->i_sb, &attr);
	if (IS_ERR(inode))
		return ERR_CAST(inode);
	/* A cached inode gets the attributes we just paid for. */
	xiofs_refresh_inode(inode, &attr);
	dentry->d_time = jiffies;
	return d_splice_alias(inode, dentry);
}

/*
 * readdir-plus: instantiate dentries and inodes for a listing so the
 * lookups that follow readdir (ls -l, find, rsync) need no round trip.
 */
static void xiofs_prime_dcache(struct dentry *parent,
			       const struct xiofs_dirent *ents, size_t nents,
			       unsigned long stamp)
{
	struct super_block *sb = parent->d_sb;
	size_t i;

	for (i = 0; i < nents; i++) {
		const struct xiofs_dirent *de = &ents[i];
		struct qstr q = QSTR_INIT(de->name, strlen(de->name));
		struct dentry *dentry, *alias;
		struct inode *inode;
		struct xiofs_attr attr;
		DECLARE_WAIT_QUEUE_HEAD_ONSTACK(wq);

		if (!q.len)
			continue;
		xiofs_dirent_to_attr(de, &attr);

		dentry = d_hash_and_lookup(parent, &q);
		if (IS_ERR(dentry))
			continue;
		if (dentry) {
			inode = d_inode(dentry);
			if (!inode) {
				/* Negative but the server has it now. */
				d_drop(dentry);
			} else if (!xiofs_is_deferred(inode) &&
				   (!attr.id || !XIOFS_I(inode)->remote_id ||
				    attr.id == XIOFS_I(inode)->remote_id)) {
				xiofs_refresh_inode(inode, &attr);
				XIOFS_I(inode)->attr_jiffies = stamp;
				dentry->d_time = stamp;
			}
			dput(dentry);
			continue;
		}

		dentry = d_alloc_parallel(parent, &q, &wq);
		if (IS_ERR(dentry))
			continue;
		if (!d_in_lookup(dentry)) {
			/* A real lookup won the race. */
			dput(dentry);
			continue;
		}
		inode = xiofs_iget(sb, &attr);
		if (IS_ERR(inode)) {
			d_lookup_done(dentry);
			dput(dentry);
			continue;
		}
		xiofs_refresh_inode(inode, &attr);
		XIOFS_I(inode)->attr_jiffies = stamp;
		alias = d_splice_alias(inode, dentry);
		d_lookup_done(dentry);
		if (alias && !IS_ERR(alias))
			dput(alias);
		dentry->d_time = stamp;
		dput(dentry);
	}
}

/*
 * Make the listing of dir current. Caller holds dir_lock. *fresh tells
 * whether the server sent a new body (then the dcache is re-primed).
 */
static int xiofs_dir_fetch(struct inode *dir, bool *fresh)
{
	struct xiofs_inode_info *di = XIOFS_I(dir);
	unsigned long timeout = xiofs_actimeo_jiffies(XIOFS_SB(dir->i_sb));
	struct xiofs_dirent *ents = NULL;
	size_t nents = 0;
	char etag[XIOFS_ETAG_MAX];
	int err;

	*fresh = false;
	if (di->dir_valid && timeout &&
	    time_before(jiffies, di->dir_jiffies + timeout))
		return 0;

	err = xiofs_http_readdir(dir, di->dir_etag[0] ? di->dir_etag : NULL,
				 &ents, &nents, etag, sizeof(etag));
	if (err == XIOFS_NOT_MODIFIED) {
		di->dir_valid = true;
		di->dir_jiffies = jiffies;
		return 0;
	}
	if (err)
		return err;
	kvfree(di->dir_ents);
	di->dir_ents = ents;
	di->dir_nents = nents;
	strscpy(di->dir_etag, etag, sizeof(di->dir_etag));
	di->dir_jiffies = jiffies;
	di->dir_valid = true;
	*fresh = true;
	return 0;
}

static unsigned int xiofs_dirent_type(const struct xiofs_dirent *de)
{
	if (de->is_dir)
		return DT_DIR;
	if (de->is_lnk)
		return DT_LNK;
	if (de->is_fifo)
		return DT_FIFO;
	if (de->is_chr)
		return DT_CHR;
	if (de->is_blk)
		return DT_BLK;
	return DT_REG;
}

static bool xiofs_listing_has(const struct xiofs_dirent *ents, size_t nents,
			      const char *name)
{
	size_t i;

	for (i = 0; i < nents; i++)
		if (!strcmp(ents[i].name, name))
			return true;
	return false;
}

static int xiofs_iterate(struct file *file, struct dir_context *ctx)
{
	struct inode *dir = file_inode(file);
	struct xiofs_inode_info *di = XIOFS_I(dir);
	struct xiofs_deferred *d;
	loff_t pos;
	size_t i;
	bool fresh;
	int err;

	if (!dir_emit_dots(file, ctx))
		return 0;

	mutex_lock(&di->dir_lock);
	err = xiofs_dir_fetch(dir, &fresh);
	if (err) {
		mutex_unlock(&di->dir_lock);
		return err;
	}
	if (fresh || ctx->pos == 2)
		xiofs_prime_dcache(file->f_path.dentry, di->dir_ents,
				   di->dir_nents, di->dir_jiffies);

	for (i = 0; i < di->dir_nents; i++) {
		const struct xiofs_dirent *de = &di->dir_ents[i];

		pos = i + 2;
		if (ctx->pos > pos)
			continue;
		ctx->pos = pos;
		/* Same number stat() will show: the server identity. */
		if (!dir_emit(ctx, de->name, strlen(de->name),
			      de->id ? de->id :
				full_name_hash(NULL, de->name, strlen(de->name)),
			      xiofs_dirent_type(de)))
			goto out;
		ctx->pos = pos + 1;
	}

	/* Children created locally that the server has not seen yet. */
	pos = di->dir_nents + 2;
	list_for_each_entry(d, &di->deferred, list) {
		if (ctx->pos > pos) {
			pos++;
			continue;
		}
		ctx->pos = pos;
		if (!xiofs_listing_has(di->dir_ents, di->dir_nents, d->name) &&
		    !dir_emit(ctx, d->name, strlen(d->name), d->inode->i_ino,
			      DT_REG))
			goto out;
		pos++;
		ctx->pos = pos;
	}
out:
	mutex_unlock(&di->dir_lock);
	return 0;
}

/*                               i n o d e   o p s                           */

static int xiofs_getattr(xiofs_idmap_t idmap, const struct path *path,
			    struct kstat *stat, u32 request_mask,
			    unsigned int flags)
{
	struct inode *inode = d_inode(path->dentry);
	int err;

	err = xiofs_revalidate_inode(inode);
	if (err)
		return err;
	xiofs_fillattr(idmap, request_mask, inode, stat);
	return 0;
}

static int xiofs_setattr(xiofs_idmap_t idmap, struct dentry *dentry,
			    struct iattr *attr)
{
	struct inode *inode = d_inode(dentry);
	int mode = -1;
	u32 uid = (u32)-1, gid = (u32)-1;
	time64_t at = -1, mt = -1;
	int err;

	err = setattr_prepare(idmap, dentry, attr);
	if (err)
		return err;

	if (xiofs_is_deferred(inode)) {
		if ((attr->ia_valid & ATTR_SIZE) && attr->ia_size == 0 &&
		    !(attr->ia_valid & (ATTR_UID | ATTR_GID | ATTR_ATIME_SET |
					ATTR_MTIME_SET))) {
			/* Nothing on the server yet: a local change is enough. */
			truncate_setsize(inode, 0);
			setattr_copy(idmap, inode, attr);
			mark_inode_dirty(inode);
			return 0;
		}
		if (!(attr->ia_valid & (ATTR_SIZE | ATTR_UID | ATTR_GID |
					ATTR_ATIME_SET | ATTR_MTIME_SET))) {
			/* chmod/touch before the first flush rides on the PUT. */
			setattr_copy(idmap, inode, attr);
			mark_inode_dirty(inode);
			return 0;
		}
		err = xiofs_ensure_created(inode);
		if (err)
			return err;
	}

	if (attr->ia_valid & ATTR_SIZE) {
		err = xiofs_http_truncate(inode, attr->ia_size);
		if (err)
			return err;
		truncate_setsize(inode, attr->ia_size);
	}
	if (attr->ia_valid & ATTR_MODE)
		mode = attr->ia_mode & 07777;
	if (attr->ia_valid & ATTR_UID)
		uid = from_kuid(&init_user_ns, attr->ia_uid);
	if (attr->ia_valid & ATTR_GID)
		gid = from_kgid(&init_user_ns, attr->ia_gid);
	if (attr->ia_valid & ATTR_ATIME)
		at = (attr->ia_valid & ATTR_ATIME_SET) ? attr->ia_atime.tv_sec
						       : ktime_get_real_seconds();
	if (attr->ia_valid & ATTR_MTIME)
		mt = (attr->ia_valid & ATTR_MTIME_SET) ? attr->ia_mtime.tv_sec
						       : ktime_get_real_seconds();
	/* One PROPPATCH for mode, owner and times together. */
	err = xiofs_http_setattr(inode, mode, uid, gid, at, mt);
	if (err)
		return err;
	setattr_copy(idmap, inode, attr);
	XIOFS_I(inode)->attr_jiffies = jiffies;
	mark_inode_dirty(inode);
	return 0;
}

static int xiofs_create(xiofs_idmap_t idmap, struct inode *dir,
			   struct dentry *dentry, umode_t mode, bool excl)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_attr attr = {};
	struct inode *inode;
	int err;

	attr.mode = mode & 07777;

	if (!sbi->defer_create) {
		char *path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);

		if (!path)
			return -ENOMEM;
		err = xiofs_dentry_path(dentry, path, XIOFS_PATH_MAX);
		if (!err)
			err = xiofs_http_create(dir, path, mode, excl, &attr);
		kfree(path);
		if (err)
			return err;
		inode = xiofs_iget(dir->i_sb, &attr);
		if (IS_ERR(inode))
			return PTR_ERR(inode);
		d_instantiate(dentry, inode);
		xiofs_dir_invalidate(dir);
		return 0;
	}

	/*
	 * Deferred: no round trip here. The negative lookup that preceded
	 * us already said the name is free; O_EXCL is still enforced by the
	 * server (If-None-Match: *) when the PUT goes out.
	 */
	inode = xiofs_new_inode(dir->i_sb, &attr);
	if (IS_ERR(inode))
		return PTR_ERR(inode);
	set_bit(XIOFS_I_DEFERRED, &XIOFS_I(inode)->flags);
	if (excl)
		set_bit(XIOFS_I_EXCL, &XIOFS_I(inode)->flags);
	err = xiofs_deferred_add(dir, inode, &dentry->d_name);
	if (err) {
		iput(inode);
		return err;
	}
	d_instantiate(dentry, inode);
	dentry->d_time = jiffies;
	xiofs_dir_invalidate(dir);
	return 0;
}

static int xiofs_mkdir(xiofs_idmap_t idmap, struct inode *dir,
			  struct dentry *dentry, umode_t mode)
{
	struct xiofs_attr attr = {};
	struct inode *inode;
	char *path;
	int err;

	path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	if (!path)
		return -ENOMEM;
	err = xiofs_dentry_path(dentry, path, XIOFS_PATH_MAX);
	if (!err)
		err = xiofs_http_mkdir(dir, path, mode, &attr);
	kfree(path);
	if (err)
		return err;
	inode = xiofs_iget(dir->i_sb, &attr);
	if (IS_ERR(inode))
		return PTR_ERR(inode);
	d_instantiate(dentry, inode);
	inc_nlink(dir);
	xiofs_dir_invalidate(dir);
	return 0;
}

static int xiofs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode = d_inode(dentry);
	struct xiofs_inode_info *ki = XIOFS_I(inode);
	int err = 0;

	mutex_lock(&ki->dir_lock);
	if (test_bit(XIOFS_I_DEFERRED, &ki->flags)) {
		/* Never reached the server: drop it and its dirty pages. */
		set_bit(XIOFS_I_GONE, &ki->flags);
		clear_bit(XIOFS_I_DEFERRED, &ki->flags);
		mutex_unlock(&ki->dir_lock);
		xiofs_deferred_del(inode);
		truncate_inode_pages(inode->i_mapping, 0);
	} else {
		mutex_unlock(&ki->dir_lock);
		err = xiofs_http_unlink(inode);
		if (err)
			return err;
	}
	drop_nlink(inode);
	xiofs_dir_invalidate(dir);
	return 0;
}

static int xiofs_rmdir(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode = d_inode(dentry);
	int err;

	err = xiofs_http_unlink(inode);
	if (err)
		return err;
	clear_nlink(inode);
	drop_nlink(dir);
	xiofs_dir_invalidate(dir);
	return 0;
}

static int xiofs_rename(xiofs_idmap_t idmap, struct inode *old_dir,
			   struct dentry *old_dentry, struct inode *new_dir,
			   struct dentry *new_dentry, unsigned int flags)
{
	struct inode *inode = d_inode(old_dentry);
	struct inode *target = d_inode(new_dentry);
	char *new_path;
	int err;

	if (flags)
		return -EINVAL;
	/* The server needs the object before it can MOVE it. */
	err = xiofs_ensure_created(inode);
	if (err)
		return err;
	if (target && xiofs_is_deferred(target)) {
		/* Overwriting a never-sent file: forget it locally. */
		struct xiofs_inode_info *ti = XIOFS_I(target);

		mutex_lock(&ti->dir_lock);
		set_bit(XIOFS_I_GONE, &ti->flags);
		clear_bit(XIOFS_I_DEFERRED, &ti->flags);
		mutex_unlock(&ti->dir_lock);
		xiofs_deferred_del(target);
		truncate_inode_pages(target->i_mapping, 0);
	}
	new_path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	if (!new_path)
		return -ENOMEM;
	err = xiofs_dentry_path(new_dentry, new_path, XIOFS_PATH_MAX);
	if (!err)
		err = xiofs_http_rename(inode, new_path);
	kfree(new_path);
	if (err)
		return err;
	/* Rename changes the server ctime: the cached tag is a stale validator
	 * (identity stays valid); let the next stat pick the new one up. */
	XIOFS_I(inode)->attr_jiffies = 0;
	if (target && !S_ISDIR(target->i_mode))
		drop_nlink(target);
	xiofs_dir_invalidate(old_dir);
	if (new_dir != old_dir)
		xiofs_dir_invalidate(new_dir);
	return 0;
}

static int xiofs_link(struct dentry *old_dentry, struct inode *dir,
		      struct dentry *new_dentry)
{
	struct inode *inode = d_inode(old_dentry);
	char *new_path;
	int err;

	err = xiofs_ensure_created(inode);
	if (err)
		return err;
	new_path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	if (!new_path)
		return -ENOMEM;
	err = xiofs_dentry_path(new_dentry, new_path, XIOFS_PATH_MAX);
	if (!err)
		err = xiofs_http_link(inode, new_path);
	kfree(new_path);
	if (err)
		return err;
	ihold(inode);
	inc_nlink(inode);
	d_instantiate(new_dentry, inode);
	xiofs_dir_invalidate(dir);
	return 0;
}

static int xiofs_symlink(xiofs_idmap_t idmap, struct inode *dir,
			 struct dentry *dentry, const char *symname)
{
	struct xiofs_attr attr = { .is_lnk = true };
	struct inode *inode;
	char *path, *target;
	int err;

	(void)idmap;
	if (!symname || !*symname)
		return -EINVAL;
	path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	if (!path)
		return -ENOMEM;
	err = xiofs_dentry_path(dentry, path, XIOFS_PATH_MAX);
	if (!err)
		err = xiofs_http_symlink(dir, path, symname);
	kfree(path);
	if (err)
		return err;
	target = kstrdup(symname, GFP_KERNEL);
	if (!target)
		return -ENOMEM;
	attr.size = strlen(symname);
	inode = xiofs_new_inode(dir->i_sb, &attr);
	if (IS_ERR(inode)) {
		kfree(target);
		return PTR_ERR(inode);
	}
	XIOFS_I(inode)->link_target = target;
	d_instantiate(dentry, inode);
	xiofs_dir_invalidate(dir);
	return 0;
}

static const char *xiofs_get_link(struct dentry *dentry, struct inode *inode,
				  struct delayed_call *done)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);
	char *buf;
	int err;

	(void)done;
	if (!dentry)
		return ERR_PTR(-ECHILD);
	if (ki->link_target)
		return ki->link_target;
	buf = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	if (!buf)
		return ERR_PTR(-ENOMEM);
	err = xiofs_http_readlink(inode, buf, XIOFS_PATH_MAX);
	if (err) {
		kfree(buf);
		return ERR_PTR(err);
	}
	/* Lost race with another reader: keep theirs. */
	if (cmpxchg(&ki->link_target, NULL, buf) != NULL)
		kfree(buf);
	return ki->link_target;
}

static int xiofs_mknod(xiofs_idmap_t idmap, struct inode *dir,
		       struct dentry *dentry, umode_t mode, dev_t rdev)
{
	struct xiofs_attr attr = {};
	struct inode *inode;
	char *path;
	int err;

	(void)idmap;
	if (S_ISREG(mode) || (mode & S_IFMT) == 0)
		return xiofs_create(idmap, dir, dentry, mode, false);
	path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	if (!path)
		return -ENOMEM;
	err = xiofs_dentry_path(dentry, path, XIOFS_PATH_MAX);
	if (!err)
		err = xiofs_http_mknod(dir, path, mode, rdev);
	kfree(path);
	if (err)
		return err;
	attr.mode = mode & 07777;
	attr.is_fifo = S_ISFIFO(mode);
	attr.is_chr = S_ISCHR(mode);
	attr.is_blk = S_ISBLK(mode);
	attr.rdev = new_encode_dev(rdev);
	inode = xiofs_new_inode(dir->i_sb, &attr);
	if (IS_ERR(inode))
		return PTR_ERR(inode);
	d_instantiate(dentry, inode);
	xiofs_dir_invalidate(dir);
	return 0;
}

/*                                 x a t t r                                 */

static int xiofs_listxattr(struct dentry *dentry, char *buffer, size_t size)
{
	struct inode *inode = d_inode(dentry);

	if (xiofs_is_deferred(inode))
		return 0;
	return xiofs_http_listxattr(inode, buffer, size);
}

static int xiofs_xattr_get(const struct xattr_handler *handler,
			   struct dentry *dentry, struct inode *inode,
			   const char *name, void *buffer, size_t size)
{
	(void)handler;
	(void)dentry;
	if (xiofs_is_deferred(inode))
		return -ENODATA;
	return xiofs_http_getxattr(inode, name, buffer, size);
}

static int xiofs_xattr_set(const struct xattr_handler *handler,
			   XIOFS_XATTR_SET_IDMAP,
			   struct dentry *dentry, struct inode *inode,
			   const char *name, const void *buffer, size_t size,
			   int flags)
{
	int err;

	(void)handler;
	(void)idmap;
	(void)dentry;
	(void)flags;
	err = xiofs_ensure_created(inode);
	if (err)
		return err;
	if (!buffer)
		return xiofs_http_removexattr(inode, name);
	return xiofs_http_setxattr(inode, name, buffer, size);
}

static const struct xattr_handler xiofs_xattr_handler = {
	.prefix	= "",
	.get	= xiofs_xattr_get,
	.set	= xiofs_xattr_set,
};

const struct xattr_handler * const xiofs_xattr_handlers[] = {
	&xiofs_xattr_handler,
	NULL
};

/*                                 t a b l e s                               */

const struct inode_operations xiofs_dir_inode_ops = {
	.lookup		= xiofs_lookup,
	.getattr	= xiofs_getattr,
	.create		= xiofs_create,
	.mkdir		= xiofs_mkdir,
	.unlink		= xiofs_unlink,
	.rmdir		= xiofs_rmdir,
	.rename		= xiofs_rename,
	.link		= xiofs_link,
	.symlink	= xiofs_symlink,
	.mknod		= xiofs_mknod,
	.setattr	= xiofs_setattr,
	.listxattr	= xiofs_listxattr,
};

const struct inode_operations xiofs_file_inode_ops = {
	.getattr	= xiofs_getattr,
	.setattr	= xiofs_setattr,
	.listxattr	= xiofs_listxattr,
};

const struct inode_operations xiofs_symlink_inode_ops = {
	.get_link	= xiofs_get_link,
	.getattr	= xiofs_getattr,
	.setattr	= xiofs_setattr,
	.listxattr	= xiofs_listxattr,
};

const struct file_operations xiofs_dir_ops = {
	.owner		= THIS_MODULE,
	.iterate_shared	= xiofs_iterate,
	.llseek		= generic_file_llseek,
};
