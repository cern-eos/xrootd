// SPDX-License-Identifier: GPL-2.0
/*
 * Inode and dentry ops. Lookup/getattr go over HTTP; the kernel dcache
 * holds the result until it is invalidated.
 */
#include <linux/fs.h>
#include <linux/jiffies.h>
#include <linux/module.h>
#include <linux/namei.h>
#include <linux/slab.h>
#include <linux/string.h>

#include "xiofs.h"
#include "xiofs_compat.h"

static unsigned long xiofs_actimeo_jiffies(struct xiofs_sb_info *sbi)
{
	if (!sbi->actimeo_sec)
		return 0;
	return msecs_to_jiffies(sbi->actimeo_sec * 1000u);
}

static int xiofs_d_revalidate(struct dentry *dentry, unsigned int flags)
{
	struct inode *inode = d_inode(dentry);
	struct xiofs_sb_info *sbi;
	struct xiofs_attr attr = {};
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

	if (timeout && XIOFS_I(inode)->attr_jiffies &&
	    time_before(jiffies, XIOFS_I(inode)->attr_jiffies + timeout))
		return 1;

	err = xiofs_http_getattr(inode, &attr);
	if (err == -ENOENT || err == -ESTALE)
		return 0;
	if (err)
		return err;
	if (attr.etag[0])
		strscpy(XIOFS_I(inode)->etag, attr.etag,
			sizeof(XIOFS_I(inode)->etag));
	i_size_write(inode, attr.size);
	xiofs_set_times(inode, attr.mtime);
	XIOFS_I(inode)->attr_jiffies = jiffies;
	dentry->d_time = jiffies;
	return 1;
}

const struct dentry_operations xiofs_dops = {
	.d_revalidate	= xiofs_d_revalidate,
};

static struct dentry *xiofs_lookup(struct inode *dir, struct dentry *dentry,
				      unsigned int flags)
{
	struct xiofs_inode_info *di = XIOFS_I(dir);
	struct xiofs_attr attr = {};
	struct inode *inode;
	char path[XIOFS_PATH_MAX];
	int err;

	if (dentry->d_name.len >= 256)
		return ERR_PTR(-ENAMETOOLONG);

	err = xiofs_join_path(path, sizeof(path), di->remote_path,
				 dentry->d_name.name);
	if (err)
		return ERR_PTR(err);

	err = xiofs_http_getattr_path(XIOFS_SB(dir->i_sb), path, &attr);
	if (err == -ENOENT) {
		dentry->d_time = jiffies;
		d_add(dentry, NULL);
		return NULL;
	}
	if (err)
		return ERR_PTR(err);

	inode = xiofs_iget(dir->i_sb, path, &attr);
	if (IS_ERR(inode))
		return ERR_CAST(inode);
	dentry->d_time = jiffies;
	return d_splice_alias(inode, dentry);
}

static int xiofs_getattr(xiofs_idmap_t idmap, const struct path *path,
			    struct kstat *stat, u32 request_mask,
			    unsigned int flags)
{
	struct inode *inode = d_inode(path->dentry);
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_inode_info *ki = XIOFS_I(inode);
	struct xiofs_attr attr = {};
	unsigned long timeout = xiofs_actimeo_jiffies(sbi);
	int err;

	if (!(timeout && ki->attr_jiffies &&
	      time_before(jiffies, ki->attr_jiffies + timeout))) {
		err = xiofs_http_getattr(inode, &attr);
		if (err)
			return err;
		if (attr.etag[0])
			strscpy(ki->etag, attr.etag, sizeof(ki->etag));
		i_size_write(inode, attr.size);
		xiofs_set_times(inode, attr.mtime);
		ki->attr_jiffies = jiffies;
	}
	xiofs_fillattr(idmap, request_mask, inode, stat);
	return 0;
}

static int xiofs_setattr(xiofs_idmap_t idmap, struct dentry *dentry,
			    struct iattr *attr)
{
	struct inode *inode = d_inode(dentry);
	int err;

	err = setattr_prepare(idmap, dentry, attr);
	if (err)
		return err;
	if (attr->ia_valid & ATTR_SIZE) {
		err = xiofs_http_truncate(inode, attr->ia_size);
		if (err)
			return err;
		truncate_setsize(inode, attr->ia_size);
	}
	setattr_copy(idmap, inode, attr);
	XIOFS_I(inode)->attr_jiffies = jiffies;
	mark_inode_dirty(inode);
	return 0;
}

static int xiofs_create(xiofs_idmap_t idmap, struct inode *dir,
			   struct dentry *dentry, umode_t mode, bool excl)
{
	struct xiofs_attr attr = {};
	struct inode *inode;
	char path[XIOFS_PATH_MAX];
	int err;

	err = xiofs_join_path(path, sizeof(path),
				 XIOFS_I(dir)->remote_path, dentry->d_name.name);
	if (err)
		return err;
	err = xiofs_http_create(dir, path, &attr);
	if (err)
		return err;
	inode = xiofs_iget(dir->i_sb, path, &attr);
	if (IS_ERR(inode))
		return PTR_ERR(inode);
	d_instantiate(dentry, inode);
	return 0;
}

static int xiofs_mkdir(xiofs_idmap_t idmap, struct inode *dir,
			  struct dentry *dentry, umode_t mode)
{
	struct xiofs_attr attr = { .is_dir = true };
	struct inode *inode;
	char path[XIOFS_PATH_MAX];
	int err;

	err = xiofs_join_path(path, sizeof(path),
				 XIOFS_I(dir)->remote_path, dentry->d_name.name);
	if (err)
		return err;
	err = xiofs_http_mkdir(dir, path);
	if (err)
		return err;
	inode = xiofs_iget(dir->i_sb, path, &attr);
	if (IS_ERR(inode))
		return PTR_ERR(inode);
	d_instantiate(dentry, inode);
	inc_nlink(dir);
	return 0;
}

static int xiofs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct inode *inode = d_inode(dentry);
	int err;

	err = xiofs_http_unlink(inode);
	if (err)
		return err;
	drop_nlink(inode);
	return 0;
}

static int xiofs_rmdir(struct inode *dir, struct dentry *dentry)
{
	int err = xiofs_unlink(dir, dentry);

	if (!err)
		drop_nlink(dir);
	return err;
}

static int xiofs_rename(xiofs_idmap_t idmap, struct inode *old_dir,
			   struct dentry *old_dentry, struct inode *new_dir,
			   struct dentry *new_dentry, unsigned int flags)
{
	char new_path[XIOFS_PATH_MAX];
	int err;

	if (flags)
		return -EINVAL;
	err = xiofs_join_path(new_path, sizeof(new_path),
				 XIOFS_I(new_dir)->remote_path,
				 new_dentry->d_name.name);
	if (err)
		return err;
	return xiofs_http_rename(d_inode(old_dentry), new_path);
}

static int xiofs_iterate(struct file *file, struct dir_context *ctx)
{
	struct inode *dir = file_inode(file);
	struct xiofs_dirent *ents = NULL;
	size_t nents = 0, i;
	int err;

	if (!dir_emit_dots(file, ctx))
		return 0;

	err = xiofs_http_readdir(dir, &ents, &nents);
	if (err)
		return err;

	for (i = 0; i < nents; i++) {
		unsigned int type = ents[i].is_dir ? DT_DIR : DT_REG;
		loff_t pos = i + 2;

		if (ctx->pos > pos)
			continue;
		ctx->pos = pos;
		if (!dir_emit(ctx, ents[i].name, strlen(ents[i].name),
			      full_name_hash(NULL, ents[i].name,
					     strlen(ents[i].name)),
			      type))
			break;
		ctx->pos = pos + 1;
	}
	kfree(ents);
	return 0;
}

const struct inode_operations xiofs_dir_inode_ops = {
	.lookup		= xiofs_lookup,
	.getattr	= xiofs_getattr,
	.create		= xiofs_create,
	.mkdir		= xiofs_mkdir,
	.unlink		= xiofs_unlink,
	.rmdir		= xiofs_rmdir,
	.rename		= xiofs_rename,
	.setattr	= xiofs_setattr,
};

const struct inode_operations xiofs_file_inode_ops = {
	.getattr	= xiofs_getattr,
	.setattr	= xiofs_setattr,
};

const struct file_operations xiofs_dir_ops = {
	.owner		= THIS_MODULE,
	.iterate_shared	= xiofs_iterate,
	.llseek		= generic_file_llseek,
};
