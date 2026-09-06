// SPDX-License-Identifier: GPL-2.0
/*
 * Superblock: mount options, inode slab, 4 MiB readahead BDI.
 */
#include <linux/fs.h>
#include <linux/fs_context.h>
#include <linux/fs_parser.h>
#include <linux/kmod.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/statfs.h>
#include <linux/string.h>

#include "xiofs.h"
#include "xiofs_compat.h"

static struct kmem_cache *xiofs_inode_cachep;

enum {
	Opt_host,
	Opt_port,
	Opt_path,
};

static const struct fs_parameter_spec xiofs_fs_parameters[] = {
	fsparam_string("host", Opt_host),
	fsparam_u32("port", Opt_port),
	fsparam_string("path", Opt_path),
	{}
};

struct xiofs_fc_ctx {
	char		host[256];
	unsigned int	port;
	char		export_path[256];
};

static void xiofs_set_hosthdr(struct xiofs_sb_info *sbi)
{
	if (sbi->port == 80 || sbi->port == 443)
		snprintf(sbi->hosthdr, sizeof(sbi->hosthdr), "%s", sbi->host);
	else
		snprintf(sbi->hosthdr, sizeof(sbi->hosthdr), "%s:%u",
			 sbi->host, sbi->port);
}

int xiofs_join_path(char *dst, size_t dstsz, const char *parent,
		       const char *name)
{
	size_t plen = parent ? strlen(parent) : 0;
	size_t nlen = name ? strlen(name) : 0;

	if (!nlen) {
		if (plen >= dstsz)
			return -ENAMETOOLONG;
		memcpy(dst, parent ? parent : "/", plen + 1);
		return 0;
	}
	while (plen && parent[plen - 1] == '/')
		plen--;
	while (nlen && name[0] == '/') {
		name++;
		nlen--;
	}
	if (plen + 1 + nlen + 1 > dstsz)
		return -ENAMETOOLONG;
	memcpy(dst, parent, plen);
	dst[plen] = '/';
	memcpy(dst + plen + 1, name, nlen);
	dst[plen + 1 + nlen] = 0;
	return 0;
}

static struct inode *xiofs_alloc_inode(struct super_block *sb)
{
	struct xiofs_inode_info *ki;

	ki = kmem_cache_alloc(xiofs_inode_cachep, GFP_KERNEL);
	if (!ki)
		return NULL;
	ki->remote_path[0] = 0;
	ki->etag[0] = 0;
	return &ki->vfs_inode;
}

static void xiofs_free_inode(struct inode *inode)
{
	kmem_cache_free(xiofs_inode_cachep, XIOFS_I(inode));
}

static void xiofs_put_super(struct super_block *sb)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(sb);

	if (!sbi)
		return;
	xiofs_session_unregister(sbi);
	xiofs_session_close(sbi);
	kfree(sbi);
	sb->s_fs_info = NULL;
}

static const struct super_operations xiofs_sops = {
	.statfs		= simple_statfs,
	.alloc_inode	= xiofs_alloc_inode,
	.free_inode	= xiofs_free_inode,
	.drop_inode	= generic_delete_inode,
	.put_super	= xiofs_put_super,
};

static void xiofs_inode_init_once(void *obj)
{
	struct xiofs_inode_info *ki = obj;

	inode_init_once(&ki->vfs_inode);
}

struct inode *xiofs_iget(struct super_block *sb, const char *path,
			    const struct xiofs_attr *attr)
{
	struct inode *inode;
	struct xiofs_inode_info *ki;
	unsigned long hash = full_name_hash(NULL, path, strlen(path));

	inode = iget_locked(sb, hash);
	if (!inode)
		return ERR_PTR(-ENOMEM);
	ki = XIOFS_I(inode);
	if (!(inode->i_state & I_NEW))
		return inode;

	strscpy(ki->remote_path, path, sizeof(ki->remote_path));
	if (attr && attr->etag[0])
		strscpy(ki->etag, attr->etag, sizeof(ki->etag));

	inode->i_ino = hash;
	inode->i_uid = current_fsuid();
	inode->i_gid = current_fsgid();
	inode->i_mapping->a_ops = &xiofs_aops;
	mapping_set_gfp_mask(inode->i_mapping, GFP_HIGHUSER);
	inode->i_blkbits = PAGE_SHIFT;

	if (attr && attr->is_dir) {
		inode->i_mode = S_IFDIR | 0755;
		inode->i_op = &xiofs_dir_inode_ops;
		inode->i_fop = &xiofs_dir_ops;
		set_nlink(inode, 2);
		inode->i_size = 0;
	} else {
		inode->i_mode = S_IFREG | 0644;
		inode->i_op = &xiofs_file_inode_ops;
		inode->i_fop = &xiofs_file_ops;
		set_nlink(inode, 1);
		inode->i_size = attr ? attr->size : 0;
	}
	if (attr)
		xiofs_set_times(inode, attr->mtime);

	unlock_new_inode(inode);
	return inode;
}

int xiofs_fill_super(struct super_block *sb, struct fs_context *fc)
{
	struct xiofs_fc_ctx *ctx = fc->fs_private;
	struct xiofs_sb_info *sbi;
	struct xiofs_attr rootattr = { .is_dir = true };
	struct inode *root;
	int err;

	if (!ctx || !ctx->host[0])
		return -EINVAL;

	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi)
		return -ENOMEM;
	mutex_init(&sbi->io_lock);
	INIT_LIST_HEAD(&sbi->list);
	sbi->sb = sb;
	sbi->port = ctx->port ? ctx->port : 443;
	strscpy(sbi->host, ctx->host, sizeof(sbi->host));
	strscpy(sbi->export_path,
		ctx->export_path[0] ? ctx->export_path : "/",
		sizeof(sbi->export_path));
	xiofs_set_hosthdr(sbi);

	sb->s_magic = XIOFS_MAGIC;
	sb->s_op = &xiofs_sops;
	sb->s_time_gran = 1;
	sb->s_blocksize = PAGE_SIZE;
	sb->s_blocksize_bits = PAGE_SHIFT;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_fs_info = sbi;

	err = super_setup_bdi(sb);
	if (err)
		goto out_sbi;
	sb->s_bdi->ra_pages = XIOFS_RA_BYTES / PAGE_SIZE;
	sb->s_bdi->io_pages = sb->s_bdi->ra_pages;

	root = xiofs_iget(sb, sbi->export_path, &rootattr);
	if (IS_ERR(root)) {
		err = PTR_ERR(root);
		goto out_sbi;
	}
	sb->s_root = d_make_root(root);
	if (!sb->s_root) {
		err = -ENOMEM;
		goto out_sbi;
	}

	xiofs_session_register(sbi);
	return 0;

out_sbi:
	kfree(sbi);
	sb->s_fs_info = NULL;
	return err;
}

static int xiofs_fc_parse_param(struct fs_context *fc,
				   struct fs_parameter *param)
{
	struct xiofs_fc_ctx *ctx = fc->fs_private;
	struct fs_parse_result result;
	int opt;

	opt = fs_parse(fc, xiofs_fs_parameters, param, &result);
	if (opt < 0)
		return opt;
	switch (opt) {
	case Opt_host:
		strscpy(ctx->host, param->string, sizeof(ctx->host));
		return 0;
	case Opt_port:
		ctx->port = result.uint_32;
		return 0;
	case Opt_path:
		strscpy(ctx->export_path, param->string,
			sizeof(ctx->export_path));
		return 0;
	default:
		return -EINVAL;
	}
}

static int xiofs_get_tree(struct fs_context *fc)
{
	return get_tree_nodev(fc, xiofs_fill_super);
}

static void xiofs_fc_free(struct fs_context *fc)
{
	kfree(fc->fs_private);
}

static const struct fs_context_operations xiofs_fc_ops = {
	.parse_param	= xiofs_fc_parse_param,
	.get_tree	= xiofs_get_tree,
	.free		= xiofs_fc_free,
};

static int xiofs_init_fs_context(struct fs_context *fc)
{
	struct xiofs_fc_ctx *ctx;

	ctx = kzalloc(sizeof(*ctx), GFP_KERNEL);
	if (!ctx)
		return -ENOMEM;
	ctx->port = 443;
	strscpy(ctx->export_path, "/", sizeof(ctx->export_path));
	fc->fs_private = ctx;
	fc->ops = &xiofs_fc_ops;
	return 0;
}

static void xiofs_kill_sb(struct super_block *sb)
{
	kill_anon_super(sb);
}

static struct file_system_type xiofs_type = {
	.owner			= THIS_MODULE,
	.name			= "xiofs",
	.init_fs_context	= xiofs_init_fs_context,
	.parameters		= xiofs_fs_parameters,
	.kill_sb		= xiofs_kill_sb,
};

static int __init xiofs_init(void)
{
	int err;

	request_module("tls");

	xiofs_inode_cachep = kmem_cache_create("xiofs_inode_cache",
			sizeof(struct xiofs_inode_info), 0,
			SLAB_RECLAIM_ACCOUNT | SLAB_ACCOUNT,
			xiofs_inode_init_once);
	if (!xiofs_inode_cachep)
		return -ENOMEM;

	err = xiofs_session_init();
	if (err)
		goto out_cache;

	err = register_filesystem(&xiofs_type);
	if (err)
		goto out_session;
	return 0;

out_session:
	xiofs_session_exit();
out_cache:
	kmem_cache_destroy(xiofs_inode_cachep);
	return err;
}

static void __exit xiofs_exit(void)
{
	unregister_filesystem(&xiofs_type);
	xiofs_session_exit();
	kmem_cache_destroy(xiofs_inode_cachep);
}

module_init(xiofs_init);
module_exit(xiofs_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("XRootD Collaboration");
MODULE_DESCRIPTION("XIOFS — Cross-transport I/O File System");
MODULE_SOFTDEP("pre: tls");
