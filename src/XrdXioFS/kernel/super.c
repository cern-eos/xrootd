// SPDX-License-Identifier: GPL-2.0
/*
 * Superblock: mount options, inode slab, 4 MiB readahead BDI.
 *
 * Inodes are keyed by the server identity (dev:ino carried in the ETag),
 * not by a path hash, so hard links share one inode and a rename never
 * aliases two inodes onto one file. Remote paths are derived from the
 * dentry tree at request time, which keeps a renamed directory's children
 * addressable without touching each inode.
 */
#include <linux/dcache.h>
#include <linux/fs.h>
#include <linux/fs_context.h>
#include <linux/fs_parser.h>
#include <linux/jiffies.h>
#include <linux/kmod.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/statfs.h>
#include <linux/string.h>
#include <linux/uidgid.h>
#include <linux/user_namespace.h>
#include <linux/wait.h>

#include "xiofs.h"
#include "xiofs_compat.h"

static struct kmem_cache *xiofs_inode_cachep;

enum {
	Opt_host,
	Opt_port,
	Opt_path,
	Opt_actimeo,
	Opt_timeo,
	Opt_conns,
	Opt_http2,
	Opt_krb5,
	Opt_jwt,
	Opt_defer,
	Opt_nodefer,
};

static const struct fs_parameter_spec xiofs_fs_parameters[] = {
	fsparam_string("host", Opt_host),
	fsparam_u32("port", Opt_port),
	fsparam_string("path", Opt_path),
	fsparam_u32("actimeo", Opt_actimeo),
	fsparam_u32("timeo", Opt_timeo),
	fsparam_u32("conns", Opt_conns),
	fsparam_flag("http2", Opt_http2),
	fsparam_flag("krb5", Opt_krb5),
	fsparam_flag("jwt", Opt_jwt),
	fsparam_flag("defer", Opt_defer),
	fsparam_flag("nodefer", Opt_nodefer),
	{}
};

struct xiofs_fc_ctx {
	char		host[256];
	unsigned int	port;
	char		export_path[256];
	unsigned int	actimeo_sec;
	unsigned int	timeo_sec;
	unsigned int	nconns;
	bool		http2;
	bool		krb5;
	bool		jwt;
	bool		defer_create;
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

	while (nlen && name[0] == '/') {
		name++;
		nlen--;
	}
	if (!nlen) {
		if (plen >= dstsz)
			return -ENAMETOOLONG;
		memcpy(dst, parent ? parent : "/", plen ? plen + 1 : 2);
		return 0;
	}
	while (plen && parent[plen - 1] == '/')
		plen--;
	if (plen + 1 + nlen + 1 > dstsz)
		return -ENAMETOOLONG;
	memcpy(dst, parent, plen);
	dst[plen] = '/';
	memcpy(dst + plen + 1, name, nlen);
	dst[plen + 1 + nlen] = 0;
	return 0;
}

/*
 * Remote path of a dentry: export_path + path below the mount root. Works
 * for negative dentries (create/mkdir/lookup) and after renames.
 */
int xiofs_dentry_path(struct dentry *dentry, char *buf, size_t sz)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dentry->d_sb);
	char *tmp, *p;
	int err;

	tmp = kmalloc(sz, GFP_KERNEL);
	if (!tmp)
		return -ENOMEM;
	p = dentry_path_raw(dentry, tmp, sz);
	if (IS_ERR(p)) {
		err = PTR_ERR(p);
		goto out;
	}
	err = xiofs_join_path(buf, sz, sbi->export_path,
			      (p[0] == '/' && !p[1]) ? NULL : p);
out:
	kfree(tmp);
	return err;
}

int xiofs_inode_path(struct inode *inode, char *buf, size_t sz)
{
	struct dentry *dentry;
	int err;

	if (inode == d_inode(inode->i_sb->s_root))
		return xiofs_dentry_path(inode->i_sb->s_root, buf, sz);
	dentry = d_find_any_alias(inode);
	if (!dentry)
		return -ESTALE;
	err = xiofs_dentry_path(dentry, buf, sz);
	dput(dentry);
	return err;
}

/* "<id>-<ctime>-<size>" (optionally quoted): the leading id, 0 if absent. */
u64 xiofs_etag_id(const char *etag)
{
	char tmp[32];
	size_t i = 0;
	long long v;

	if (!etag)
		return 0;
	if (*etag == '"')
		etag++;
	if (*etag == '-')
		tmp[i++] = *etag++;
	while (*etag >= '0' && *etag <= '9' && i < sizeof(tmp) - 1)
		tmp[i++] = *etag++;
	tmp[i] = 0;
	if (!i || *etag != '-')
		return 0;
	if (kstrtoll(tmp, 10, &v))
		return 0;
	return (u64)v;
}

static struct inode *xiofs_alloc_inode(struct super_block *sb)
{
	struct xiofs_inode_info *ki;

	ki = kmem_cache_alloc(xiofs_inode_cachep, GFP_KERNEL);
	if (!ki)
		return NULL;
	ki->remote_id = 0;
	ki->etag[0] = 0;
	ki->link_target = NULL;
	ki->attr_jiffies = 0;
	ki->flags = 0;
	ki->defer_parent = NULL;
	ki->defer_ent = NULL;
	INIT_LIST_HEAD(&ki->deferred);
	ki->dir_ents = NULL;
	ki->dir_nents = 0;
	ki->dir_etag[0] = 0;
	ki->dir_jiffies = 0;
	ki->dir_valid = false;
	return &ki->vfs_inode;
}

static void xiofs_free_inode(struct inode *inode)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);

	kvfree(ki->dir_ents);
	ki->dir_ents = NULL;
	kfree(ki->link_target);
	ki->link_target = NULL;
	kmem_cache_free(xiofs_inode_cachep, ki);
}

static void xiofs_evict_inode(struct inode *inode)
{
	truncate_inode_pages_final(&inode->i_data);
	clear_inode(inode);
	/* A deferred child that never reached the server leaves its parent. */
	xiofs_deferred_del(inode);
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
	.evict_inode	= xiofs_evict_inode,
	.drop_inode	= generic_delete_inode,
	.put_super	= xiofs_put_super,
};

static void xiofs_inode_init_once(void *obj)
{
	struct xiofs_inode_info *ki = obj;

	inode_init_once(&ki->vfs_inode);
	mutex_init(&ki->dir_lock);
}

static void xiofs_init_inode(struct inode *inode, const struct xiofs_attr *attr)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);

	if (attr && attr->etag[0])
		strscpy(ki->etag, attr->etag, sizeof(ki->etag));
	ki->attr_jiffies = jiffies;

	inode->i_uid = current_fsuid();
	inode->i_gid = current_fsgid();
	if (attr && attr->have_uid)
		inode->i_uid = make_kuid(&init_user_ns, attr->uid);
	if (attr && attr->have_gid)
		inode->i_gid = make_kgid(&init_user_ns, attr->gid);
	inode->i_mapping->a_ops = &xiofs_aops;
	mapping_set_gfp_mask(inode->i_mapping, GFP_HIGHUSER);
	inode->i_blkbits = PAGE_SHIFT;

	if (attr && attr->is_dir) {
		inode->i_mode = S_IFDIR | (attr->mode ? (attr->mode & 07777) : 0755);
		inode->i_op = &xiofs_dir_inode_ops;
		inode->i_fop = &xiofs_dir_ops;
		set_nlink(inode, 2);
		inode->i_size = 0;
	} else if (attr && attr->is_lnk) {
		inode->i_mode = S_IFLNK | (attr->mode ? (attr->mode & 07777) : 0777);
		inode->i_op = &xiofs_symlink_inode_ops;
		inode->i_fop = NULL;
		set_nlink(inode, 1);
		inode->i_size = attr->size;
	} else if (attr && attr->is_fifo) {
		inode->i_mode = S_IFIFO | (attr->mode ? (attr->mode & 07777) : 0666);
		inode->i_op = &xiofs_file_inode_ops;
		inode->i_fop = &xiofs_file_ops;
		set_nlink(inode, 1);
		inode->i_size = 0;
	} else if (attr && attr->is_chr) {
		inode->i_mode = S_IFCHR | (attr->mode ? (attr->mode & 07777) : 0666);
		inode->i_op = &xiofs_file_inode_ops;
		inode->i_fop = &xiofs_file_ops;
		inode->i_rdev = attr->rdev;
		set_nlink(inode, 1);
		inode->i_size = 0;
	} else if (attr && attr->is_blk) {
		inode->i_mode = S_IFBLK | (attr->mode ? (attr->mode & 07777) : 0666);
		inode->i_op = &xiofs_file_inode_ops;
		inode->i_fop = &xiofs_file_ops;
		inode->i_rdev = attr->rdev;
		set_nlink(inode, 1);
		inode->i_size = 0;
	} else {
		inode->i_mode = S_IFREG | ((attr && attr->mode) ? (attr->mode & 07777) : 0644);
		inode->i_op = &xiofs_file_inode_ops;
		inode->i_fop = &xiofs_file_ops;
		set_nlink(inode, 1);
		inode->i_size = attr ? attr->size : 0;
	}
	if (attr) {
		xiofs_set_times2(inode, attr->mtime, attr->atime);
		xiofs_set_ctime(inode, attr->ctime ? attr->ctime : attr->mtime);
	}
}

static int xiofs_inode_test(struct inode *inode, void *data)
{
	return XIOFS_I(inode)->remote_id == *(u64 *)data;
}

static int xiofs_inode_set(struct inode *inode, void *data)
{
	XIOFS_I(inode)->remote_id = *(u64 *)data;
	inode->i_ino = (unsigned long)*(u64 *)data;
	return 0;
}

/*
 * Inode for a server object whose identity is known (attr->id != 0).
 * A cached inode is returned as is; the caller refreshes its attributes.
 */
struct inode *xiofs_iget(struct super_block *sb, const struct xiofs_attr *attr)
{
	struct inode *inode;
	u64 id = attr ? attr->id : 0;

	if (!id)
		return xiofs_new_inode(sb, attr);

	inode = iget5_locked(sb, (unsigned long)id, xiofs_inode_test,
			     xiofs_inode_set, &id);
	if (!inode)
		return ERR_PTR(-ENOMEM);
	if (!(inode->i_state & I_NEW))
		return inode;
	xiofs_init_inode(inode, attr);
	unlock_new_inode(inode);
	return inode;
}

/*
 * Inode without a server identity yet: the mount root before the first
 * stat, deferred creates, or servers that do not tag responses. Not in the
 * inode hash until xiofs_inode_set_id() learns the id.
 */
struct inode *xiofs_new_inode(struct super_block *sb,
			      const struct xiofs_attr *attr)
{
	struct inode *inode = new_inode(sb);

	if (!inode)
		return ERR_PTR(-ENOMEM);
	inode->i_ino = get_next_ino();
	xiofs_init_inode(inode, attr);
	return inode;
}

void xiofs_inode_set_id(struct inode *inode, u64 id)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);

	if (!id || ki->remote_id)
		return;
	ki->remote_id = id;
	if (inode != d_inode(inode->i_sb->s_root))
		inode->i_ino = (unsigned long)id;
	__insert_inode_hash(inode, (unsigned long)id);
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
	if (ctx->krb5 && ctx->jwt)
		return -EINVAL;

	sbi = kzalloc(sizeof(*sbi), GFP_KERNEL);
	if (!sbi)
		return -ENOMEM;
	mutex_init(&sbi->conns_lock);
	INIT_LIST_HEAD(&sbi->conns);
	INIT_LIST_HEAD(&sbi->list);
	init_waitqueue_head(&sbi->conn_free);
	sbi->sb = sb;
	sbi->port = ctx->port ? ctx->port : 443;
	sbi->actimeo_sec = ctx->actimeo_sec;
	sbi->timeo_sec = ctx->timeo_sec ? ctx->timeo_sec : XIOFS_DEF_TIMEO_SEC;
	sbi->nconns = clamp_t(unsigned int, ctx->nconns, 1u, XIOFS_MAX_CONNS);
	sbi->http2 = ctx->http2;
	sbi->krb5 = ctx->krb5;
	sbi->jwt = ctx->jwt;
	sbi->defer_create = ctx->defer_create;
	strscpy(sbi->host, ctx->host, sizeof(sbi->host));
	strscpy(sbi->export_path,
		ctx->export_path[0] ? ctx->export_path : "/",
		sizeof(sbi->export_path));
	xiofs_set_hosthdr(sbi);

	sb->s_magic = XIOFS_MAGIC;
	sb->s_op = &xiofs_sops;
	sb->s_d_op = &xiofs_dops;
	sb->s_xattr = xiofs_xattr_handlers;
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

	root = xiofs_new_inode(sb, &rootattr);
	if (IS_ERR(root)) {
		err = PTR_ERR(root);
		goto out_sbi;
	}
	root->i_ino = 1;
	/* Force the first stat so uid/gid/mode of the export are real. */
	XIOFS_I(root)->attr_jiffies = 0;
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
	case Opt_actimeo:
		ctx->actimeo_sec = result.uint_32;
		return 0;
	case Opt_timeo:
		ctx->timeo_sec = result.uint_32;
		return 0;
	case Opt_conns:
		ctx->nconns = result.uint_32;
		return 0;
	case Opt_http2:
		ctx->http2 = true;
		return 0;
	case Opt_krb5:
		ctx->krb5 = true;
		return 0;
	case Opt_jwt:
		ctx->jwt = true;
		return 0;
	case Opt_defer:
		ctx->defer_create = true;
		return 0;
	case Opt_nodefer:
		ctx->defer_create = false;
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
	ctx->actimeo_sec = XIOFS_DEF_ACTIMEO_SEC;
	ctx->timeo_sec = XIOFS_DEF_TIMEO_SEC;
	ctx->nconns = XIOFS_DEF_CONNS;
	ctx->defer_create = true;
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
MODULE_ALIAS_FS("xiofs");
