// SPDX-License-Identifier: GPL-2.0
/*
 * Userspace TLS handshake agent imports a connected socket after
 * installing kTLS TX/RX. Steady-state HTTP then stays in-kernel.
 *
 * Kerberos: one imported socket per uid (default ccache principal).
 * GSS/SPNEGO is finished in xiofsagent; the kernel only multiplexes
 * already-authenticated HTTP channels keyed by current_fsuid().
 */
#include <linux/cred.h>
#include <linux/jiffies.h>
#include <linux/miscdevice.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/net.h>
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/uidgid.h>
#include <linux/user_namespace.h>
#include <linux/wait.h>
#include <net/inet_connection_sock.h>
#include <net/sock.h>
#include <net/tcp.h>

#include "xiofs.h"

struct xiofs_need {
	struct list_head	list;
	struct xiofs_sb_info	*sbi;
	u32			uid;
};

static LIST_HEAD(xiofs_mounts);
static DEFINE_MUTEX(xiofs_mounts_lock);
static LIST_HEAD(xiofs_needs);
static DEFINE_MUTEX(xiofs_needs_lock);
static DECLARE_WAIT_QUEUE_HEAD(xiofs_need_wait);

static bool xiofs_sock_has_tls_ulp(struct socket *sock)
{
	struct inet_connection_sock *icsk;

	if (!sock || !sock->sk)
		return false;
	if (sock->sk->sk_protocol != IPPROTO_TCP)
		return false;
	icsk = inet_csk(sock->sk);
	return icsk->icsk_ulp_ops &&
	       !strcmp(icsk->icsk_ulp_ops->name, "tls");
}

static void xiofs_sock_set_timeo(struct socket *sock, unsigned int sec)
{
	struct sock *sk = sock->sk;
	long t;

	if (!sk)
		return;
	if (!sec)
		sec = XIOFS_DEF_TIMEO_SEC;
	t = msecs_to_jiffies(sec * 1000u);
	lock_sock(sk);
	sk->sk_rcvtimeo = t;
	sk->sk_sndtimeo = t;
	release_sock(sk);
}

void xiofs_session_register(struct xiofs_sb_info *sbi)
{
	mutex_lock(&xiofs_mounts_lock);
	list_add(&sbi->list, &xiofs_mounts);
	mutex_unlock(&xiofs_mounts_lock);
}

void xiofs_session_unregister(struct xiofs_sb_info *sbi)
{
	mutex_lock(&xiofs_mounts_lock);
	list_del_init(&sbi->list);
	mutex_unlock(&xiofs_mounts_lock);
}

static void xiofs_need_purge_sbi(struct xiofs_sb_info *sbi)
{
	struct xiofs_need *n, *tmp;

	mutex_lock(&xiofs_needs_lock);
	list_for_each_entry_safe(n, tmp, &xiofs_needs, list) {
		if (n->sbi == sbi) {
			list_del(&n->list);
			kfree(n);
		}
	}
	mutex_unlock(&xiofs_needs_lock);
	wake_up_all(&xiofs_need_wait);
}

static void xiofs_need_cancel(struct xiofs_sb_info *sbi, u32 uid)
{
	struct xiofs_need *n, *tmp;

	mutex_lock(&xiofs_needs_lock);
	list_for_each_entry_safe(n, tmp, &xiofs_needs, list) {
		if (n->sbi == sbi && n->uid == uid) {
			list_del(&n->list);
			kfree(n);
		}
	}
	mutex_unlock(&xiofs_needs_lock);
}

static void xiofs_conn_enqueue_need(struct xiofs_conn *c)
{
	struct xiofs_need *n;

	if (c->pending)
		return;
	n = kzalloc(sizeof(*n), GFP_KERNEL);
	if (!n)
		return;
	n->sbi = c->sbi;
	n->uid = c->uid;
	INIT_LIST_HEAD(&n->list);
	mutex_lock(&xiofs_needs_lock);
	list_add_tail(&n->list, &xiofs_needs);
	mutex_unlock(&xiofs_needs_lock);
	c->pending = true;
	c->last_err = 0;
	wake_up_all(&xiofs_need_wait);
}

static void xiofs_conn_sock_release(struct xiofs_conn *c)
{
	if (!c->sock)
		return;
	kernel_sock_shutdown(c->sock, SHUT_RDWR);
	sockfd_put(c->sock);
	c->sock = NULL;
	xiofs_h2_reset(c);
}

void xiofs_conn_drop(struct xiofs_conn *c)
{
	xiofs_conn_sock_release(c);
	c->pending = false;
	c->last_err = 0;
}

int xiofs_conn_wait(struct xiofs_conn *c)
{
	struct xiofs_sb_info *sbi = c->sbi;
	unsigned int sec = sbi->timeo_sec ? sbi->timeo_sec : XIOFS_DEF_TIMEO_SEC;
	long timeout = msecs_to_jiffies(sec * 1000u);
	int ret;

	if (c->sock)
		return 0;
	if (sbi->shutting_down)
		return -ENOTCONN;

	xiofs_conn_enqueue_need(c);
	mutex_unlock(&c->io_lock);
	ret = wait_event_interruptible_timeout(c->wait,
			sbi->shutting_down || c->sock || c->last_err, timeout);
	mutex_lock(&c->io_lock);

	if (sbi->shutting_down)
		return -ENOTCONN;
	if (c->sock)
		return 0;
	if (c->last_err) {
		ret = c->last_err;
		c->last_err = 0;
		c->pending = false;
		return ret;
	}
	if (ret == 0)
		return -ETIMEDOUT;
	if (ret < 0)
		return ret;
	return -ENOTCONN;
}

static struct xiofs_conn *xiofs_conn_lookup(struct xiofs_sb_info *sbi, u32 uid)
{
	struct xiofs_conn *c;

	list_for_each_entry(c, &sbi->conns, list) {
		if (c->uid == uid)
			return c;
	}
	return NULL;
}

static struct xiofs_conn *xiofs_conn_create(struct xiofs_sb_info *sbi, u32 uid)
{
	struct xiofs_conn *c;

	c = kzalloc(sizeof(*c), GFP_KERNEL);
	if (!c)
		return NULL;
	c->sbi = sbi;
	c->uid = uid;
	mutex_init(&c->io_lock);
	init_waitqueue_head(&c->wait);
	INIT_LIST_HEAD(&c->list);
	c->http2 = sbi->krb5 ? false : sbi->http2;
	if (sbi->bearer[0])
		strscpy(c->bearer, sbi->bearer, sizeof(c->bearer));
	list_add_tail(&c->list, &sbi->conns);
	return c;
}

int xiofs_conn_get(struct xiofs_sb_info *sbi, struct xiofs_conn **out)
{
	struct xiofs_conn *c;
	u32 uid = 0;
	int err;

	if (sbi->krb5) {
		uid = from_kuid(&init_user_ns, current_fsuid());
		if (uid == (u32)-1)
			return -EOVERFLOW;
	}

	mutex_lock(&sbi->conns_lock);
	c = xiofs_conn_lookup(sbi, uid);
	if (!c) {
		c = xiofs_conn_create(sbi, uid);
		if (!c) {
			mutex_unlock(&sbi->conns_lock);
			return -ENOMEM;
		}
	}
	mutex_unlock(&sbi->conns_lock);

	mutex_lock(&c->io_lock);
	err = xiofs_conn_wait(c);
	if (err) {
		mutex_unlock(&c->io_lock);
		return err;
	}
	*out = c;
	return 0;
}

void xiofs_conn_put(struct xiofs_conn *c)
{
	if (c)
		mutex_unlock(&c->io_lock);
}

static void xiofs_conn_free_all(struct xiofs_sb_info *sbi)
{
	struct xiofs_conn *c, *tmp;

	mutex_lock(&sbi->conns_lock);
	list_for_each_entry(c, &sbi->conns, list) {
		mutex_lock(&c->io_lock);
		xiofs_conn_sock_release(c);
		mutex_unlock(&c->io_lock);
		wake_up_all(&c->wait);
	}
	list_for_each_entry_safe(c, tmp, &sbi->conns, list) {
		mutex_lock(&c->io_lock);
		mutex_unlock(&c->io_lock);
		list_del(&c->list);
		kfree(c);
	}
	mutex_unlock(&sbi->conns_lock);
}

void xiofs_session_close(struct xiofs_sb_info *sbi)
{
	sbi->shutting_down = true;
	xiofs_need_purge_sbi(sbi);
	xiofs_conn_free_all(sbi);
}

static struct xiofs_sb_info *xiofs_find_sbi(const char *host, __u16 port,
					   const char *export_path)
{
	struct xiofs_sb_info *sbi;
	const char *path = export_path && export_path[0] ? export_path : "/";

	list_for_each_entry(sbi, &xiofs_mounts, list) {
		if (sbi->port == port &&
		    !strcmp(sbi->host, host) &&
		    !strcmp(sbi->export_path, path))
			return sbi;
	}
	return NULL;
}

static int xiofs_import_sock(struct xiofs_import_sock *im)
{
	struct xiofs_sb_info *sbi;
	struct xiofs_conn *c;
	struct socket *sock;
	int err = 0, sockerr = 0;
	u32 uid;

	sock = sockfd_lookup(im->sockfd, &sockerr);
	if (!sock)
		return sockerr ? sockerr : -EBADF;

	if ((im->flags & XIOFS_IMPORT_TLS) && !xiofs_sock_has_tls_ulp(sock)) {
		pr_warn("xiofs: import fd %d has no kTLS ULP\n", im->sockfd);
		sockfd_put(sock);
		return -EPROTO;
	}

	mutex_lock(&xiofs_mounts_lock);
	sbi = xiofs_find_sbi(im->host, im->port, im->export_path);
	if (!sbi) {
		err = -ENOENT;
		goto out;
	}
	if (sbi->shutting_down) {
		err = -ESHUTDOWN;
		goto out;
	}

	uid = sbi->krb5 ? im->uid : 0;
	mutex_lock(&sbi->conns_lock);
	c = xiofs_conn_lookup(sbi, uid);
	if (!c) {
		c = xiofs_conn_create(sbi, uid);
		if (!c) {
			mutex_unlock(&sbi->conns_lock);
			err = -ENOMEM;
			goto out;
		}
	}
	mutex_unlock(&sbi->conns_lock);

	mutex_lock(&c->io_lock);
	if (c->sock)
		sockfd_put(c->sock);
	c->sock = sock;
	sock = NULL;
	c->tls = !!(im->flags & XIOFS_IMPORT_TLS);
	if (sbi->krb5 || (im->flags & XIOFS_IMPORT_KRB5)) {
		c->http2 = false;
	} else if (im->flags & XIOFS_IMPORT_H2) {
		c->http2 = true;
	} else {
		c->http2 = sbi->http2;
	}
	xiofs_h2_reset(c);
	if (im->flags & XIOFS_IMPORT_BEARER)
		strscpy(c->bearer, im->bearer, sizeof(c->bearer));
	xiofs_sock_set_timeo(c->sock, sbi->timeo_sec);
	c->pending = false;
	c->last_err = 0;
	mutex_unlock(&c->io_lock);
	xiofs_need_cancel(sbi, uid);
	wake_up_all(&c->wait);
out:
	mutex_unlock(&xiofs_mounts_lock);
	if (sock)
		sockfd_put(sock);
	return err;
}

static int xiofs_wait_need(struct xiofs_need_conn *uc)
{
	struct xiofs_need *n;
	int ret;

	for (;;) {
		ret = wait_event_interruptible(xiofs_need_wait,
					       !list_empty(&xiofs_needs));
		if (ret)
			return ret;

		mutex_lock(&xiofs_needs_lock);
		if (list_empty(&xiofs_needs)) {
			mutex_unlock(&xiofs_needs_lock);
			continue;
		}
		n = list_first_entry(&xiofs_needs, struct xiofs_need, list);
		list_del(&n->list);
		memset(uc, 0, sizeof(*uc));
		uc->uid = n->uid;
		uc->port = n->sbi->port;
		if (n->sbi->krb5)
			uc->flags |= XIOFS_IMPORT_KRB5;
		strscpy(uc->host, n->sbi->host, sizeof(uc->host));
		strscpy(uc->export_path, n->sbi->export_path,
			sizeof(uc->export_path));
		kfree(n);
		mutex_unlock(&xiofs_needs_lock);
		return 0;
	}
}

static int xiofs_need_fail(const struct xiofs_need_conn *uc)
{
	struct xiofs_sb_info *sbi;
	struct xiofs_conn *c;
	int err = -ENOENT;

	mutex_lock(&xiofs_mounts_lock);
	sbi = xiofs_find_sbi(uc->host, uc->port, uc->export_path);
	if (!sbi)
		goto out;
	mutex_lock(&sbi->conns_lock);
	c = xiofs_conn_lookup(sbi, uc->uid);
	mutex_unlock(&sbi->conns_lock);
	if (!c)
		goto out;
	mutex_lock(&c->io_lock);
	c->last_err = uc->err ? uc->err : -EACCES;
	c->pending = false;
	mutex_unlock(&c->io_lock);
	wake_up_all(&c->wait);
	err = 0;
out:
	mutex_unlock(&xiofs_mounts_lock);
	return err;
}

static long xiofs_ctl_ioctl(struct file *file, unsigned int cmd,
			  unsigned long arg)
{
	if (cmd == XIOFS_IOC_IMPORT_SOCK) {
		struct xiofs_import_sock im;

		if (copy_from_user(&im, (void __user *)arg, sizeof(im)))
			return -EFAULT;
		im.host[sizeof(im.host) - 1] = 0;
		im.export_path[sizeof(im.export_path) - 1] = 0;
		im.bearer[sizeof(im.bearer) - 1] = 0;
		return xiofs_import_sock(&im);
	}
	if (cmd == XIOFS_IOC_WAIT_NEED) {
		struct xiofs_need_conn uc;
		int err;

		err = xiofs_wait_need(&uc);
		if (err)
			return err;
		if (copy_to_user((void __user *)arg, &uc, sizeof(uc)))
			return -EFAULT;
		return 0;
	}
	if (cmd == XIOFS_IOC_NEED_FAIL) {
		struct xiofs_need_conn uc;

		if (copy_from_user(&uc, (void __user *)arg, sizeof(uc)))
			return -EFAULT;
		uc.host[sizeof(uc.host) - 1] = 0;
		uc.export_path[sizeof(uc.export_path) - 1] = 0;
		if (uc.err > 0)
			uc.err = -uc.err;
		if (!uc.err)
			uc.err = -EACCES;
		return xiofs_need_fail(&uc);
	}
	return -ENOTTY;
}

static const struct file_operations xiofs_ctl_fops = {
	.owner		= THIS_MODULE,
	.unlocked_ioctl	= xiofs_ctl_ioctl,
#ifdef CONFIG_COMPAT
	.compat_ioctl	= xiofs_ctl_ioctl,
#endif
};

static struct miscdevice xiofs_ctl_dev = {
	.minor	= MISC_DYNAMIC_MINOR,
	.name	= "xiofsctl",
	.fops	= &xiofs_ctl_fops,
	.mode	= 0600,
};

int xiofs_session_init(void)
{
	return misc_register(&xiofs_ctl_dev);
}

void xiofs_session_exit(void)
{
	misc_deregister(&xiofs_ctl_dev);
}
