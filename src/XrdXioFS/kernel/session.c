// SPDX-License-Identifier: GPL-2.0
/*
 * Userspace TLS handshake agent imports a connected socket after
 * installing kTLS TX/RX. Steady-state HTTP then stays in-kernel.
 */
#include <linux/jiffies.h>
#include <linux/miscdevice.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/net.h>
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/string.h>
#include <linux/uaccess.h>
#include <linux/wait.h>
#include <net/inet_connection_sock.h>
#include <net/sock.h>
#include <net/tcp.h>

#include "xiofs.h"

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

static LIST_HEAD(xiofs_mounts);
static DEFINE_MUTEX(xiofs_mounts_lock);

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

void xiofs_session_drop(struct xiofs_sb_info *sbi)
{
	if (sbi->sock) {
		kernel_sock_shutdown(sbi->sock, SHUT_RDWR);
		sockfd_put(sbi->sock);
		sbi->sock = NULL;
	}
}

int xiofs_session_wait(struct xiofs_sb_info *sbi)
{
	unsigned int sec = sbi->timeo_sec ? sbi->timeo_sec : XIOFS_DEF_TIMEO_SEC;
	long timeout = msecs_to_jiffies(sec * 1000u);
	int ret;

	if (sbi->sock)
		return 0;
	if (sbi->shutting_down)
		return -ENOTCONN;

	mutex_unlock(&sbi->io_lock);
	ret = wait_event_interruptible_timeout(sbi->sock_wait,
			sbi->shutting_down || sbi->sock, timeout);
	mutex_lock(&sbi->io_lock);

	if (sbi->shutting_down)
		return -ENOTCONN;
	if (ret == 0)
		return -ETIMEDOUT;
	if (ret < 0)
		return ret;
	if (!sbi->sock)
		return -ENOTCONN;
	return 0;
}

void xiofs_session_close(struct xiofs_sb_info *sbi)
{
	mutex_lock(&sbi->io_lock);
	sbi->shutting_down = true;
	xiofs_session_drop(sbi);
	mutex_unlock(&sbi->io_lock);
	wake_up_all(&sbi->sock_wait);
}

static struct xiofs_sb_info *xiofs_find_sbi(const struct xiofs_import_sock *im)
{
	struct xiofs_sb_info *sbi;

	list_for_each_entry(sbi, &xiofs_mounts, list) {
		if (sbi->port == im->port &&
		    !strcmp(sbi->host, im->host) &&
		    !strcmp(sbi->export_path, im->export_path[0] ?
			    im->export_path : "/"))
			return sbi;
	}
	return NULL;
}

static int xiofs_import_sock(struct xiofs_import_sock *im)
{
	struct xiofs_sb_info *sbi;
	struct socket *sock;
	int err = 0, sockerr = 0;

	sock = sockfd_lookup(im->sockfd, &sockerr);
	if (!sock)
		return sockerr ? sockerr : -EBADF;

	if ((im->flags & XIOFS_IMPORT_TLS) && !xiofs_sock_has_tls_ulp(sock)) {
		pr_warn("xiofs: import fd %d has no kTLS ULP\n", im->sockfd);
		sockfd_put(sock);
		return -EPROTO;
	}

	mutex_lock(&xiofs_mounts_lock);
	sbi = xiofs_find_sbi(im);
	if (!sbi) {
		err = -ENOENT;
		goto out;
	}
	mutex_lock(&sbi->io_lock);
	if (sbi->shutting_down) {
		err = -ESHUTDOWN;
		mutex_unlock(&sbi->io_lock);
		goto out;
	}
	if (sbi->sock)
		sockfd_put(sbi->sock);
	sbi->sock = sock;
	sock = NULL;
	sbi->tls = !!(im->flags & XIOFS_IMPORT_TLS);
	if (im->flags & XIOFS_IMPORT_BEARER)
		strscpy(sbi->bearer, im->bearer, sizeof(sbi->bearer));
	xiofs_sock_set_timeo(sbi->sock, sbi->timeo_sec);
	mutex_unlock(&sbi->io_lock);
	wake_up_all(&sbi->sock_wait);
out:
	mutex_unlock(&xiofs_mounts_lock);
	if (sock)
		sockfd_put(sock);
	return err;
}

static long xiofs_ctl_ioctl(struct file *file, unsigned int cmd,
			  unsigned long arg)
{
	struct xiofs_import_sock im;

	if (cmd != XIOFS_IOC_IMPORT_SOCK)
		return -ENOTTY;
	if (copy_from_user(&im, (void __user *)arg, sizeof(im)))
		return -EFAULT;
	im.host[sizeof(im.host) - 1] = 0;
	im.export_path[sizeof(im.export_path) - 1] = 0;
	im.bearer[sizeof(im.bearer) - 1] = 0;
	return xiofs_import_sock(&im);
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
