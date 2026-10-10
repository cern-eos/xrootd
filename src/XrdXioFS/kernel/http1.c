// SPDX-License-Identifier: GPL-2.0
/*
 * Persistent HTTP/1.1 client on a kTLS (or plaintext) socket.
 *
 * The module sends plaintext HTTP. kTLS, if installed by the handshake
 * agent, encrypts records below this layer.
 *
 * Metadata is fetched conditionally: PROPFIND carries If-None-Match with
 * the ETag of the last answer and XrdHttp replies 304 with no body when
 * the object (or the directory, for listings) has not changed.
 */
#include <linux/errno.h>
#include <linux/fcntl.h>
#include <linux/fs.h>
#include <linux/kdev_t.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/net.h>
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/string.h>
#include <linux/tcp.h>
#include <net/sock.h>

#include "xiofs.h"

#ifndef LOCK_SH
#define LOCK_SH	1
#define LOCK_EX	2
#define LOCK_NB	4
#define LOCK_UN	8
#endif

int xiofs_http_status_to_errno(int status)
{
	switch (status) {
	case 200:
	case 201:
	case 204:
	case 206:
	case 207:
		return 0;
	case 304:
		/* Callers that send validators test meta.status first. */
		return -EEXIST;
	case 401:
	case 403:
		return -EACCES;
	case 404:
		return -ENOENT;
	case 405:
		return -EPERM;
	case 409:
		return -EEXIST;
	case 412:
		return -ESTALE;
	case 416:
		return -EINVAL;
	case 423:
		return -EAGAIN;
	case 507:
		return -ENOSPC;
	default:
		return status >= 500 ? -EIO : -EPROTO;
	}
}

/*                              s o c k e t s                                */

int xiofs_sock_send(struct socket *sock, const void *buf, size_t len)
{
	struct kvec iov = { .iov_base = (void *)buf, .iov_len = len };
	struct msghdr msg = { .msg_flags = MSG_NOSIGNAL };
	int sent, done = 0;

	while (done < (int)len) {
		iov.iov_base = (char *)buf + done;
		iov.iov_len = len - done;
		sent = kernel_sendmsg(sock, &msg, &iov, 1, iov.iov_len);
		if (sent <= 0)
			return sent ? sent : -ECONNRESET;
		done += sent;
	}
	return 0;
}

int xiofs_sock_recv(struct socket *sock, void *buf, size_t len)
{
	struct kvec iov = { .iov_base = buf, .iov_len = len };
	struct msghdr msg = { .msg_flags = MSG_NOSIGNAL };
	int got, done = 0;

	while (done < (int)len) {
		iov.iov_base = (char *)buf + done;
		iov.iov_len = len - done;
		got = kernel_recvmsg(sock, &msg, &iov, 1, iov.iov_len, 0);
		if (got == 0)
			return done ? done : -ECONNRESET;
		if (got < 0)
			return got;
		done += got;
	}
	return done;
}

int xiofs_sock_recv_some(struct socket *sock, void *buf, size_t len)
{
	struct kvec iov = { .iov_base = buf, .iov_len = len };
	struct msghdr msg = { .msg_flags = MSG_NOSIGNAL };

	return kernel_recvmsg(sock, &msg, &iov, 1, len, 0);
}

/* Make meta->body hold at least need bytes (geometric growth). */
int xiofs_resp_body_grow(struct xiofs_http_resp *meta, size_t need)
{
	size_t cap = meta->body ? meta->body_cap : 0;
	size_t ncap;
	char *nb;

	if (need > meta->alloc_max)
		return -EFBIG;
	if (meta->body && need <= cap)
		return 0;
	ncap = cap ? cap : 64 * 1024;
	while (ncap < need)
		ncap = min_t(size_t, ncap * 2, meta->alloc_max);
	nb = kvmalloc(ncap, GFP_KERNEL);
	if (!nb)
		return -ENOMEM;
	if (meta->body) {
		memcpy(nb, meta->body, meta->body_len);
		kvfree(meta->body);
	}
	meta->body = nb;
	meta->body_cap = ncap;
	return 0;
}

/*                              h e a d e r s                                */

static void xiofs_trim(char *s)
{
	size_t n;

	while (*s == ' ' || *s == '\t' || *s == '"')
		memmove(s, s + 1, strlen(s));
	n = strlen(s);
	while (n && (s[n - 1] == ' ' || s[n - 1] == '\t' || s[n - 1] == '"' ||
		     s[n - 1] == '\r' || s[n - 1] == '\n'))
		s[--n] = 0;
}

static int xiofs_header_value(const char *hdrs, const char *name, char *out,
			    size_t outsz)
{
	size_t nlen = strlen(name);
	const char *p = hdrs;

	while (*p) {
		const char *eol = strstr(p, "\r\n");
		size_t line = eol ? (size_t)(eol - p) : strlen(p);

		if (line > nlen + 1 && !strncasecmp(p, name, nlen) &&
		    p[nlen] == ':') {
			size_t vlen;
			const char *v = p + nlen + 1;

			while (*v == ' ' || *v == '\t')
				v++;
			vlen = (p + line) - v;
			if (vlen >= outsz)
				vlen = outsz - 1;
			memcpy(out, v, vlen);
			out[vlen] = 0;
			xiofs_trim(out);
			return 0;
		}
		if (!eol)
			break;
		p = eol + 2;
	}
	return -ENOENT;
}

/*                             t r a n s p o r t                             */

static int xiofs_drain(struct socket *sock, size_t skip)
{
	char dump[256];

	while (skip) {
		size_t n = min(skip, sizeof(dump));
		int r = xiofs_sock_recv(sock, dump, n);

		if (r < 0)
			return r;
		skip -= r;
	}
	return 0;
}

static int xiofs_transact_once(struct xiofs_conn *c, const char *req,
			     size_t reqlen, const void *body, size_t bodylen,
			     void *out, size_t outcap, size_t *outlen,
			     struct xiofs_http_resp *meta)
{
	char *hdrbuf;
	size_t filled = 0;
	char *sep, *extra;
	int err, status;
	long long clen = -1, reported;
	size_t extra_len, want, got;
	size_t alloc_max = meta->alloc_max;
	char etag[XIOFS_ETAG_MAX] = {};
	char clbuf[32] = {};

	memset(meta, 0, sizeof(*meta));
	meta->alloc_max = alloc_max;
	meta->content_length = -1;
	if (outlen)
		*outlen = 0;

	err = xiofs_conn_wait(c);
	if (err)
		return err;

	err = xiofs_sock_send(c->sock, req, reqlen);
	if (err)
		return err;
	if (bodylen) {
		err = xiofs_sock_send(c->sock, body, bodylen);
		if (err)
			return err;
	}

	hdrbuf = kzalloc(XIOFS_MAX_HDR, GFP_KERNEL);
	if (!hdrbuf)
		return -ENOMEM;

	while (filled < XIOFS_MAX_HDR - 1) {
		int n = xiofs_sock_recv_some(c->sock, hdrbuf + filled,
					   XIOFS_MAX_HDR - 1 - filled);

		if (n <= 0) {
			err = n ? n : -ECONNRESET;
			goto out_hdr;
		}
		filled += n;
		hdrbuf[filled] = 0;
		if (strstr(hdrbuf, "\r\n\r\n"))
			break;
	}
	sep = strstr(hdrbuf, "\r\n\r\n");
	if (!sep) {
		err = -EPROTO;
		goto out_hdr;
	}
	*sep = 0;
	if (sscanf(hdrbuf, "HTTP/%*s %d", &status) != 1) {
		err = -EPROTO;
		goto out_hdr;
	}
	xiofs_header_value(hdrbuf, "ETag", etag, sizeof(etag));
	if (!xiofs_header_value(hdrbuf, "Content-Length", clbuf, sizeof(clbuf)))
		if (kstrtoll(clbuf, 10, &clen))
			clen = -1;

	reported = clen;
	extra = sep + 4;
	extra_len = filled - (extra - hdrbuf);
	got = 0;

	/* HEAD / 204 / 304 never carry an entity. Content-Length on
	 * HEAD is the resource size, not a body to drain. */
	if (status == 204 || status == 304 || !strncmp(req, "HEAD ", 5))
		clen = 0;

	if (!out && alloc_max && clen > 0) {
		/* Growable body for listings. */
		if ((size_t)clen > alloc_max)
			err = -EFBIG;
		else
			err = xiofs_resp_body_grow(meta, (size_t)clen);
		if (err) {
			/* Keep the channel in sync before failing. */
			size_t skip = (size_t)clen;

			if (extra_len)
				skip = skip > extra_len ? skip - extra_len : 0;
			if (xiofs_drain(c->sock, skip))
				err = -ECONNRESET;
			goto out_hdr;
		}
		out = meta->body;
		outcap = (size_t)clen;
	}

	if (out && outcap && clen > 0) {
		want = min_t(size_t, (size_t)clen, outcap);
		if (extra_len) {
			size_t take = min(extra_len, want);

			memcpy(out, extra, take);
			got = take;
		}
		if (got < want) {
			err = xiofs_sock_recv(c->sock, (char *)out + got,
					    want - got);
			if (err < 0)
				goto out_hdr;
			got += err;
		}
		if (clen > (long long)want) {
			err = xiofs_drain(c->sock, (size_t)clen - want);
			if (err)
				goto out_hdr;
		}
		if (outlen)
			*outlen = got;
		if (out == meta->body)
			meta->body_len = got;
	} else if (clen > 0) {
		size_t skip = (size_t)clen;

		if (extra_len)
			skip = skip > extra_len ? skip - extra_len : 0;
		err = xiofs_drain(c->sock, skip);
		if (err)
			goto out_hdr;
	} else if (out && extra_len) {
		size_t take = min(extra_len, outcap);

		memcpy(out, extra, take);
		if (outlen)
			*outlen = take;
	}

	meta->content_length = reported;
	meta->status = status;
	strscpy(meta->etag, etag, sizeof(meta->etag));
	err = 0;
out_hdr:
	kfree(hdrbuf);
	return err;
}

static int xiofs_transact(struct xiofs_conn *c, const char *req,
			size_t reqlen, const void *body, size_t bodylen,
			void *out, size_t outcap, size_t *outlen,
			struct xiofs_http_resp *meta)
{
	int err, attempt;
	size_t alloc_max = meta->alloc_max;

	for (attempt = 0; attempt < 2; attempt++) {
		kvfree(meta->body);
		meta->body = NULL;
		meta->body_len = 0;
		meta->alloc_max = alloc_max;
		if (c->http2)
			err = xiofs_h2_transact(c, req, reqlen, body, bodylen,
						out, outcap, outlen, meta);
		else
			err = xiofs_transact_once(c, req, reqlen, body, bodylen,
						out, outcap, outlen, meta);
		if (!xiofs_connerr(err))
			return err;
		xiofs_conn_drop(c);
	}
	return err;
}

/*                        r e q u e s t   b u i l d e r                       */

struct xiofs_rq {
	char	*hdr;
	size_t	n;
	char	*path;
};

static int xiofs_rq_init(struct xiofs_rq *rq)
{
	rq->n = 0;
	rq->hdr = kmalloc(XIOFS_MAX_HDR, GFP_KERNEL);
	rq->path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	if (!rq->hdr || !rq->path) {
		kfree(rq->hdr);
		kfree(rq->path);
		rq->hdr = NULL;
		rq->path = NULL;
		return -ENOMEM;
	}
	rq->path[0] = 0;
	return 0;
}

static void xiofs_rq_free(struct xiofs_rq *rq)
{
	kfree(rq->hdr);
	kfree(rq->path);
	rq->hdr = NULL;
	rq->path = NULL;
}

static __printf(2, 3) void xiofs_rq_add(struct xiofs_rq *rq, const char *fmt, ...)
{
	va_list ap;
	int w;

	if (rq->n >= XIOFS_MAX_HDR - 1)
		return;
	va_start(ap, fmt);
	w = vsnprintf(rq->hdr + rq->n, XIOFS_MAX_HDR - rq->n, fmt, ap);
	va_end(ap);
	if (w > 0)
		rq->n = min_t(size_t, rq->n + w, XIOFS_MAX_HDR - 1);
}

static void xiofs_rq_start(struct xiofs_rq *rq, struct xiofs_sb_info *sbi,
			   const char *method, const char *path)
{
	rq->n = 0;
	xiofs_rq_add(rq, "%s %s HTTP/1.1\r\nHost: %s\r\n"
		     "Connection: keep-alive\r\n", method, path, sbi->hosthdr);
}

static void xiofs_rq_match(struct xiofs_rq *rq, const char *name,
			   const char *etag)
{
	if (!etag || !etag[0])
		return;
	if (etag[0] == '"' || etag[0] == '*')
		xiofs_rq_add(rq, "%s: %s\r\n", name, etag);
	else
		xiofs_rq_add(rq, "%s: \"%s\"\r\n", name, etag);
}

static void xiofs_rq_auth(struct xiofs_rq *rq, struct xiofs_conn *c)
{
	const char *bearer = c->bearer[0] ? c->bearer : c->sbi->bearer;

	if (bearer[0])
		xiofs_rq_add(rq, "Authorization: Bearer %s\r\n", bearer);
}

/*
 * Take a channel, append Authorization and the terminator, run the
 * exchange, release the channel. meta->status is valid on return 0.
 */
static int xiofs_rq_run(struct xiofs_sb_info *sbi, struct xiofs_rq *rq,
			const void *body, size_t bodylen, void *out,
			size_t outcap, size_t *outlen,
			struct xiofs_http_resp *meta)
{
	struct xiofs_conn *c;
	size_t n0 = rq->n;
	int err;

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	xiofs_rq_auth(rq, c);
	xiofs_rq_add(rq, "\r\n");
	err = xiofs_transact(c, rq->hdr, rq->n, body, bodylen, out, outcap,
			     outlen, meta);
	xiofs_conn_put(c);
	rq->n = n0;
	return err;
}

/* Run and map the status; conditional callers look at meta first. */
static int xiofs_rq_run_simple(struct xiofs_sb_info *sbi, struct xiofs_rq *rq,
			       const void *body, size_t bodylen,
			       struct xiofs_http_resp *meta)
{
	int err = xiofs_rq_run(sbi, rq, body, bodylen, NULL, 0, NULL, meta);

	if (!err)
		err = xiofs_http_status_to_errno(meta->status);
	return err;
}

static void xiofs_learn_etag(struct inode *inode, const char *etag)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);

	if (!etag || !etag[0])
		return;
	strscpy(ki->etag, etag, sizeof(ki->etag));
	xiofs_inode_set_id(inode, xiofs_etag_id(etag));
}

/*
 * Identity precondition for mutations. XrdHttp accepts the bare "<id>" as
 * "same object, any version", so writeback is not failed by a chmod or an
 * append from another client; only a replace-by-new-inode gets 412.
 * The full tag is used only when the server gave no parsable id.
 */
static void xiofs_rq_if_match_id(struct xiofs_rq *rq, struct inode *inode)
{
	struct xiofs_inode_info *ki = XIOFS_I(inode);

	if (ki->remote_id)
		xiofs_rq_add(rq, "If-Match: \"%lld\"\r\n", (long long)ki->remote_id);
	else
		xiofs_rq_match(rq, "If-Match", ki->etag);
}

/*                             D A V   p a r s e                             */

static bool xiofs_in_resp(const char *p, const char *end, const char *needle)
{
	const char *g = strstr(p, needle);

	return g && g < end;
}

static int xiofs_xml_text(const char *p, const char *end, const char *tag,
			  char *out, size_t outsz)
{
	const char *g = strstr(p, tag);
	size_t i = 0;

	if (!g || g >= end)
		return -ENOENT;
	g += strlen(tag);
	while (g < end && *g && *g != '<' && i < outsz - 1)
		out[i++] = *g++;
	out[i] = 0;
	return i ? 0 : -EINVAL;
}

static int xiofs_xml_ulong(const char *p, const char *end, const char *tag,
			   unsigned int base, unsigned long *out)
{
	char tmp[32];
	size_t i, j = 0;

	if (xiofs_xml_text(p, end, tag, tmp, sizeof(tmp)))
		return -ENOENT;
	for (i = 0; tmp[i]; i++)
		if (tmp[i] != ' ' && tmp[i] != '\n' && tmp[i] != '\r' &&
		    tmp[i] != '\t')
			tmp[j++] = tmp[i];
	tmp[j] = 0;
	if (!j)
		return -EINVAL;
	return kstrtoul(tmp, base, out);
}

static int xiofs_xml_ll(const char *p, const char *end, const char *tag,
			long long *out)
{
	char tmp[32];
	size_t i, j = 0;

	if (xiofs_xml_text(p, end, tag, tmp, sizeof(tmp)))
		return -ENOENT;
	for (i = 0; tmp[i]; i++)
		if (tmp[i] != ' ' && tmp[i] != '\n' && tmp[i] != '\r' &&
		    tmp[i] != '\t')
			tmp[j++] = tmp[i];
	tmp[j] = 0;
	if (!j)
		return -EINVAL;
	return kstrtoll(tmp, 10, out);
}

static int xiofs_dirents_grow(struct xiofs_dirent **ents, size_t *cap)
{
	size_t ncap = *cap ? *cap * 2 : 64;
	struct xiofs_dirent *nbuf;

	if (ncap > XIOFS_MAX_DIRENTS)
		return -EFBIG;
	nbuf = kvmalloc_array(ncap, sizeof(*nbuf), GFP_KERNEL);
	if (!nbuf)
		return -ENOMEM;
	if (*ents) {
		memcpy(nbuf, *ents, *cap * sizeof(*nbuf));
		kvfree(*ents);
	}
	*ents = nbuf;
	*cap = ncap;
	return 0;
}

/*
 * Parse a 207 multistatus. self_path (trailing slash stripped) is the
 * requested resource; its own <D:response> fills *single and is not
 * listed as a child. Every property test is bounded to the current
 * response: an unbounded strstr() would let a later <D:collection/>
 * mark earlier files as directories.
 */
static int xiofs_parse_dav(const char *xml, size_t len, const char *self_path,
			   struct xiofs_dirent **ents, size_t *nents,
			   struct xiofs_attr *single)
{
	const char *p = xml;
	const char *xml_end = xml + len;
	size_t cap = 0, n = 0;
	struct xiofs_dirent *out = NULL;
	size_t self_len = self_path ? strlen(self_path) : 0;
	bool seen_self = false;
	int err = 0;

	while (self_len > 1 && self_path[self_len - 1] == '/')
		self_len--;

	if (ents) {
		*ents = NULL;
		*nents = 0;
	}

	while (p && p < xml_end && *p) {
		const char *resp = strstr(p, "<D:response");
		const char *resp2 = strstr(p, "<d:response");
		const char *end;
		char href[XIOFS_PATH_MAX] = {};
		char etag[XIOFS_ETAG_MAX] = {};
		char clen[32] = {};
		struct xiofs_dirent de;
		bool is_self = false;
		unsigned long uv;
		long long lv;
		const char *hs, *he;

		if (resp2 && (!resp || resp2 < resp))
			resp = resp2;
		if (!resp)
			break;
		p = resp;

		end = strstr(p, "</D:response>");
		if (!end)
			end = strstr(p, "</d:response>");
		if (!end)
			break;

		memset(&de, 0, sizeof(de));

		hs = strstr(p, "<D:href>");
		if (!hs)
			hs = strstr(p, "<d:href>");
		if (hs && hs < end) {
			hs = strchr(hs, '>') + 1;
			he = strchr(hs, '<');
			if (he) {
				size_t l = he - hs;

				if (l >= sizeof(href))
					l = sizeof(href) - 1;
				memcpy(href, hs, l);
				href[l] = 0;
				while (l > 1 && href[l - 1] == '/')
					href[--l] = 0;
				if (self_len && l == self_len &&
				    !strncmp(href, self_path, self_len))
					is_self = true;
			}
		}
		if (xiofs_in_resp(p, end, "<D:collection") ||
		    xiofs_in_resp(p, end, "<d:collection") ||
		    xiofs_in_resp(p, end, "<lp1:iscollection>1"))
			de.is_dir = true;
		if (!de.is_dir &&
		    (xiofs_in_resp(p, end, "<D:symlink") ||
		     xiofs_in_resp(p, end, "<d:symlink") ||
		     xiofs_in_resp(p, end, "<X:other>1") ||
		     xiofs_in_resp(p, end, "<x:other>1")))
			de.is_lnk = true;
		if (xiofs_in_resp(p, end, "<X:file-type>fifo") ||
		    xiofs_in_resp(p, end, "<x:file-type>fifo"))
			de.is_fifo = true;
		if (xiofs_in_resp(p, end, "<X:file-type>chr") ||
		    xiofs_in_resp(p, end, "<x:file-type>chr"))
			de.is_chr = true;
		if (xiofs_in_resp(p, end, "<X:file-type>blk") ||
		    xiofs_in_resp(p, end, "<x:file-type>blk"))
			de.is_blk = true;
		if (!xiofs_xml_ulong(p, end, "<X:rdev>", 10, &uv) ||
		    !xiofs_xml_ulong(p, end, "<x:rdev>", 10, &uv))
			de.rdev = (u32)uv;
		if (!xiofs_xml_ulong(p, end, "<X:unix-mode>", 8, &uv) ||
		    !xiofs_xml_ulong(p, end, "<X:mode>", 8, &uv) ||
		    !xiofs_xml_ulong(p, end, "<x:mode>", 8, &uv))
			de.mode = (umode_t)uv;
		if (!xiofs_xml_ulong(p, end, "<X:uid>", 10, &uv) ||
		    !xiofs_xml_ulong(p, end, "<x:uid>", 10, &uv)) {
			de.uid = (u32)uv;
			de.have_uid = true;
		}
		if (!xiofs_xml_ulong(p, end, "<X:gid>", 10, &uv) ||
		    !xiofs_xml_ulong(p, end, "<x:gid>", 10, &uv)) {
			de.gid = (u32)uv;
			de.have_gid = true;
		}
		if (!xiofs_xml_ll(p, end, "<X:mtime>", &lv) ||
		    !xiofs_xml_ll(p, end, "<x:mtime>", &lv))
			de.mtime = lv;
		if (!xiofs_xml_ll(p, end, "<X:ctime>", &lv) ||
		    !xiofs_xml_ll(p, end, "<x:ctime>", &lv))
			de.ctime = lv;
		if (!xiofs_xml_ll(p, end, "<X:atime>", &lv) ||
		    !xiofs_xml_ll(p, end, "<x:atime>", &lv))
			de.atime = lv;
		if (!xiofs_xml_text(p, end, "getcontentlength>", clen,
				    sizeof(clen))) {
			long long sz;

			if (!kstrtoll(clen, 10, &sz))
				de.size = sz;
		}
		if (!xiofs_xml_text(p, end, "getetag>", etag, sizeof(etag))) {
			xiofs_trim(etag);
			strscpy(de.etag, etag, sizeof(de.etag));
			de.id = xiofs_etag_id(etag);
		}

		/* Depth 0 has one response: take it even if the href was
		 * escaped differently from the path we asked for. */
		if (single && !seen_self && (is_self || !ents)) {
			single->id = de.id;
			single->is_dir = de.is_dir;
			single->is_lnk = de.is_lnk;
			single->is_fifo = de.is_fifo;
			single->is_chr = de.is_chr;
			single->is_blk = de.is_blk;
			single->rdev = de.rdev;
			single->mode = de.mode;
			single->mtime = de.mtime;
			single->ctime = de.ctime;
			single->atime = de.atime;
			single->have_uid = de.have_uid;
			single->have_gid = de.have_gid;
			single->uid = de.uid;
			single->gid = de.gid;
			single->size = de.size;
			if (de.etag[0])
				strscpy(single->etag, de.etag,
					sizeof(single->etag));
			seen_self = true;
		} else if (ents && href[0] && !is_self) {
			const char *slash = strrchr(href, '/');
			const char *name = slash ? slash + 1 : href;

			if (name[0] && strcmp(name, ".") && strcmp(name, "..") &&
			    strlen(name) < sizeof(de.name)) {
				if (n == cap) {
					err = xiofs_dirents_grow(&out, &cap);
					if (err)
						goto fail;
				}
				strscpy(de.name, name, sizeof(de.name));
				out[n++] = de;
			}
		}
		p = end + 1;
	}

	if (ents) {
		*ents = out;
		*nents = n;
	} else {
		kvfree(out);
	}
	return 0;
fail:
	kvfree(out);
	return err;
}

/*                          m e t a d a t a   o p s                          */

static int xiofs_do_propfind(struct xiofs_sb_info *sbi, struct xiofs_rq *rq,
			     const char *path, int depth,
			     const char *if_none_match, void *out, size_t cap,
			     size_t *len, struct xiofs_http_resp *meta)
{
	xiofs_rq_start(rq, sbi, "PROPFIND", path);
	xiofs_rq_add(rq, "Depth: %d\r\n", depth);
	xiofs_rq_match(rq, "If-None-Match", if_none_match);
	return xiofs_rq_run(sbi, rq, NULL, 0, out, cap, len, meta);
}

/*
 * Stat one path. With if_none_match set, returns XIOFS_NOT_MODIFIED on
 * 304 and leaves *attr untouched. 404 is final (no HEAD retry); HEAD is
 * only tried when PROPFIND itself is refused (405).
 */
int xiofs_http_getattr_path(struct xiofs_sb_info *sbi, const char *path,
			    const char *if_none_match, struct xiofs_attr *attr)
{
	struct xiofs_rq rq;
	char *body;
	size_t len = 0;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	body = kvmalloc(XIOFS_MAX_DAV, GFP_KERNEL);
	if (!body) {
		xiofs_rq_free(&rq);
		return -ENOMEM;
	}
	err = xiofs_do_propfind(sbi, &rq, path, 0, if_none_match, body,
				XIOFS_MAX_DAV, &len, &meta);
	if (err)
		goto out;
	if (meta.status == 304 && if_none_match && if_none_match[0]) {
		err = XIOFS_NOT_MODIFIED;
		goto out;
	}
	err = xiofs_http_status_to_errno(meta.status);
	if (!err) {
		memset(attr, 0, sizeof(*attr));
		xiofs_parse_dav(body, len, path, NULL, NULL, attr);
		if (meta.etag[0]) {
			strscpy(attr->etag, meta.etag, sizeof(attr->etag));
			attr->id = xiofs_etag_id(meta.etag);
		}
		goto out;
	}
	if (err == -EPERM) {
		xiofs_rq_start(&rq, sbi, "HEAD", path);
		xiofs_rq_match(&rq, "If-None-Match", if_none_match);
		err = xiofs_rq_run(sbi, &rq, NULL, 0, NULL, 0, NULL, &meta);
		if (err)
			goto out;
		if (meta.status == 304 && if_none_match && if_none_match[0]) {
			err = XIOFS_NOT_MODIFIED;
			goto out;
		}
		err = xiofs_http_status_to_errno(meta.status);
		if (!err) {
			memset(attr, 0, sizeof(*attr));
			if (meta.etag[0]) {
				strscpy(attr->etag, meta.etag,
					sizeof(attr->etag));
				attr->id = xiofs_etag_id(meta.etag);
			}
			if (meta.content_length > 0)
				attr->size = meta.content_length;
		}
	}
out:
	kvfree(body);
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_getattr(struct inode *inode, const char *if_none_match,
		       struct xiofs_attr *attr)
{
	char *path = kmalloc(XIOFS_PATH_MAX, GFP_KERNEL);
	int err;

	if (!path)
		return -ENOMEM;
	err = xiofs_inode_path(inode, path, XIOFS_PATH_MAX);
	if (!err)
		err = xiofs_http_getattr_path(XIOFS_SB(inode->i_sb), path,
					      if_none_match, attr);
	kfree(path);
	return err;
}

/*
 * Depth 1 listing. The body is sized from Content-Length (up to
 * XIOFS_MAX_LISTING), so large directories are not truncated. With
 * if_none_match set, 304 yields XIOFS_NOT_MODIFIED and no entries.
 */
int xiofs_http_readdir(struct inode *dir, const char *if_none_match,
		       struct xiofs_dirent **ents, size_t *nents,
		       char *etag, size_t etag_sz)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	*ents = NULL;
	*nents = 0;
	if (etag && etag_sz)
		etag[0] = 0;
	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(dir, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	meta.alloc_max = XIOFS_MAX_LISTING;
	err = xiofs_do_propfind(sbi, &rq, rq.path, 1, if_none_match, NULL, 0,
				NULL, &meta);
	if (err)
		goto out;
	if (meta.status == 304 && if_none_match && if_none_match[0]) {
		err = XIOFS_NOT_MODIFIED;
		goto out;
	}
	err = xiofs_http_status_to_errno(meta.status);
	if (err)
		goto out;
	if (meta.body && meta.body_len)
		err = xiofs_parse_dav(meta.body, meta.body_len, rq.path, ents,
				      nents, NULL);
	if (!err && etag && etag_sz && meta.etag[0])
		strscpy(etag, meta.etag, etag_sz);
out:
	kvfree(meta.body);
	xiofs_rq_free(&rq);
	return err;
}

/*                               d a t a   o p s                             */

int xiofs_http_read(struct inode *inode, loff_t off, size_t len,
		       void *buf, size_t *nread)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	*nread = 0;
	if (!len)
		return 0;
	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "GET", rq.path);
	xiofs_rq_add(&rq, "Range: bytes=%llu-%llu\r\n",
		     (unsigned long long)off,
		     (unsigned long long)off + len - 1);
	err = xiofs_rq_run(sbi, &rq, NULL, 0, buf, len, nread, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
out:
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_write(struct inode *inode, loff_t off, size_t len,
			const void *buf, size_t *nwritten)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	*nwritten = 0;
	if (!len)
		return 0;
	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "PATCH", rq.path);
	xiofs_rq_add(&rq, "Content-Type: application/octet-stream\r\n"
		     "Content-Range: bytes %llu-%llu/*\r\n"
		     "Content-Length: %zu\r\n",
		     (unsigned long long)off,
		     (unsigned long long)off + len - 1, len);
	xiofs_rq_if_match_id(&rq, inode);
	err = xiofs_rq_run_simple(sbi, &rq, buf, len, &meta);
	if (!err) {
		*nwritten = len;
		xiofs_learn_etag(inode, meta.etag);
	}
out:
	xiofs_rq_free(&rq);
	return err;
}

/*
 * PUT the whole entity for an inode: deferred create (empty or with the
 * first contiguous extent) and truncate(0). if_none_star turns the PUT
 * into an exclusive create.
 */
int xiofs_http_put(struct inode *inode, const void *buf, size_t len,
		   bool if_match, bool if_none_star)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "PUT", rq.path);
	xiofs_rq_add(&rq, "Content-Type: application/octet-stream\r\n"
		     "Content-Length: %zu\r\n"
		     "Xrd-Mode: %o\r\n", len, inode->i_mode & 07777);
	if (if_match)
		xiofs_rq_if_match_id(&rq, inode);
	if (if_none_star)
		xiofs_rq_add(&rq, "If-None-Match: *\r\n");
	err = xiofs_rq_run(sbi, &rq, buf, len, NULL, 0, NULL, &meta);
	if (err)
		goto out;
	if (meta.status == 412 && if_none_star)
		err = -EEXIST;
	else
		err = xiofs_http_status_to_errno(meta.status);
	if (!err)
		xiofs_learn_etag(inode, meta.etag);
out:
	xiofs_rq_free(&rq);
	return err;
}

/* Synchronous create (nodefer mounts): empty PUT, identity from the 201. */
int xiofs_http_create(struct inode *dir, const char *path, umode_t mode,
		      bool excl, struct xiofs_attr *attr)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	xiofs_rq_start(&rq, sbi, "PUT", path);
	xiofs_rq_add(&rq, "Content-Type: application/octet-stream\r\n"
		     "Content-Length: 0\r\nXrd-Mode: %o\r\n", mode & 07777);
	if (excl)
		xiofs_rq_add(&rq, "If-None-Match: *\r\n");
	err = xiofs_rq_run(sbi, &rq, NULL, 0, NULL, 0, NULL, &meta);
	if (err)
		goto out;
	if (meta.status == 412 && excl)
		err = -EEXIST;
	else
		err = xiofs_http_status_to_errno(meta.status);
	if (!err && attr) {
		memset(attr, 0, sizeof(*attr));
		attr->mode = mode & 07777;
		if (meta.etag[0]) {
			strscpy(attr->etag, meta.etag, sizeof(attr->etag));
			attr->id = xiofs_etag_id(meta.etag);
		}
	}
out:
	xiofs_rq_free(&rq);
	return err;
}

/* MKCOL; XrdHttp answers 201 with the new collection's ETag. */
int xiofs_http_mkdir(struct inode *dir, const char *path, umode_t mode,
		     struct xiofs_attr *attr)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	xiofs_rq_start(&rq, sbi, "MKCOL", path);
	xiofs_rq_add(&rq, "Xrd-Mode: %o\r\n", mode & 07777);
	err = xiofs_rq_run_simple(sbi, &rq, NULL, 0, &meta);
	if (!err && attr) {
		memset(attr, 0, sizeof(*attr));
		attr->is_dir = true;
		attr->mode = mode & 07777;
		if (meta.etag[0]) {
			strscpy(attr->etag, meta.etag, sizeof(attr->etag));
			attr->id = xiofs_etag_id(meta.etag);
		}
	}
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_unlink(struct inode *inode)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "DELETE", rq.path);
	xiofs_rq_if_match_id(&rq, inode);
	err = xiofs_rq_run_simple(sbi, &rq, NULL, 0, &meta);
out:
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_rename(struct inode *old_inode, const char *new_path)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(old_inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(old_inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "MOVE", rq.path);
	xiofs_rq_add(&rq, "Destination: https://%s%s\r\n", sbi->hosthdr,
		     new_path);
	xiofs_rq_if_match_id(&rq, old_inode);
	err = xiofs_rq_run_simple(sbi, &rq, NULL, 0, &meta);
out:
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_truncate(struct inode *inode, loff_t size)
{
	if (size == 0)
		return xiofs_http_put(inode, NULL, 0, true, false);
	if (size > i_size_read(inode)) {
		char z = 0;
		size_t nw = 0;

		return xiofs_http_write(inode, size - 1, 1, &z, &nw);
	}
	return -EOPNOTSUPP;
}

static int xiofs_http_proppatch(struct inode *inode, const char *body, size_t blen)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "PROPPATCH", rq.path);
	xiofs_rq_add(&rq, "Content-Type: application/xml; charset=\"utf-8\"\r\n"
		     "Content-Length: %zu\r\n", blen);
	err = xiofs_rq_run_simple(sbi, &rq, body, blen, &meta);
out:
	xiofs_rq_free(&rq);
	return err;
}

#define XIOFS_PP_OPEN \
	"<?xml version=\"1.0\" encoding=\"utf-8\"?>" \
	"<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">" \
	"<D:set><D:prop>"
#define XIOFS_PP_CLOSE "</D:prop></D:set></D:propertyupdate>"

/*
 * One PROPPATCH for everything setattr() changes. mode < 0, uid/gid of
 * (u32)-1 and times < 0 mean "leave alone".
 */
int xiofs_http_setattr(struct inode *inode, int mode, u32 uid, u32 gid,
		       time64_t atime, time64_t mtime)
{
	char body[640];
	size_t n = 0;
	bool any = false;

	n += scnprintf(body + n, sizeof(body) - n, XIOFS_PP_OPEN);
	if (mode >= 0) {
		n += scnprintf(body + n, sizeof(body) - n,
			       "<X:mode>%o</X:mode>", (unsigned int)(mode & 07777));
		any = true;
	}
	if (uid != (u32)-1) {
		n += scnprintf(body + n, sizeof(body) - n, "<X:uid>%u</X:uid>", uid);
		any = true;
	}
	if (gid != (u32)-1) {
		n += scnprintf(body + n, sizeof(body) - n, "<X:gid>%u</X:gid>", gid);
		any = true;
	}
	if (atime >= 0) {
		n += scnprintf(body + n, sizeof(body) - n,
			       "<X:atime>%lld</X:atime>", (long long)atime);
		any = true;
	}
	if (mtime >= 0) {
		n += scnprintf(body + n, sizeof(body) - n,
			       "<X:mtime>%lld</X:mtime>", (long long)mtime);
		any = true;
	}
	if (!any)
		return 0;
	n += scnprintf(body + n, sizeof(body) - n, XIOFS_PP_CLOSE);
	return xiofs_http_proppatch(inode, body, n);
}

int xiofs_http_link(struct inode *old_inode, const char *new_path)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(old_inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(old_inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "LINK", rq.path);
	xiofs_rq_add(&rq, "Destination: https://%s%s\r\n", sbi->hosthdr,
		     new_path);
	err = xiofs_rq_run_simple(sbi, &rq, NULL, 0, &meta);
out:
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_symlink(struct inode *dir, const char *path, const char *target)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	if (!target || !*target || strpbrk(target, "\r\n"))
		return -EINVAL;
	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	xiofs_rq_start(&rq, sbi, "LINK", path);
	xiofs_rq_add(&rq, "Xrd-Link-Type: symbolic\r\n"
		     "Xrd-Symlink-Target: %s\r\n", target);
	err = xiofs_rq_run_simple(sbi, &rq, NULL, 0, &meta);
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_readlink(struct inode *inode, char *buf, size_t buflen)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	size_t got = 0;
	int err;

	if (!buf || buflen < 2)
		return -EINVAL;
	buf[0] = 0;
	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "GET", rq.path);
	xiofs_rq_add(&rq, "Xrd-Readlink: 1\r\n");
	err = xiofs_rq_run(sbi, &rq, NULL, 0, buf, buflen - 1, &got, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (err)
		goto out;
	if (got >= buflen)
		got = buflen - 1;
	buf[got] = 0;
	while (got && (buf[got - 1] == '\n' || buf[got - 1] == '\r' ||
		       buf[got - 1] == '\0'))
		buf[--got] = 0;
out:
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_mknod(struct inode *dir, const char *path, umode_t mode, dev_t rdev)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	xiofs_rq_start(&rq, sbi, "PUT", path);
	xiofs_rq_add(&rq, "Xrd-Mknod: 1\r\n"
		     "Xrd-Mode: %o\r\n"
		     "Xrd-Dev: %u:%u\r\n"
		     "Content-Length: 0\r\n",
		     (unsigned int)mode, MAJOR(rdev), MINOR(rdev));
	err = xiofs_rq_run_simple(sbi, &rq, NULL, 0, &meta);
	xiofs_rq_free(&rq);
	return err;
}

/*                                 x a t t r                                 */

static int xiofs_http_xattr_get(struct inode *inode, const char *hdr,
				const char *val, void *buf, size_t size)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	char *tmp;
	size_t got = 0;
	int err;

	tmp = kvmalloc(XIOFS_MAX_DAV, GFP_KERNEL);
	if (!tmp)
		return -ENOMEM;
	err = xiofs_rq_init(&rq);
	if (err) {
		kvfree(tmp);
		return err;
	}
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	xiofs_rq_start(&rq, sbi, "GET", rq.path);
	xiofs_rq_add(&rq, "%s: %s\r\n", hdr, val);
	err = xiofs_rq_run(sbi, &rq, NULL, 0, tmp, XIOFS_MAX_DAV, &got, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (!err) {
		if (!buf)
			err = (int)got;
		else if (size < got)
			err = -ERANGE;
		else {
			memcpy(buf, tmp, got);
			err = (int)got;
		}
	}
out:
	xiofs_rq_free(&rq);
	kvfree(tmp);
	return err;
}

int xiofs_http_getxattr(struct inode *inode, const char *name, void *buf,
			size_t size)
{
	if (!name || !*name || strpbrk(name, "\r\n"))
		return -EINVAL;
	return xiofs_http_xattr_get(inode, "Xrd-Xattr", name, buf, size);
}

int xiofs_http_listxattr(struct inode *inode, char *buf, size_t size)
{
	return xiofs_http_xattr_get(inode, "Xrd-Xattr-List", "1", buf, size);
}

int xiofs_http_setxattr(struct inode *inode, const char *name, const void *buf,
			size_t size)
{
	char body[1024];
	size_t n = 0;
	const char *val = buf ? buf : "";

	if (!name || !*name || strpbrk(name, "\r\n<>") ||
	    memchr(val, '<', size) || memchr(val, '>', size))
		return -EINVAL;
	if (size > 512)
		return -E2BIG;
	n += scnprintf(body + n, sizeof(body) - n, XIOFS_PP_OPEN
		       "<X:xattr-name>%s</X:xattr-name><X:xattr-value>", name);
	if (size)
		n += scnprintf(body + n, sizeof(body) - n, "%.*s", (int)size, val);
	n += scnprintf(body + n, sizeof(body) - n,
		       "</X:xattr-value>" XIOFS_PP_CLOSE);
	return xiofs_http_proppatch(inode, body, n);
}

int xiofs_http_removexattr(struct inode *inode, const char *name)
{
	char body[512];
	size_t n;

	if (!name || !*name || strpbrk(name, "\r\n<>"))
		return -EINVAL;
	n = scnprintf(body, sizeof(body), XIOFS_PP_OPEN
		      "<X:xattr-del>%s</X:xattr-del>" XIOFS_PP_CLOSE, name);
	return xiofs_http_proppatch(inode, body, n);
}

/*                                 l o c k s                                 */

int xiofs_http_lock(struct inode *inode, int cmd, int type, int whence,
		    loff_t start, loff_t len)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;
	const char *lcmd = "SETLK";
	const char *ltype = "WRLCK";
	const char *lwh = "SET";

	if (cmd == F_GETLK)
		lcmd = "GETLK";
	else if (cmd == F_SETLKW)
		lcmd = "SETLKW";
	if (type == F_RDLCK)
		ltype = "RDLCK";
	else if (type == F_UNLCK)
		ltype = "UNLCK";
	if (whence == SEEK_CUR)
		lwh = "CUR";
	else if (whence == SEEK_END)
		lwh = "END";

	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	if (type == F_UNLCK) {
		xiofs_rq_start(&rq, sbi, "UNLOCK", rq.path);
	} else {
		xiofs_rq_start(&rq, sbi, "LOCK", rq.path);
		xiofs_rq_add(&rq, "Xrd-Lock-Cmd: %s\r\n"
			     "Xrd-Lock-Type: %s\r\n"
			     "Xrd-Lock-Whence: %s\r\n"
			     "Xrd-Lock-Start: %lld\r\n"
			     "Xrd-Lock-Len: %lld\r\n",
			     lcmd, ltype, lwh, (long long)start, (long long)len);
	}
	err = xiofs_rq_run_simple(sbi, &rq, NULL, 0, &meta);
out:
	xiofs_rq_free(&rq);
	return err;
}

int xiofs_http_flock(struct inode *inode, int op)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_rq rq;
	struct xiofs_http_resp meta = {};
	int err;
	const char *lop = "EX";

	if (op & LOCK_UN)
		lop = "UN";
	else if (op & LOCK_SH)
		lop = "SH";
	err = xiofs_rq_init(&rq);
	if (err)
		return err;
	err = xiofs_inode_path(inode, rq.path, XIOFS_PATH_MAX);
	if (err)
		goto out;
	if (op & LOCK_UN) {
		xiofs_rq_start(&rq, sbi, "UNLOCK", rq.path);
	} else {
		xiofs_rq_start(&rq, sbi, "LOCK", rq.path);
		xiofs_rq_add(&rq, "Xrd-Lock-Cmd: FLOCK\r\n"
			     "Xrd-Lock-Op: %s%s\r\n", lop,
			     (op & LOCK_NB) ? "NB" : "");
	}
	err = xiofs_rq_run_simple(sbi, &rq, NULL, 0, &meta);
out:
	xiofs_rq_free(&rq);
	return err;
}
