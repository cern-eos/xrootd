// SPDX-License-Identifier: GPL-2.0
/*
 * Persistent HTTP/1.1 client on a kTLS (or plaintext) socket.
 *
 * The module sends plaintext HTTP. kTLS, if installed by the handshake
 * agent, encrypts records below this layer.
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
	char etag[128] = {};
	char clbuf[32] = {};

	memset(meta, 0, sizeof(*meta));
	meta->content_length = -1;

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
			size_t skip = (size_t)clen - want;
			char dump[256];

			while (skip) {
				size_t n = min(skip, sizeof(dump));
				int r = xiofs_sock_recv(c->sock, dump, n);

				if (r < 0) {
					err = r;
					goto out_hdr;
				}
				skip -= r;
			}
		}
		*outlen = got;
	} else if (clen > 0) {
		char dump[256];
		size_t skip = (size_t)clen;

		if (extra_len)
			skip = skip > extra_len ? skip - extra_len : 0;
		while (skip) {
			size_t n = min(skip, sizeof(dump));
			int r = xiofs_sock_recv(c->sock, dump, n);

			if (r < 0) {
				err = r;
				goto out_hdr;
			}
			skip -= r;
		}
	} else if (out && extra_len) {
		size_t take = min(extra_len, outcap);

		memcpy(out, extra, take);
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

	for (attempt = 0; attempt < 2; attempt++) {
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

static int xiofs_add_auth(char *buf, size_t sz, size_t *n,
			struct xiofs_conn *c)
{
	const char *bearer = c->bearer[0] ? c->bearer : c->sbi->bearer;

	if (!bearer[0])
		return 0;
	*n += snprintf(buf + *n, sz - *n, "Authorization: Bearer %s\r\n",
		       bearer);
	return 0;
}

static int xiofs_add_match(char *buf, size_t sz, size_t *n, const char *etag)
{
	if (!etag || !etag[0])
		return 0;
	if (etag[0] == '"')
		*n += snprintf(buf + *n, sz - *n, "If-Match: %s\r\n", etag);
	else
		*n += snprintf(buf + *n, sz - *n, "If-Match: \"%s\"\r\n", etag);
	return 0;
}

static bool xiofs_in_resp(const char *p, const char *end, const char *needle)
{
	const char *g = strstr(p, needle);

	return g && g < end;
}

static int xiofs_xml_ulong(const char *p, const char *end, const char *tag,
			   unsigned int base, unsigned long *out)
{
	const char *g;
	char tmp[32];
	size_t i = 0;

	g = strstr(p, tag);
	if (!g || g >= end)
		return -ENOENT;
	g += strlen(tag);
	while (g < end && i < sizeof(tmp) - 1 && *g && *g != '<') {
		if (*g != ' ' && *g != '\n' && *g != '\r' && *g != '\t')
			tmp[i++] = *g;
		g++;
	}
	tmp[i] = 0;
	if (!i)
		return -EINVAL;
	return kstrtoul(tmp, base, out);
}

static int xiofs_xml_ll(const char *p, const char *end, const char *tag,
			long long *out)
{
	const char *g;
	char tmp[32];
	size_t i = 0;

	g = strstr(p, tag);
	if (!g || g >= end)
		return -ENOENT;
	g += strlen(tag);
	while (g < end && i < sizeof(tmp) - 1 && *g && *g != '<') {
		if (*g != ' ' && *g != '\n' && *g != '\r' && *g != '\t')
			tmp[i++] = *g;
		g++;
	}
	tmp[i] = 0;
	if (!i)
		return -EINVAL;
	return kstrtoll(tmp, 10, out);
}

static int xiofs_parse_dav(const char *xml, size_t len,
			 struct xiofs_dirent **ents, size_t *nents,
			 struct xiofs_attr *single)
{
	const char *p = xml;
	size_t cap = 8, n = 0;
	struct xiofs_dirent *out = NULL;

	if (ents) {
		out = kcalloc(cap, sizeof(*out), GFP_KERNEL);
		if (!out)
			return -ENOMEM;
	}

	while (p && *p) {
		const char *resp = strstr(p, "<D:response");
		const char *resp2 = strstr(p, "<d:response");
		const char *end;
		char href[256] = {};
		char clen[32] = {};
		bool is_dir = false, is_lnk = false, have_uid = false, have_gid = false;
		bool is_fifo = false, is_chr = false, is_blk = false;
		umode_t mode = 0;
		time64_t mtime = 0, atime = 0;
		u32 uid = 0, gid = 0, rdev = 0;
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

		hs = strstr(p, "<D:href>");
		if (!hs)
			hs = strstr(p, "<d:href>");
		if (hs && hs < end) {
			hs = strchr(hs, '>') + 1;
			he = strchr(hs, '<');
			if (he) {
				size_t l = he - hs;
				const char *slash;

				if (l >= sizeof(href))
					l = sizeof(href) - 1;
				memcpy(href, hs, l);
				href[l] = 0;
				while (l && href[l - 1] == '/')
					href[--l] = 0;
				slash = strrchr(href, '/');
				if (slash)
					memmove(href, slash + 1,
						strlen(slash + 1) + 1);
			}
		}
		if (strstr(p, "<D:collection") || strstr(p, "<d:collection") ||
		    strstr(p, "<lp1:iscollection>1"))
			is_dir = true;
		if (xiofs_in_resp(p, end, "<D:symlink") ||
		    xiofs_in_resp(p, end, "<d:symlink") ||
		    xiofs_in_resp(p, end, "<X:other>1") ||
		    xiofs_in_resp(p, end, "<x:other>1"))
			is_lnk = true;
		if (is_dir)
			is_lnk = false;
		if (xiofs_in_resp(p, end, "<X:file-type>fifo") ||
		    xiofs_in_resp(p, end, "<x:file-type>fifo"))
			is_fifo = true;
		if (xiofs_in_resp(p, end, "<X:file-type>chr") ||
		    xiofs_in_resp(p, end, "<x:file-type>chr"))
			is_chr = true;
		if (xiofs_in_resp(p, end, "<X:file-type>blk") ||
		    xiofs_in_resp(p, end, "<x:file-type>blk"))
			is_blk = true;
		if (!xiofs_xml_ulong(p, end, "<X:rdev>", 10, &uv) ||
		    !xiofs_xml_ulong(p, end, "<x:rdev>", 10, &uv))
			rdev = (u32)uv;
		if (!xiofs_xml_ulong(p, end, "unix-mode>", 8, &uv) ||
		    !xiofs_xml_ulong(p, end, "<X:unix-mode>", 8, &uv))
			mode = (umode_t)uv;
		else if (!xiofs_xml_ulong(p, end, "<X:mode>", 8, &uv) ||
			 !xiofs_xml_ulong(p, end, "<x:mode>", 8, &uv))
			mode = (umode_t)uv;
		if (!xiofs_xml_ulong(p, end, "<X:uid>", 10, &uv) ||
		    !xiofs_xml_ulong(p, end, "<x:uid>", 10, &uv)) {
			uid = (u32)uv;
			have_uid = true;
		}
		if (!xiofs_xml_ulong(p, end, "<X:gid>", 10, &uv) ||
		    !xiofs_xml_ulong(p, end, "<x:gid>", 10, &uv)) {
			gid = (u32)uv;
			have_gid = true;
		}
		if (!xiofs_xml_ll(p, end, "<X:mtime>", &lv) ||
		    !xiofs_xml_ll(p, end, "<x:mtime>", &lv))
			mtime = lv;
		if (!xiofs_xml_ll(p, end, "<X:atime>", &lv) ||
		    !xiofs_xml_ll(p, end, "<x:atime>", &lv))
			atime = lv;
		{
			const char *g = strstr(p, "getcontentlength>");

			if (g && g < end) {
				sscanf(g + strlen("getcontentlength>"), "%31s", clen);
			}
		}

		if (single && n == 0) {
			single->is_dir = is_dir;
			single->is_lnk = is_lnk;
			single->is_fifo = is_fifo;
			single->is_chr = is_chr;
			single->is_blk = is_blk;
			single->rdev = rdev;
			single->mode = mode;
			single->mtime = mtime;
			single->atime = atime;
			single->have_uid = have_uid;
			single->have_gid = have_gid;
			single->uid = uid;
			single->gid = gid;
			if (clen[0])
				sscanf(clen, "%lld", (long long *)&single->size);
		}
		if (out && href[0] && n < 4096) {
			if (n == cap) {
				struct xiofs_dirent *nbuf;
				size_t ncap = cap * 2;

				nbuf = kcalloc(ncap, sizeof(*nbuf), GFP_KERNEL);
				if (!nbuf) {
					kfree(out);
					return -ENOMEM;
				}
				memcpy(nbuf, out, cap * sizeof(*nbuf));
				kfree(out);
				out = nbuf;
				cap = ncap;
			}
			strscpy(out[n].name, href, sizeof(out[n].name));
			out[n].is_dir = is_dir;
			out[n].is_lnk = is_lnk;
			out[n].is_fifo = is_fifo;
			out[n].is_chr = is_chr;
			out[n].is_blk = is_blk;
			out[n].rdev = rdev;
			out[n].mode = mode;
			out[n].mtime = mtime;
			if (clen[0])
				sscanf(clen, "%lld", (long long *)&out[n].size);
			n++;
		}
		p = end + 1;
	}

	if (ents) {
		*ents = out;
		*nents = n;
	} else {
		kfree(out);
	}
	return 0;
}

static int xiofs_do_propfind(struct xiofs_conn *c, const char *path,
			   int depth, void *body, size_t cap, size_t *len,
			   struct xiofs_http_resp *meta)
{
	char req[1024];
	size_t n = 0;

	n += snprintf(req + n, sizeof(req) - n,
		      "PROPFIND %s HTTP/1.1\r\n"
		      "Host: %s\r\n"
		      "Depth: %d\r\n"
		      "Connection: keep-alive\r\n",
		      path, c->sbi->hosthdr, depth);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	return xiofs_transact(c, req, n, NULL, 0, body, cap, len, meta);
}

int xiofs_http_getattr_path(struct xiofs_sb_info *sbi, const char *path,
			       struct xiofs_attr *attr)
{
	struct xiofs_conn *c;
	char *body;
	size_t len = 0;
	struct xiofs_http_resp meta;
	int err;

	memset(attr, 0, sizeof(*attr));
	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	body = kvmalloc(XIOFS_MAX_DAV, GFP_KERNEL);
	if (!body) {
		xiofs_conn_put(c);
		return -ENOMEM;
	}
	err = xiofs_do_propfind(c, path, 0, body, XIOFS_MAX_DAV, &len, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (!err) {
		xiofs_parse_dav(body, len, NULL, NULL, attr);
		if (meta.etag[0])
			strscpy(attr->etag, meta.etag, sizeof(attr->etag));
	}
	if (err == -ENOENT || err == -EPERM) {
		char req[512];
		size_t n = 0;

		n += snprintf(req + n, sizeof(req) - n,
			      "HEAD %s HTTP/1.1\r\nHost: %s\r\n"
			      "Connection: keep-alive\r\n",
			      path, c->sbi->hosthdr);
		xiofs_add_auth(req, sizeof(req), &n, c);
		n += snprintf(req + n, sizeof(req) - n, "\r\n");
		err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
		if (!err)
			err = xiofs_http_status_to_errno(meta.status);
		if (!err) {
			attr->is_dir = false;
			if (meta.etag[0])
				strscpy(attr->etag, meta.etag, sizeof(attr->etag));
			if (meta.content_length > 0)
				attr->size = meta.content_length;
		}
	}
	kvfree(body);
	xiofs_conn_put(c);
	return err;
}

int xiofs_http_getattr(struct inode *inode, struct xiofs_attr *attr)
{
	return xiofs_http_getattr_path(XIOFS_SB(inode->i_sb),
					  XIOFS_I(inode)->remote_path, attr);
}

int xiofs_http_readdir(struct inode *dir, struct xiofs_dirent **ents,
			  size_t *nents)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_conn *c;
	char *body;
	size_t len = 0;
	struct xiofs_http_resp meta;
	int err;

	*ents = NULL;
	*nents = 0;
	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	body = kvmalloc(XIOFS_MAX_DAV, GFP_KERNEL);
	if (!body) {
		xiofs_conn_put(c);
		return -ENOMEM;
	}
	err = xiofs_do_propfind(c, XIOFS_I(dir)->remote_path, 1, body,
			      XIOFS_MAX_DAV, &len, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (!err)
		err = xiofs_parse_dav(body, len, ents, nents, NULL);
	kvfree(body);
	xiofs_conn_put(c);
	return err;
}

int xiofs_http_read(struct inode *inode, loff_t off, size_t len,
		       void *buf, size_t *nread)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_conn *c;
	char req[768];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	*nread = 0;
	if (!len)
		return 0;
	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "GET %s HTTP/1.1\r\n"
		      "Host: %s\r\n"
		      "Range: bytes=%llu-%llu\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, c->sbi->hosthdr,
		      (unsigned long long)off,
		      (unsigned long long)off + len - 1);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, buf, len, nread, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	xiofs_conn_put(c);
	return err;
}

int xiofs_http_write(struct inode *inode, loff_t off, size_t len,
			const void *buf, size_t *nwritten)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_conn *c;
	char req[896];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	*nwritten = 0;
	if (!len)
		return 0;
	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "PATCH %s HTTP/1.1\r\n"
		      "Host: %s\r\n"
		      "Content-Type: application/octet-stream\r\n"
		      "Content-Range: bytes %llu-%llu/*\r\n"
		      "Content-Length: %zu\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, c->sbi->hosthdr,
		      (unsigned long long)off,
		      (unsigned long long)off + len - 1, len);
	xiofs_add_match(req, sizeof(req), &n, XIOFS_I(inode)->etag);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, buf, len, NULL, 0, NULL, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (!err) {
		*nwritten = len;
		if (meta.etag[0])
			strscpy(XIOFS_I(inode)->etag, meta.etag,
				sizeof(XIOFS_I(inode)->etag));
	}
	xiofs_conn_put(c);
	return err;
}

static int xiofs_put(struct xiofs_sb_info *sbi, const char *path,
		   const void *buf, size_t len, const char *if_match,
		   const char *if_none, struct xiofs_attr *attr)
{
	struct xiofs_conn *c;
	char req[896];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "PUT %s HTTP/1.1\r\n"
		      "Host: %s\r\n"
		      "Content-Type: application/octet-stream\r\n"
		      "Content-Length: %zu\r\n"
		      "Connection: keep-alive\r\n",
		      path, c->sbi->hosthdr, len);
	xiofs_add_match(req, sizeof(req), &n, if_match);
	if (if_none && if_none[0])
		n += snprintf(req + n, sizeof(req) - n,
			      "If-None-Match: %s\r\n", if_none);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, buf, len, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (!err && attr) {
		attr->size = len;
		attr->is_dir = false;
		if (meta.etag[0])
			strscpy(attr->etag, meta.etag, sizeof(attr->etag));
	}
	return err;
}

int xiofs_http_create(struct inode *dir, const char *path,
			 struct xiofs_attr *attr)
{
	return xiofs_put(XIOFS_SB(dir->i_sb), path, NULL, 0, NULL, NULL, attr);
}

int xiofs_http_mkdir(struct inode *dir, const char *path)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_conn *c;
	char req[512];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "MKCOL %s HTTP/1.1\r\nHost: %s\r\n"
		      "Connection: keep-alive\r\n",
		      path, c->sbi->hosthdr);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_unlink(struct inode *inode)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_conn *c;
	char req[640];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "DELETE %s HTTP/1.1\r\nHost: %s\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, c->sbi->hosthdr);
	xiofs_add_match(req, sizeof(req), &n, XIOFS_I(inode)->etag);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_rename(struct inode *old_inode, const char *new_path)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(old_inode->i_sb);
	struct xiofs_conn *c;
	char req[1024];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "MOVE %s HTTP/1.1\r\nHost: %s\r\n"
		      "Destination: https://%s%s\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(old_inode)->remote_path, c->sbi->hosthdr,
		      c->sbi->hosthdr, new_path);
	xiofs_add_match(req, sizeof(req), &n, XIOFS_I(old_inode)->etag);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (!err)
		strscpy(XIOFS_I(old_inode)->remote_path, new_path,
			sizeof(XIOFS_I(old_inode)->remote_path));
	return err;
}

int xiofs_http_truncate(struct inode *inode, loff_t size)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);

	if (size == 0)
		return xiofs_put(sbi, XIOFS_I(inode)->remote_path, NULL, 0,
			       XIOFS_I(inode)->etag, NULL, NULL);
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
	struct xiofs_conn *c;
	char req[896];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "PROPPATCH %s HTTP/1.1\r\nHost: %s\r\n"
		      "Content-Type: application/xml; charset=\"utf-8\"\r\n"
		      "Content-Length: %zu\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, c->sbi->hosthdr, blen);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, body, blen, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_chmod(struct inode *inode, umode_t mode)
{
	char body[512];
	size_t blen;

	blen = scnprintf(body, sizeof(body),
			 "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
			 "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
			 "<D:set><D:prop><X:mode>%o</X:mode></D:prop></D:set>"
			 "</D:propertyupdate>",
			 (unsigned int)(mode & 07777));
	return xiofs_http_proppatch(inode, body, blen);
}

int xiofs_http_chown(struct inode *inode, u32 uid, u32 gid)
{
	char body[512];
	size_t n = 0;

	if (uid == (u32)-1 && gid == (u32)-1)
		return 0;
	n += scnprintf(body + n, sizeof(body) - n,
		       "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
		       "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
		       "<D:set><D:prop>");
	if (uid != (u32)-1)
		n += scnprintf(body + n, sizeof(body) - n, "<X:uid>%u</X:uid>", uid);
	if (gid != (u32)-1)
		n += scnprintf(body + n, sizeof(body) - n, "<X:gid>%u</X:gid>", gid);
	n += scnprintf(body + n, sizeof(body) - n,
		       "</D:prop></D:set></D:propertyupdate>");
	return xiofs_http_proppatch(inode, body, n);
}

int xiofs_http_utimens(struct inode *inode, time64_t atime, time64_t mtime)
{
	char body[512];
	size_t n = 0;

	if (atime < 0 && mtime < 0)
		return 0;
	n += scnprintf(body + n, sizeof(body) - n,
		       "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
		       "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
		       "<D:set><D:prop>");
	if (atime >= 0)
		n += scnprintf(body + n, sizeof(body) - n,
			       "<X:atime>%lld</X:atime>", (long long)atime);
	if (mtime >= 0)
		n += scnprintf(body + n, sizeof(body) - n,
			       "<X:mtime>%lld</X:mtime>", (long long)mtime);
	n += scnprintf(body + n, sizeof(body) - n,
		       "</D:prop></D:set></D:propertyupdate>");
	return xiofs_http_proppatch(inode, body, n);
}

int xiofs_http_link(struct inode *old_inode, const char *new_path)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(old_inode->i_sb);
	struct xiofs_conn *c;
	char req[1024];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "LINK %s HTTP/1.1\r\nHost: %s\r\n"
		      "Destination: https://%s%s\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(old_inode)->remote_path, c->sbi->hosthdr,
		      c->sbi->hosthdr, new_path);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_symlink(struct inode *dir, const char *path, const char *target)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_conn *c;
	char req[1536];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	if (!target || !*target || strpbrk(target, "\r\n"))
		return -EINVAL;
	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "LINK %s HTTP/1.1\r\nHost: %s\r\n"
		      "Xrd-Link-Type: symbolic\r\n"
		      "Xrd-Symlink-Target: %s\r\n"
		      "Connection: keep-alive\r\n",
		      path, c->sbi->hosthdr, target);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_readlink(struct inode *inode, char *buf, size_t buflen)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_conn *c;
	char req[768];
	size_t n = 0, got = 0;
	struct xiofs_http_resp meta;
	int err;

	if (!buf || buflen < 2)
		return -EINVAL;
	buf[0] = 0;
	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "GET %s HTTP/1.1\r\nHost: %s\r\n"
		      "Xrd-Readlink: 1\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, c->sbi->hosthdr);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, buf, buflen - 1, &got, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (err)
		return err;
	if (got >= buflen)
		got = buflen - 1;
	buf[got] = 0;
	while (got && (buf[got - 1] == '\n' || buf[got - 1] == '\r' ||
		       buf[got - 1] == '\0'))
		buf[--got] = 0;
	return 0;
}

int xiofs_http_mknod(struct inode *dir, const char *path, umode_t mode, dev_t rdev)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(dir->i_sb);
	struct xiofs_conn *c;
	char req[768];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	n += snprintf(req + n, sizeof(req) - n,
		      "PUT %s HTTP/1.1\r\nHost: %s\r\n"
		      "Xrd-Mknod: 1\r\n"
		      "Xrd-Mode: %o\r\n"
		      "Xrd-Dev: %u:%u\r\n"
		      "Content-Length: 0\r\n"
		      "Connection: keep-alive\r\n",
		      path, c->sbi->hosthdr, (unsigned int)mode,
		      MAJOR(rdev), MINOR(rdev));
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_getxattr(struct inode *inode, const char *name, void *buf,
			size_t size)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_conn *c;
	char req[1024];
	char *tmp;
	size_t n = 0, got = 0;
	struct xiofs_http_resp meta;
	int err;

	if (!name || !*name || strpbrk(name, "\r\n"))
		return -EINVAL;
	tmp = kvmalloc(XIOFS_MAX_DAV, GFP_KERNEL);
	if (!tmp)
		return -ENOMEM;
	err = xiofs_conn_get(sbi, &c);
	if (err) {
		kvfree(tmp);
		return err;
	}
	n += snprintf(req + n, sizeof(req) - n,
		      "GET %s HTTP/1.1\r\nHost: %s\r\n"
		      "Xrd-Xattr: %s\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, c->sbi->hosthdr, name);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, tmp, XIOFS_MAX_DAV, &got, &meta);
	xiofs_conn_put(c);
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
	kvfree(tmp);
	return err;
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
	n += scnprintf(body + n, sizeof(body) - n,
		       "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
		       "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
		       "<D:set><D:prop><X:xattr-name>%s</X:xattr-name>"
		       "<X:xattr-value>", name);
	if (size)
		n += scnprintf(body + n, sizeof(body) - n, "%.*s", (int)size, val);
	n += scnprintf(body + n, sizeof(body) - n,
		       "</X:xattr-value></D:prop></D:set></D:propertyupdate>");
	return xiofs_http_proppatch(inode, body, n);
}

int xiofs_http_listxattr(struct inode *inode, char *buf, size_t size)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_conn *c;
	char req[768];
	char *tmp;
	size_t n = 0, got = 0;
	struct xiofs_http_resp meta;
	int err;

	tmp = kvmalloc(XIOFS_MAX_DAV, GFP_KERNEL);
	if (!tmp)
		return -ENOMEM;
	err = xiofs_conn_get(sbi, &c);
	if (err) {
		kvfree(tmp);
		return err;
	}
	n += snprintf(req + n, sizeof(req) - n,
		      "GET %s HTTP/1.1\r\nHost: %s\r\n"
		      "Xrd-Xattr-List: 1\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, c->sbi->hosthdr);
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, tmp, XIOFS_MAX_DAV, &got, &meta);
	xiofs_conn_put(c);
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
	kvfree(tmp);
	return err;
}

int xiofs_http_removexattr(struct inode *inode, const char *name)
{
	char body[512];
	size_t n;

	if (!name || !*name || strpbrk(name, "\r\n<>"))
		return -EINVAL;
	n = scnprintf(body, sizeof(body),
		      "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
		      "<D:propertyupdate xmlns:D=\"DAV:\" xmlns:X=\"http://xrootd.org/ns\">"
		      "<D:set><D:prop><X:xattr-del>%s</X:xattr-del></D:prop></D:set>"
		      "</D:propertyupdate>", name);
	return xiofs_http_proppatch(inode, body, n);
}

int xiofs_http_lock(struct inode *inode, int cmd, int type, int whence,
		    loff_t start, loff_t len)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_conn *c;
	char req[1024];
	size_t n = 0;
	struct xiofs_http_resp meta;
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

	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	if (type == F_UNLCK) {
		n += snprintf(req + n, sizeof(req) - n,
			      "UNLOCK %s HTTP/1.1\r\nHost: %s\r\n"
			      "Connection: keep-alive\r\n",
			      XIOFS_I(inode)->remote_path, c->sbi->hosthdr);
	} else {
		n += snprintf(req + n, sizeof(req) - n,
			      "LOCK %s HTTP/1.1\r\nHost: %s\r\n"
			      "Xrd-Lock-Cmd: %s\r\n"
			      "Xrd-Lock-Type: %s\r\n"
			      "Xrd-Lock-Whence: %s\r\n"
			      "Xrd-Lock-Start: %lld\r\n"
			      "Xrd-Lock-Len: %lld\r\n"
			      "Connection: keep-alive\r\n",
			      XIOFS_I(inode)->remote_path, c->sbi->hosthdr, lcmd, ltype,
			      lwh, (long long)start, (long long)len);
	}
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_flock(struct inode *inode, int op)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	struct xiofs_conn *c;
	char req[768];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;
	const char *lop = "EX";

	if (op & LOCK_UN)
		lop = "UN";
	else if (op & LOCK_SH)
		lop = "SH";
	err = xiofs_conn_get(sbi, &c);
	if (err)
		return err;
	if ((op & LOCK_UN)) {
		n += snprintf(req + n, sizeof(req) - n,
			      "UNLOCK %s HTTP/1.1\r\nHost: %s\r\n"
			      "Connection: keep-alive\r\n",
			      XIOFS_I(inode)->remote_path, c->sbi->hosthdr);
	} else {
		n += snprintf(req + n, sizeof(req) - n,
			      "LOCK %s HTTP/1.1\r\nHost: %s\r\n"
			      "Xrd-Lock-Cmd: FLOCK\r\n"
			      "Xrd-Lock-Op: %s%s\r\n"
			      "Connection: keep-alive\r\n",
			      XIOFS_I(inode)->remote_path, c->sbi->hosthdr, lop,
			      (op & LOCK_NB) ? "NB" : "");
	}
	xiofs_add_auth(req, sizeof(req), &n, c);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(c, req, n, NULL, 0, NULL, 0, NULL, &meta);
	xiofs_conn_put(c);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}
