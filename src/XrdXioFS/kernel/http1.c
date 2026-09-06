// SPDX-License-Identifier: GPL-2.0
/*
 * Persistent HTTP/1.1 client on a kTLS (or plaintext) socket.
 *
 * The module sends plaintext HTTP. kTLS, if installed by the handshake
 * agent, encrypts records below this layer.
 */
#include <linux/errno.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/net.h>
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/string.h>
#include <linux/tcp.h>
#include <net/sock.h>

#include "xiofs.h"

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
	case 507:
		return -ENOSPC;
	default:
		return status >= 500 ? -EIO : -EPROTO;
	}
}

static int xiofs_sock_send(struct socket *sock, const void *buf, size_t len)
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

static int xiofs_sock_recv(struct socket *sock, void *buf, size_t len)
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

static int xiofs_sock_recv_some(struct socket *sock, void *buf, size_t len)
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

struct xiofs_http_resp {
	int		status;
	char		etag[128];
	long long	content_length;
	char		*body;
	size_t		body_len;
};

static int xiofs_transact_once(struct xiofs_sb_info *sbi, const char *req,
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

	err = xiofs_session_wait(sbi);
	if (err)
		return err;

	err = xiofs_sock_send(sbi->sock, req, reqlen);
	if (err)
		return err;
	if (bodylen) {
		err = xiofs_sock_send(sbi->sock, body, bodylen);
		if (err)
			return err;
	}

	hdrbuf = kzalloc(XIOFS_MAX_HDR, GFP_KERNEL);
	if (!hdrbuf)
		return -ENOMEM;

	while (filled < XIOFS_MAX_HDR - 1) {
		int n = xiofs_sock_recv_some(sbi->sock, hdrbuf + filled,
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
			err = xiofs_sock_recv(sbi->sock, (char *)out + got,
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
				int r = xiofs_sock_recv(sbi->sock, dump, n);

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
			int r = xiofs_sock_recv(sbi->sock, dump, n);

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

static int xiofs_transact(struct xiofs_sb_info *sbi, const char *req,
			size_t reqlen, const void *body, size_t bodylen,
			void *out, size_t outcap, size_t *outlen,
			struct xiofs_http_resp *meta)
{
	int err, attempt;

	for (attempt = 0; attempt < 2; attempt++) {
		err = xiofs_transact_once(sbi, req, reqlen, body, bodylen,
					out, outcap, outlen, meta);
		if (!xiofs_connerr(err))
			return err;
		xiofs_session_drop(sbi);
	}
	return err;
}

static int xiofs_add_auth(char *buf, size_t sz, size_t *n,
			struct xiofs_sb_info *sbi)
{
	if (!sbi->bearer[0])
		return 0;
	*n += snprintf(buf + *n, sz - *n, "Authorization: Bearer %s\r\n",
		       sbi->bearer);
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
		bool is_dir = false;
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
		{
			const char *g = strstr(p, "getcontentlength>");

			if (g && g < end) {
				sscanf(g + strlen("getcontentlength>"), "%31s", clen);
			}
		}

		if (single && n == 0) {
			single->is_dir = is_dir;
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

static int xiofs_do_propfind(struct xiofs_sb_info *sbi, const char *path,
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
		      path, sbi->hosthdr, depth);
	xiofs_add_auth(req, sizeof(req), &n, sbi);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	return xiofs_transact(sbi, req, n, NULL, 0, body, cap, len, meta);
}

int xiofs_http_getattr_path(struct xiofs_sb_info *sbi, const char *path,
			       struct xiofs_attr *attr)
{
	char *body;
	size_t len = 0;
	struct xiofs_http_resp meta;
	int err;

	memset(attr, 0, sizeof(*attr));
	mutex_lock(&sbi->io_lock);
	body = kvmalloc(XIOFS_MAX_DAV, GFP_KERNEL);
	if (!body) {
		mutex_unlock(&sbi->io_lock);
		return -ENOMEM;
	}
	err = xiofs_do_propfind(sbi, path, 0, body, XIOFS_MAX_DAV, &len, &meta);
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
			      path, sbi->hosthdr);
		xiofs_add_auth(req, sizeof(req), &n, sbi);
		n += snprintf(req + n, sizeof(req) - n, "\r\n");
		err = xiofs_transact(sbi, req, n, NULL, 0, NULL, 0, NULL, &meta);
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
	mutex_unlock(&sbi->io_lock);
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
	char *body;
	size_t len = 0;
	struct xiofs_http_resp meta;
	int err;

	*ents = NULL;
	*nents = 0;
	mutex_lock(&sbi->io_lock);
	body = kvmalloc(XIOFS_MAX_DAV, GFP_KERNEL);
	if (!body) {
		mutex_unlock(&sbi->io_lock);
		return -ENOMEM;
	}
	err = xiofs_do_propfind(sbi, XIOFS_I(dir)->remote_path, 1, body,
			      XIOFS_MAX_DAV, &len, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (!err)
		err = xiofs_parse_dav(body, len, ents, nents, NULL);
	kvfree(body);
	mutex_unlock(&sbi->io_lock);
	return err;
}

int xiofs_http_read(struct inode *inode, loff_t off, size_t len,
		       void *buf, size_t *nread)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	char req[768];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	*nread = 0;
	if (!len)
		return 0;
	mutex_lock(&sbi->io_lock);
	n += snprintf(req + n, sizeof(req) - n,
		      "GET %s HTTP/1.1\r\n"
		      "Host: %s\r\n"
		      "Range: bytes=%llu-%llu\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, sbi->hosthdr,
		      (unsigned long long)off,
		      (unsigned long long)off + len - 1);
	xiofs_add_auth(req, sizeof(req), &n, sbi);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(sbi, req, n, NULL, 0, buf, len, nread, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	mutex_unlock(&sbi->io_lock);
	return err;
}

int xiofs_http_write(struct inode *inode, loff_t off, size_t len,
			const void *buf, size_t *nwritten)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	char req[896];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	*nwritten = 0;
	if (!len)
		return 0;
	mutex_lock(&sbi->io_lock);
	n += snprintf(req + n, sizeof(req) - n,
		      "PATCH %s HTTP/1.1\r\n"
		      "Host: %s\r\n"
		      "Content-Type: application/octet-stream\r\n"
		      "Content-Range: bytes %llu-%llu/*\r\n"
		      "Content-Length: %zu\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, sbi->hosthdr,
		      (unsigned long long)off,
		      (unsigned long long)off + len - 1, len);
	xiofs_add_match(req, sizeof(req), &n, XIOFS_I(inode)->etag);
	xiofs_add_auth(req, sizeof(req), &n, sbi);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	err = xiofs_transact(sbi, req, n, buf, len, NULL, 0, NULL, &meta);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	if (!err) {
		*nwritten = len;
		if (meta.etag[0])
			strscpy(XIOFS_I(inode)->etag, meta.etag,
				sizeof(XIOFS_I(inode)->etag));
	}
	mutex_unlock(&sbi->io_lock);
	return err;
}

static int xiofs_put(struct xiofs_sb_info *sbi, const char *path,
		   const void *buf, size_t len, const char *if_match,
		   const char *if_none, struct xiofs_attr *attr)
{
	char req[896];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	n += snprintf(req + n, sizeof(req) - n,
		      "PUT %s HTTP/1.1\r\n"
		      "Host: %s\r\n"
		      "Content-Type: application/octet-stream\r\n"
		      "Content-Length: %zu\r\n"
		      "Connection: keep-alive\r\n",
		      path, sbi->hosthdr, len);
	xiofs_add_match(req, sizeof(req), &n, if_match);
	if (if_none && if_none[0])
		n += snprintf(req + n, sizeof(req) - n,
			      "If-None-Match: %s\r\n", if_none);
	xiofs_add_auth(req, sizeof(req), &n, sbi);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	mutex_lock(&sbi->io_lock);
	err = xiofs_transact(sbi, req, n, buf, len, NULL, 0, NULL, &meta);
	mutex_unlock(&sbi->io_lock);
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
	char req[512];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	n += snprintf(req + n, sizeof(req) - n,
		      "MKCOL %s HTTP/1.1\r\nHost: %s\r\n"
		      "Connection: keep-alive\r\n",
		      path, sbi->hosthdr);
	xiofs_add_auth(req, sizeof(req), &n, sbi);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	mutex_lock(&sbi->io_lock);
	err = xiofs_transact(sbi, req, n, NULL, 0, NULL, 0, NULL, &meta);
	mutex_unlock(&sbi->io_lock);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_unlink(struct inode *inode)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(inode->i_sb);
	char req[640];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	n += snprintf(req + n, sizeof(req) - n,
		      "DELETE %s HTTP/1.1\r\nHost: %s\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(inode)->remote_path, sbi->hosthdr);
	xiofs_add_match(req, sizeof(req), &n, XIOFS_I(inode)->etag);
	xiofs_add_auth(req, sizeof(req), &n, sbi);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	mutex_lock(&sbi->io_lock);
	err = xiofs_transact(sbi, req, n, NULL, 0, NULL, 0, NULL, &meta);
	mutex_unlock(&sbi->io_lock);
	if (!err)
		err = xiofs_http_status_to_errno(meta.status);
	return err;
}

int xiofs_http_rename(struct inode *old_inode, const char *new_path)
{
	struct xiofs_sb_info *sbi = XIOFS_SB(old_inode->i_sb);
	char req[1024];
	size_t n = 0;
	struct xiofs_http_resp meta;
	int err;

	n += snprintf(req + n, sizeof(req) - n,
		      "MOVE %s HTTP/1.1\r\nHost: %s\r\n"
		      "Destination: https://%s%s\r\n"
		      "Connection: keep-alive\r\n",
		      XIOFS_I(old_inode)->remote_path, sbi->hosthdr,
		      sbi->hosthdr, new_path);
	xiofs_add_match(req, sizeof(req), &n, XIOFS_I(old_inode)->etag);
	xiofs_add_auth(req, sizeof(req), &n, sbi);
	n += snprintf(req + n, sizeof(req) - n, "\r\n");
	mutex_lock(&sbi->io_lock);
	err = xiofs_transact(sbi, req, n, NULL, 0, NULL, 0, NULL, &meta);
	mutex_unlock(&sbi->io_lock);
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
