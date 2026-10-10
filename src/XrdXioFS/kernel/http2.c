// SPDX-License-Identifier: GPL-2.0
/*
 * Serial in-kernel HTTP/2 client. One stream at a time. Request builders
 * stay HTTP/1.1; this file parses that buffer and re-encodes as HPACK.
 *
 * Huffman tables: RFC 7541 Appendix B (golang.org/x/net/http2/hpack).
 */
#include <linux/errno.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/slab.h>
#include <linux/socket.h>
#include <linux/string.h>
#include <linux/types.h>

#include "xiofs.h"

#define H2_DATA			0
#define H2_HEADERS		1
#define H2_RST_STREAM		3
#define H2_SETTINGS		4
#define H2_PING			6
#define H2_GOAWAY		7
#define H2_WINDOW_UPDATE	8
#define H2_CONTINUATION		9

#define H2_END_STREAM		0x01
#define H2_END_HEADERS		0x04
#define H2_PADDED		0x08
#define H2_PRIORITY		0x20
#define H2_ACK			0x01

#define H2_MAX_FRAME		16384
#define H2_INIT_WIN		65535

static const u32 huff_code[256] = {
	0x1ff8, 0x7fffd8, 0xfffffe2, 0xfffffe3, 0xfffffe4, 0xfffffe5,
	0xfffffe6, 0xfffffe7, 0xfffffe8, 0xffffea, 0x3ffffffc, 0xfffffe9,
	0xfffffea, 0x3ffffffd, 0xfffffeb, 0xfffffec, 0xfffffed, 0xfffffee,
	0xfffffef, 0xffffff0, 0xffffff1, 0xffffff2, 0x3ffffffe, 0xffffff3,
	0xffffff4, 0xffffff5, 0xffffff6, 0xffffff7, 0xffffff8, 0xffffff9,
	0xffffffa, 0xffffffb, 0x14, 0x3f8, 0x3f9, 0xffa, 0x1ff9, 0x15,
	0xf8, 0x7fa, 0x3fa, 0x3fb, 0xf9, 0x7fb, 0xfa, 0x16, 0x17, 0x18,
	0x0, 0x1, 0x2, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x5c,
	0xfb, 0x7ffc, 0x20, 0xffb, 0x3fc, 0x1ffa, 0x21, 0x5d, 0x5e, 0x5f,
	0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a,
	0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0xfc, 0x73, 0xfd,
	0x1ffb, 0x7fff0, 0x1ffc, 0x3ffc, 0x22, 0x7ffd, 0x3, 0x23, 0x4, 0x24,
	0x5, 0x25, 0x26, 0x27, 0x6, 0x74, 0x75, 0x28, 0x29, 0x2a, 0x7, 0x2b,
	0x76, 0x2c, 0x8, 0x9, 0x2d, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7ffe,
	0x7fc, 0x3ffd, 0x1ffd, 0xffffffc, 0xfffe6, 0x3fffd2, 0xfffe7, 0xfffe8,
	0x3fffd3, 0x3fffd4, 0x3fffd5, 0x7fffd9, 0x3fffd6, 0x7fffda, 0x7fffdb,
	0x7fffdc, 0x7fffdd, 0x7fffde, 0xffffeb, 0x7fffdf, 0xffffec, 0xffffed,
	0x3fffd7, 0x7fffe0, 0xffffee, 0x7fffe1, 0x7fffe2, 0x7fffe3, 0x7fffe4,
	0x1fffdc, 0x3fffd8, 0x7fffe5, 0x3fffd9, 0x7fffe6, 0x7fffe7, 0xffffef,
	0x3fffda, 0x1fffdd, 0xfffe9, 0x3fffdb, 0x3fffdc, 0x7fffe8, 0x7fffe9,
	0x1fffde, 0x7fffea, 0x3fffdd, 0x3fffde, 0xfffff0, 0x1fffdf, 0x3fffdf,
	0x7fffeb, 0x7fffec, 0x1fffe0, 0x1fffe1, 0x3fffe0, 0x1fffe2, 0x7fffed,
	0x3fffe1, 0x7fffee, 0x7fffef, 0xfffea, 0x3fffe2, 0x3fffe3, 0x3fffe4,
	0x7ffff0, 0x3fffe5, 0x3fffe6, 0x7ffff1, 0x3ffffe0, 0x3ffffe1, 0xfffeb,
	0x7fff1, 0x3fffe7, 0x7ffff2, 0x3fffe8, 0x1ffffec, 0x3ffffe2, 0x3ffffe3,
	0x3ffffe4, 0x7ffffde, 0x7ffffdf, 0x3ffffe5, 0xfffff1, 0x1ffffed, 0x7fff2,
	0x1fffe3, 0x3ffffe6, 0x7ffffe0, 0x7ffffe1, 0x3ffffe7, 0x7ffffe2,
	0xfffff2, 0x1fffe4, 0x1fffe5, 0x3ffffe8, 0x3ffffe9, 0xffffffd,
	0x7ffffe3, 0x7ffffe4, 0x7ffffe5, 0xfffec, 0xfffff3, 0xfffed, 0x1fffe6,
	0x3fffe9, 0x1fffe7, 0x1fffe8, 0x7ffff3, 0x3fffea, 0x3fffeb, 0x1ffffee,
	0x1ffffef, 0xfffff4, 0xfffff5, 0x3ffffea, 0x7ffff4, 0x3ffffeb,
	0x7ffffe6, 0x3ffffec, 0x3ffffed, 0x7ffffe7, 0x7ffffe8, 0x7ffffe9,
	0x7ffffea, 0x7ffffeb, 0xffffffe, 0x7ffffec, 0x7ffffed, 0x7ffffee,
	0x7ffffef, 0x7fffff0, 0x3ffffee
};

static const u8 huff_len[256] = {
	13, 23, 28, 28, 28, 28, 28, 28, 28, 24, 30, 28, 28, 30, 28, 28,
	28, 28, 28, 28, 28, 28, 30, 28, 28, 28, 28, 28, 28, 28, 28, 28,
	6, 10, 10, 12, 13, 6, 8, 11, 10, 10, 8, 11, 8, 6, 6, 6,
	5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 7, 8, 15, 6, 12, 10,
	13, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7, 8, 7, 8, 13, 19, 13, 14, 6,
	15, 5, 6, 5, 6, 5, 6, 6, 6, 5, 7, 7, 6, 6, 6, 5,
	6, 7, 6, 5, 5, 6, 7, 7, 7, 7, 7, 15, 11, 14, 13, 28,
	20, 22, 20, 20, 22, 22, 22, 23, 22, 23, 23, 23, 23, 23, 24, 23,
	24, 24, 22, 23, 24, 23, 23, 23, 23, 21, 22, 23, 22, 23, 23, 24,
	22, 21, 20, 22, 22, 23, 23, 21, 23, 22, 22, 24, 21, 22, 23, 23,
	21, 21, 22, 21, 23, 22, 23, 23, 20, 22, 22, 22, 23, 22, 22, 23,
	26, 26, 20, 19, 22, 23, 22, 25, 26, 26, 26, 27, 27, 26, 24, 25,
	19, 21, 26, 27, 27, 26, 27, 24, 21, 21, 26, 26, 28, 27, 27, 27,
	20, 24, 20, 21, 22, 21, 21, 23, 22, 22, 25, 25, 24, 24, 26, 23,
	26, 27, 26, 26, 27, 27, 27, 27, 27, 28, 27, 27, 27, 27, 27, 26
};

void xiofs_h2_reset(struct xiofs_conn *c)
{
	c->h2_ready = false;
	c->h2_next_sid = 1;
	c->h2_send_win = H2_INIT_WIN;
}

static u32 be24(const u8 *p)
{
	return ((u32)p[0] << 16) | ((u32)p[1] << 8) | p[2];
}

static void put_be24(u8 *p, u32 v)
{
	p[0] = (v >> 16) & 0xff;
	p[1] = (v >> 8) & 0xff;
	p[2] = v & 0xff;
}

static void put_be32(u8 *p, u32 v)
{
	p[0] = (v >> 24) & 0xff;
	p[1] = (v >> 16) & 0xff;
	p[2] = (v >> 8) & 0xff;
	p[3] = v & 0xff;
}

static u32 get_be32(const u8 *p)
{
	return ((u32)p[0] << 24) | ((u32)p[1] << 16) | ((u32)p[2] << 8) | p[3];
}

static int h2_send_frame(struct socket *sock, u8 type, u8 flags, u32 sid,
			 const void *payload, size_t len)
{
	u8 hdr[9];

	if (len > H2_MAX_FRAME)
		return -E2BIG;
	put_be24(hdr, (u32)len);
	hdr[3] = type;
	hdr[4] = flags;
	put_be32(hdr + 5, sid & 0x7fffffff);
	if (xiofs_sock_send(sock, hdr, 9))
		return -EIO;
	if (len && xiofs_sock_send(sock, payload, len))
		return -EIO;
	return 0;
}

static int h2_recv_frame(struct socket *sock, u8 *type, u8 *flags, u32 *sid,
			 u8 **payload, size_t *len)
{
	u8 hdr[9];
	int n;
	u32 plen;
	u8 *buf;

	n = xiofs_sock_recv(sock, hdr, 9);
	if (n < 0)
		return n;
	if (n != 9)
		return -ECONNRESET;
	plen = be24(hdr);
	if (plen > (1u << 20))
		return -EPROTO;
	*type = hdr[3];
	*flags = hdr[4];
	*sid = get_be32(hdr + 5) & 0x7fffffff;
	*len = plen;
	*payload = NULL;
	if (!plen)
		return 0;
	buf = kvmalloc(plen, GFP_KERNEL);
	if (!buf)
		return -ENOMEM;
	n = xiofs_sock_recv(sock, buf, plen);
	if (n < 0) {
		kvfree(buf);
		return n;
	}
	if ((size_t)n != plen) {
		kvfree(buf);
		return -ECONNRESET;
	}
	*payload = buf;
	return 0;
}

static int huff_decode(const u8 *in, size_t inlen, char *out, size_t outsz,
		       size_t *outlen)
{
	u64 acc = 0;
	int nbits = 0;
	size_t oi = 0, i;

	*outlen = 0;
	for (i = 0; i < inlen; i++) {
		int matched;

		acc = (acc << 8) | in[i];
		nbits += 8;
		do {
			int s;

			matched = 0;
			for (s = 0; s < 256; s++) {
				u8 bl = huff_len[s];
				u32 code, got;

				if (nbits < bl)
					continue;
				code = huff_code[s];
				got = (u32)((acc >> (nbits - bl)) &
					    (bl == 32 ? 0xffffffffu :
					     ((1u << bl) - 1)));
				if (got != code)
					continue;
				if (oi >= outsz)
					return -ENOSPC;
				out[oi++] = (char)s;
				nbits -= bl;
				if (nbits)
					acc &= ((1ull << nbits) - 1);
				else
					acc = 0;
				matched = 1;
				break;
			}
		} while (matched);
	}
	if (nbits > 7)
		return -EPROTO;
	if (nbits) {
		u32 pad = (u32)((acc) & ((1u << nbits) - 1));
		u32 ones = (1u << nbits) - 1;

		if (pad != ones)
			return -EPROTO;
	}
	*outlen = oi;
	return 0;
}

static int hpack_int(const u8 **pp, const u8 *end, int prefix, u32 *out)
{
	u32 n;
	int i, max = (1 << prefix) - 1;
	const u8 *p = *pp;

	if (p >= end)
		return -EPROTO;
	n = *p & max;
	p++;
	if (n < (u32)max) {
		*out = n;
		*pp = p;
		return 0;
	}
	for (i = 0; i < 5 && p < end; i++) {
		u8 b = *p++;

		n += (u32)(b & 0x7f) << (7 * i);
		if (!(b & 0x80)) {
			*out = n;
			*pp = p;
			return 0;
		}
	}
	return -EPROTO;
}

static int hpack_string(const u8 **pp, const u8 *end, char *out, size_t outsz,
			size_t *outlen)
{
	const u8 *p = *pp;
	u32 slen;
	int huff, err;

	if (p >= end)
		return -EPROTO;
	huff = (*p & 0x80) != 0;
	err = hpack_int(&p, end, 7, &slen);
	if (err)
		return err;
	if (p + slen > end)
		return -EPROTO;
	if (huff) {
		err = huff_decode(p, slen, out, outsz - 1, outlen);
		if (err)
			return err;
	} else {
		if (slen >= outsz)
			return -ENOSPC;
		memcpy(out, p, slen);
		*outlen = slen;
	}
	out[*outlen] = 0;
	p += slen;
	*pp = p;
	return 0;
}

static void apply_static_status(u32 idx, int *status)
{
	switch (idx) {
	case 8:
		*status = 200;
		break;
	case 9:
		*status = 204;
		break;
	case 10:
		*status = 206;
		break;
	case 11:
		*status = 304;
		break;
	case 12:
		*status = 400;
		break;
	case 13:
		*status = 404;
		break;
	case 14:
		*status = 500;
		break;
	default:
		break;
	}
}

static const char *static_name(u32 idx)
{
	switch (idx) {
	case 8:
		return ":status";
	case 28:
		return "content-length";
	case 34:
		return "etag";
	default:
		return NULL;
	}
}

static int ncaseeq(const char *a, const char *b)
{
	for (; *a && *b; a++, b++) {
		char ca = *a, cb = *b;

		if (ca >= 'A' && ca <= 'Z')
			ca += 32;
		if (cb >= 'A' && cb <= 'Z')
			cb += 32;
		if (ca != cb)
			return 0;
	}
	return *a == *b;
}

static void take_header(const char *name, const char *value,
			struct xiofs_http_resp *meta)
{
	if (ncaseeq(name, ":status"))
		kstrtoint(value, 10, &meta->status);
	else if (ncaseeq(name, "etag"))
		strscpy(meta->etag, value, sizeof(meta->etag));
	else if (ncaseeq(name, "content-length"))
		kstrtoll(value, 10, &meta->content_length);
}

static int hpack_decode_headers(const u8 *buf, size_t len,
				struct xiofs_http_resp *meta)
{
	const u8 *p = buf, *end = buf + len;

	while (p < end) {
		u8 b = *p;
		int err;
		u32 idx;
		char name[64], val[256];
		size_t nl = 0, vl = 0;
		const char *sn;

		if (b & 0x80) {
			err = hpack_int(&p, end, 7, &idx);
			if (err)
				return err;
			apply_static_status(idx, &meta->status);
			continue;
		}
		if ((b & 0xc0) == 0x40) {
			err = hpack_int(&p, end, 6, &idx);
			if (err)
				return err;
		} else if ((b & 0xf0) == 0x00 || (b & 0xf0) == 0x10) {
			err = hpack_int(&p, end, 4, &idx);
			if (err)
				return err;
		} else if ((b & 0xe0) == 0x20) {
			err = hpack_int(&p, end, 5, &idx);
			if (err)
				return err;
			continue;
		} else {
			return -EPROTO;
		}
		if (idx == 0) {
			err = hpack_string(&p, end, name, sizeof(name), &nl);
			if (err)
				return err;
		} else {
			sn = static_name(idx);
			if (sn)
				strscpy(name, sn, sizeof(name));
			else
				name[0] = 0;
		}
		err = hpack_string(&p, end, val, sizeof(val), &vl);
		if (err)
			return err;
		if (name[0])
			take_header(name, val, meta);
	}
	return 0;
}

static int hpack_enc_int(u8 *out, size_t cap, size_t *n, int prefix, u32 v)
{
	u32 max = (1u << prefix) - 1;

	if (*n >= cap)
		return -ENOSPC;
	if (v < max) {
		out[*n] = (out[*n] & (u8)(~max & 0xff)) | (u8)v;
		(*n)++;
		return 0;
	}
	out[*n] = (out[*n] & (u8)(~max & 0xff)) | (u8)max;
	(*n)++;
	v -= max;
	while (v >= 128) {
		if (*n >= cap)
			return -ENOSPC;
		out[(*n)++] = (u8)((v & 0x7f) | 0x80);
		v >>= 7;
	}
	if (*n >= cap)
		return -ENOSPC;
	out[(*n)++] = (u8)v;
	return 0;
}

static int hpack_enc_str(u8 *out, size_t cap, size_t *n, const char *s)
{
	size_t sl = strlen(s);

	if (*n >= cap)
		return -ENOSPC;
	out[*n] = 0;
	if (hpack_enc_int(out, cap, n, 7, (u32)sl))
		return -ENOSPC;
	if (*n + sl > cap)
		return -ENOSPC;
	memcpy(out + *n, s, sl);
	*n += sl;
	return 0;
}

static int hpack_lit(u8 *out, size_t cap, size_t *n, const char *name,
		     const char *val)
{
	if (*n >= cap)
		return -ENOSPC;
	out[*n] = 0x00;
	if (hpack_enc_int(out, cap, n, 4, 0))
		return -ENOSPC;
	if (hpack_enc_str(out, cap, n, name))
		return -ENOSPC;
	return hpack_enc_str(out, cap, n, val);
}

static int skip_hdr(const char *name)
{
	return ncaseeq(name, "connection") ||
	       ncaseeq(name, "keep-alive") ||
	       ncaseeq(name, "proxy-connection") ||
	       ncaseeq(name, "transfer-encoding") ||
	       ncaseeq(name, "upgrade") ||
	       ncaseeq(name, "host") ||
	       ncaseeq(name, "content-length");
}

static int http1_to_hpack(struct xiofs_conn *c, const char *req,
			  size_t reqlen, u8 *out, size_t cap, size_t *n)
{
	char method[32] = {}, path[XIOFS_PATH_MAX] = {};
	const char *p, *eol, *col;
	char *val;
	int err;

	*n = 0;
	if (sscanf(req, "%31s %1023s", method, path) != 2)
		return -EINVAL;
	err = hpack_lit(out, cap, n, ":method", method);
	if (err)
		return err;
	err = hpack_lit(out, cap, n, ":path", path);
	if (err)
		return err;
	err = hpack_lit(out, cap, n, ":scheme", c->tls ? "https" : "http");
	if (err)
		return err;
	err = hpack_lit(out, cap, n, ":authority", c->sbi->hosthdr);
	if (err)
		return err;

	p = strstr(req, "\r\n");
	if (!p)
		return 0;
	p += 2;
	/* Authorization: Bearer <JWT> alone can exceed 4 KiB. */
	val = kmalloc(XIOFS_MAX_HDR, GFP_KERNEL);
	if (!val)
		return -ENOMEM;
	while (p < req + reqlen && *p) {
		char name[64];
		size_t nl, vl;

		if (p[0] == '\r' && p[1] == '\n')
			break;
		eol = strstr(p, "\r\n");
		if (!eol)
			break;
		col = memchr(p, ':', (size_t)(eol - p));
		if (!col) {
			p = eol + 2;
			continue;
		}
		nl = (size_t)(col - p);
		if (nl >= sizeof(name))
			nl = sizeof(name) - 1;
		memcpy(name, p, nl);
		name[nl] = 0;
		col++;
		while (col < eol && (*col == ' ' || *col == '\t'))
			col++;
		vl = (size_t)(eol - col);
		if (vl >= XIOFS_MAX_HDR)
			vl = XIOFS_MAX_HDR - 1;
		memcpy(val, col, vl);
		val[vl] = 0;
		if (!skip_hdr(name)) {
			err = hpack_lit(out, cap, n, name, val);
			if (err) {
				kfree(val);
				return err;
			}
		}
		p = eol + 2;
	}
	kfree(val);
	return 0;
}

static int h2_window_update(struct socket *sock, u32 sid, u32 incr)
{
	u8 payload[4];

	put_be32(payload, incr & 0x7fffffff);
	return h2_send_frame(sock, H2_WINDOW_UPDATE, 0, sid, payload, 4);
}

static int h2_preface(struct xiofs_conn *c)
{
	int err, saw = 0;
	const u32 init_win = 1u << 20;

	if (c->h2_ready)
		return 0;
	err = xiofs_sock_send(c->sock, XIOFS_H2_PREFACE, XIOFS_H2_PREFACE_LEN);
	if (err)
		return err;
	{
		u8 pl[12];

		pl[0] = 0;
		pl[1] = 0x04;
		put_be32(pl + 2, init_win);
		pl[6] = 0;
		pl[7] = 0x03;
		put_be32(pl + 8, 1);
		err = h2_send_frame(c->sock, H2_SETTINGS, 0, 0, pl, 12);
	}
	if (err)
		return err;
	c->h2_send_win = H2_INIT_WIN;
	c->h2_next_sid = c->h2_next_sid ? c->h2_next_sid : 1;

	while (saw < 8) {
		u8 type, flags, *pay = NULL;
		u32 sid;
		size_t plen = 0;

		err = h2_recv_frame(c->sock, &type, &flags, &sid, &pay, &plen);
		if (err)
			return err;
		if (type == H2_SETTINGS && !(flags & H2_ACK)) {
			err = h2_send_frame(c->sock, H2_SETTINGS, H2_ACK, 0,
					    NULL, 0);
			kvfree(pay);
			if (err)
				return err;
			c->h2_ready = true;
			return 0;
		}
		if (type == H2_WINDOW_UPDATE && plen >= 4)
			c->h2_send_win += get_be32(pay) & 0x7fffffff;
		if (type == H2_PING && !(flags & H2_ACK) && plen == 8) {
			err = h2_send_frame(c->sock, H2_PING, H2_ACK, 0, pay, 8);
			kvfree(pay);
			if (err)
				return err;
			pay = NULL;
		}
		kvfree(pay);
		saw++;
	}
	return -EPROTO;
}

int xiofs_h2_transact(struct xiofs_conn *c, const char *req, size_t reqlen,
		      const void *body, size_t bodylen, void *out, size_t outcap,
		      size_t *outlen, struct xiofs_http_resp *meta)
{
	u8 *hpack;
	size_t hlen = 0;
	u32 sid;
	int err;
	size_t got = 0;
	bool end = false, head_only;
	u8 hflags;
	size_t alloc_max = meta->alloc_max;

	memset(meta, 0, sizeof(*meta));
	meta->alloc_max = alloc_max;
	meta->content_length = -1;
	if (outlen)
		*outlen = 0;

	err = xiofs_conn_wait(c);
	if (err)
		return err;
	err = h2_preface(c);
	if (err)
		return err;

	/* Headers (incl. a bearer token) stay below one 16 KiB frame. */
	hpack = kmalloc(XIOFS_MAX_HDR + 1024, GFP_KERNEL);
	if (!hpack)
		return -ENOMEM;
	err = http1_to_hpack(c, req, reqlen, hpack, XIOFS_MAX_HDR + 1024, &hlen);
	if (err) {
		kfree(hpack);
		return err;
	}

	sid = c->h2_next_sid;
	if (sid & 1)
		c->h2_next_sid = sid + 2;
	else
		c->h2_next_sid = sid + 1;

	hflags = H2_END_HEADERS;
	if (!bodylen)
		hflags |= H2_END_STREAM;
	err = h2_send_frame(c->sock, H2_HEADERS, hflags, sid, hpack, hlen);
	kfree(hpack);
	if (err)
		return err;

	if (bodylen) {
		const u8 *bp = body;
		size_t left = bodylen;

		while (left) {
			size_t chunk = min_t(size_t, left, H2_MAX_FRAME);
			u8 dflags = (chunk == left) ? H2_END_STREAM : 0;

			if (c->h2_send_win < chunk)
				chunk = c->h2_send_win ? c->h2_send_win : 1;
			err = h2_send_frame(c->sock, H2_DATA, dflags, sid, bp,
					    chunk);
			if (err)
				return err;
			c->h2_send_win -= (u32)chunk;
			bp += chunk;
			left -= chunk;
		}
	}

	head_only = !strncmp(req, "HEAD ", 5);

	while (!end) {
		u8 type, flags, *pay = NULL;
		u32 fsid;
		size_t plen = 0;

		err = h2_recv_frame(c->sock, &type, &flags, &fsid, &pay, &plen);
		if (err)
			return err;
		if (type == H2_SETTINGS && !(flags & H2_ACK)) {
			kvfree(pay);
			err = h2_send_frame(c->sock, H2_SETTINGS, H2_ACK, 0,
					    NULL, 0);
			if (err)
				return err;
			continue;
		}
		if (type == H2_PING && !(flags & H2_ACK) && plen == 8) {
			err = h2_send_frame(c->sock, H2_PING, H2_ACK, 0, pay, 8);
			kvfree(pay);
			if (err)
				return err;
			continue;
		}
		if (type == H2_WINDOW_UPDATE && plen >= 4) {
			c->h2_send_win += get_be32(pay) & 0x7fffffff;
			kvfree(pay);
			continue;
		}
		if (type == H2_GOAWAY) {
			kvfree(pay);
			return -ECONNRESET;
		}
		if (type == H2_RST_STREAM) {
			kvfree(pay);
			return -ECONNRESET;
		}
		if (fsid != sid) {
			kvfree(pay);
			continue;
		}
		if (type == H2_HEADERS || type == H2_CONTINUATION) {
			u8 *hp = pay;
			size_t hl = plen;

			if (type == H2_HEADERS && (flags & H2_PADDED) && hl) {
				u8 pad = hp[0];

				hp++;
				hl--;
				if (hl < pad) {
					kvfree(pay);
					return -EPROTO;
				}
				hl -= pad;
			}
			if (type == H2_HEADERS && (flags & H2_PRIORITY) &&
			    hl >= 5) {
				hp += 5;
				hl -= 5;
			}
			err = hpack_decode_headers(hp, hl, meta);
			kvfree(pay);
			if (err)
				return err;
			if (flags & H2_END_STREAM)
				end = true;
			if (head_only && (flags & H2_END_HEADERS))
				end = true;
			continue;
		}
		if (type == H2_DATA) {
			u8 *dp = pay;
			size_t dl = plen;

			if (flags & H2_PADDED && dl) {
				u8 pad = dp[0];

				dp++;
				dl--;
				if (dl < pad) {
					kvfree(pay);
					return -EPROTO;
				}
				dl -= pad;
			}
			if (out && outcap && got < outcap && dl) {
				size_t take = min(dl, outcap - got);

				memcpy((char *)out + got, dp, take);
				got += take;
			} else if (!out && meta->alloc_max && dl) {
				/* Growable body (directory listings). */
				err = xiofs_resp_body_grow(meta, got + dl);
				if (err) {
					kvfree(pay);
					return err;
				}
				memcpy(meta->body + got, dp, dl);
				got += dl;
				meta->body_len = got;
			}
			if (dl) {
				err = h2_window_update(c->sock, 0, (u32)dl);
				if (!err)
					err = h2_window_update(c->sock, sid,
							       (u32)dl);
				if (err) {
					kvfree(pay);
					return err;
				}
			}
			kvfree(pay);
			if (flags & H2_END_STREAM)
				end = true;
			continue;
		}
		kvfree(pay);
	}
	if (outlen)
		*outlen = got;
	if (!meta->status)
		meta->status = 200;
	if (meta->content_length < 0)
		meta->content_length = (long long)got;
	return 0;
}
