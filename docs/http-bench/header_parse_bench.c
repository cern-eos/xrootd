/* Microbench: llhttp vs a BuffgetLine/parseLine-style scanner.
 * Compile with vendored llhttp (see run.sh). */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>

#include "llhttp.h"

static uint64_t nsec(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Same fields XrdHttp1Session collects before XrdHttpReq. */
struct parsed {
  char method[16];
  char url[512];
  int nheaders;
  char name[48][64];
  char value[48][384];
};

struct llctx {
  struct parsed *out;
  char field[64];
  size_t flen;
  char value[384];
  size_t vlen;
};

static int on_method(llhttp_t *p, const char *at, size_t n)
{
  struct llctx *c = p->data;
  size_t have = strlen(c->out->method);
  size_t m = n < sizeof(c->out->method) - 1 - have ? n : sizeof(c->out->method) - 1 - have;
  memcpy(c->out->method + have, at, m);
  c->out->method[have + m] = 0;
  return 0;
}

static int on_url(llhttp_t *p, const char *at, size_t n)
{
  struct llctx *c = p->data;
  size_t have = strlen(c->out->url);
  size_t m = n < sizeof(c->out->url) - 1 - have ? n : sizeof(c->out->url) - 1 - have;
  memcpy(c->out->url + have, at, m);
  c->out->url[have + m] = 0;
  return 0;
}

static int on_header_field(llhttp_t *p, const char *at, size_t n)
{
  struct llctx *c = p->data;
  if (c->flen + n >= sizeof(c->field))
    return -1;
  memcpy(c->field + c->flen, at, n);
  c->flen += n;
  return 0;
}

static int on_header_value(llhttp_t *p, const char *at, size_t n)
{
  struct llctx *c = p->data;
  if (c->vlen + n >= sizeof(c->value))
    return -1;
  memcpy(c->value + c->vlen, at, n);
  c->vlen += n;
  return 0;
}

static int on_header_value_complete(llhttp_t *p)
{
  struct llctx *c = p->data;
  if (c->out->nheaders >= 48)
    return -1;
  int i = c->out->nheaders++;
  size_t fn = c->flen < 63 ? c->flen : 63;
  memcpy(c->out->name[i], c->field, fn);
  c->out->name[i][fn] = 0;
  size_t vn = c->vlen < 383 ? c->vlen : 383;
  memcpy(c->out->value[i], c->value, vn);
  c->out->value[i][vn] = 0;
  c->flen = c->vlen = 0;
  return 0;
}

static int on_headers_complete(llhttp_t *p)
{
  (void)p;
  return 1; /* pause; body is not part of this bench */
}

static llhttp_settings_t g_settings;
static int g_inited;

static void llhttp_setup(void)
{
  if (g_inited)
    return;
  llhttp_settings_init(&g_settings);
  g_settings.on_method = on_method;
  g_settings.on_url = on_url;
  g_settings.on_header_field = on_header_field;
  g_settings.on_header_value = on_header_value;
  g_settings.on_header_value_complete = on_header_value_complete;
  g_settings.on_headers_complete = on_headers_complete;
  g_inited = 1;
}

static int parse_llhttp(const char *buf, size_t n, size_t chunk, struct parsed *out)
{
  struct llctx ctx;
  memset(out, 0, sizeof(*out));
  memset(&ctx, 0, sizeof(ctx));
  ctx.out = out;
  llhttp_t parser;
  llhttp_init(&parser, HTTP_REQUEST, &g_settings);
  parser.data = &ctx;
  size_t off = 0;
  while (off < n) {
    size_t take = chunk && chunk < n - off ? chunk : n - off;
    llhttp_errno_t err = llhttp_execute(&parser, buf + off, take);
    if (err == HPE_PAUSED)
      return 0;
    if (err != HPE_OK)
      return -1;
    off += take;
  }
  return out->nheaders > 0 ? 0 : -1;
}

/* Reused scratch, like BuffgetLine copying into a dest string. */
static char g_scratch[65536];

static int parse_naive(const char *buf, size_t n, size_t chunk, struct parsed *out)
{
  (void)chunk; /* line scanner needs a full line anyway; copy whole message */
  if (n >= sizeof(g_scratch))
    return -1;
  memcpy(g_scratch, buf, n);
  g_scratch[n] = 0;
  memset(out, 0, sizeof(*out));

  char *line = g_scratch;
  int first = 1;
  for (;;) {
    char *nl = strchr(line, '\n');
    if (!nl)
      break;
    *nl = 0;
    if (nl > line && nl[-1] == '\r')
      nl[-1] = 0;
    if (first) {
      char *sp1 = strchr(line, ' ');
      if (!sp1)
        return -1;
      *sp1 = 0;
      strncpy(out->method, line, sizeof(out->method) - 1);
      char *url = sp1 + 1;
      char *sp2 = strchr(url, ' ');
      if (!sp2)
        return -1;
      *sp2 = 0;
      strncpy(out->url, url, sizeof(out->url) - 1);
      first = 0;
    } else if (line[0] == 0) {
      break;
    } else {
      char *col = strchr(line, ':');
      if (!col)
        return -1;
      *col = 0;
      char *val = col + 1;
      while (*val == ' ')
        val++;
      if (out->nheaders >= 48)
        return -1;
      int i = out->nheaders++;
      strncpy(out->name[i], line, sizeof(out->name[0]) - 1);
      strncpy(out->value[i], val, sizeof(out->value[0]) - 1);
    }
    line = nl + 1;
  }
  return first ? -1 : 0;
}

static void make_typical(char *buf, size_t cap, size_t *n)
{
  int w = snprintf(buf, cap,
                   "GET /data/run12345/file.root HTTP/1.1\r\n"
                   "Host: storage.example.org:1094\r\n"
                   "User-Agent: curl/8.7.1\r\n"
                   "Accept: */*\r\n"
                   "Range: bytes=0-1048575\r\n"
                   "Connection: Keep-Alive\r\n"
                   "Authorization: Bearer "
                   "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.aaaaaaaaaaaaaaaa\r\n"
                   "Want-Digest: adler32,crc32c\r\n"
                   "Accept-Encoding: identity\r\n"
                   "\r\n");
  *n = (size_t)w;
}

static void make_heavy(char *buf, size_t cap, size_t *n)
{
  char *p = buf;
  size_t left = cap;
  int w = snprintf(p, left,
                   "GET /store/mc/RunII/AOD/file-0001.root HTTP/1.1\r\n"
                   "Host: eos.cern.ch\r\n"
                   "User-Agent: davix/0.8\r\n");
  p += w;
  left -= (size_t)w;
  for (int i = 0; i < 24; i++) {
    w = snprintf(p, left, "X-Meta-%02d: value-for-header-%02d-padding-xxxxxxxx\r\n", i, i);
    p += w;
    left -= (size_t)w;
  }
  w = snprintf(p, left, "\r\n");
  p += w;
  *n = (size_t)(p - buf);
}

static void bench(const char *name,
                  int (*fn)(const char *, size_t, size_t, struct parsed *),
                  const char *buf, size_t n, size_t chunk, int iters)
{
  struct parsed out;
  if (fn(buf, n, chunk, &out) != 0) {
    fprintf(stderr, "parse failed: %s\n", name);
    exit(1);
  }
  for (int i = 0; i < 200; i++)
    fn(buf, n, chunk, &out);
  uint64_t t0 = nsec();
  for (int i = 0; i < iters; i++)
    fn(buf, n, chunk, &out);
  uint64_t dt = nsec() - t0;
  double ns = (double)dt / iters;
  double mps = 1e9 / ns / 1e6;
  printf("%-28s  %6zu B  chunk=%-4zu  %7.1f ns/req  %6.2f Mreq/s  (%s %s, %d hdrs)\n",
         name, n, chunk ? chunk : n, ns, mps, out.method, out.url, out.nheaders);
}

int main(void)
{
  llhttp_setup();
  char typical[2048], heavy[4096];
  size_t nt, nh;
  make_typical(typical, sizeof(typical), &nt);
  make_heavy(heavy, sizeof(heavy), &nh);

  const int iters = 200000;
  printf("Header parse microbench  (this machine, %d iterations, -O2)\n", iters);
  printf("naive = BuffgetLine + split-on-colon (master style)\n");
  printf("llhttp = Node.js state machine used by XrdHttp1Session\n\n");

  bench("naive typical GET", parse_naive, typical, nt, 0, iters);
  bench("llhttp typical GET", parse_llhttp, typical, nt, 0, iters);
  bench("llhttp typical 16B chunks", parse_llhttp, typical, nt, 16, iters);

  bench("naive 24 extra headers", parse_naive, heavy, nh, 0, iters);
  bench("llhttp 24 extra headers", parse_llhttp, heavy, nh, 0, iters);
  bench("llhttp heavy 16B chunks", parse_llhttp, heavy, nh, 16, iters);
  return 0;
}
