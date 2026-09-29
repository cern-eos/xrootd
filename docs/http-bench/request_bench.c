/* Sequential keepalive HEAD/GET: HTTP/1.1 vs HTTP/2 (h2c prior knowledge).
 * Cleartext on purpose: Apple curl/SecureTransport cannot handshake the
 * OpenSSL 3 server, and TLS 1.3 + SO_RCVTIMEO makes HTTPS stall 10s/req
 * on this Mac. Both protocols hit the same XrdHttpReq + Bridge path. */
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>
#include <stdint.h>

#include <nghttp2/nghttp2.h>

static uint64_t nsec(void)
{
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void die(const char *m)
{
  fprintf(stderr, "%s (errno=%d %s)\n", m, errno, strerror(errno));
  exit(1);
}

struct urlparts {
  char host[128];
  char path[512];
  int port;
};

static void parse_url(const char *url, struct urlparts *u)
{
  memset(u, 0, sizeof(*u));
  const char *p = url;
  if (!strncmp(p, "http://", 7))
    p += 7;
  else
    die("URL must be http://host:port/path");
  const char *slash = strchr(p, '/');
  const char *colon = strchr(p, ':');
  if (!slash || !colon || colon > slash)
    die("URL must be http://host:port/path");
  size_t hl = (size_t)(colon - p);
  if (hl >= sizeof(u->host))
    die("host too long");
  memcpy(u->host, p, hl);
  u->port = atoi(colon + 1);
  strncpy(u->path, slash, sizeof(u->path) - 1);
}

static int tcp_connect(const char *host, int port)
{
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    die("socket");
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  struct timeval tv;
  tv.tv_sec = 30;
  tv.tv_usec = 0;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
  struct sockaddr_in a;
  memset(&a, 0, sizeof(a));
  a.sin_family = AF_INET;
  a.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, host, &a.sin_addr) != 1)
    die("inet_pton");
  if (connect(fd, (struct sockaddr *)&a, sizeof(a)) < 0)
    die("connect");
  return fd;
}

static int write_all(int fd, const char *p, size_t n)
{
  size_t off = 0;
  while (off < n) {
    ssize_t w = write(fd, p + off, n - off);
    if (w <= 0)
      return -1;
    off += (size_t)w;
  }
  return 0;
}

static int read_h1(int fd, int expect_body, int64_t *body_out)
{
  char buf[16384];
  size_t have = 0;
  char *eoh = NULL;
  while (!eoh) {
    if (have >= sizeof(buf) - 1)
      return -1;
    ssize_t n = read(fd, buf + have, sizeof(buf) - 1 - have);
    if (n <= 0)
      return -1;
    have += (size_t)n;
    buf[have] = 0;
    eoh = strstr(buf, "\r\n\r\n");
  }
  if (strncmp(buf, "HTTP/1.1 ", 9) != 0 && strncmp(buf, "HTTP/1.0 ", 9) != 0)
    return -1;
  int code = atoi(buf + 9);
  if (code < 200 || code >= 400)
    return -1;
  int64_t clen = -1;
  for (char *p = buf; p < eoh;) {
    if (!strncasecmp(p, "Content-Length:", 15))
      clen = strtoll(p + 15, NULL, 10);
    char *nl = strstr(p, "\r\n");
    if (!nl)
      break;
    p = nl + 2;
  }
  int64_t extra = (int64_t)(have - (size_t)(eoh + 4 - buf));
  if (!expect_body) {
    *body_out = 0;
    return extra == 0 ? 0 : -1;
  }
  if (clen < 0)
    return -1;
  int64_t got = extra;
  while (got < clen) {
    char tmp[16384];
    size_t want = (size_t)(clen - got);
    if (want > sizeof(tmp))
      want = sizeof(tmp);
    ssize_t n = read(fd, tmp, want);
    if (n <= 0)
      return -1;
    got += n;
  }
  *body_out = clen;
  return 0;
}

static void report(const char *label, const char *ver, int nops, uint64_t dt, int64_t last_body)
{
  double us = (double)dt / nops / 1000.0;
  double rps = 1e9 / ((double)dt / nops);
  printf("%-28s  %-7s  %6d ops  %8.1f us/op  %8.0f ops/s  last body %lld B\n",
         label, ver, nops, us, rps, (long long)last_body);
  fflush(stdout);
}

static void run_h1(const char *label, const struct urlparts *u, const char *method,
                   int expect_body, int nops)
{
  int fd = tcp_connect(u->host, u->port);
  char req[1024];
  int reqn = snprintf(req, sizeof(req),
                     "%s %s HTTP/1.1\r\nHost: %s:%d\r\nConnection: keep-alive\r\n"
                     "User-Agent: xrdhttp-bench/1\r\n\r\n",
                     method, u->path, u->host, u->port);
  int64_t last_body = 0;
  for (int i = 0; i < 20; i++) {
    if (write_all(fd, req, (size_t)reqn) < 0 || read_h1(fd, expect_body, &last_body) < 0)
      die("HTTP/1.1 warmup");
  }
  uint64_t t0 = nsec();
  for (int i = 0; i < nops; i++) {
    if (write_all(fd, req, (size_t)reqn) < 0 || read_h1(fd, expect_body, &last_body) < 0)
      die("HTTP/1.1 request");
  }
  uint64_t dt = nsec() - t0;
  close(fd);
  report(label, "HTTP/1.1", nops, dt, last_body);
}

struct h2ctx {
  int fd;
  int done;
  int32_t sid;
  int64_t body;
};

static nghttp2_ssize h2_send(nghttp2_session *s, const uint8_t *data, size_t len, int flags, void *user)
{
  (void)s;
  (void)flags;
  struct h2ctx *c = user;
  ssize_t n = write(c->fd, data, len);
  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return NGHTTP2_ERR_WOULDBLOCK;
    return NGHTTP2_ERR_CALLBACK_FAILURE;
  }
  return n;
}

static int h2_stream_close(nghttp2_session *s, int32_t sid, uint32_t err, void *user)
{
  (void)s;
  (void)err;
  struct h2ctx *c = user;
  if (sid == c->sid)
    c->done = 1;
  return 0;
}

static int h2_data(nghttp2_session *s, uint8_t flags, int32_t sid, const uint8_t *data, size_t len, void *user)
{
  (void)s;
  (void)flags;
  (void)data;
  struct h2ctx *c = user;
  if (sid == c->sid)
    c->body += (int64_t)len;
  return 0;
}

static int h2_one(nghttp2_session *session, struct h2ctx *c, const char *method,
                   const char *path, const char *authority)
{
  char mbuf[16], pbuf[512], abuf[160];
  strncpy(mbuf, method, sizeof(mbuf) - 1);
  strncpy(pbuf, path, sizeof(pbuf) - 1);
  strncpy(abuf, authority, sizeof(abuf) - 1);
  nghttp2_nv hdrs[] = {
      {(uint8_t *)":method", (uint8_t *)mbuf, 7, strlen(mbuf), NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":path", (uint8_t *)pbuf, 5, strlen(pbuf), NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":scheme", (uint8_t *)"http", 7, 4, NGHTTP2_NV_FLAG_NONE},
      {(uint8_t *)":authority", (uint8_t *)abuf, 10, strlen(abuf), NGHTTP2_NV_FLAG_NONE},
  };
  c->done = 0;
  c->body = 0;
  c->sid = nghttp2_submit_request(session, NULL, hdrs, 4, NULL, NULL);
  if (c->sid < 0)
    return -1;
  if (nghttp2_session_send(session) != 0)
    return -1;
  uint8_t rbuf[16384];
  while (!c->done) {
    ssize_t n = read(c->fd, rbuf, sizeof(rbuf));
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return -1;
    nghttp2_ssize rv = nghttp2_session_mem_recv2(session, rbuf, (size_t)n);
    if (rv < 0)
      return -1;
    if (nghttp2_session_send(session) != 0)
      return -1;
  }
  return 0;
}

static void run_h2(const char *label, const struct urlparts *u, const char *method, int nops)
{
  int fd = tcp_connect(u->host, u->port);
  struct h2ctx c;
  memset(&c, 0, sizeof(c));
  c.fd = fd;
  nghttp2_session_callbacks *cbs;
  nghttp2_session_callbacks_new(&cbs);
  nghttp2_session_callbacks_set_send_callback2(cbs, h2_send);
  nghttp2_session_callbacks_set_on_stream_close_callback(cbs, h2_stream_close);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, h2_data);
  nghttp2_session *session = NULL;
  if (nghttp2_session_client_new(&session, cbs, &c) != 0)
    die("nghttp2_session_client_new");
  nghttp2_session_callbacks_del(cbs);
  nghttp2_settings_entry iv[] = {
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 16 * 1024 * 1024},
  };
  if (nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, iv, 1) != 0)
    die("settings");
  nghttp2_session_set_local_window_size(session, NGHTTP2_FLAG_NONE, 0, 32 * 1024 * 1024);
  if (nghttp2_session_send(session) != 0)
    die("h2 preface");

  char authority[160];
  snprintf(authority, sizeof(authority), "%s:%d", u->host, u->port);
  for (int i = 0; i < 20; i++) {
    if (h2_one(session, &c, method, u->path, authority) < 0)
      die("HTTP/2 warmup");
  }
  uint64_t t0 = nsec();
  for (int i = 0; i < nops; i++) {
    if (h2_one(session, &c, method, u->path, authority) < 0)
      die("HTTP/2 request");
  }
  uint64_t dt = nsec() - t0;
  int64_t last_body = c.body;
  nghttp2_session_del(session);
  close(fd);
  report(label, "HTTP/2", nops, dt, last_body);
}

int main(int argc, char **argv)
{
  if (argc < 2) {
    fprintf(stderr, "usage: %s http://127.0.0.1:PORT/small [http://.../medium]\n", argv[0]);
    return 1;
  }
  struct urlparts small, med;
  parse_url(argv[1], &small);
  setvbuf(stdout, NULL, _IOLBF, 0);

  printf("Request handling  (cleartext keepalive, sequential; HTTP/2 = h2c prior knowledge)\n");
  printf("small %s\n", argv[1]);

  run_h1("HEAD HTTP/1.1", &small, "HEAD", 0, 2000);
  run_h2("HEAD HTTP/2", &small, "HEAD", 2000);
  run_h1("GET 64 B HTTP/1.1", &small, "GET", 1, 2000);
  run_h2("GET 64 B HTTP/2", &small, "GET", 2000);

  if (argc >= 3) {
    parse_url(argv[2], &med);
    printf("medium %s\n", argv[2]);
    run_h1("GET 8 MiB HTTP/1.1", &med, "GET", 1, 20);
    run_h2("GET 8 MiB HTTP/2", &med, "GET", 20);
  }
  return 0;
}
