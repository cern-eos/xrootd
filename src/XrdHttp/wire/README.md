# XrdHTTP wire layer

HTTP/1.1 and HTTP/2 wire-protocol substrate for XrdHTTP.

## Components

- `XrdHttp1Session` — llhttp-backed request-line and header parsing. Body
  framing continues to use the existing read path in `XrdHttpReq`.
- `XrdHttp1ResponseWriter` — HTTP/1.1 response header and body framing.
- `XrdHttp2Session` — nghttp2-backed HTTP/2 session (HTTPS ALPN `h2`,
  h2c prior knowledge, and `Upgrade: h2c`). Streams request bodies as
  DATA arrives and queues concurrent streams through the single
  application request. `http.h2push` advertises and serves promised GETs
  when the client enables push.
- `XrdHttp2ResponseWriter` — HTTP/2 response headers and DATA frames.
- `XrdHttpParsockListener` — libparsocket listener enabling HTTP/2 over
  parallel TCP or RDMA RC transport (`BUILD_PARSOCKET`). Accepts connections
  on a dedicated SPT port and drives HTTP/2 streaming sessions.

Both protocols funnel parsed requests into `XrdHttpReq` and
`XrdHttpProtocol::processParsedRequest()`.

## Configuration

```cfg
http.parser llhttp    # default for HTTP/1.1 header parsing
http.parser legacy    # previous line-based parser
```

HTTP/2 is negotiated via TLS ALPN, the h2c connection preface, or
`Upgrade: h2c` when built with nghttp2 (`BUILD_HTTP2`).

### Parsock (Parallel TCP / RDMA)

When built with `ENABLE_PARSOCKET=TRUE` and `libparsocket` is available,
HTTP/2 can be carried across multiple parallel TCP lanes, RDMA RC queue
pairs, or QUIC:

```cfg
http.parsock port 8443 [bind <addr>] [transport tcp|rdma|quic] [sockets <n>] [protection confidential|authenticated]
```

## Build

HTTP/2 requires the **libnghttp2** development library (not the Homebrew
`nghttp2` tools-only formula). On macOS:

```bash
brew install libnghttp2
cmake .. -DENABLE_HTTP=ON
```

## Tests

- `tests/XRootD/httpparser` — llhttp smoke tests (port 7095)
- `tests/XRootD/httpparserlegacy` — legacy parser regression (port 7098)
- `tests/XRootD/http` — full HTTP/1.1 integration suite (port 7094)
- `tests/XRootD/httph2` — HTTPS + ALPN h2 integration tests (port 7097, TLS fixture)
- `tests/XRootD/httpparser` — also covers h2c prior knowledge and Upgrade
