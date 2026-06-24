# XrdHTTP

XrdHTTP is the HTTP and HTTPS protocol handler for XRootD. It exposes storage
operations (GET, PUT, HEAD, DELETE, WebDAV, checksums, third-party copy, and
more) over standard HTTP semantics and bridges authenticated requests into the
native XRootD protocol via `XrdXrootdTransit`.

The implementation lives primarily in `XrdHttpProtocol` (connection lifecycle,
TLS, configuration) and `XrdHttpReq` (per-request state, body I/O, Bridge
interaction). Since the wire-layer re-platform, HTTP framing is handled by
components under `wire/` while application logic remains shared across HTTP/1
and HTTP/2.

## Architecture

```
  Client
    |
    v
  XrdHttpProtocol::Process()
    |
    +-- TLS handshake (HTTPS)
    +-- detectWireMode()  (ALPN / H2 preface / SETTINGS heuristic)
    |
    +-- HTTP/1.1                          +-- HTTP/2 (HTTPS + ALPN h2)
    |     getDataOneShot()                |     XrdHttp2Session::drive()
    |     header parse (llhttp or legacy) |     nghttp2 stream assembly
    |                                     |
    +------------------+------------------+
                       v
         XrdHttpProtocol::processParsedRequest()
                       |
                       v
              XrdHttpReq (body, auth, handlers)
                       |
                       v
              XrdXrootdTransit / Bridge->Run()
                       |
                       v
         XrdHttp1ResponseWriter  or  XrdHttp2ResponseWriter
```

Both wire protocols funnel parsed requests into the same application entry
point. Auth, CGI mapping, redirects, range reads, WebDAV, and Bridge semantics
are unchanged above that boundary.

### Wire layer (`wire/`)

| Component | Role |
|-----------|------|
| `XrdHttp1Session` | HTTP/1.1 request-line and header parsing via [llhttp](vendor/llhttp/) |
| `XrdHttp1ResponseWriter` | HTTP/1.1 response headers, fixed-length bodies, and chunked encoding |
| `XrdHttp2Session` | nghttp2 session management, stream state, `drive()` I/O loop |
| `XrdHttp2ResponseWriter` | HTTP/2 response headers and DATA frames |
| `XrdHttpConnection.hh` | `XrdHttpWireMode` enum (`kHttp1`, `kHttp2`) |

See [`wire/README.md`](wire/README.md) for wire-layer specifics.

### Application layer (largely unchanged)

| File | Role |
|------|------|
| `XrdHttpProtocol.cc` | `Process()`, TLS, configuration, `processParsedRequest()` |
| `XrdHttpReq.cc` | Request state machine, body framing, file ops, Bridge calls |
| `XrdHttpSecurity.cc` | TLS client auth, token validation |
| `XrdHttpExtHandler.cc` | Pluggable external handlers (e.g. TPC) |
| `XrdHttpReadRangeHandler.cc` | Byte-range and multipart responses |
| `XrdHttpChecksum*.cc` | Checksum discovery and computation |
| `XrdHttpMon.cc` | Monitoring hooks |

Feature-specific documentation:

- [Kerberos over HTTPS](README-KRB5.md)
- [Checksums](README-CKSUM.md)
- [HTTPS third-party copy](../XrdHttpTpc/README.md)

## HTTP/1.1: new wire path vs legacy parser

HTTP/1.1 header parsing has **two** selectable implementations. The default is
llhttp; the previous line-based parser remains for regression testing.

| Layer | Default (llhttp) | Legacy (`http.parser legacy`) |
|-------|------------------|-------------------------------|
| Request headers | `XrdHttp1Session` + llhttp | `BuffgetLine()` + `XrdHttpReq::parseFirstLine()` / `parseLine()` |
| Request body | Existing `XrdHttpReq` read path | Same |
| Responses | `XrdHttp1ResponseWriter` | Same |

llhttp parses **headers only**. Content-Length, chunked upload bodies, and
range-read streaming still use the long-standing read path in `XrdHttpReq`.

The legacy parser is the original naive line/token implementation. It is not
removed; select it explicitly when comparing behaviour or running the legacy
regression test suite.

## HTTP/2

HTTP/2 is available when XRootD is built with nghttp2 (`BUILD_HTTP2`).

- Negotiated on **HTTPS** connections via TLS ALPN (`h2`).
- Plain HTTP connections remain HTTP/1.1.
- `detectWireMode()` also recognises the cleartext connection preface and a
  SETTINGS-frame heuristic for edge cases.
- Multiple requests on one connection are supported (same-connection reuse).
- `Http2OutboundPending()` defers connection close while response DATA is still
  queued.

## Request lifecycle

1. **`Process(lp)`** — Main scheduler entry point. Reads socket data (HTTP/1),
   drives the nghttp2 session (HTTP/2), or re-enters after Bridge callbacks
   (`lp == nullptr`).

2. **Header parsing** — Populates `CurrentReq` (`XrdHttpReq`). On success,
   `CurrentReq.headerok` is set.

3. **`processParsedRequest(lp)`** — Shared application handler: auth checks,
   self-redirect, login, method dispatch (GET, PUT, PROPFIND, …).

4. **`Bridge->Run()`** — Async file operations via `XrdXrootdTransit`. Bridge
   callbacks re-invoke `Process(nullptr)`.

5. **Response** — `StartSimpleResp()` / `SendSimpleResp()` route to
   `XrdHttp1ResponseWriter` or `XrdHttp2ResponseWriter` based on
   `wireMode_`.

### Scheduler return codes

`Process()` return values follow XRootD scheduler semantics (see
`src/Xrd/XrdLinkXeq.cc`):

| Return | Meaning |
|--------|---------|
| `0` | Continue processing (stick loop or Transit `reInvoke`) |
| `1` | Idle — wait for poll I/O |
| `< 0` | Close the connection |

HTTP/2 `drive()` must preserve this contract: return `1` when idle and waiting
for bytes, but return `0` after `Bridge->Run()` so Transit can re-enter. Forcing
idle `0 → 1` globally breaks Bridge integration.

## Configuration

Example server stanza: [`xrootd-http.cf`](xrootd-http.cf).

```cfg
xrd.protocol XrdHttp /usr/lib64/libXrdHttp.so

http.cert /etc/grid-security/hostcert.pem
http.key  /etc/grid-security/hostkey.pem
http.cadir /etc/grid-security/certificates

# HTTP/1.1 header parser (default: llhttp)
http.parser llhttp
# http.parser legacy

# Optional feature modules
# http.exthandler xrdtpc libXrdHttpTPC.so
# http.auth krb5
```

| Directive | Values | Notes |
|-----------|--------|-------|
| `http.parser` | `llhttp`, `legacy` | HTTP/1.1 header parsing only |
| `http.tlsclientauth` | `on`, `off` | TLS client certificate authentication |
| `http.selfhttps2http` | `yes`, `no` | Signed redirect from HTTPS to HTTP |
| `http.auth` | `krb5`, `tpc`, … | See feature READMEs |

HTTP/2 requires no separate config directive; it is negotiated at the TLS layer
when the build includes nghttp2 and the client offers ALPN `h2`.

## Build

HTTP support is controlled by `ENABLE_HTTP`. HTTP/2 additionally requires
**libnghttp2** (the development library, not tools-only packages):

```bash
# macOS
brew install libnghttp2

cmake .. -DENABLE_HTTP=ON
# HTTP/2 is enabled automatically when libnghttp2 is found (BUILD_HTTP2)

make XrdHttpUtils XrdHttp
```

Optional Kerberos HTTP auth: `-DENABLE_KRB5=ON` (see [README-KRB5.md](README-KRB5.md)).

Build outputs:

- `libXrdHttpUtils.so` — Protocol implementation (shared; plugins link against it)
- `libXrdHttp.so` — Protocol plugin loaded by `xrd.protocol`

## Tests

From the build directory, after configuring with HTTP enabled:

```bash
# TLS fixture required for httph2
bash tests/tls/tls.sh setup

ctest -R 'XRootD::(http|httpparser|httph2)' --output-on-failure
```

| CTest name | Script | Port | Coverage |
|------------|--------|------|----------|
| `XRootD::http` | `tests/XRootD/http.sh` | 7094 | Full HTTP/1.1 integration |
| `XRootD::httpparser` | `tests/XRootD/httpparser.sh` | 7095 | llhttp header parsing |
| `XRootD::httpparserlegacy` | `tests/XRootD/httpparserlegacy.sh` | 7096 | Legacy line parser regression |
| `XRootD::httph2` | `tests/XRootD/httph2.sh` | 7097 | HTTPS + ALPN h2 (GET/PUT/HEAD/DELETE, same-connection reuse) |

## Source layout

```
src/XrdHttp/
  README.md                 — this file
  README-KRB5.md            — Kerberos / SPNEGO authentication
  README-CKSUM.md           — Checksum support
  xrootd-http.cf            — Example configuration
  XrdHttpProtocol.{cc,hh}   — Protocol handler
  XrdHttpReq.{cc,hh}        — Per-request logic
  XrdHttpModule.cc          — Plugin entry point
  wire/                     — HTTP/1 and HTTP/2 wire substrate
  vendor/llhttp/              — Bundled llhttp (MIT, v9.4.1)
  static/                   — Embedded CSS and favicon for directory listings
```

## Vendored dependencies

- **llhttp** — HTTP/1.1 request parser ([nodejs/llhttp](https://github.com/nodejs/llhttp)).
  See [`vendor/llhttp/README`](vendor/llhttp/README).
- **nghttp2** — External dependency for HTTP/2 (not vendored).
