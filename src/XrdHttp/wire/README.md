# XrdHTTP wire layer

HTTP/1.1 and HTTP/2 wire-protocol substrate for XrdHTTP.

## Components

- `XrdHttp1Session` — llhttp-backed request-line and header parsing. Body
  framing continues to use the existing read path in `XrdHttpReq`.
- `XrdHttp1ResponseWriter` — HTTP/1.1 response header and body framing.
- `XrdHttp2Session` — nghttp2-backed HTTP/2 session (HTTPS + ALPN `h2`).
- `XrdHttp2ResponseWriter` — HTTP/2 response headers and DATA frames.

Both protocols funnel parsed requests into `XrdHttpReq` and
`XrdHttpProtocol::processParsedRequest()`.

## Configuration

```cfg
http.parser llhttp    # default for HTTP/1.1 header parsing
http.parser legacy    # previous line-based parser
```

HTTP/2 is negotiated via TLS ALPN when built with nghttp2 (`BUILD_HTTP2`).
Plain HTTP connections remain HTTP/1.1.

## Build

HTTP/2 requires the **libnghttp2** development library (not the Homebrew
`nghttp2` tools-only formula). On macOS:

```bash
brew install libnghttp2
cmake .. -DENABLE_HTTP=ON
```

## Tests

- `tests/XRootD/httpparser` — llhttp smoke tests (port 7095)
- `tests/XRootD/httpparserlegacy` — legacy parser regression (port 7096)
- `tests/XRootD/http` — full HTTP/1.1 integration suite (port 7094)
- `tests/XRootD/httph2` — HTTPS + ALPN h2 smoke tests (port 7097, TLS fixture)
