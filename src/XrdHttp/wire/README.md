# XrdHTTP wire layer (Phase 1)

This directory contains the HTTP/1.1 wire-protocol substrate for XrdHTTP.

## Components

- `XrdHttp1Session` — llhttp-backed request-line and header parsing. Body
  framing continues to use the existing read path in `XrdHttpReq`.
- `XrdHttp1ResponseWriter` — HTTP/1.1 response header and body framing.

## Configuration

```cfg
http.parser llhttp    # default
http.parser legacy    # previous line-based parser
```

## Tests

- `tests/XRootD/httpparser` — llhttp smoke tests (GET/PUT/HEAD/DELETE, OPTIONS,
  chunked upload, keep-alive, malformed request handling)
- `tests/XRootD/httpparserlegacy` — legacy parser regression on port 7096
- `tests/XRootD/http` — full HTTP integration suite (runs with `http.parser llhttp`)

## Phase 2

HTTP/2 (`nghttp2`) will branch from `HttpConnection` after ALPN negotiation
(`http/1.1` is already advertised during TLS setup).
