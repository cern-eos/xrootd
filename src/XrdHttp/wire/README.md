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

## Phase 2

HTTP/2 (`nghttp2`) will branch from `HttpConnection` after ALPN negotiation
(`http/1.1` is already advertised during TLS setup).
