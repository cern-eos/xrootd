# KernelFS — XrdHttp client (HTTP/2 FUSE + kernel scaffolding)

A Linux-network-filesystem client whose **server** is XrdHttp (HTTP/1.1 or
HTTP/2). Filesystem semantics live in the client; HTTP is only the
universal transport.

```
POSIX app
    |
    v
  FUSE (kfsd)  or  kfscli
    |
    | HTTP/2  ALPN h2   (nghttp2 + OpenSSL)
    v
  XrdHttp  (same verbs on HTTP/1.1)
    |
    v
  XRootD storage
```

Later, `kernelfs.ko` keeps the data path in-kernel:

```
VFS -> kernelfs.ko -> HTTP/1.1 -> kTLS -> TCP -> XrdHttp
```

Userspace does TLS handshake / certificates only. See
[kernel/README.md](kernel/README.md).

## XrdHttp verb map

| Client op | HTTP |
|-----------|------|
| getattr / lookup | `PROPFIND` Depth 0 (fallback `HEAD`) |
| readdir | `PROPFIND` Depth 1 |
| read | `GET` with `Range` |
| write | `PATCH` with `Content-Range: bytes first-last/*` |
| write (replace) | `PUT` (whole-object replace) |
| mkdir | `MKCOL` |
| unlink | `DELETE` |
| rename | `MOVE` |

Identity is **URL path + ETag** (XrdHttp `ETag` from `StatGen`).

## Build

Requires `BUILD_HTTP2` (libnghttp2) and OpenSSL.

```bash
cmake .. -DENABLE_HTTP=ON -DENABLE_HTTP2=ON
make kfscli
# libfuse (Linux) or macFUSE (/usr/local, /opt/homebrew, /opt/brew):
make kfsd
```

## kfscli

```bash
kfscli --cacert ca.pem https://localhost:7097/path/file.txt stat
kfscli --cacert ca.pem https://localhost:7097/path/file.txt cat
kfscli --cacert ca.pem https://localhost:7097/path/file.txt read 0 4096
kfscli --cacert ca.pem https://localhost:7097/path/dir ls
kfscli --cacert ca.pem https://localhost:7097/path/new.txt put ./local.bin
kfscli --cacert ca.pem https://localhost:7097/path/new.txt write 4 ./patch.bin
```

XrdHttp's `xrd.tls` context did not advertise ALPN `h2`, and TLS 1.3
left the HTTP/2 preface in `SSL_pending()` so `detectWireMode()` never
saw it. `XrdHttpProtocol` now installs the ALPN callback on the CTX used
for `SSL_accept` and always buffers pending TLS data before detection.

## kfsd (FUSE)

Linux libfuse or macFUSE:

```bash
kfsd --cacert ca.pem https://localhost:7097/export /mnt/kfs -f
```

FUSE I/O uses Range GETs for reads and PATCH (`Content-Range`) for
`pwrite`. `create` / truncate-to-empty is `PUT`. mkdir / unlink / rename
are MKCOL / DELETE / MOVE. `auto_cache` lets the kernel page cache absorb
repeated 4 KiB reads; `kfscli` remains the non-FUSE client.

## Layout

```
include/kernelfs_ops.h   transport / memory-target vocabulary
http/                    URL, DAV parser, HTTP/2 session, Client
fuse/kfscli.cc           command-line client
fuse/kfsd.cc             FUSE daemon
kernel/                  Linux module stubs (HTTP/1.1 + kTLS notes)
```

This is **not** XrdFfs (`root://` + XrdPosix) and **not** XrdClHttp (libcurl).
The HTTP/2 session is intentionally thin so a kernel HTTP/1.1 client can
follow the same verb map.
