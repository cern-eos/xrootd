# XIOFS — Cross-transport I/O File System

A Linux-network-filesystem client whose **server** is XrdHttp (HTTP/1.1 or
HTTP/2). Filesystem semantics live in the client; HTTP is only the
universal transport.

```
POSIX app
    |
    v
  FUSE (xiofsd)  or  xiofscli
    |
    | HTTP/2  ALPN h2   (nghttp2 + OpenSSL)
    v
  XrdHttp  (same verbs on HTTP/1.1)
    |
    v
  XRootD storage
```

Later, `xiofs.ko` keeps the data path in-kernel on **AlmaLinux 9
(kernel 5.14)** and **AlmaLinux 10 (kernel 6.12)**:

```
VFS -> page cache / readahead / writeback
    -> xiofs.ko -> HTTP/1.1 -> kTLS -> TCP -> XrdHttp
```

`xiofsagent` (Linux) does the TLS handshake and certificate checks, installs
kTLS, and imports the socket via `/dev/xiofsctl`. See
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
| chmod | `PROPPATCH` (`X:mode` / Apache `executable`) |
| hard link | `LINK` (`Destination:`) |

Identity is **URL path + ETag** (XrdHttp `ETag` from `StatGen`).

## Build

Requires `BUILD_HTTP2` (libnghttp2) and OpenSSL.

```bash
cmake .. -DENABLE_HTTP=ON -DENABLE_HTTP2=ON
make xiofscli
# libfuse (Linux) or macFUSE (/usr/local, /opt/homebrew, /opt/brew):
make xiofsd
# Linux only (kTLS handshake agent for xiofs.ko):
make xiofsagent
```

## xiofscli

```bash
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt stat
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt cat
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt read 0 4096
xiofscli --cacert ca.pem https://localhost:7097/path/dir ls
xiofscli --cacert ca.pem https://localhost:7097/path/new.txt put ./local.bin
xiofscli --cacert ca.pem https://localhost:7097/path/new.txt write 4 ./patch.bin
```

XrdHttp's `xrd.tls` context did not advertise ALPN `h2`, and TLS 1.3
left the HTTP/2 preface in `SSL_pending()` so `detectWireMode()` never
saw it. `XrdHttpProtocol` now installs the ALPN callback on the CTX used
for `SSL_accept` and always buffers pending TLS data before detection.

## xiofsd (FUSE)

Linux libfuse or macFUSE:

```bash
xiofsd --cacert ca.pem https://localhost:7097/export /mnt/xiofs -f
```

FUSE I/O uses Range GETs for reads and PATCH (`Content-Range`) for
`pwrite`. `create` / truncate-to-empty is `PUT`. mkdir / unlink / rename
are MKCOL / DELETE / MOVE. `chmod` is PROPPATCH; `link` is LINK. `auto_cache`
repeated 4 KiB reads; `xiofscli` remains the non-FUSE client.

The HTTP/2 session keeps one TLS connection and multiplexes streams on
an I/O thread, so concurrent FUSE reads and writes do not wait for each
other to finish.

## xiofsagent (Linux kernel mount)

Needs `xiofs.ko` (`modprobe tls; insmod xiofs.ko`) and OpenSSL built with
`enable-ktls`. The kernel path speaks HTTP/1.1; kTLS RX on Alma 9's OpenSSL
3.0 needs TLS 1.2 AES-GCM or ChaCha20 (the agent retries TLS 1.2 if TLS 1.3
only got TX).

```bash
xiofsagent --cacert ca.pem https://storage.example:1094/export /mnt/xiofs
# or, after ln -s $(which xiofsagent) /sbin/mount.xiofs:
mount -t xiofs -o host=storage.example,port=1094,path=/export,cacert=ca.pem \
    none /mnt/xiofs
```

`--import-only` attaches a new kTLS socket to an existing mount (same
host/port/path) after a drop. `--actimeo` / `--timeo` set metadata TTL and
socket wait (seconds). RDMA and GPU-direct are not implemented.

## Layout

```
include/xiofs_ops.h   transport / memory-target vocabulary
http/                    URL, DAV parser, HTTP/2 session, Client
fuse/xiofscli.cc           command-line client
fuse/xiofsd.cc             FUSE daemon
agent/xiofsagent.cc        Linux TLS handshake + kTLS import
kernel/                  Linux module for AlmaLinux 9 (5.14) and 10 (6.12):
                         page cache, readahead, writeback, HTTP/1.1 + kTLS
                         import (kbuild, not CMake)
```

This is **not** XrdFfs (`root://` + XrdPosix) and **not** XrdClHttp (libcurl).
The HTTP/2 session is intentionally thin so a kernel HTTP/1.1 client can
follow the same verb map.
