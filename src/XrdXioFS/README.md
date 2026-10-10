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
    -> xiofs.ko -> HTTP/1.1 or serial HTTP/2 -> kTLS -> TCP -> XrdHttp
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
| chown | `PROPPATCH` (`X:uid` / `X:gid`) |
| utimens | `PROPPATCH` (`X:atime` / `X:mtime`) |
| hard link | `LINK` (`Destination:`) |
| symlink | `SYMLINK` (`Xrd-Symlink-Target`) or `LINK` + `Xrd-Link-Type: symbolic` |
| readlink | `READLINK` or `GET` + `Xrd-Readlink: 1` |
| mknod (fifo/device) | `MKNOD` or `PUT` + `Xrd-Mknod: 1` (`Xrd-Mode`, `Xrd-Dev`) |
| getxattr | `GET` + `Xrd-Xattr` |
| setxattr / removexattr | `PROPPATCH` `X:xattr-name` / `X:xattr-value` / `X:xattr-del` |
| listxattr | `GET` + `Xrd-Xattr-List: 1` |
| POSIX lock / flock | `LOCK` / `UNLOCK` |

XrdHttp's `ETag` is `"<dev:ino>-<ctime>[.ns]-<size>"`: the prefix before
the first `-` is the object identity (what `st_ino` and `If-Match` on
mutations use), the full tag is a validator for `If-None-Match`
revalidation (`304`). `Last-Modified` / `If-Modified-Since` /
`If-Unmodified-Since` are supported as well, including on `PROPFIND`
against the collection. `PROPFIND` entries carry `getetag` and `X:ctime`.

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

## Packages

RPM:

```bash
dnf install xrootd-xiofs xrootd-xiofs-dkms
```

Debian/Ubuntu:

```bash
apt install xrootd-xiofs xrootd-xiofs-dkms
```

`xrootd-xiofs` has `xiofscli`, `xiofsd`, `xiofsagent`, and `/usr/sbin/mount.xiofs`.
`xrootd-xiofs-dkms` builds `xiofs.ko` against the running kernel (needs
`kernel-devel` / `linux-headers`). This is not `xrootdfs` (`xrootd-fuse`).

## xiofscli

```bash
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt stat
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt cat
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt read 0 4096
xiofscli --cacert ca.pem https://localhost:7097/path/dir ls
xiofscli --cacert ca.pem https://localhost:7097/path/new.txt put ./local.bin
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt chmod 0644
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt chown 1000 1000
xiofscli --cacert ca.pem https://localhost:7097/path/link symlink /target
xiofscli --cacert ca.pem https://localhost:7097/path/link readlink
xiofscli --cacert ca.pem https://localhost:7097/path/pipe mknod 010644
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt setxattr user.foo bar
xiofscli --cacert ca.pem https://localhost:7097/path/file.txt lock SETLK WRLCK 0 0
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
are MKCOL / DELETE / MOVE. `chmod` is PROPPATCH; `chown` and `utimens` are
PROPPATCH `X:uid`/`X:gid`/`X:atime`/`X:mtime`. `link` is LINK. `symlink` is
SYMLINK (HTTP/2) with a LINK + `Xrd-Link-Type: symbolic` fallback. `readlink`
is READLINK, or GET + `Xrd-Readlink: 1` on HTTP/1. getattr uses PROPFIND
`X:unix-mode`/`X:uid`/`X:gid`/`X:file-type`/`X:rdev`/`D:symlink`. `mknod` of
fifo/device is MKNOD (PUT + `Xrd-Mknod` fallback). xattrs are GET
`Xrd-Xattr` / PROPPATCH. POSIX `lock`/`flock` are LOCK/UNLOCK. `auto_cache`
repeated 4 KiB reads; `xiofscli` remains the
non-FUSE client.

The HTTP/2 session keeps one TLS connection and multiplexes streams on
an I/O thread, so concurrent FUSE reads and writes do not wait for each
other to finish.

## xiofsagent (Linux kernel mount)

Needs `xiofs.ko` (`modprobe tls; insmod xiofs.ko`) and OpenSSL built with
`enable-ktls`. The kernel path speaks HTTP/1.1 by default, or serial HTTP/2 with `--http2`
(ALPN `h2`, mount option `http2`). kTLS RX on Alma 9's OpenSSL
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

`--krb5` mounts with `krb5` and runs a persistent `WAIT_NEED` loop:
SPNEGO as each requesting uid, then import with `XIOFS_IMPORT_KRB5`.
`--jwt` does the same with a WLCG `bt_u<uid>` bearer file (`XIOFS_IMPORT_JWT`).
The token file must be owned by that uid and not group/world accessible;
the client does not parse the JWT. `--workers N` forks N processes.
See `kernel/README.md`.

```bash
xiofsagent --krb5 --workers 4 --cacert ca.pem \
    https://storage.example:1094/export /mnt/xiofs
xiofsagent --jwt --workers 4 --cacert ca.pem \
    https://storage.example:1094/export /mnt/xiofs
```

## Layout

```
include/xiofs_ops.h   transport / memory-target vocabulary
http/                    URL, DAV parser, HTTP/2 session, Client
fuse/xiofscli.cc           command-line client
fuse/xiofsd.cc             FUSE daemon
agent/xiofsagent.cc        Linux TLS handshake + kTLS import
agent/xiofsagent_krb5.cc   SPNEGO for --krb5 (HAVE_KRB5)
http/XioBearer.cc          WLCG bt_u<uid> discovery (owner-only file)
kernel/                  Linux module for AlmaLinux 9 (5.14) and 10 (6.12):
                         page cache, readahead, writeback, HTTP/1.1 + kTLS
                         import (kbuild, not CMake)
```

This is **not** XrdFfs (`root://` + XrdPosix) and **not** XrdClHttp (libcurl).
The HTTP/2 session is intentionally thin so a kernel HTTP/1.1 client can
follow the same verb map.
