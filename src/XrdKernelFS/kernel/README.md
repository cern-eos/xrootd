# KernelFS Linux module (scaffolding)

This directory is a **Linux-only** skeleton for `kernelfs.ko`. It is not
built by the XRootD CMake tree on macOS (or at all, until a Linux CI job
opts in). Userspace `kfscli` / `kfsd` remain the supported prototype.

## Why HTTP/1.1 here, HTTP/2 in userspace

XrdHttp serves the **same application verbs** on HTTP/1.1 and HTTP/2
(see `src/XrdHttp/README.md`). The FUSE client uses HTTP/2 (ALPN `h2`,
nghttp2) because multiplexing is free in userspace.

The kernel data path must **not** grow an HTTP/2 stack:

- kTLS encrypts/decrypts TLS **records**, not HTTP/2 frames.
- HTTP/2 needs HPACK, stream state, flow-control windows, SETTINGS,
  GOAWAY, and RST_STREAM. That is a large, security-sensitive parser
  to keep in-kernel.

So the kernel transport is:

```
VFS -> kernelfs.ko -> plaintext HTTP/1.1 -> kTLS -> TCP -> XrdHttp
```

Userspace is used only for TLS handshake / certificate validation /
rekey, then installs kTLS TX/RX keys. Do **not** extract keys and
implement a private TLS record layer with the kernel crypto API.

## Handshake agent (UAPI sketch)

A small userspace helper will:

1. Connect TCP, perform TLS 1.2/1.3 with OpenSSL (or equivalent).
2. ALPN may advertise `http/1.1` (not `h2`) for this path.
3. Call `setsockopt(fd, SOL_TLS, TLS_TX, ...)` and `TLS_RX` (kTLS).
4. Pass the connected socket to the module via an ioctl / netlink
   handshake on `/dev/kernelfs` (to be defined).

The filesystem module then writes HTTP/1.1 requests as plaintext.

## Verb map (shared with FUSE)

Defined in `include/kernelfs_ops.h`:

| op        | HTTP                          |
|-----------|-------------------------------|
| getattr   | `PROPFIND` Depth 0 or `HEAD`  |
| readdir   | `PROPFIND` Depth 1            |
| read      | `GET` `Range: bytes=`         |
| write     | `PUT` (whole-object replace)  |
| mkdir     | `MKCOL`                       |
| unlink    | `DELETE`                      |
| rename    | `MOVE`                        |

Identity is **URL path + ETag** (XrdHttp `ETag` is the stat inode from
`StatGen`). A dedicated object-id API is not required for this stage.

## Building on Linux

```bash
make -C /lib/modules/$(uname -r)/build M=$(pwd) modules
# insmod kernelfs.ko
# mount -t kernelfs none /mnt/kfs
```

The stub mounts an empty directory and returns `-ENOSYS` for file I/O.
Wire `http1.c` next, then kTLS socket import.

## Explicitly deferred

- HTTP/2 in-kernel
- RDMA / GPU-direct
- Byte-range PUT (XrdHttp PUT is a full replace today)
- Writeback / byte-range locks

## License

Linux kernel symbols require a GPL-compatible module license.
Files in this directory are **GPL-2.0**. The userspace client under
`../http` and `../fuse` stays **LGPL-3.0** like the rest of XRootD.
