# XIOFS — Cross-transport I/O File System

`xiofs.ko` is the in-kernel data path for XIOFS. It is **not** built by
the XRootD CMake tree. Target kernels are the **default AlmaLinux /
RHEL** ones:

| Distro | Default kernel | VFS APIs used |
|--------|----------------|---------------|
| AlmaLinux 9 / RHEL 9 | `5.14.0-*.el9` | `readpage`, `user_namespace`, `write_cache_pages` |
| AlmaLinux 10 / RHEL 10 | `6.12.0-*.el10` | `read_folio`, `mnt_idmap`, `writeback_iter` |

On the distro you will run:

```bash
# AlmaLinux 9 or 10, matching kernel-devel
sudo dnf install kernel-devel-$(uname -r) kernel-headers-$(uname -r)
sudo modprobe tls
make -C /lib/modules/$(uname -r)/build M=$(pwd) modules
sudo insmod xiofs.ko
# Handshake, kTLS, mount, import (from the XRootD build tree):
sudo xiofsagent --cacert /path/ca.pem \
    https://storage.example:1094/export /mnt/xiofs
```

`xiofsagent` is the userspace TLS handshake helper (`src/XrdXioFS/agent`).
Until it imports a kTLS socket, I/O returns `-ENOTCONN`. HTTPS imports
without kTLS TX+RX are rejected (`-EPROTO`). Plain `http://` skips TLS.

OpenSSL 3.0 on Alma 9 often enables kTLS TX only for TLS 1.3; the agent
reconnects at TLS 1.2 (AES-GCM or ChaCha20) so RX works too. Need
`enable-ktls` in the distro OpenSSL (Alma/RHEL 9 include it).

## What is wired (VFS)

This is no longer an empty mount. POSIX I/O goes through the **page
cache**, not a userspace FUSE daemon:

```
read(2)
  -> generic_file_read_iter
  -> page cache HIT  -> return
  -> MISS: a_ops.readahead  (one GET Range, up to 4 MiB)
        or a_ops.read_folio (single page)

write(2)
  -> generic_file_write_iter
  -> dirty pages in the page cache
  -> writeback / fsync
  -> a_ops.writepages  (coalesced PATCH Content-Range)
```

| Piece | Implementation |
|-------|----------------|
| Page cache | `address_space_operations` (`read_folio`, `write_begin`/`write_end`, `dirty_folio`) |
| Readahead | `a_ops.readahead` + `s_bdi->ra_pages` = 4 MiB |
| Writeback | `a_ops.writepages` coalesces dirty folios into PATCH |
| `fsync` | `filemap_write_and_wait_range` |
| `mmap` | `generic_file_mmap` |
| Dcache | `lookup` + `d_splice_alias`; getattr refreshes size/mtime/ETag |
| Identity | URL path + ETag (`If-Match` on PATCH/DELETE) |

## HTTP/1.1 + kTLS

XrdHttp serves the same verbs on HTTP/1.1 and HTTP/2. The kernel
transport is HTTP/1.1 on a socket that userspace has already wrapped
with **kTLS**:

```
userspace TLS handshake agent
        |  setsockopt(SOL_TLS, TLS_TX/TLS_RX)
        |  ioctl(XIOFS_IOC_IMPORT_SOCK)
        v
/dev/xiofsctl
        |
        v
xiofs.ko  -- plaintext HTTP/1.1 -->  kTLS  --> TCP  --> XrdHttp
```

Do **not** extract TLS keys and implement a private record layer. The
agent sets `SSL_OP_ENABLE_KTLS` so OpenSSL installs `TLS_TX`/`TLS_RX`,
then donates the fd.

UAPI: `xiofs_uapi.h`. Match `host`, `port`, and `path` to the mount.
`--import-only` re-handshakes into an existing mount (same host/port/path).

```c
struct xiofs_import_sock im = {
    .sockfd = tls_fd,
    .flags  = XIOFS_IMPORT_TLS | XIOFS_IMPORT_BEARER,
    .port   = 1094,
};
strcpy(im.host, "storage.example");
strcpy(im.export_path, "/export");
ioctl(ctlfd, XIOFS_IOC_IMPORT_SOCK, &im);
```

One socket is used with a mutex (HTTP/1.1 cannot multiplex). No chunked
encoding; XrdHttp sends `Content-Length`. Send/recv use `sk_rcvtimeo` /
`sk_sndtimeo` (`timeo=`, default 30s). On connection errors the socket is
dropped and the request waits once for `xiofsagent --import-only`. Dirty
pages are redirtied so writeback can retry after a new kTLS socket.

Metadata uses a dentry/inode TTL (`actimeo=`, default 30s, `0` always
revalidates). `d_revalidate` issues PROPFIND/HEAD when the cache expires.

## Verb map

| VFS | HTTP |
|-----|------|
| lookup / getattr | `PROPFIND` Depth 0, `HEAD` fallback |
| readdir | `PROPFIND` Depth 1 |
| read / readahead | `GET` `Range` |
| writeback | `PATCH` `Content-Range` + `If-Match` |
| create / trunc 0 | `PUT` |
| mkdir | `MKCOL` |
| unlink | `DELETE` + `If-Match` |
| rename | `MOVE` |
| chmod | `PROPPATCH` `X:mode` |
| hard link | `LINK` + `Destination` |

## Explicitly not done

- HTTP/2 in-kernel (HPACK / streams / flow control)
- Automatic handshake upcall (re-import is still `xiofsagent --import-only`)
- Chunked responses
- Byte-range locks (local VFS locks still apply)
- Symlinks, mknod
- Persistent chown / utimens (no protocol verb)
- Writeback congestion / batching PATCH across folios
- RDMA / GPU-direct (`XIOFS_IOC_GPU_READ` returns `-EOPNOTSUPP`)

## License

Linux kernel symbols require a GPL-compatible module license.
Files in this directory are **GPL-2.0**. Userspace under `../http` and
`../fuse` stays **LGPL-3.0**.
