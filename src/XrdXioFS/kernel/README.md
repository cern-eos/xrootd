# XIOFS — Cross-transport I/O File System

`xiofs.ko` is the in-kernel data path for XIOFS. It is **not** built by
the XRootD CMake tree. Target kernels are the **default AlmaLinux /
RHEL** ones:

| Distro | Default kernel | VFS APIs used |
|--------|----------------|---------------|
| AlmaLinux 9 / RHEL 9 | `5.14.0-*.el9` | `readpage`, `user_namespace`, `write_cache_pages` |
| AlmaLinux 10 / RHEL 10 | `6.12.0-*.el10` | `read_folio`, `mnt_idmap`, `writeback_iter` |

From packages:

```bash
# RPM (AlmaLinux / RHEL 9 or 10)
sudo dnf install xrootd-xiofs xrootd-xiofs-dkms kernel-devel-$(uname -r)
# Debian / Ubuntu
sudo apt install xrootd-xiofs xrootd-xiofs-dkms linux-headers-$(uname -r)
sudo xiofsagent --cacert /path/ca.pem \
    https://storage.example:1094/export /mnt/xiofs
# or: mount -t xiofs -o host=...,port=...,path=...,cacert=... none /mnt/xiofs
```

From a source tree on the distro you will run:

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

## HTTP/1.1 or serial HTTP/2 + kTLS

XrdHttp serves the same verbs on HTTP/1.1 and HTTP/2. The kernel
transport is HTTP/1.1 by default, or **serial HTTP/2** (`http2` mount
option / `xiofsagent --http2` / `XIOFS_IMPORT_H2`) on a socket that
userspace has already wrapped with **kTLS**:

```
userspace TLS handshake agent
        |  setsockopt(SOL_TLS, TLS_TX/TLS_RX)
        |  ioctl(XIOFS_IOC_IMPORT_SOCK)
        v
/dev/xiofsctl
        |
        v
xiofs.ko  -- plaintext HTTP/1.1 or serial HTTP/2 -->  kTLS  --> TCP  --> XrdHttp
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

A non-krb5/jwt mount uses one socket with a mutex (uid 0). A `krb5` or
`jwt` mount keeps a uid-to-conn table: each `current_fsuid()` gets its
own already authenticated HTTP channel. HTTP/2 is one stream at a time
(HPACK from the HTTP/1 request builders) and is forced off after a
Kerberos import. JWT imports may keep serial HTTP/2.
No chunked encoding; XrdHttp sends `Content-Length` on HTTP/1.1.
Send/recv use `sk_rcvtimeo` / `sk_sndtimeo` (`timeo=`, default 30s). On
connection errors the socket is dropped and the request waits for a new
import (`xiofsagent --import-only`, or `XIOFS_IOC_WAIT_NEED` on krb5
or jwt mounts). Dirty pages are redirtied so writeback can retry after a new
kTLS socket.

Metadata uses a dentry/inode TTL (`actimeo=`, default 30s, `0` always
revalidates). `d_revalidate` issues PROPFIND/HEAD when the cache expires.

## Kerberos (SPNEGO) via xiofsagent

The kernel never runs GSS. `xiofsagent --krb5` is the rpc.gssd analogue:
it waits on `XIOFS_IOC_WAIT_NEED`, impersonates the requesting uid
(`seteuid` + default ccache), finishes HTTP/1 Negotiate on a kTLS
socket, then imports with `XIOFS_IMPORT_KRB5` (and `im.uid`). Failure
is reported with `XIOFS_IOC_NEED_FAIL`. Several agents or
`--workers N` can run at once; each `WAIT_NEED` dequeues one uid.

The server identity is the Kerberos principal username, not the client
numeric uid. krb5 mounts keep the imported socket on HTTP/1.1 (no
in-kernel HTTP/2 after SPNEGO). Do not set `XIOFS_IMPORT_H2` on a
Kerberos import.

```bash
# as the user who will do I/O
kinit alice@EXAMPLE.ORG

# as root: mount with krb5, then run the agent
sudo xiofsagent --krb5 --workers 4 --cacert /path/ca.pem \
    https://storage.example:1094/export /mnt/xiofs
```

Or mount first, then attach the agent:

```bash
sudo mount -t xiofs \
    -o host=storage.example,port=1094,path=/export,krb5 \
    none /mnt/xiofs
sudo xiofsagent --krb5 --workers 4 --import-only \
    https://storage.example:1094/export
```

`WAIT_NEED` is the automatic handshake upcall. `mount.xiofs` with `-o krb5`
or `-o jwt` only mounts; run `xiofsagent --krb5` or `--jwt --import-only`
as a separate daemon. Non-krb5 remounts still use `xiofsagent --import-only`.

## JWT / OIDC bearer (WLCG `bt_u<uid>`)

The kernel never reads or parses the token. `xiofsagent --jwt` waits on
`XIOFS_IOC_WAIT_NEED`, loads the calling uid's bearer file, and imports
with `XIOFS_IMPORT_JWT` plus `Authorization: Bearer` on that uid's
HTTP channel. Discovery (same as `XrdSecztn`):

```text
/run/user/<uid>/bt_u<uid>
/tmp/bt_u<uid>
```

The file must be a regular file owned by that uid, with no group or
world access bits (`0600` / `0400`). Symlinks are rejected (`O_NOFOLLOW`).
The agent copies the bytes as-is; it does not inspect the JWT.

```bash
# as the user who will do I/O (oidc-agent, htgettoken, ...)
chmod 0600 /tmp/bt_u${UID}

# as root
sudo xiofsagent --jwt --workers 4 --cacert /path/ca.pem \
    https://storage.example:1094/export /mnt/xiofs
```

Or mount first:

```bash
sudo mount -t xiofs \
    -o host=storage.example,port=1094,path=/export,jwt \
    none /mnt/xiofs
sudo xiofsagent --jwt --workers 4 --import-only \
    https://storage.example:1094/export
```

`krb5` and `jwt` cannot be combined on one mount. JWT imports may use
serial HTTP/2; Kerberos imports stay on HTTP/1.1.

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
| chown | `PROPPATCH` `X:uid` / `X:gid` |
| utimens | `PROPPATCH` `X:atime` / `X:mtime` |
| hard link | `LINK` + `Destination` |
| symlink | `LINK` + `Xrd-Link-Type: symbolic` + `Xrd-Symlink-Target` |
| readlink | `GET` + `Xrd-Readlink: 1` |
| mknod (fifo/chr/blk) | `PUT` + `Xrd-Mknod: 1` (`Xrd-Mode`, `Xrd-Dev`) |
| getxattr / listxattr | `GET` + `Xrd-Xattr` / `Xrd-Xattr-List` |
| setxattr / removexattr | `PROPPATCH` `X:xattr-*` |
| fcntl lock / flock | `LOCK` / `UNLOCK` |

## Explicitly not done

- Chunked responses
- Multiplexed HTTP/2 streams (the kernel client is serial)
- Writeback congestion / batching PATCH across folios
- RDMA / GPU-direct (`XIOFS_IOC_GPU_READ` returns `-EOPNOTSUPP`)

## License

Linux kernel symbols require a GPL-compatible module license.
Files in this directory are **GPL-2.0**. Userspace under `../http` and
`../fuse` stays **LGPL-3.0**.
