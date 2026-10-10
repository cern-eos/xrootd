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
| Dcache | `lookup` + `d_splice_alias`; readdir primes dentries+inodes from the Depth 1 listing |
| Icache | `iget5_locked` keyed by the server identity (`<dev:ino>` prefix of the ETag) |
| Identity | ETag `"<dev:ino>-<ctime>-<size>"`; mutations send the bare `"<dev:ino>"` as `If-Match` |

## Round trips and caches

The ETag XrdHttp returns is **versioned**: `"<dev:ino>-<ctime>[.ns]-<size>"`.
The part before the first `-` is the object identity; the whole tag is
a validator. The server accepts both in `If-Match` / `If-None-Match`:
the bare identity means "same object, any version", the full tag means
"unchanged". `Last-Modified`, `If-Modified-Since` and
`If-Unmodified-Since` are honoured too, and `PROPFIND` evaluates them
against the collection itself (`304` with no body when nothing changed).

| Cache | Policy |
|-------|--------|
| inode attributes | trusted for `actimeo=` (default 30s); then one `PROPFIND` Depth 0 with `If-None-Match: <tag>` — `304` costs no body. A lookup that hits an existing inode refreshes it in place. |
| directory listing | kept on the directory inode with the listing's ETag; `getdents` continuations are served from it; after `actimeo` it is revalidated with `PROPFIND` Depth 1 + `If-None-Match` (`304` keeps it). Listings are not truncated (growable body, up to 256 MiB / 4 Mi entries). Local mutations (create, unlink, mkdir, rename, …) invalidate it. |
| readdir-plus | every entry of a listing instantiates a dentry and an inode (mode, size, times, owner, ETag from `getetag` / `X:ctime`), so `ls -l`, `find`, `rsync` after `readdir` do no further round trips. |
| negative dentries | kept for `actimeo`. `404` on `PROPFIND` is final (no `HEAD` fallback; `HEAD` is only tried on `403`). |
| setattr | mode, uid, gid, atime, mtime go out in a **single** `PROPPATCH`. |

Writes:

- `create()` is **deferred** (`defer`, default): the inode exists locally
  at once; the `PUT` goes out with the first writeback — as one request
  carrying the whole file when it is small and fully dirty — or on
  `close()`/`fsync()` for an empty file, or before the first operation
  that needs the object on the server (rename, link, xattr, lock).
  `O_EXCL` is enforced by the server through `If-None-Match: *` at that
  point. `nodefer` sends the empty `PUT` synchronously in `create()`.
  `Xrd-Mode` on `PUT`/`MKCOL` creates with the caller's mode, so no
  `PROPPATCH` follows.
- `write_begin` does not read a page from the server when the file is
  deferred or the page lies at/after a trusted EOF: it zero-fills.
- `writepages` coalesces runs of contiguous dirty pages into one `PATCH`
  of up to 4 MiB with `If-Match: "<dev:ino>"`.
- `close()` of a writable descriptor flushes (close-to-open) and reports
  writeback errors to the writer.

Connections: a mount (or, with `krb5`/`jwt`, each uid) owns up to
`conns=` channels (default 4, max 16); each is serial, idle ones are
picked first, so `stat`/`readdir` are not queued behind a 4 MiB
`GET`/`PATCH`. `xiofsagent --conns N` imports N sockets; on `krb5`/`jwt`
mounts the kernel issues one `WAIT_NEED` per missing socket. The UAPI is
unchanged.

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

A non-krb5/jwt mount uses a pool of `conns=` sockets (uid 0). A `krb5` or
`jwt` mount keeps a uid-to-pool table: each `current_fsuid()` gets its
own already authenticated HTTP channels. HTTP/2 is one stream at a time
(HPACK from the HTTP/1 request builders) and is forced off after a
Kerberos import. JWT imports may keep serial HTTP/2.
No chunked encoding; XrdHttp sends `Content-Length` on HTTP/1.1.
Send/recv use `sk_rcvtimeo` / `sk_sndtimeo` (`timeo=`, default 30s). On
connection errors the socket is dropped and the request waits for a new
import (`xiofsagent --import-only`, or `XIOFS_IOC_WAIT_NEED` on krb5
or jwt mounts). Dirty pages are redirtied so writeback can retry after a new
kTLS socket.

Metadata uses a dentry/inode TTL (`actimeo=`, default 30s, `0` always
revalidates). `d_revalidate` issues a conditional PROPFIND when the
cache expires (see "Round trips and caches").

Mount options: `host=`, `port=`, `path=`, `actimeo=`, `timeo=`,
`conns=` (1..16, default 4), `defer`/`nodefer`, `http2`, `krb5`, `jwt`.

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
| lookup / getattr | `PROPFIND` Depth 0 (+ `If-None-Match` on revalidation; `HEAD` only on `403`) |
| readdir | `PROPFIND` Depth 1 (+ `If-None-Match` on revalidation) |
| read / readahead | `GET` `Range` |
| writeback | `PATCH` `Content-Range` + `If-Match: "<dev:ino>"` (coalesced, up to 4 MiB) |
| create / trunc 0 | `PUT` + `Xrd-Mode` (deferred to first flush by default; `If-None-Match: *` for `O_EXCL`) |
| mkdir | `MKCOL` + `Xrd-Mode` (201 carries the ETag) |
| unlink | `DELETE` + `If-Match: "<dev:ino>"` |
| rename | `MOVE` + `If-Match: "<dev:ino>"` |
| chmod / chown / utimens | one `PROPPATCH` with `X:mode`, `X:uid`/`X:gid`, `X:atime`/`X:mtime` |
| hard link | `LINK` + `Destination` |
| symlink | `LINK` + `Xrd-Link-Type: symbolic` + `Xrd-Symlink-Target` |
| readlink | `GET` + `Xrd-Readlink: 1` |
| mknod (fifo/chr/blk) | `PUT` + `Xrd-Mknod: 1` (`Xrd-Mode`, `Xrd-Dev`) |
| getxattr / listxattr | `GET` + `Xrd-Xattr` / `Xrd-Xattr-List` |
| setxattr / removexattr | `PROPPATCH` `X:xattr-*` |
| fcntl lock / flock | `LOCK` / `UNLOCK` |

## Explicitly not done

- Chunked responses
- Multiplexed HTTP/2 streams (each channel is serial; concurrency comes
  from the `conns=` pool)
- Server-side change notification: cached metadata is only as fresh as
  `actimeo=` and the conditional revalidation that follows it
- RDMA / GPU-direct (`XIOFS_IOC_GPU_READ` returns `-EOPNOTSUPP`)

## License

Linux kernel symbols require a GPL-compatible module license.
Files in this directory are **GPL-2.0**. Userspace under `../http` and
`../fuse` stays **LGPL-3.0**.
