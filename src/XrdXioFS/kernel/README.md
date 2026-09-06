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
make -C /lib/modules/$(uname -r)/build M=$(pwd) modules
sudo insmod xiofs.ko
sudo mount -t xiofs -o host=storage.example,port=1094,path=/export none /mnt/xiofs
```

Until a kTLS socket is imported, I/O returns `-ENOTCONN`.

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

Do **not** extract TLS keys and implement a private record layer.

UAPI: `xiofs_uapi.h`. Match `host`, `port`, and `path` to the mount.

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
encoding; XrdHttp sends `Content-Length`.

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

## Explicitly not done

- HTTP/2 in-kernel (HPACK / streams / flow control)
- Connection recovery / reconnect
- Chunked responses
- Byte-range locks, hard links
- `writeback_iter` error retry / congestion
- RDMA / GPU-direct (`XIOFS_IOC_GPU_READ` returns `-EOPNOTSUPP`)
- Handshake agent binary (userspace; ioctl only)

## License

Linux kernel symbols require a GPL-compatible module license.
Files in this directory are **GPL-2.0**. Userspace under `../http` and
`../fuse` stays **LGPL-3.0**.
