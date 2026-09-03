# JournalCache

<img width="25%" alt="journalcache" src="https://github.com/user-attachments/assets/738b1efc-038e-4249-97fb-cc660e6cabbc" />

XRootD **client read cache**. Only the byte ranges an application actually reads are stored, as an append-only per-file journal. There is no page size and no read amplification beyond the workload.

Use it in a desktop client (`xrdcp`, ROOT, any XrdCl app) or inside a proxy (PSS / XrdHttp). A forwarding proxy that accepts `/https://origin/path` can be bootstrapped with **`xjcd`**.

| | |
|---|---|
| **Browseable docs** | [html/index.html](html/index.html) |
| **Same text (Markdown)** | [html/index.md](html/index.md) |
| **Man page** | `docs/man/xjcd.1` |

```
Application / browser
        │
        ▼
  XrdHttp  (optional)  ── HTTP ext: 304, CGI, allowlist
        │
        ▼
  XrdCl + JournalCache file plugin
        │                    │
        │ hit               │ miss
        ▼                    ▼
   local journal         origin (then journal)
```

Run with `XRD_LOGLEVEL=Info` to see attach paths and hit rates.

---

## Quick start

### Client on a laptop

```bash
mkdir -p /var/tmp/journalcache/
export XRD_PLUGIN=libXrdClJournalCachePlugin-5.so
export XRD_JOURNALCACHE_CACHE=/var/tmp/journalcache/
xrdcp root://host:1094//store/file.root /tmp/file.root
```

Or a numbered plugin without changing the default:

```bash
export XRD_PLUGIN_1="lib=libXrdClJournalCachePlugin-5.so,enable=true,url=*,cache=/var/tmp/journalcache/"
```

### Forwarding cache proxy (`xjcd`)

TLS cert and key are required. Initial policy is **closed** (empty `allow_origin` denies chained origins) until you add rules with `xjc`.

```bash
xjcd init --journal /var/tmp/journalcache \
  --xroot-port 1094 --https-port 8443 \
  --tls-cert /etc/xrootd/tls.crt --tls-key /etc/xrootd/tls.key \
  --install-systemd          # root: install and enable xjcd + xjccleand

xjc --journal /var/tmp/journalcache allow-origin add '^https://.*\.example\.org/'
xjc --journal /var/tmp/journalcache cleaner enable on
xjc --journal /var/tmp/journalcache cleaner set high 10737418240
```

Clients then GET `https://proxy/https://cdn.example.org/store/file.dat`.

---

## What gets cached

| I/O | Behaviour |
|-----|----------|
| **Read** / **ReadV** | Served from the journal when the **entire** request is already present; otherwise fetched from the origin and appended. |
| **PgRead** | Always fetched from the origin (page checksums are not stored). Successful pages are still written into the journal for later **Read**. |
| **Write / Truncate / PgWrite** | Passed through; the journal for that file is reset. |
| **DirList / Stat** | Optional filesystem plugin (`system = true`). |

A journal is identified by the **on-disk path**, not the client URL. Two opens of the same file share one journal object. The first process to attach takes an exclusive POSIX lock; later processes on the same file fall back to the origin.

`..` in remote paths is collapsed. A layout that would escape the cache root is rewritten to a SHA256 directory under that root.

---

## On-disk layout

Default (hierarchical):

```
<cache>/<host>:<port>/<remote-path>/journal
<cache>/<host>:<port>/<dir>/.journalcache_list
<cache>/<host>:<port>/<path>/.journalcache_stat
```

With `flat = true`:

```
<cache>/<sha256(url+path)>/journal
```

With `basepath = /store/`, the host prefix is dropped and names start at `/store/…` (typical for federations; use `async = 1` and treat data as WORM).

Bootstrap and runtime files live under the cache root:

| Path | Role | Edit with |
|------|------|-----------|
| `$journal/.xjc/state.conf` | Ports, TLS, lib dir | `xjcd` / editor, then `xjcd render` |
| `$journal/.xjc/policy.conf` | bypass, allow_origin, redirects | **`xjc`** |
| `$journal/.xjc/cleaner.conf` | xjccleand watermarks | **`xjc cleaner`** |
| `$journal/.xjc/etc/` | Generated xrootd.cf, HTTP ext, client plugins, systemd units | `xjcd render` |

The cleaner never deletes `.xjc/`. Eviction only removes `journal`, `.journalcache_list*`, and `.journalcache_stat` files.

### Journal format

Little-endian header, then append-only fragments:

```
jheader_t { magic=0xcafecafecafecafe, mtime, mtime_nsec, filesize,
            placeholder1=version (1=legacy, 2=crc32c), … }
header_t  { offset, size } + data[size] [ + uint32 crc32c if v2 ]
```

`crc = true` writes version 2. Legacy journals stay readable. CRC is checked on load and on read; a bad fragment is skipped (treated as a miss).

HTTP freshness lives in journal xattrs: `user.journalcache.cache-control`, `expires`, `etag`, `last-modified`, `cached-at`.

---

## Configuration

Create `/etc/xrootd/client.plugins.d/journalcache.conf` (path must exist and end with `/`):

```ini
url = *
lib = /usr/lib64/libXrdClJournalCachePlugin-5.so
enable = true
cache = /var/tmp/journalcache/
```

### Plugin keys

| Key | Default | Meaning |
|-----|---------|---------|
| `cache` | *(required)* | Journal root. Trailing `/` recommended. |
| `journal` | `true` | Store and serve file journals. |
| `crc` | `false` | CRC32c trailers on new journals. |
| `async` | `false` | Detached open: attach from an existing journal without waiting for the remote open (WORM only). |
| `bypass` | `false` | Count I/O but do not store or serve. |
| `flat` | `false` | SHA256 directory per file instead of the remote tree. |
| `basepath` | empty | Strip host + prefix so journals start at this path. |
| `system` | `true` | Cache DirList (and Stat if `liststat`). |
| `listttl` | `0` | Listing/stat TTL in seconds; `0` = until invalidated. |
| `liststat` | `true` | Cache standalone Stat. |
| `size` | `0` | In-process cleaner high watermark (bytes). `0` = off. Minimum 1 GiB. |
| `stats` | `0` | Periodic summary interval in seconds (proxies). `0` = exit only. |
| `summary` | `true` | Print hit-rate summary on process exit. |
| `json` | empty | Directory for JSON summaries; empty disables. |
| `noapp` | empty | Comma-separated `XRD_APPNAME` values forced to bypass (`xrdcp,eoscp`). |
| `multi_origin` | `false` | Unwrap chained `root://proxy//root://origin//path` to the inner URL. |
| `allow_origin` | empty | Regex or hostname allowlist for unwrapped upstreams. Empty = deny chained origins. |
| `external_redirect` | empty | `prefix\|target,...` — open becomes a client redirect. |
| `policy` | `$cache/.xjc/policy.conf` | Hot-reloadable policy file. |
| `policy_poll` | from env | Policy mtime poll (seconds). |
| `demux` | `false` | Per-thread named connections (experimental). |

Environment overrides (same names, `XRD_JOURNALCACHE_*`):

```
XRD_JOURNALCACHE_CACHE  XRD_JOURNALCACHE_CRC  XRD_JOURNALCACHE_ASYNC
XRD_JOURNALCACHE_BYPASS XRD_JOURNALCACHE_FLAT XRD_JOURNALCACHE_BASEPATH
XRD_JOURNALCACHE_SYSTEM XRD_JOURNALCACHE_LISTTTL XRD_JOURNALCACHE_LISTSTAT
XRD_JOURNALCACHE_SIZE   XRD_JOURNALCACHE_STATS XRD_JOURNALCACHE_SUMMARY
XRD_JOURNALCACHE_JSON   XRD_JOURNALCACHE_NOAPP XRD_JOURNALCACHE_JOURNAL
XRD_JOURNALCACHE_MULTI_ORIGIN XRD_JOURNALCACHE_ALLOW_ORIGIN
XRD_JOURNALCACHE_EXTERNAL_REDIRECT XRD_JOURNALCACHE_POLICY XRD_JOURNALCACHE_POLICY_POLL
XRD_APPNAME
```

Recursive, chunked, and ZIP directory listings are never cached. Mutations (`Rm`, `MkDir`, `RmDir`, `Mv`, `Truncate`, xattr changes) invalidate listing/stat caches.

---

## HTTP freshness

CGI on the file URL (or injected by the HTTP ext handler):

| CGI | Role |
|-----|------|
| `xrd.journalcache.cache-control` | Origin `Cache-Control` |
| `xrd.journalcache.expires` | `Expires` |
| `xrd.journalcache.etag` | `ETag` (also filled from Stat checksum) |
| `xrd.journalcache.last-modified` | `Last-Modified` (also from Stat mtime) |
| `xrd.journalcache.if-none-match` | Validator — mismatch refreshes |
| `xrd.journalcache.if-modified-since` | Validator — newer object refreshes |
| `xrd.journalcache.async=1` | Per-file detached open |
| `xrd.journalcache.bypass=1` | Skip journal for this open |
| `xrd.journalcache.clean=1` | Delete the local journal before attach |

| `Cache-Control` | Effect |
|----------------|--------|
| `no-store` | Do not journal this file. |
| `no-cache` | Require a remote Stat this session before serving. |
| `private` | Do not store in the shared on-disk journal. |
| `max-age=N` / `s-maxage=N` | Stale `N` seconds after `cached-at` (`s-maxage` wins). |
| `Expires` | Stale at that HTTP date. |

Build the HTTP ext with `BUILD_HTTP=ON` and `XRDCL_ONLY=OFF`. libcurl enables origin `HEAD`.

```
xrd.protocol http:/usr/lib64/libXrdHttp.so
http.header2cgi If-None-Match xrd.journalcache.if-none-match
http.header2cgi If-Modified-Since xrd.journalcache.if-modified-since
http.exthandler journalcache libXrdClJournalCacheHttpExt-5.so \
  /etc/xrootd/journalcache-http.ext.conf
```

Example ext config (`http/journalcache-http.ext.conf`):

```ini
server = root://localhost:1094
cache = /var/tmp/journalcache/
flat = 0
basepath = /store
prefix = /
exclude = /static/
# http_origin = https://origin.example.org
# http_origin_strip = /store
```

Metadata order: client validators → file xattrs (`http.cache-control`, …) → optional `HEAD` to `http_origin`. On a fresh validator match the ext handler returns **304**; otherwise it continues XrdHttp processing and emits stored getter headers.

`policy.bypass` skips 304/CGI injection but still enforces `allow_origin`. Outbound HEAD does not follow redirects.

---

## Forwarding, allowlist, redirects

**Path-embedded origin** (browser or HTTP client):

```
GET /https://cdn.example.org/store/file.dat
GET /root://origin.cern.ch:1094//store/file.dat
```

**Chained XRootD URL** (N hops; unwrap with `multi_origin = 1`):

```
root://proxy//root://relay//root://origin.cern.ch:1094//store/file.dat
```

Allowlist matches the **fully unwrapped** URL. Patterns that look like URLs (`^root://…` or contain `://`) are regex-searched against the URL. Bare host patterns (`example.com`) must match the hostname exactly — `notexample.com` is not allowed.

Empty allowlist denies every chained origin. That is the `xjcd init` default. Add `allow_origin` via `xjc` before clients can fetch `/https://origin/path`. Also enable `pss.permit` in `xrootd.cf` (commented in the generated file).

External redirect (no journal; HTTP 302 or XrdCl `errRedirect`):

```ini
# HTTP ext (repeatable)
external_redirect = /live/ https://stream.example.org/live/

# Client plugin (comma + pipe)
external_redirect = /live/|https://stream.example.org/live/,/raw/|root://data//raw/
```

---

## Tools

### `xjc` — runtime policy and cleaner

```
xjc [--journal PATH] [--policy PATH] [--cleaner PATH] <command>
```

```bash
xjc --journal /var/tmp/journalcache show
xjc bypass off
xjc multi-origin on
xjc allow-origin add '^https://.*\.example\.org/'
xjc allow-origin list
xjc redirect add /live/ https://stream.example.org/live/

xjc cleaner show
xjc cleaner enable on
xjc cleaner set high 10737418240
xjc cleaner set interval 60
```

Policy is watched and reloaded on mtime change (`policy_poll`).

### `xjcd` — generate a systemd-managed proxy

```
xjcd [--journal PATH] init|render|show|validate
```

`init` writes `$journal/.xjc/` (open policy, `cleaner.conf` **disabled**, generated `xrootd.cf` + units). There is no `xjcd run`; systemd starts xrootd.

```bash
xjcd init --journal /var/tmp/journalcache \
  --xroot-port 1094 --https-port 8443 \
  --tls-cert /path/tls.crt --tls-key /path/tls.key \
  --install-systemd --systemd-unit xjcd.service --systemd-cleaner-unit xjccleand.service

xjcd show --journal /var/tmp/journalcache
xjcd validate --journal /var/tmp/journalcache
# after editing state.conf:
xjcd render --journal /var/tmp/journalcache
sudo systemctl restart xjcd.service xjccleand.service
```

### `xjccleand` — standalone evictor

Reads `$journal/.xjc/cleaner.conf` (hot-reloaded). The systemd unit is enabled on `xjcd init --install-systemd`, but cleaning stays **off** until `xjc cleaner enable on`.

```ini
enabled = 0
journal = /var/tmp/journalcache
high_watermark = 10737418240
low_watermark = 0          # 0 → 90% of high
interval = 60
config_poll = 2
```

```bash
xjccleand --journal /var/tmp/journalcache
# legacy:
xjccleand /var/tmp/journalcache 10737418240 0 60
```

Do not run the in-plugin cleaner (`size =`) and `xjccleand` against the same tree at once.

---

## In-process cleaner

`size = 10000000000` (or `XRD_JOURNALCACHE_SIZE`) starts a thread in the client/proxy process. High watermark is the configured size; low is 90%. Interval is 60 s. Same eviction rules as `xjccleand` (skip `.xjc/`, only journal/list/stat files). Disabled when `size` &lt; 1 GiB.

---

## Statistics

Exit summary (and `stats = N` interval dumps) reports hit rate, bytes, IOPS, unique files, open time, dataset fraction read, and an ASCII rate plot. Bypass mode replaces the hit-rate block.

JSON (if `json` is set): `journalcache.<XRD_APPNAME|none>.<pid>.json`.

---

## Manual proxy (no `xjcd`)

```
pss.origin xrootd.cern.ch
all.export /xrootd/
ofs.osslib libXrdPss.so
```

Client plugin for that xrootd process: `cache = …`, `stats = 60`. Directory must exist and be writable by the daemon user.

Forwarding mode: `pss.origin =root,http,https`, `pss.permit` for allowed hosts, HTTP ext `forwarding = 1`. Examples: `http/journalcache-forwarding.cf`, `http/journalcache-http-forwarding.ext.conf`.

---

## Tests

Unit tests live in `tests/XrdCl/XrdClJournalCache.cc` (`xrdcl-unit-tests`): journal/CRC, list/stat cache, headers, forwarding URLs, allowlist, policy reload, `xjcd` render.

```bash
./build/bin/xrdcl-unit-tests --gtest_filter='Journal*:ListCache*:CacheHeaders*:Forwarding*:Origin*:Policy*:Xjcd*:Cleaner*:CachePath*'
```

There is no live XRootD/HTTP integration suite in that binary.

---

## Limitations

- Shared journal: exclusive flock — a second process on the same file does not read the journal.
- Large journals: attach scans every fragment (no compacted index yet).
- `async` / federation `basepath`: no remote Stat on attach; stale data possible if the origin changes.
- Empty `allow_origin` denies chained/forwarded origins until you add rules (and `pss.permit`).
- Optional: dynamic read-ahead; automatic connection demux under contention.
