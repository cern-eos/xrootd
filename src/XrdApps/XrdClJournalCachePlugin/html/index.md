# JournalCache

Client-side read cache for XRootD. Files are cached as append-only journals on local disk, with optional directory/stat caching and HTTP freshness semantics for proxy deployments.

See also the [plugin README](../README.md) for journal format details, statistics, and the full to-do list.

**Table of contents:** [Overview](#overview) · [Components](#components) · [Configuration](#configuration) · [Journal layout](#journal-on-disk-layout) · [Filesystem cache](#filesystem-cache) · [HTTP cache](#http-cache-integration) · [Proxy setup](#proxy-and-http-front-end-setup) · [Cleaning](#cache-cleaning) · [Statistics](#statistics-and-monitoring) · [Serving docs](#serving-this-documentation)

---

## Overview

JournalCache is an XRootD **client plugin** that intercepts read I/O and stores only the byte ranges your application actually requests. Unlike a fixed page cache, there is no page size and no read amplification beyond what the workload asks for.

| Feature | Description |
|---------|-------------|
| **Read, PgRead, VectorRead** | Cached on miss; served from the local journal on hit when the full request is covered. |
| **Per-file journals** | Each remote file gets a `journal` file under the configured cache directory. |
| **DirList & Stat** | Optional filesystem plugin caches listings and standalone stat responses. |
| **HTTP semantics** | Cache-Control, Expires, ETag, and validators steer invalidation when used behind XrdHttp. |

> **Tip:** Run with `XRD_LOGLEVEL=Info` to see cache directory setup, journal attach paths, and per-file hit rates in the log.

---

## Components

### 1. File plugin (`libXrdClJournalCachePlugin`)

Loaded by the XRootD client (xrdcp, root, applications using XrdCl, and the PSS client inside a proxy). Handles open/read for individual files and maintains journals.

### 2. Filesystem plugin (same library, `system = true`)

Caches `DirList` and optional `Stat` results on disk. Invalidates on namespace mutations (rm, mkdir, mv, truncate, xattr changes).

### 3. HTTP ext handler (`libXrdClJournalCacheHttpExt`)

Optional. Runs inside XrdHttp before GET/HEAD processing. Injects JournalCache CGI parameters into the backend open URL from client validators, file xattrs, and/or an HTTP origin HEAD request.

Build with `BUILD_HTTP=ON` and `XRDCL_ONLY=OFF`. Optional libcurl enables origin `HEAD` lookups.

### Data flow as an HTTP cache

```
HTTP client → XrdHttp → JournalCache HTTP ext → PSS / XRootD client
            → JournalCache file plugin → Origin
```

On a cache hit, reads are satisfied from the local journal without contacting the origin. Freshness rules (max-age, no-cache, ETag mismatch, etc.) decide whether the journal is reused or purged first.

---

## Configuration

### Client plugin file

Typical location: `/etc/xrootd/client.plugin.d/journalcache.conf`

```ini
url = *
lib = /usr/lib64/libXrdClJournalCachePlugin-5.so
enable = true
cache = /var/tmp/journalcache/
journal = true
crc = false
summary = true
stats = 0
size = 0
async = 0
bypass = 0
flat = false
basepath =
system = true
listttl = 0
liststat = true
noapp =
json =
```

| Key | Description |
|-----|-------------|
| `cache` | Local cache root directory (must exist; trailing `/` recommended). |
| `journal` | Enable file journal caching (default on). |
| `crc` | CRC32c checksum per journal fragment (format v2). |
| `async` | Detached open using journal mtime/size only — for WORM data. |
| `bypass` | Collect statistics but do not read from or write to cache. |
| `flat` | SHA-256 URL hash layout instead of mirroring remote paths. |
| `basepath` | Strip federation prefix (e.g. `/store/`) from cache keys. |
| `system` | Enable filesystem listing/stat cache plugin. |
| `listttl` | Listing/stat TTL in seconds (`0` = no time expiry). |
| `liststat` | Cache standalone stat responses. |
| `size` | Cache high-water mark for built-in cleaner (bytes, min 1 GB). |
| `stats` | Periodic stats interval in seconds (`0` = exit summary only). |
| `noapp` | Comma-separated app names forced into bypass mode. |
| `json` | Directory for JSON summary files (empty disables). |

### Quick start with environment variables

```bash
mkdir -p /var/tmp/journalcache/

# Default plugin for one command
env XRD_PLUGIN=libXrdClJournalCachePlugin-5.so \
    XRD_JOURNALCACHE_CACHE=/var/tmp/journalcache/ \
    xrdcp root://host//path/file.root /tmp/

# Numbered plugin entry
env XRD_PLUGIN_1="lib=libXrdClJournalCachePlugin-5.so,enable=true,url=*,cache=/var/tmp/journalcache/" \
    xrdcp root://host//path/file.root /tmp/
```

### Environment overrides

```bash
XRD_JOURNALCACHE_CACHE=/var/tmp/journalcache/
XRD_JOURNALCACHE_JOURNAL=1
XRD_JOURNALCACHE_CRC=0
XRD_JOURNALCACHE_ASYNC=0
XRD_JOURNALCACHE_BYPASS=0
XRD_JOURNALCACHE_FLAT=0
XRD_JOURNALCACHE_BASEPATH=/store/
XRD_JOURNALCACHE_SYSTEM=1
XRD_JOURNALCACHE_LISTTTL=3600
XRD_JOURNALCACHE_LISTSTAT=1
XRD_JOURNALCACHE_SIZE=10000000000
XRD_JOURNALCACHE_SUMMARY=1
XRD_JOURNALCACHE_JSON=/tmp/
XRD_JOURNALCACHE_NOAPP=xrdcp,eoscp
XRD_APPNAME=my-app
```

---

## Journal on-disk layout

### Default path layout

```
<cache>/<remote-path-tree>/journal
```

Example: `/var/tmp/journalcache/data/higgs.root/journal`

### Flat layout (`flat = true`)

```
<cache>/<sha256(URL)>/journal
```

### Journal file format

```c
struct jheader_t {
  uint64_t magic;        // 0xcafecafecafecafe
  uint64_t mtime;
  uint64_t mtime_nsec;
  uint64_t filesize;
  uint64_t placeholder1; // format: 1=legacy, 2=crc32c
  uint64_t placeholder2;
  uint64_t placeholder3;
  uint64_t placeholder4;
};

struct header_t {
  uint64_t offset;
  uint64_t size;
};
// followed by <size> bytes of data
// optional uint32_t crc32c when crc mode is enabled
```

> **Partial coverage:** a read is served from cache only if the journal contains the *entire* requested range. Otherwise the plugin falls back to a remote read.

> **Shared journals:** the first client holds an exclusive lock on a journal file. Concurrent attach to the same journal on shared storage may not serve from cache until the lock is released.

---

## Filesystem cache

When `system = true`, directory listings and stat results are stored beside the journal layout:

```
<cache>/<dir>/.journalcache_list[.<flags>]
<cache>/<path>/.journalcache_stat
```

- Plain `Stat` listings are cached; recursive, chunked, and ZIP listings are not.
- `listttl` expires entries after N seconds.
- Mutations (rm, mkdir, rmdir, mv, truncate, xattr) invalidate affected listing/stat caches.
- `bypass = true` disables filesystem caching as well.

---

## HTTP cache integration

JournalCache understands HTTP cache semantics via CGI parameters on file URLs. The file plugin stores them as xattrs on the local journal and applies them when attaching to a cached file.

### CGI parameters and journal xattrs

| CGI key | Journal xattr | HTTP header / role |
|---------|---------------|-------------------|
| `xrd.journalcache.cache-control` | `user.journalcache.cache-control` | Setter — `Cache-Control` from origin |
| `xrd.journalcache.expires` | `user.journalcache.expires` | Setter — `Expires` |
| `xrd.journalcache.etag` | `user.journalcache.etag` | Getter/setter — `ETag` |
| `xrd.journalcache.last-modified` | `user.journalcache.last-modified` | Getter/setter — `Last-Modified` |
| `xrd.journalcache.if-none-match` | *(not stored)* | Validator — client `If-None-Match` |
| `xrd.journalcache.if-modified-since` | *(not stored)* | Validator — client `If-Modified-Since` |
| `xrd.journalcache.bypass` | *(not stored)* | Per-file bypass (`1`/`true`/`yes`) |
| `xrd.journalcache.clean` | *(not stored)* | Force-delete local journal before attach |
| `xrd.journalcache.async` | *(not stored)* | Per-file async/detached open (`1` = WORM attach) |

All stored headers also record `user.journalcache.cached-at` (unix time when setter headers were written).

When `ETag` / `Last-Modified` are not supplied via CGI, the file plugin derives them from the remote `Stat` response when available.

### Cache-Control behaviour

| Directive | Effect |
|-----------|--------|
| `no-store` | Do not cache this file. |
| `no-cache` | Require a remote `Stat` before serving cached bytes this session. |
| `max-age` / `s-maxage` | Purge stale journal data after freshness interval (`s-maxage` preferred). |
| `Expires` | Purge when current time ≥ parsed expiry date. |
| ETag / Last-Modified mismatch | Purge when client validators do not match stored values. |

### Example URL with setter header

```
root://host//data/file.root?xrd.journalcache.cache-control=s-maxage%3D3600%2Cpublic
```

### Per-file CGI toggles

```
# Async/detached open for this file only (WORM)
?xrd.journalcache.async=1

# Skip journal read/write for this file only
?xrd.journalcache.bypass=1

# Delete local journal before attach
?xrd.journalcache.clean=1
```

### XrdHttp server configuration

```ini
xrd.protocol http:/usr/lib64/libXrdHttp.so

http.header2cgi If-None-Match xrd.journalcache.if-none-match
http.header2cgi If-Modified-Since xrd.journalcache.if-modified-since

http.exthandler journalcache libXrdClJournalCacheHttpExt-5.so \
  /etc/xrootd/journalcache-http.ext.conf
```

The ext handler matches GET and HEAD only. When `cache` is set (matching the client plugin `cache=` path), it loads getter headers from the on-disk journal, returns **304 Not Modified** when validators match and the entry is fresh, and emits `ETag`, `Last-Modified`, `Cache-Control`, and `Expires` on **200**/**HEAD** responses. Otherwise it returns `XrdHttpExtContinueProcessing` after appending CGI.

### HTTP ext handler config

File: `journalcache-http.ext.conf` (see plugin `http/` directory)

```ini
server = root://localhost:1094
cache = /var/tmp/journalcache/
flat = 0
basepath = /store
prefix = /
exclude = /static/
http_origin = https://origin.example.org
http_origin_strip = /store
# xattr http.cache-control xrd.journalcache.cache-control
# xattr http.expires xrd.journalcache.expires
# xattr http.etag xrd.journalcache.etag
# xattr http.last-modified xrd.journalcache.last-modified
```

The ext handler collects metadata from:

1. Client validation headers on the HTTP request
2. File xattrs on the XRootD path (`http.cache-control`, `http.etag`, …)
3. Optional HTTP `HEAD` to `http_origin` (requires libcurl at build time)

Origin files can publish cache policy by setting xattrs:

```bash
xrdfs setxattr /store/data/file.root http.cache-control 'public, s-maxage=3600'
xrdfs setxattr /store/data/file.root http.etag '"abc123"'
xrdfs setxattr /store/data/file.root http.last-modified 'Wed, 21 Oct 2015 07:28:00 GMT'
```

### HTTP forwarding proxy (dynamic origins)

For browser clients that embed the upstream URL in the path (`/https://host/path`), enable PSS forwarding and set `forwarding = 1` in the HTTP ext config. Example files: `http/journalcache-forwarding.cf`, `http/journalcache-http-forwarding.ext.conf`, `http/journalcache-forwarding-client.conf`.

```ini
# xrootd server
pss.origin =root,http,https
pss.permit /* .example.org
http.exthandler journalcache libXrdClJournalCacheHttpExt-5.so \
  /etc/xrootd/journalcache-http-forwarding.ext.conf

# ext handler
forwarding = 1
cache = /var/tmp/journalcache/
```

Load **XrdClHttp** alongside JournalCache in the xrootd process client plugins. PSS fetches via HTTP(S); the ext handler parses each embedded URL for journal lookup, 304, and response headers.

### Chained multi-origin URLs (`root://` and friends)

Clients can name a dynamic upstream inside a proxy URL:

```
root://proxy.example:1095//root://origin.cern.ch:1094//store/file.dat
```

PSS forwarding (`pss.origin =root,http,https`) also accepts path-embedded upstreams such as `/root://origin.cern.ch:1094//store/file.dat`.

| Key | Meaning |
|-----|---------|
| `multi_origin = 1` | Unwrap chained URLs to the inner upstream for open + journal cache key |
| `allow_origin = <regex>` | Allowed upstream patterns (comma-separated or repeated); matched against full URL, location, or host |

Environment overrides: `XRD_JOURNALCACHE_MULTI_ORIGIN`, `XRD_JOURNALCACHE_ALLOW_ORIGIN`.

The HTTP ext handler accepts the same `allow_origin` lines (repeatable in `.ext.conf`) and rejects disallowed upstreams with **403**.

```ini
multi_origin = 1
allow_origin = ^root://([a-z0-9.-]+\.)?cern\.ch(:1094)?/,^https://([a-z0-9.-]+\.)?example\.org/
```

On client-only deployments (no unwrap), omit `multi_origin` so chained URLs are passed through to the proxy unchanged.

---

## Proxy and HTTP front-end setup

### 1. PSS proxy configuration

```ini
pss.origin xrootd.example.org
all.export /store/
ofs.osslib libXrdPss.so
```

### 2. Client plugin for the proxy process

```ini
# /etc/xrootd/client.plugin.d/journalcache.conf
url = root://*
lib = libXrdClJournalCachePlugin-5.so
enable = true
cache = /var/tmp/journalcache/
stats = 60
```

### 3. XrdHttp with cache header forwarding

```ini
xrd.protocol http:/usr/lib64/libXrdHttp.so

http.header2cgi If-None-Match xrd.journalcache.if-none-match
http.header2cgi If-Modified-Since xrd.journalcache.if-modified-since

http.exthandler journalcache libXrdClJournalCacheHttpExt-5.so \
  /etc/xrootd/journalcache-http.ext.conf
```

### 4. Prepare cache directory

```bash
mkdir -p /var/tmp/journalcache/
chown xroot:xroot /var/tmp/journalcache/
```

### End-to-end HTTP GET (cache miss, then hit)

1. Client sends `GET /store/file.root` with optional `If-None-Match`.
2. XrdHttp ext handler appends JournalCache CGI to the backend open URL.
3. PSS opens the origin file through the JournalCache client plugin.
4. First read populates the journal; setter headers are stored as xattrs.
5. Subsequent GETs within freshness limits are served from the local journal.

> **Tip:** Set `stats = 60` in long-running proxies to print rolling hit-rate summaries every minute.

---

## Cache cleaning

Built-in cleaner (when `size` ≥ 1 GB): evicts oldest files by access time down to 90% of the limit, checked every minute.

```ini
# In journalcache.conf
size = 10000000000
```

```bash
# Or via environment
export XRD_JOURNALCACHE_SIZE=10000000000
```

Standalone cleaner for complex deployments:

```bash
xrdclcacheclean <directory> <highwatermark> <lowwatermark> <interval_seconds>
```

---

## Statistics and monitoring

### Per-file log line (Info level)

```
JournalCache : read:readv-ops:readv-read-ops: 194:0:0s hit-rate: total [read/readv]=100.00%
  remote-bytes-read/readv: 0 / 0 cached-bytes-read/readv: 1626911438 / 0
```

### Application exit summary

Printed when the process exits (unless disabled): combined hit rate, bytes read from cache vs remote, open counts, dataset coverage, and an ASCII IO rate histogram.

### JSON summary

When `json = /path/to/dir/` is set, a JSON file named `journalcache.<XRD_APPNAME>.<pid>.json` is written on exit.

---

## Serving this documentation

Host the `html/` directory with any web server, or expose it through XrdHttp static preload:

```ini
http.staticpreload /static/journalcache \
  /usr/share/xrootd/journalcache/html
```

Then open `/static/journalcache/index.html` or link to `index.md` in the repository.

---

*JournalCache — XRootD client plugin documentation*
