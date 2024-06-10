# 1 JCache Client Plugin

This XRootD Client Plugin provides a client side read cache. 

There are two ways of caching, which can be configured individually:
1. **Read Journal Cache** (journalling)
2. **Vector Read Cache** (vector read responses are stored in binary blobs)

# Plug-in Configuration
To enable the plugin create a configuration file e.g. in the default machine wide location */etc/xrootd/client.plugin.d/jcache.conf*

## Config File Format

**jcache.conf:**
```bash
url = *
lib = /usr/lib64/libXrdClJCachePlugin-5.so
enable = true
cache = /var/tmp/jcache/
vector = false
journal = true
```
**cache** points to a local or shared directory where accessed files are stored. 

# 2 Read Journal Cache

Each URL accessed for reading creates a journal file under the following path:
```
<config:cache>/<hex::sha256(URL)>/journal
```
E.g */var/tmp/jcache/c04250faea5ae18d9a0024148da4c798852ee6b198848b2abd2ba8293b64af8e/journal*

The format of the journal file starts with the following header structure (in little endian format):
```
  struct jheader_t {
    uint64_t magic;        // 0xcafecafecafecafe
    uint64_t mtime;        // unix timestamp
    uint64_t mtime_nsec;   // 0: XRootD does not support finegrained modification times
    uint64_t filesize;     // bytes
    uint64_t placeholder1; // unused
    uint64_t placeholder2; // unused
    uint64_t placeholder3; // unused
    uint64_t placeholder4; // unused
  };
```

Each application read request is then written in an append log style using a chunk header struct:
```
  struct header_t {
    uint64_t offset;
    uint64_t size;
  };
```
followed by a data blob of ```<size>``` bytes.

The advantage of the journal approach is that unlike in a page cache like XCACHE, only requested data is cached! There is no such thing as pagesize.

When a file is opened for reading and a journal exists, the journal is scanned on startup and stored in a red-black tree structure. If in subsequent readings 
new pages are requested, the journal is append accordingly and supports even re-writing and merging of chunks (which mostly not required for this use case).
Requests are served by the journal only, if they can be fully satisfied.


All **Read**, **PgRead** and **ReadV** requests are stored into the journal in the order they appeared from the first application run.

# 3 Vector Read Cache
If the vector read cache is enabled in the configuration file, **ReadV** requests are stored/retrieved using a vector blob for each ReadV request.
The prefix path of these blob files is the same as the journal location, but all the chunk offsets and lengths are hashed into an SHA256 filename to identity 
a unique vector read request e.g.:

```/var/tmp/jcache/c04250faea5ae18d9a0024148da4c798852ee6b198848b2abd2ba8293b64af8e/7daf70f7f2fb3fff224623bcdbd111fe76dc0633f08ff83f0bcd6a443b1398eb```

The vector read cache does not provide any additional benefit to the journal cache approach. Conceptionally a cached vector read request can be served by a single read request on the caching device. If vector reads are only slightly modified a new vector blob entry is stored, which is inefficient if the read pattern is changing. For the time being the vector cache implementation is kept. 

# 4 Cache Hit Rate

When running in INFO logging mode, the plug-in provides some informative messages:

```
[2024-06-07 15:51:09.121100 +0200][Info   ][App               ] JCache : cache directory: /var/tmp/jcache/
[2024-06-07 15:51:09.121215 +0200][Info   ][App               ] JCache : caching readv in vector cache : true
[2024-06-07 15:51:09.121219 +0200][Info   ][App               ] JCache : caching reads in journal cache: true
[2024-06-07 15:51:09.163481 +0200][Info   ][App               ] JCache : attached to cache directory: /var/tmp/jcache//c04250faea5ae18d9a0024148da4c798852ee6b198848b2abd2ba8293b64af8e/journal
[2024-06-07 15:51:09.534252 +0200][Info   ][App               ] JCache : read:readv-ops:readv-read-ops: 194:0:0s hit-rate: total [read/readv]=100.00% [100.00%/100.00%] remote-bytes-read/readv: 0 / 0 cached-bytes-read/readv: 1626911438 / 0
```

```read:readv-ops:readv-read-ops```
- number of read operations, number of vector read operations, number of read operations contained in vector read operations

```hit-rate total [read/readv]```
- percentual hit-rate of all read requests, of simple read requests, of vector read requests served from the cache

```remote-bytes-read/readv```
- bytes read remote for simple read or vector read requests

```cached-bytes-read/readv```
- bytes read from the cache for simple read or vector read requests

# 5 Application Exit Summary

When an application exits, the globally collected JCache statistics for this application is printed. It is summing all individual file IO statistics passing through the plug-in.

```
# ----------------------------------------------------------- #
# JCache : cache combined hit rate  : 100.00 %
# JCache : cache read     hit rate  : 100.00 %
# JCache : cache readv    hit rate  : 100.00 %
# ----------------------------------------------------------- #
# JCache : total bytes    read      : 687778082
# JCache : total bytes    readv     : 67595537046
# ----------------------------------------------------------- #
# JCache : total iops     read      : 5540
# JCache : total iops     readv     : 21588
# JCache : total iops     readvread : 212418
# ----------------------------------------------------------- #
# JCache : open files     read      : 923
# JCache : open unique f. read      : 796
# ----------------------------------------------------------- #
# JCache : total unique files bytes : 1053908070952
# JCache : total unique files size  : 1.05 TB
# JCache : percentage dataset read  : 6.48 %
# ----------------------------------------------------------- #
# JCache : app user time            : 1883.94 s
# JCache : app real time            : 87.82 s
# JCache : app sys  time            : 73.13 s
# JCache : app acceleration         : 21.45x
# JCache : app readrate             : 777.55 MB/s
# ----------------------------------------------------------- #
 1478.73 MB/s |         *                               
 1314.53      |                                         
 1150.33      |                                         
  986.13      |             *                           
  821.93      |     *                   *   *           
  657.72      |                 *   *           *       
  493.52      |                                     *   
  329.32      |                                         
  165.12      |                                         
    0.92      | *                                       
               ----------------------------------------
                0   1   2   3   4   5   6   7   8   9 [ 10 :=87.82s ]
```

Most of these fields are self explanatory. The field *readvread* are the number of individual read requests which are contained inside all *readv* IO operations. The statistics shows the total number of files opened for read (only!) and the unique files. To distinguish unique files the CGI information and named connections are removed. The percentage of a dataset read is computed by adding all read bytes from *read/pgread* + *readv* normalized to the total filesize of all unique files opened for reading. The application acceleration is simply the ratio of CPU time over REAL time. The application IO rate is computed from total read bytes over the REAL time in MB/s.

The ASCII plot shows the IO request rate over time. The total runtime (REAL time) is divided into 10 equal bins and in each bin the data requested is plotted. 

# 6 Cache-Cleaning

For the time being the levelling of the cache directory is not part of the Client plug-in. A simple cleaner executable is provides:
```
xrdclcacheclean 
Usage: xrdclcacheclean <directory> <highwatermark> <lowwatermark> <interval> 
```

# 7 To-Do List
- ~~Pre-shard cache directory structure not to have all cached files in a flat directory listing~~ (won't do)
- ~~Add async response handler to allow fully asynchronous open through the cache~~ (not required, already fully asynchronous)
- Add optional dynamic read-ahead with window scaling
- Add cache-cleaning as option to client plug-in
- Make xrdclcacheclean a daemon with systemd support
- Add automatix connection de-multi-plexing if contention to storage servers is detected
























