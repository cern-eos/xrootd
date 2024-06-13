# 1 JCache Client Plugin
<img width="128" alt="Screenshot 2024-06-11 at 16 14 16" src="https://github.com/cern-eos/xrootd/assets/2655845/5915ac77-675d-4b5e-a3fc-9cce2f9f1955">

This XRootD Client Plugin provides a client side read cache, which is implemented as a journal.


# Plug-in Configuration
To enable the plugin create a configuration file e.g. in the default machine wide location */etc/xrootd/client.plugin.d/jcache.conf* or enable in a one-liner using an environment variable:

## Quicksetup via Environment
```
mkdir -p /var/tmp/jcache/
env XRD_PLUGIN_1="lib=libXrdClJCachePlugin-5.so,enable=true,url=*,cache=/var/tmp/jcache/,size=10000000000" xrdcp root://... /localpath/... 
```
## Config File Format

**jcache.conf:**
```bash
url = *
lib = /usr/lib64/libXrdClJCachePlugin-5.so
enable = true
cache = /var/tmp/jcache/
journal = true
summary = true
stats = 0
size = 0
json =

```
```cache``` points to a local or shared directory where accessed files are stored. This directory has to exist and the configuration path should be terminated with a '/'. 

> [!TIP]
> By default JCache prints a summary at application exit. If you don't want the summary set ```summary = false```. 

> [!TIP]
> By default JCache does not write a JSON summary.  If you want JSON summaries define a directory whare stored change ```json = /tmp/```. If you don't want any json summary file use the defualt or set it to an empty string. The name of the json summary file is ```jcache.env{"XRD_APPNAME"}:"none".{pid}.json```

> [!TIP]
> If you want to run the plug-in inside a proxy server, you can set ```stats = 60```. In this case it will print every 60s a summary with the current cache statistics. The default is not print in intervals the cache statistics (```stats = 0```).

> [!NOTE]  
> The easiest way to verifyt the plug-in functionning is to run with ```XRD_LOGLEVEL=Info``` since the plug-in will provide
> some startup information and also prints where a file is cached locally once it is attached.

### Overwriting Configuration using Environment Variables
It is possible to overwrite defaults or settings in the configuration file using the following environment variables:
```
XRD_JCACHE_SUMMARY=true|false
XRD_JCACHE_JOURNAL=true|false
XRD_JCACHE_CACHE=directory-path-to-cache
XRD_JCACHE_JSON=directory-path-for-json|""
XRD_JCACHE_SIZE=number-in-bytes
XRD_APPNAME=application-name-used-in-json-file
```
> [!TIP]
> These are in particular useful, if you want to configure the plug-in using the default mechanism or want to overwrite some default settings in your environment without changing configuration fiels.
> ```mkdir -p /var/tmp/jcache/; env XRD_PLUGIN=libXrdClJCachePlugin-5.so XRD_JCACHE_CACHE=/var/tmp/jcache/ xrdcp ```

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

When a file is opened for reading and a journal exists, the journal is scanned on startup and stored in a red-black tree structure. If in subsequent readings new pages are requested, the journal is appended accordingly and supports even re-writing and merging of chunks (which is not required for read-only caching).

> [!IMPORTANT]  
> Requests are served by journal contents only, if they can be fully satisfied. As an example if a read requires more data than available in the journal, the read triggers a full remote read operation.

All **Read**, **PgRead** and **ReadV** requests are stored into the journal in the order they appeared from the first application run. As a result a repeated run of the same application creates perfect sequential IO when reading the journal.

> [!IMPORTANT]
> When several clients attach to a local journal cache or to a journal stored on a shared filesystem, the first client creates an exclusive lock on the journal. As a result client attaching later to the same journal in use will not get data served from the journal cache. This behaviour can be optimized in the future.

# 3 Cache Hit Rate

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

# 4 Application Exit Summary

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
# JCache : app user time            : 1930.11 s
# JCache : app real time            : 86.56 s
# JCache : app sys  time            : 54.17 s
# JCache : app acceleration         : 22.30x
# JCache : app readrate             : 788.90 MB/s [ peak 1.91 GB/s ]
# ----------------------------------------------------------- #
 1928.52 MB/s |        *                                
 1714.24      |                                         
 1499.96      |         *                               
 1285.68      |          ***                            
 1071.40      |             *  **  **  * *  *  *        
  857.12      |       *       *  *    *         * *     
  642.84      |                   *  *  * ** *   *      
  428.56      |                               *    * * *
  214.28      |              *                      *   
    0.00      | ******                                * 
               ----------------------------------------
               0   10  20  30  40  50  60  70  80  90  [ 100 % = 86.56s ]
```

Most of these fields are self explanatory. The field *readvread* are the number of individual read requests which are contained inside all *readv* IO operations. The statistics shows the total number of files opened for read (only!) and the unique files. To distinguish unique files the CGI information and named connections are removed. The percentage of a dataset read is computed by adding all read bytes from *read/pgread* + *readv* normalized to the total filesize of all unique files opened for reading. The application acceleration is simply the ratio of cputime over realtime. The application IO rate is computed from total read bytes over realtime in MB/s.

The ASCII plot shows the IO request rate over time. The total runtime (REAL time) is divided into 40 equal bins and in each bin the data requested is plotted. 

# 5 JSON Summary File

As mentioned in the configuration section JCache writes by default a summary file under the current working directory. The prefix can be changed in the plug-in configuration. If the prefix is empty, the JSON summary is disabled.

# 6 Cache-Cleaning

For the time being the levelling of the cache directory is not part of the Client plug-in. A simple cleaner executable is provides:
```
xrdclcacheclean 
Usage: xrdclcacheclean <directory> <highwatermark> <lowwatermark> <interval> 
```

# 7 JCache in a Proxy server

To run a proxy server with JCache you create a usual proxy configuration file:
```
pss.origin xrootd.cern.ch
all.export /xrootd/
ofs.osslib libXrdPss.so
```
Before startup you should configure the JCache plugin:
```
/etc/xrootd/client.plugins.d/jcache.conf:
url = root://*
lib = libXrdClJCachePlugin-5.so
enable = true
cache = /var/tmp/jcache/
```
You have to make sure, that the cache directory exists and is owned by the user running your XRootD process.

For interactive testing a quick startup of a server without client plugin configuration would look like this:
```
mkdir -p /var/tmp/jcache/
chown daemon:daemon /var/tmp/jcache/
env XRD_PLUGIN=/usr/lib64/libXrdClJCachePlugin-5.so XRD_JCACHE_CACHE=/var/tmp/jcache/ xrootd -c jcache.cf -Rdaemon -p 8443
```







# 8 To-Do List
- ~~Pre-shard cache directory structure not to have all cached files in a flat directory listing~~ (won't do)
- ~~Add async response handler to allow fully asynchronous open through the cache~~ (not required, already fully asynchronous)
- Add optional dynamic read-ahead with window scaling
- ~~Add cache-cleaning as option to client plug-in~~
- Make xrdclcacheclean a daemon with systemd support
- Add automatix connection de-multi-plexing if contention to storage servers is detected
- Attaching large files which are not in the buffercache is slow. We should write a compacted journal index, when we detach and read it on attach in a single read
- We should make a configuration variable, which allows to regularily dump cache statistics and reset counters. This is useful when the plug-in runs inside a proxy server.
























