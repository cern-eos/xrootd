# XrdHttpS3

S3 on the XrdHttp port. The plugin is an `http.exthandler`. It is loaded with
the library line and stays idle until `s3.switch on`. Native HTTP, WebDAV, and
TPC keep the same port. A request is S3 only when it carries
`Authorization: AWS4-HMAC-SHA256` or the query parameter `X-Amz-Algorithm`.
Presigned URLs are claimed and verified as SigV4 query authentication.

```cfg
xrd.protocol XrdHttp libXrdHttp.so

http.exthandler xrds3 +notls libXrdHttpS3.so

s3.switch on
s3.region us-east-1
s3.sysmeta /s3meta
s3.userprefix /prefix
s3.userhash first
s3.skew 15m
s3.metacache 30s
s3.metaentity s3meta
s3.trace info
```

`s3.switch` defaults to off. `+notls` lets the handler load on a plain HTTP
port. SigV4 does not encrypt the secret, so production should use HTTPS.
There are four external-handler slots; S3 uses one.

## Namespace

User `foo` with `s3.userhash first`:

```text
/prefix/f/foo/.s3/keys/<access-key-id>
/prefix/f/foo/.s3/buckets/<bucket>
/prefix/f/foo/<bucket>/<object-key>
```

`s3.userhash none` drops the one-character directory. A key that ends in `/`
is stored as `.s3folder` inside that directory. `.s3` is never listed as an
object.

The system index avoids scanning every user on each request:

```text
/s3meta/keys/<access-key-id>
/s3meta/buckets/<bucket>
```

Access key file:

```text
secret=<hmac secret>
user=foo
status=active
expires=0
```

`expires` is a unix time, or `0` for no expiry. The same secret is stored
under the user's `.s3/keys/` tree. v1 keys are provisioned by writing these
files. Bucket file:

```text
owner=foo
created=<unix time>
acl=private
region=us-east-1
```

`CreateBucket` writes both bucket records and the data directory. Object
ETags are the MD5 of the bytes, kept in `.s3/objmeta/` and, when the
filesystem allows it, as `user.s3.etag` / `user.s3.content-type`.

Data-plane calls run as the bucket owner: the security entity name is the
access-key user with anything after `@` removed, and `uid` stays unset so
`XrdFsOss` can map that name with `getpwnam`. The bucket directory is created
as that user when the parent allows it, and otherwise as the meta identity
followed by a best-effort `chown` to the passwd uid. `acc.authdb` still applies
to object paths. Meta reads and writes run as `s3.metaentity` (the daemon, when
that directive is empty). Hide the control plane from ordinary HTTP clients:

```text
u s3meta /s3meta a
u * /s3meta -n
u * /*/.s3 n
```

Adjust the names to the site's authdb. The plugin will not serve `.s3` or
`/s3meta` as objects, but native HTTP on the same port still can.

## v1 operations

ListBuckets, CreateBucket, DeleteBucket (empty only), HeadBucket, PutObject,
GetObject (one byte range, `x-amz-meta-*`, and `If-Match` / `If-None-Match` /
`If-Modified-Since` / `If-Unmodified-Since`), HeadObject, DeleteObject, and ListObjectsV2
(`list-type=2`, `prefix`, `delimiter`, `max-keys`, `continuation-token`).
A listing without `list-type`, or with `list-type=1`, is ListObjects v1
(`marker`, `NextMarker`).

Header SigV4 accepts a payload hash, `UNSIGNED-PAYLOAD`,
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (aws-chunked, one signature per chunk),
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`, and
`STREAMING-UNSIGNED-PAYLOAD-TRAILER`. HTTP `Transfer-Encoding: chunked` is
decoded for PutObject and UploadPart.
Presigned query URLs are accepted for the same operations. Every `x-amz-*`
header on a signed request has to be listed in `SignedHeaders`.

```cfg
s3.vhost s3.example.com
```

With that suffix, `Host: bucket.s3.example.com` selects the bucket and the
URL path is the object key. Path-style `Host` values that do not end in the
suffix stay `/<bucket>/<key>`.

Also implemented: `CopyObject` (`x-amz-copy-source`), `DeleteObjects`
(`POST /bucket?delete`, `Content-MD5` required), and multipart upload
(`POST ?uploads`, `PUT ?partNumber&uploadId`, `PUT` with `x-amz-copy-source`
to copy a byte range into a part, `POST ?uploadId`,
`DELETE ?uploadId`, `GET ?uploadId` to list parts, and `GET ?uploads` to
list uploads in a bucket). The multipart ETag is the MD5 of the part MD5s plus the
part count. Part files live under the user's `.s3/uploads/` tree.
`GET ?acl` returns the stored ACL. The default is a private `FULL_CONTROL`
grant for the owning user. `PUT` accepts the canned values `private`,
`public-read`, `public-read-write`, `authenticated-read`, `bucket-owner-read`,
`bucket-owner-full-control`, and `aws-exec-read`, or `x-amz-grant-*` headers, or
an `AccessControlPolicy` document with `Content-MD5`. A canned ACL and grant
headers together are rejected. GetObject accepts `response-content-type`,
`response-content-disposition`, `response-cache-control`,
`response-content-encoding`, `response-content-language`, and `response-expires`.
`GET /bucket?location` returns the configured region. For `us-east-1` the
constraint element is empty, matching S3.

`PUT`, `GET`, and `DELETE` of `?cors`, `?tagging`, and `?policy` store the
document on the bucket. `GET ?notification` returns an empty configuration
when none is stored, and `GET ?requestPayment` defaults to `BucketOwner`.
Object `?tagging` is stored beside the object; a missing tag set reads back
as an empty `TagSet`. `Content-MD5` is required on these `PUT`s. `HEAD` of a
multipart part (`?uploadId&partNumber`) returns that part's `ETag` and size.
A browser `POST` with `multipart/form-data` is accepted when the policy
signature, expiration, bucket, and key conditions match. `OPTIONS` with an
`Origin` header is answered from the bucket CORS rules, and the same
`Access-Control-Allow-Origin` headers are added to later responses that match
a rule.

`PUT` and `GET` of `?versioning` turn versioning on or suspend it. While it is
enabled, each `PUT` keeps the new bytes at the object path and stores that
generation under `.s3/versions/`. `GET` and `DELETE` accept `versionId`. A
delete without a version id writes a delete marker and removes the live file.
`GET ?versions` lists the generations. `CopyObject` accepts `versionId` on
`x-amz-copy-source`.

`PUT` and `GET` of `?website` store the index document, error document, and
routing rules. `s3.website` is a DNS suffix: `Host: bucket.<suffix>` is an
anonymous read of that bucket. A key that is a directory redirects to the
index suffix, and a missing key returns the error document. An archived object
is skipped, so the error document is used instead of reading it off tape.

An object is `GLACIER` when `stat` reports it offline (`XRDSFS_OFFLINE` in
`st_rdev`, or `st_dev` and `st_ino` both zero). `HEAD` still returns its size
and `ETag`, with `x-amz-storage-class: GLACIER`. `GET`, `CopyObject`, and
`UploadPartCopy` of that object return **403 InvalidObjectState** until it
has been restored. `POST ?restore` with a `RestoreRequest` (`Days`, and an
optional `Tier` of `Expedited`, `Standard`, or `Bulk`) records the recall on
the live file and asks the filesystem to stage it:

* `user.s3.restore-ongoing` is `true` while the offline bit is still set, and
  `false` once the file is readable again
* `user.s3.restore-expiry` is the HTTP date `Days` after the request

The response is **202** after `prepare` is called with `Prep_STAGE`. `Bulk`
uses `Prep_PRTY0`, `Standard` uses `Prep_PRTY1`, and `Expedited` uses
`Prep_PRTY3`. A second restore while the recall is still running returns
**409 RestoreAlreadyInProgress**. Once `stat` shows the file online, `GET`
returns the bytes and `x-amz-restore` carries `ongoing-request="false"` with
the stored expiry. Extending that period overwrites the expiry attribute and
returns **200**. After the expiry, `GET` is refused and `prepare` is called
with `Prep_EVICT`. The two attributes stay until a later `stat` shows the
file offline again, so the object does not flip back to `STANDARD` while the
disk copy is still present. A `PUT`, copy, multipart complete, or form `POST`
that replaces the object removes both attributes. `ListObjects` reports
`GLACIER` for an offline or restored object. Version copies under
`.s3/versions` are ordinary files; the tape state applies to the live object.
A `RestoreRequest` that selects part of an object is not implemented.

Lifecycle, logging, and replication are not implemented. Completing a multipart
upload still copies part bytes into the object.
