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

Data-plane calls run as the bucket owner, so `acc.authdb` still applies to
object paths. Meta reads and writes run as `s3.metaentity` (the daemon, when
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
GetObject (one byte range), HeadObject, DeleteObject, and ListObjectsV2
(`list-type=2`, `prefix`, `delimiter`, `max-keys`, `continuation-token`).
A listing without `list-type`, or with `list-type=1`, is ListObjects v1
(`marker`, `NextMarker`).

Header SigV4 accepts a payload hash, `UNSIGNED-PAYLOAD`, and
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (aws-chunked, one signature per chunk).
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
`GET /bucket?location` returns the configured region. For `us-east-1` the
constraint element is empty, matching S3.
