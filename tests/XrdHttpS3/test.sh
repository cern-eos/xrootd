#!/bin/bash
# Exercise the S3 plugin against a local OSS xrootd, then a second server
# with s3.switch off.
set -euo pipefail

if [ -z "${XROOTD:-}" ] || [ -z "${XRD_HTTP_LIB:-}" ] || [ -z "${XRD_S3_LIB:-}" ] || [ -z "${S3SIGN:-}" ]; then
  echo "XROOTD, XRD_HTTP_LIB, XRD_S3_LIB and S3SIGN must be set" >&2
  exit 1
fi

ROOT="$(pwd)/run"
rm -rf "$ROOT"
mkdir -p "$ROOT/data/s3meta/keys" "$ROOT/data/s3meta/buckets" \
  "$ROOT/data/prefix/f/foo/.s3/keys" "$ROOT/data/prefix/f/foo/.s3/buckets" \
  "$ROOT/data/prefix/f/foo/mybucket" "$ROOT/admin-on" "$ROOT/admin-off"

SECRET='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
AKID='AKIAIOSFODNN7EXAMPLE'
cat > "$ROOT/data/s3meta/keys/$AKID" << EOF
secret=$SECRET
user=foo
status=active
expires=0
EOF
cp "$ROOT/data/s3meta/keys/$AKID" "$ROOT/data/prefix/f/foo/.s3/keys/$AKID"
cat > "$ROOT/data/s3meta/buckets/mybucket" << EOF
owner=foo
created=1700000000
acl=private
region=us-east-1
EOF
cp "$ROOT/data/s3meta/buckets/mybucket" "$ROOT/data/prefix/f/foo/.s3/buckets/mybucket"
printf 'native-ok\n' > "$ROOT/data/plain-http.txt"
printf 'hello-s3\n' > "$ROOT/body.txt"

PORT_ON=$((20000 + ($$ % 10000)))
PORT_OFF=$((PORT_ON + 1))
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT

write_cfg() {
  switch=$1
  port=$2
  admin=$3
  cfg=$4
  cat > "$cfg" << EOF
xrd.port $port
xrd.network nodnr assumev4
all.export /
all.adminpath $admin
all.pidpath $admin
oss.localroot $ROOT/data
xrd.protocol XrdHttp:$port $XRD_HTTP_LIB
http.exthandler xrds3 +notls $XRD_S3_LIB
s3.switch $switch
s3.region us-east-1
s3.sysmeta /s3meta
s3.userprefix /prefix
s3.userhash first
s3.skew 15m
s3.metacache 0
s3.vhost s3.test
s3.trace info
EOF
}

start_srv() {
  name=$1
  cfg=$2
  # -n <name> places the log at <dir>/<name>/<name>.log when -l is <dir>/<name>.log.
  log="$ROOT/$name/$name.log"
  pidfile="$ROOT/$name.pid"
  "$XROOTD" -b -n "$name" -l "$ROOT/$name.log" -s "$pidfile" -c "$cfg"
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    if grep -a -q "initialization completed" "$log" 2>/dev/null; then
      PIDS+=("$(tr -d '[:space:]' < "$pidfile")")
      return 0
    fi
    sleep 0.25
  done
  echo "xrootd $name did not initialize" >&2
  cat "$log" >&2 || true
  exit 1
}

sign() {
  method=$1
  uri=$2
  query=$3
  hash=$4
  host=$5
  SIGNED="$("$S3SIGN" "$SECRET" "$AKID" us-east-1 "$method" "$host" "$uri" "$query" "$hash")"
  printf '%s\n' "$SIGNED" > "$ROOT/sig"
  S3_DATE="$(printf '%s\n' "$SIGNED" | sed -n '1p')"
  S3_HASH="$(printf '%s\n' "$SIGNED" | sed -n '2p')"
  S3_AUTH="$(printf '%s\n' "$SIGNED" | sed -n '3p')"
}

load_sig() {
  S3_DATE="$(sed -n '1p' "$ROOT/sig")"
  S3_HASH="$(sed -n '2p' "$ROOT/sig")"
  S3_AUTH="$(sed -n '3p' "$ROOT/sig")"
}

call_s3() {
  method=$1
  port=$2
  path=$3
  hash=$4
  data=$5
  uri="${path%%\?*}"
  query=""
  case "$path" in
    *\?*) query="${path#*\?}" ;;
  esac
  host="127.0.0.1:${port}"
  sign "$method" "$uri" "$query" "$hash" "$host"
  hdr="$ROOT/hdr"
  body="$ROOT/out"
  args=(-sS -D "$hdr" -o "$body" -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" -H "Authorization: $S3_AUTH")
  if [ "$method" = "HEAD" ]; then
    args+=(--head)
  elif [ "$method" != "GET" ]; then
    args+=(-X "$method")
  fi
  if [ -n "$data" ]; then
    args+=(--data-binary "@$data")
  fi
  curl "${args[@]}" "http://${host}${path}"
  awk 'NR==1 {print $2; exit}' "$hdr"
}

expect_code() {
  got=$1
  want=$2
  what=$3
  if [ "$got" != "$want" ]; then
    echo "$what: expected HTTP $want, got $got" >&2
    echo "--- headers ---" >&2
    cat "$ROOT/hdr" >&2 || true
    echo "--- body ---" >&2
    cat "$ROOT/out" >&2 || true
    exit 1
  fi
}

write_cfg on "$PORT_ON" "$ROOT/admin-on" "$ROOT/on.cfg"
write_cfg off "$PORT_OFF" "$ROOT/admin-off" "$ROOT/off.cfg"
start_srv s3on "$ROOT/on.cfg"
start_srv s3off "$ROOT/off.cfg"

native=$(curl -sS "http://127.0.0.1:${PORT_ON}/plain-http.txt" | tr -d '\n')
if [ "$native" != "native-ok" ]; then
  echo "native GET with s3.switch on returned [$native]" >&2
  exit 1
fi
native=$(curl -sS "http://127.0.0.1:${PORT_OFF}/plain-http.txt" | tr -d '\n')
if [ "$native" != "native-ok" ]; then
  echo "native GET with s3.switch off returned [$native]" >&2
  exit 1
fi

code=$(call_s3 PUT "$PORT_ON" /mybucket/hello.txt "@$ROOT/body.txt" "$ROOT/body.txt")
expect_code "$code" 200 "PutObject"
if ! grep -q 'ETag:' "$ROOT/hdr"; then
  echo "PutObject response has no ETag" >&2
  cat "$ROOT/hdr" >&2
  exit 1
fi
if [ "$(cat "$ROOT/data/prefix/f/foo/mybucket/hello.txt")" != "$(cat "$ROOT/body.txt")" ]; then
  echo "object bytes were not stored under /prefix/f/foo/mybucket/" >&2
  exit 1
fi

code=$(call_s3 GET "$PORT_ON" /mybucket/hello.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetObject"
if [ "$(cat "$ROOT/out")" != "$(cat "$ROOT/body.txt")" ]; then
  echo "GetObject body mismatch" >&2
  exit 1
fi
load_sig
RANGE_DATE=$S3_DATE
RANGE_HASH=$S3_HASH
RANGE_AUTH=$S3_AUTH

SIGNED=$("$S3SIGN" "$SECRET" "$AKID" us-east-1 PUT "127.0.0.1:${PORT_ON}" \
  /mybucket/meta.txt "" "@$ROOT/body.txt" x-amz-meta-color blue)
printf '%s\n' "$SIGNED" > "$ROOT/sig"
load_sig
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' -X PUT \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "x-amz-meta-color: blue" \
  --data-binary "@$ROOT/body.txt" \
  "http://127.0.0.1:${PORT_ON}/mybucket/meta.txt")
expect_code "$code" 200 "PutObject user metadata"
meta_etag=$(awk 'BEGIN{IGNORECASE=1} /^[Ee][Tt][Aa][Gg]:/ {gsub(/[\r"]/,"",$2); print $2; exit}' "$ROOT/hdr")
code=$(call_s3 HEAD "$PORT_ON" /mybucket/meta.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "HeadObject user metadata"
if ! grep -q 'x-amz-meta-color: blue' "$ROOT/hdr"; then
  echo "HeadObject did not return x-amz-meta-color" >&2
  cat "$ROOT/hdr" >&2
  exit 1
fi
code=$(call_s3 GET "$PORT_ON" /mybucket/meta.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetObject user metadata"
load_sig
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "If-None-Match: \"${meta_etag}\"" \
  "http://127.0.0.1:${PORT_ON}/mybucket/meta.txt")
expect_code "$code" 304 "GetObject If-None-Match"
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "If-Match: \"deadbeef\"" \
  "http://127.0.0.1:${PORT_ON}/mybucket/meta.txt")
expect_code "$code" 412 "GetObject If-Match"
SIGNED=$("$S3SIGN" "$SECRET" "$AKID" us-east-1 PUT "127.0.0.1:${PORT_ON}" \
  /mybucket/meta.txt "" "@$ROOT/body.txt")
printf '%s\n' "$SIGNED" > "$ROOT/sig"
load_sig
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' -X PUT \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "If-None-Match: *" \
  --data-binary "@$ROOT/body.txt" \
  "http://127.0.0.1:${PORT_ON}/mybucket/meta.txt")
expect_code "$code" 412 "PutObject If-None-Match"

code=$(call_s3 HEAD "$PORT_ON" /mybucket/hello.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "HeadObject"

code=$(call_s3 GET "$PORT_ON" '/mybucket?acl' UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetBucketAcl"
if ! grep -q 'FULL_CONTROL' "$ROOT/out" || ! grep -q '>foo<' "$ROOT/out"; then
  echo "bucket ACL was not a private policy for foo" >&2
  cat "$ROOT/out" >&2
  exit 1
fi
code=$(call_s3 GET "$PORT_ON" '/mybucket/hello.txt?acl' UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetObjectAcl"
if ! grep -q 'FULL_CONTROL' "$ROOT/out"; then
  echo "object ACL was not FULL_CONTROL" >&2
  cat "$ROOT/out" >&2
  exit 1
fi
code=$(call_s3 GET "$PORT_ON" \
  '/mybucket/hello.txt?response-content-disposition=attachment%3B%20filename%3D%22a.txt%22' \
  UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetObject response override"
if ! grep -q 'Content-Disposition: attachment; filename="a.txt"' "$ROOT/hdr"; then
  echo "response-content-disposition was not applied" >&2
  cat "$ROOT/hdr" >&2
  exit 1
fi

code=$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:${PORT_ON}/mybucket/hello.txt")
if [ "$code" != "404" ]; then
  echo "unsigned GET of the S3 path should stay on native HTTP, got $code" >&2
  exit 1
fi

code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' \
  -H "x-amz-date: $RANGE_DATE" -H "x-amz-content-sha256: $RANGE_HASH" \
  -H "Authorization: $RANGE_AUTH" -H "Range: bytes=0-4" \
  "http://127.0.0.1:${PORT_ON}/mybucket/hello.txt")
expect_code "$code" 206 "GetObject range"
if [ "$(cat "$ROOT/out")" != "hello" ]; then
  echo "range body mismatch [$(cat "$ROOT/out")]" >&2
  exit 1
fi

code=$(call_s3 GET "$PORT_ON" '/mybucket?list-type=2' UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "ListObjectsV2"
if ! grep -q 'hello.txt' "$ROOT/out"; then
  echo "listing did not contain hello.txt" >&2
  cat "$ROOT/out" >&2
  exit 1
fi

code=$(call_s3 GET "$PORT_ON" /mybucket UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "ListObjects v1"
if ! grep -q '<Marker>' "$ROOT/out" || grep -q '<KeyCount>' "$ROOT/out"; then
  echo "listing without list-type was not ListObjects v1" >&2
  cat "$ROOT/out" >&2
  exit 1
fi

code=$(call_s3 POST "$PORT_ON" '/mybucket/from-copy?uploads' \
  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 "")
expect_code "$code" 200 "CreateMultipartUpload for part copy"
uid=$(sed -n 's/.*<UploadId>\([^<]*\)<\/UploadId>.*/\1/p' "$ROOT/out")
SIGNED=$("$S3SIGN" "$SECRET" "$AKID" us-east-1 PUT "127.0.0.1:${PORT_ON}" \
  /mybucket/from-copy "partNumber=1&uploadId=${uid}" UNSIGNED-PAYLOAD \
  x-amz-copy-source /mybucket/hello.txt x-amz-copy-source-range bytes=0-4)
printf '%s\n' "$SIGNED" > "$ROOT/sig"
load_sig
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' -X PUT \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "x-amz-copy-source: /mybucket/hello.txt" \
  -H "x-amz-copy-source-range: bytes=0-4" \
  "http://127.0.0.1:${PORT_ON}/mybucket/from-copy?partNumber=1&uploadId=${uid}")
expect_code "$code" 200 "UploadPartCopy"
cetag=$(awk 'BEGIN{IGNORECASE=1} /^[Ee][Tt][Aa][Gg]:/ {gsub(/[\r"]/,"",$2); print $2; exit}' "$ROOT/hdr")
printf '%s' "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"${cetag}\"</ETag></Part></CompleteMultipartUpload>" > "$ROOT/done-copy.xml"
code=$(call_s3 POST "$PORT_ON" "/mybucket/from-copy?uploadId=${uid}" "@$ROOT/done-copy.xml" "$ROOT/done-copy.xml")
expect_code "$code" 200 "Complete UploadPartCopy"
code=$(call_s3 GET "$PORT_ON" /mybucket/from-copy UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetObject part copy"
if [ "$(cat "$ROOT/out")" != "hello" ]; then
  echo "copied part body mismatch [$(cat "$ROOT/out")]" >&2
  exit 1
fi

code=$(call_s3 PUT "$PORT_ON" /otherbucket "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" "")
expect_code "$code" 200 "CreateBucket"
if [ ! -f "$ROOT/data/s3meta/buckets/otherbucket" ] || [ ! -d "$ROOT/data/prefix/f/foo/otherbucket" ]; then
  echo "CreateBucket did not write the index and data directory" >&2
  exit 1
fi

code=$(call_s3 GET "$PORT_ON" / UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "ListBuckets"
if ! grep -q otherbucket "$ROOT/out" || ! grep -q mybucket "$ROOT/out"; then
  echo "ListBuckets missing a bucket" >&2
  cat "$ROOT/out" >&2
  exit 1
fi

code=$(call_s3 GET "$PORT_ON" '/mybucket?location' UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetBucketLocation"
if ! grep -q 'LocationConstraint' "$ROOT/out"; then
  echo "GetBucketLocation missing LocationConstraint" >&2
  cat "$ROOT/out" >&2
  exit 1
fi

code=$(call_s3 DELETE "$PORT_ON" /otherbucket UNSIGNED-PAYLOAD "")
expect_code "$code" 204 "DeleteBucket"

load_sig
bad="${S3_AUTH%?}0"
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: UNSIGNED-PAYLOAD" \
  -H "Authorization: $bad" \
  "http://127.0.0.1:${PORT_ON}/mybucket/hello.txt")
expect_code "$code" 403 "bad signature"
if ! grep -q SignatureDoesNotMatch "$ROOT/out"; then
  echo "expected SignatureDoesNotMatch" >&2
  cat "$ROOT/out" >&2
  exit 1
fi

code=$(call_s3 GET "$PORT_OFF" /mybucket/hello.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 404 "S3 GET while switched off"
if grep -q hello-s3 "$ROOT/out"; then
  echo "switched-off server served the S3 object" >&2
  exit 1
fi

q=$("$S3SIGN" presign "$SECRET" "$AKID" us-east-1 GET "127.0.0.1:${PORT_ON}" /mybucket/hello.txt 300)
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' --globoff \
  "http://127.0.0.1:${PORT_ON}/mybucket/hello.txt?${q}")
expect_code "$code" 200 "presigned GetObject"
if [ "$(cat "$ROOT/out")" != "$(cat "$ROOT/body.txt")" ]; then
  echo "presigned body mismatch" >&2
  exit 1
fi

SIGNED=$("$S3SIGN" "$SECRET" "$AKID" us-east-1 PUT "127.0.0.1:${PORT_ON}" \
  /mybucket/hello-copy.txt "" UNSIGNED-PAYLOAD x-amz-copy-source /mybucket/hello.txt)
printf '%s\n' "$SIGNED" > "$ROOT/sig"
load_sig
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' -X PUT \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "x-amz-copy-source: /mybucket/hello.txt" \
  "http://127.0.0.1:${PORT_ON}/mybucket/hello-copy.txt")
expect_code "$code" 200 "CopyObject"
if ! grep -q 'ETag' "$ROOT/out"; then
  echo "CopyObject XML has no ETag" >&2
  cat "$ROOT/out" >&2
  exit 1
fi

code=$(call_s3 GET "$PORT_ON" /mybucket/hello-copy.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetObject copy"
if [ "$(cat "$ROOT/out")" != "$(cat "$ROOT/body.txt")" ]; then
  echo "copied object mismatch" >&2
  exit 1
fi

printf '%s' '<Delete><Object><Key>hello-copy.txt</Key></Object></Delete>' > "$ROOT/del.xml"
delmd5=$(openssl dgst -md5 -binary "$ROOT/del.xml" | openssl base64 | tr -d '\n')
sign POST /mybucket delete "@$ROOT/del.xml" "127.0.0.1:${PORT_ON}"
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' -X POST \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "Content-MD5: $delmd5" \
  --data-binary "@$ROOT/del.xml" \
  "http://127.0.0.1:${PORT_ON}/mybucket?delete")
expect_code "$code" 200 "DeleteObjects"
if ! grep -q hello-copy.txt "$ROOT/out"; then
  echo "DeleteObjects did not report the key" >&2
  cat "$ROOT/out" >&2
  exit 1
fi

code=$(call_s3 POST "$PORT_ON" '/mybucket/hello-mp?uploads' \
  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 "")
expect_code "$code" 200 "CreateMultipartUpload"
uid=$(sed -n 's/.*<UploadId>\([^<]*\)<\/UploadId>.*/\1/p' "$ROOT/out")
printf 'aaa' > "$ROOT/p1"
printf 'bbb' > "$ROOT/p2"
code=$(call_s3 PUT "$PORT_ON" "/mybucket/hello-mp?partNumber=1&uploadId=${uid}" "@$ROOT/p1" "$ROOT/p1")
expect_code "$code" 200 "UploadPart 1"
etag1=$(awk 'BEGIN{IGNORECASE=1} /^[Ee][Tt][Aa][Gg]:/ {gsub(/[\r"]/,"",$2); print $2; exit}' "$ROOT/hdr")
code=$(call_s3 PUT "$PORT_ON" "/mybucket/hello-mp?partNumber=2&uploadId=${uid}" "@$ROOT/p2" "$ROOT/p2")
expect_code "$code" 200 "UploadPart 2"
etag2=$(awk 'BEGIN{IGNORECASE=1} /^[Ee][Tt][Aa][Gg]:/ {gsub(/[\r"]/,"",$2); print $2; exit}' "$ROOT/hdr")
code=$(call_s3 GET "$PORT_ON" "/mybucket/hello-mp?uploadId=${uid}" UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "ListParts"
if ! grep -q '<PartNumber>1</PartNumber>' "$ROOT/out" || ! grep -q '<PartNumber>2</PartNumber>' "$ROOT/out"; then
  echo "ListParts did not return both parts" >&2
  cat "$ROOT/out" >&2
  exit 1
fi
code=$(call_s3 GET "$PORT_ON" '/mybucket?uploads' UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "ListMultipartUploads"
if ! grep -q "$uid" "$ROOT/out"; then
  echo "ListMultipartUploads did not return the upload" >&2
  cat "$ROOT/out" >&2
  exit 1
fi
printf '%s' "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"${etag1}\"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>\"${etag2}\"</ETag></Part></CompleteMultipartUpload>" > "$ROOT/done.xml"
code=$(call_s3 POST "$PORT_ON" "/mybucket/hello-mp?uploadId=${uid}" "@$ROOT/done.xml" "$ROOT/done.xml")
expect_code "$code" 200 "CompleteMultipartUpload"
code=$(call_s3 GET "$PORT_ON" /mybucket/hello-mp UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetObject multipart"
if [ "$(cat "$ROOT/out")" != "aaabbb" ]; then
  echo "multipart body mismatch [$(cat "$ROOT/out")]" >&2
  exit 1
fi

sign GET /hello.txt "" UNSIGNED-PAYLOAD "mybucket.s3.test"
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "Host: mybucket.s3.test" \
  "http://127.0.0.1:${PORT_ON}/hello.txt")
expect_code "$code" 200 "virtual-hosted GetObject"
if [ "$(cat "$ROOT/out")" != "$(cat "$ROOT/body.txt")" ]; then
  echo "virtual-hosted body mismatch" >&2
  exit 1
fi

printf 'hello' > "$ROOT/chunk-payload"
: > "$ROOT/empty"
SIGNED=$("$S3SIGN" "$SECRET" "$AKID" us-east-1 PUT "127.0.0.1:${PORT_ON}" \
  /mybucket/chunked.txt "" STREAMING-AWS4-HMAC-SHA256-PAYLOAD \
  x-amz-decoded-content-length 5)
printf '%s\n' "$SIGNED" > "$ROOT/sig"
load_sig
SEED=${S3_AUTH##*Signature=}
SIG1=$("$S3SIGN" chunk "$SECRET" us-east-1 "$S3_DATE" "$SEED" "$ROOT/chunk-payload")
SIG0=$("$S3SIGN" chunk "$SECRET" us-east-1 "$S3_DATE" "$SIG1" "$ROOT/empty")
printf '5;chunk-signature=%s\r\nhello\r\n0;chunk-signature=%s\r\n\r\n' "$SIG1" "$SIG0" > "$ROOT/framed"
code=$(curl -sS -D "$ROOT/hdr" -o "$ROOT/out" -w '%{http_code}' -X PUT \
  -H "x-amz-date: $S3_DATE" -H "x-amz-content-sha256: $S3_HASH" \
  -H "Authorization: $S3_AUTH" -H "x-amz-decoded-content-length: 5" \
  --data-binary "@$ROOT/framed" \
  "http://127.0.0.1:${PORT_ON}/mybucket/chunked.txt")
expect_code "$code" 200 "aws-chunked PutObject"
code=$(call_s3 GET "$PORT_ON" /mybucket/chunked.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "GetObject chunked"
if [ "$(cat "$ROOT/out")" != "hello" ]; then
  echo "chunked body mismatch [$(cat "$ROOT/out")]" >&2
  exit 1
fi

code=$(call_s3 DELETE "$PORT_ON" /mybucket/hello.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 204 "DeleteObject"
code=$(call_s3 GET "$PORT_ON" /mybucket/hello.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 404 "GetObject after delete"
if ! grep -q NoSuchKey "$ROOT/out"; then
  echo "expected NoSuchKey" >&2
  cat "$ROOT/out" >&2
  exit 1
fi

echo "XrdHttpS3 integration test passed"
