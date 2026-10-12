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

code=$(call_s3 HEAD "$PORT_ON" /mybucket/hello.txt UNSIGNED-PAYLOAD "")
expect_code "$code" 200 "HeadObject"

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
