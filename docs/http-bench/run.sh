#!/usr/bin/env bash
# Header-parse microbench + HTTP/1.1 vs HTTP/2 request loop against local xrootd.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD="${ROOT}/build"
BENCH="$(cd "$(dirname "$0")" && pwd)"
WORKDIR="${TMPDIR:-/tmp}/xrdhttp-bench-$$"
XRD="${BUILD}/bin/xrootd"
HTTP_SO="${BUILD}/lib/libXrdHttp-6.so"
PORT_HTTP=18094
PORT_TCP=10951

if [[ ! -x "$XRD" ]]; then
  echo "need a built tree at $BUILD" >&2
  exit 1
fi

clang -O2 -I"${ROOT}/src/XrdHttp/vendor/llhttp" \
  -o "${BENCH}/header_parse_bench" \
  "${BENCH}/header_parse_bench.c" \
  "${ROOT}/src/XrdHttp/vendor/llhttp/api.c" \
  "${ROOT}/src/XrdHttp/vendor/llhttp/http.c" \
  "${ROOT}/src/XrdHttp/vendor/llhttp/llhttp.c"

NGHTTP2_PREFIX="${NGHTTP2_PREFIX:-/opt/homebrew/opt/libnghttp2}"
clang -O2 -o "${BENCH}/request_bench" "${BENCH}/request_bench.c" \
  -I"${NGHTTP2_PREFIX}/include" -I/opt/homebrew/include \
  -L"${NGHTTP2_PREFIX}/lib" -Wl,-rpath,"${NGHTTP2_PREFIX}/lib" \
  -lnghttp2

echo "================================================================"
"${BENCH}/header_parse_bench"
echo

mkdir -p "${WORKDIR}/data" "${WORKDIR}/admin"
dd if=/dev/urandom of="${WORKDIR}/data/small.bin" bs=64 count=1 status=none
dd if=/dev/urandom of="${WORKDIR}/data/med.bin" bs=1048576 count=8 status=none

cat > "${WORKDIR}/xrootd.cf" <<EOF
all.export /
oss.localroot ${WORKDIR}/data
all.adminpath ${WORKDIR}/admin
all.pidpath ${WORKDIR}/admin
xrd.port ${PORT_TCP}
xrd.protocol http:${PORT_HTTP} ${HTTP_SO}
http.tlsclientauth off
http.parser llhttp
EOF

export DYLD_LIBRARY_PATH="${BUILD}/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
export LD_LIBRARY_PATH="${BUILD}/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

"$XRD" -c "${WORKDIR}/xrootd.cf" -l "${WORKDIR}/xrootd.log" &
XRD_PID=$!
cleanup() { kill "$XRD_PID" 2>/dev/null || true; wait "$XRD_PID" 2>/dev/null || true; }
trap cleanup EXIT

ready=0
for i in $(seq 1 80); do
  if grep -q "initialization completed" "${WORKDIR}/xrootd.log" 2>/dev/null; then
    ready=1
    break
  fi
  if ! kill -0 "$XRD_PID" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if [[ "$ready" != 1 ]]; then
  echo "xrootd did not start; log:" >&2
  cat "${WORKDIR}/xrootd.log" >&2 || true
  exit 1
fi
sleep 0.2

URL="http://127.0.0.1:${PORT_HTTP}/small.bin"
MED="http://127.0.0.1:${PORT_HTTP}/med.bin"
echo "================================================================"
if ! "${BENCH}/request_bench" "$URL" "$MED"; then
  echo "request_bench failed; xrootd log:" >&2
  cat "${WORKDIR}/xrootd.log" >&2 || true
  exit 1
fi

echo
echo "workdir ${WORKDIR}  (removed on exit)"
