#!/usr/bin/env bash

set -x

: ${XROOTD:=$(command -v xrootd)}
: ${CMSD:=$(command -v cmsd)}

servernames=("metaman" "man1" "man2" "srv1" "srv2" "srv3" "srv4")
# HTTP ports from the per-instance .cfg files (xrd.port / xrd.protocol XrdHttp).
xrd_ports=(10970 10971 10972 10973 10974 10975 10976)

DATAFOLDER="./data"

die() {
    echo "$*" >&2
    exit 1
}

pid_alive() {
    local pidfile=$1
    local pid

    [[ -s "${pidfile}" ]] || return 1
    pid=$(tr -d '[:space:]' < "${pidfile}")
    [[ -n "${pid}" ]] || return 1
    ps -p "${pid}" >/dev/null 2>&1
}

kill_pidfile() {
    local pidfile=$1
    local pid wait_count

    [[ -s "${pidfile}" ]] || return 0
    pid=$(ps -o pid= "$(cat "${pidfile}")" 2>/dev/null | tr -d ' ')
    [[ -n "${pid}" ]] || return 0
    kill -s TERM "${pid}" 2>/dev/null || true
    for ((wait_count = 0; wait_count < 50; wait_count++)); do
        ps -p "${pid}" >/dev/null 2>&1 || return 0
        sleep 0.1
    done
    kill -s KILL "${pid}" 2>/dev/null || true
}

dump_log() {
    local path=$1
    echo "=== ${path} ===" >&2
    if [[ ! -e "${path}" ]]; then
        echo "(missing)" >&2
        return 0
    fi
    # -k fifo makes the log a pipe; a blocking cat would hang CTest.
    python3 - "${path}" <<'PY' >&2 || true
import os, select, sys, time
path = sys.argv[1]
try:
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
except OSError as exc:
    sys.stderr.write("(%s)\n" % exc)
    sys.exit(0)
deadline = time.time() + 2
data = b""
while time.time() < deadline and len(data) < 65536:
    ready, _, _ = select.select([fd], [], [], max(0.0, deadline - time.time()))
    if not ready:
        break
    chunk = os.read(fd, 4096)
    if not chunk:
        break
    data += chunk
os.close(fd)
sys.stderr.buffer.write(data if data else b"(empty)\n")
PY
}

dump_start_failure() {
    local i
    echo "authenticated_cluster failed to become ready" >&2
    ls -la . >&2 || true
    for i in "${servernames[@]}"; do
        echo "=== ${i}.start.err ===" >&2
        cat "${i}.start.err" >&2 || true
        echo "=== ${i}.cmsd.start.err ===" >&2
        cat "${i}.cmsd.start.err" >&2 || true
        dump_log "${i}/xrootd.log"
        dump_log "${i}/cmsd.log"
        if [[ -s "${i}/xrootd.pid" ]]; then
            echo "${i} xrootd.pid=$(cat "${i}/xrootd.pid") alive=$(pid_alive "${i}/xrootd.pid" && echo yes || echo no)" >&2
        else
            echo "${i} xrootd.pid missing" >&2
        fi
    done
}

port_is_open() {
    local port=$1
    python3 -c '
import socket, sys
port = int(sys.argv[1])
for host in ("127.0.0.1", "::1"):
    try:
        s = socket.create_connection((host, port), 0.25)
        s.close()
        sys.exit(0)
    except OSError:
        pass
sys.exit(1)
' "${port}"
}

wait_for_listen() {
    local name=$1
    local port=$2
    local tries

    for ((tries = 0; tries < 80; tries++)); do
        if ! pid_alive "${name}/xrootd.pid"; then
            echo "error: ${name} xrootd died before listening on ${port}" >&2
            return 1
        fi
        if port_is_open "${port}"; then
            return 0
        fi
        sleep 0.25
    done
    echo "error: ${name} did not listen on ${port}" >&2
    return 1
}

create_directories() {
    local i
    for i in "${servernames[@]}"; do
        mkdir -p "${DATAFOLDER}/${i}/data" "${i}"
    done
}

stop() {
    local i
    set +e
    for i in "${servernames[@]}"; do
        if [[ -d "${i}" ]]; then
            kill_pidfile "${i}/cmsd.pid"
            kill_pidfile "${i}/xrootd.pid"
        fi
    done
    set -e
}

start_daemon() {
    local bin=$1
    local name=$2
    local logfile=$3
    local pidfile=$4
    local cfg=$5
    local errfile=$6

    mkdir -p "${name}"
    if ! "${bin}" -b -k fifo -n "${name}" -l "${logfile}" -s "${pidfile}" -c "${cfg}" \
            >"${errfile}" 2>&1; then
        echo "error: ${bin} -b failed for ${name}" >&2
        dump_start_failure
        exit 1
    fi
}

start() {
    local i
    local idx

    stop
    create_directories

    for i in "${servernames[@]}"; do
        start_daemon "${XROOTD}" "${i}" xrootd.log xrootd.pid "${i}.cfg" "${i}.start.err"
    done

    for i in "${servernames[@]}"; do
        start_daemon "${CMSD}" "${i}" cmsd.log cmsd.pid "${i}.cfg" "${i}.cmsd.start.err"
    done

    for idx in "${!servernames[@]}"; do
        if ! wait_for_listen "${servernames[idx]}" "${xrd_ports[idx]}"; then
            dump_start_failure
            exit 1
        fi
    done
}

usage() {
    echo "$0 start or stop"
}

[[ $# == 0 ]] && usage && exit 0

CMD=$1
shift
[[ $(type -t "${CMD}") == "function" ]] || die "unknown command: ${CMD}"
"${CMD}" "$@"
