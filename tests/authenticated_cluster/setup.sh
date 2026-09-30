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
    [[ "${pid}" =~ ^[0-9]+$ ]] || return 1
    # kill -0 sees daemonized children; ps -p does not on Ubuntu 24.04
    # (new session after xrootd -b double-fork).
    if [[ -d "/proc/${pid}" ]]; then
        return 0
    fi
    kill -0 "${pid}" 2>/dev/null
}

wait_for_pidfile() {
    local pidfile=$1
    local tries

    for ((tries = 0; tries < 50; tries++)); do
        if pid_alive "${pidfile}"; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

kill_pidfile() {
    local pidfile=$1
    local pid wait_count

    [[ -s "${pidfile}" ]] || return 0
    pid=$(tr -d '[:space:]' < "${pidfile}")
    [[ "${pid}" =~ ^[0-9]+$ ]] || return 0
    kill -s TERM "${pid}" 2>/dev/null || true
    for ((wait_count = 0; wait_count < 20; wait_count++)); do
        kill -0 "${pid}" 2>/dev/null || return 0
        sleep 0.1
    done
    kill -s KILL "${pid}" 2>/dev/null || true
}

dump_log() {
    local path=$1
    echo "=== ${path} (last 120 lines) ===" >&2
    if [[ ! -f "${path}" ]]; then
        echo "(missing)" >&2
        return 0
    fi
    tail -n 120 "${path}" >&2 || true
}

dump_listen_state() {
    echo "=== listening sockets ===" >&2
    if command -v ss >/dev/null 2>&1; then
        ss -ltn >&2 || true
    elif command -v netstat >/dev/null 2>&1; then
        netstat -ltn >&2 || true
    else
        grep -H LISTEN /proc/net/tcp /proc/net/tcp6 2>/dev/null >&2 || true
    fi
}

dump_start_failure() {
    local i
    echo "authenticated_cluster failed to become ready" >&2
    dump_listen_state
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
        if [[ -s "${i}/cmsd.pid" ]]; then
            echo "${i} cmsd.pid=$(cat "${i}/cmsd.pid") alive=$(pid_alive "${i}/cmsd.pid" && echo yes || echo no)" >&2
        else
            echo "${i} cmsd.pid missing" >&2
        fi
    done
}

port_is_listening() {
    local port=$1
    local hex

    hex=$(printf '%04X' "${port}")
    if command -v ss >/dev/null 2>&1; then
        if ss -ltn 2>/dev/null | grep -E -q ":${port}[[:space:]]"; then
            return 0
        fi
    fi
    # 0A is TCP_LISTEN. IPv6 /proc lines are <32hex>:<port>, not colon-separated.
    if [[ -r /proc/net/tcp ]] || [[ -r /proc/net/tcp6 ]]; then
        if grep -E -h ":${hex}[[:space:]].*[[:space:]]0A[[:space:]]" \
                /proc/net/tcp /proc/net/tcp6 2>/dev/null | grep -q .; then
            return 0
        fi
    fi
    python3 -c '
import socket, sys
port = int(sys.argv[1])
for host in ("127.0.0.1", "localhost", "::1"):
    try:
        s = socket.create_connection((host, port), 0.5)
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

    for ((tries = 0; tries < 120; tries++)); do
        if ! pid_alive "${name}/xrootd.pid"; then
            echo "error: ${name} xrootd died before listening on ${port}" >&2
            return 1
        fi
        if port_is_listening "${port}"; then
            return 0
        fi
        if grep -a -qE '------ xrootd .+ initialization completed' "${name}/xrootd.log" 2>/dev/null; then
            return 0
        fi
        if grep -a -qE '------ xrootd .+ initialization failed' "${name}/xrootd.log" 2>/dev/null; then
            echo "error: ${name} initialization failed" >&2
            return 1
        fi
        sleep 0.5
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

# Do not use -k fifo: an unread log pipe can make xrootd -b report failure
# while the child is still initializing (see tests/xcachewithcsi/setup.sh).
# The parent exit code is also not a reliable ready signal, so we only
# require a live pidfile here and wait for the HTTP port in start().
start_daemon() {
    local bin=$1
    local name=$2
    local logfile=$3
    local pidfile=$4
    local cfg=$5
    local errfile=$6
    local rc=0

    mkdir -p "${name}"
    set +e
    "${bin}" -b -n "${name}" -l "${logfile}" -s "${pidfile}" -c "${cfg}" \
            >"${errfile}" 2>&1
    rc=$?
    set -e
    if [[ "${rc}" -ne 0 ]]; then
        echo "warning: ${bin} -b exited ${rc} for ${name}" >&2
    fi
    if ! wait_for_pidfile "${name}/${pidfile}"; then
        echo "error: ${bin} -b did not leave a live ${name}/${pidfile}" >&2
        dump_start_failure
        exit 1
    fi
}

start() {
    local i
    local idx

    stop
    create_directories

    for idx in "${!servernames[@]}"; do
        i="${servernames[idx]}"
        start_daemon "${XROOTD}" "${i}" xrootd.log xrootd.pid "${i}.cfg" "${i}.start.err"
        if ! wait_for_listen "${i}" "${xrd_ports[idx]}"; then
            dump_start_failure
            exit 1
        fi
    done

    for i in "${servernames[@]}"; do
        start_daemon "${CMSD}" "${i}" cmsd.log cmsd.pid "${i}.cfg" "${i}.cmsd.start.err"
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
