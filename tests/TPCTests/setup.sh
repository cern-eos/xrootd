#!/usr/bin/env bash

set -ex

: "${XROOTD:=$(command -v xrootd)}"

servernames=("srv1" "srv2")
DATAFOLDER="./data"

# -n NAME places logs/pids under ./NAME/. Opening NAME/xrootd.log as a relative
# -l path either fails (parent dir missing) or lands at NAME/NAME/xrootd.log.
dump_start_failure() {
    local srv=$1
    echo "failed to start ${srv}" >&2
    echo "=== ${srv}.start.err ===" >&2
    cat "${srv}.start.err" >&2 || true
    echo "=== ${srv}/xrootd.log ===" >&2
    if [[ -e "${srv}/xrootd.log" ]]; then
        # -k fifo makes the log a pipe; a blocking cat would hang CTest.
        timeout 2 cat "${srv}/xrootd.log" >&2 || true
    else
        echo "(missing)" >&2
        ls -la "${srv}" . >&2 || true
    fi
}

start_server() {
    local srv=$1
    if [[ -f "${srv}/xrootd.pid" ]]; then
        kill -TERM "$(cat "${srv}/xrootd.pid")" || true
    fi
    rm -rf "${srv}"
    mkdir -p "${srv}" "${DATAFOLDER}/${srv}"
    echo "Starting XRootD on ${srv}..."
    if ! ${XROOTD} -b -k fifo -n "${srv}" -l xrootd.log -s xrootd.pid -c "${srv}.cfg" \
            >"${srv}.start.err" 2>&1; then
        dump_start_failure "${srv}"
        exit 1
    fi
}

setup() {
    echo "Setting up XRootD with ${servernames[*]}"

    mkdir -p "${DATAFOLDER}"
    for srv in "${servernames[@]}"; do
        start_server "${srv}"
    done

    sleep 2
    echo "XRootD setup complete."
}

teardown() {
    echo "Tearing down XRootD .."
    
    for srv in "${servernames[@]}"; do
        if [[ -f "${srv}/xrootd.pid" ]]; then
            kill -TERM "$(cat "${srv}"/xrootd.pid)" || true
        fi
    done

    echo "teardown complete."
}

# Ensure script is executed with "start" or "teardown"
case "$1" in
    start)
        setup
        ;;
    teardown)
        teardown
        ;;
    *)
        echo "Usage: $0 {start|teardown}"
        exit 1
        ;;
esac
