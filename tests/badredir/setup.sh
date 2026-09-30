#!/usr/bin/env bash

set -ex

: "${XROOTD:=$(command -v xrootd)}"

servernames=("srv-bad-redir1" "srv-bad-redir2")
DATAFOLDER="./data"

setup() {
    echo "Setting up XRootD with ${servernames[*]}"

    mkdir -p "${DATAFOLDER}"
    for srv in "${servernames[@]}"; do
        mkdir -p "${DATAFOLDER}/${srv}"
    done

    # Start XRootD servers
    for srv in "${servernames[@]}"; do
        echo "Starting XRootD on ${srv}..."
        mkdir -p "${srv}"
        set +e
        ${XROOTD} -b -n "${srv}" -l xrootd.log -s xrootd.pid -c "${srv}".cfg
        rc=$?
        set -e
        if [[ "${rc}" -ne 0 ]]; then
            echo "warning: xrootd -b exited ${rc} for ${srv}" >&2
        fi
        pid=$(tr -d '[:space:]' < "${srv}/xrootd.pid" 2>/dev/null)
        if [[ ! "${pid}" =~ ^[0-9]+$ ]] || ! kill -0 "${pid}" 2>/dev/null; then
            echo "failed to start ${srv}" >&2
            [[ -f "${srv}/xrootd.log" ]] && cat "${srv}/xrootd.log" >&2
            exit 1
        fi
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
