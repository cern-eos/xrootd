#!/usr/bin/env bash

SETUP_SH="$(cd "$(dirname "$0")" && pwd)/setup.sh"

: ${ADLER32:=$(command -v xrdadler32)}
: ${CRC32C:=$(command -v xrdcrc32c)}
: ${XRDCP:=$(command -v xrdcp)}
: ${XRDFS:=$(command -v xrdfs)}
: ${XRDMAPC:=$(command -v xrdmapc)}
: ${OPENSSL:=$(command -v openssl)}
: ${CURL:=$(command -v curl)}

if [[ -f ports.env ]]; then
       # shellcheck disable=SC1091
       source ports.env
fi

: ${HOST_METAMAN:=root://localhost:10940}
: ${HOST_MAN1:=root://localhost:10941}
: ${HOST_MAN2:=root://localhost:10942}
: ${HOST_SRV1:=root://localhost:10943}
: ${HOST_SRV2:=root://localhost:10944}
: ${HOST_SRV3:=root://localhost:10945}
: ${HOST_SRV4:=root://localhost:10946}

: ${HOST_HTTP_METAMAN:=${HOST_METAMAN/root/http}}
: ${HOST_HTTP_MAN1:=${HOST_MAN1/root/http}}
: ${HOST_HTTP_MAN2:=${HOST_MAN2/root/http}}
: ${HOST_HTTP_SRV1:=${HOST_SRV1/root/http}}
: ${HOST_HTTP_SRV2:=${HOST_SRV2/root/http}}
: ${HOST_HTTP_SRV3:=${HOST_SRV3/root/http}}
: ${HOST_HTTP_SRV4:=${HOST_SRV4/root/http}}

# Parallel arrays for compatibility with Bash < 4
host_names=(metaman man1 man2 srv1 srv2 srv3 srv4)
host_roots=("${HOST_METAMAN}" "${HOST_MAN1}" "${HOST_MAN2}" \
            "${HOST_SRV1}" "${HOST_SRV2}" "${HOST_SRV3}" "${HOST_SRV4}")
host_https=("${HOST_HTTP_METAMAN}" "${HOST_HTTP_MAN1}" "${HOST_HTTP_MAN2}" \
            "${HOST_HTTP_SRV1}" "${HOST_HTTP_SRV2}" "${HOST_HTTP_SRV3}" "${HOST_HTTP_SRV4}")

get_index_for_host() {
       local host="$1"
       local i

       for ((i = 0; i < ${#host_names[@]}; i++)); do
              if [[ "${host_names[i]}" == "$host" ]]; then
                     echo "$i"
                     return
              fi
       done
       echo "-1"
}

# checking for command presence
for PROG in ${ADLER32} ${CRC32C} ${XRDCP} ${XRDFS} ${XRDMAPC} ${OPENSSL} ${CURL}; do
       if [[ ! -x "${PROG}" ]]; then
               echo 1>&2 "$(basename $0): error: '${PROG}': command not found"
               exit 1
       fi
done

# This script assumes that ${host} exports an empty / as read/write.
# It also assumes that any authentication required is already setup.

RMTDATADIR="/srvdata"
LCLDATADIR="${PWD}/localdata"  # client folder

cleanup_local_files() {
       local i
       for ((i = 0; i < ${#host_names[@]}; i++)); do
              rm -rf ${LCLDATADIR}/${host_names[i]}.dat
              rm -rf ${LCLDATADIR}/${host_names[i]}.ref
       done
}

stop_cluster() {
       [[ -x "${SETUP_SH}" ]] && "${SETUP_SH}" stop || true
}

fixture_cleanup() {
       local rc=$?

       cleanup_local_files
       if [[ ${rc} -ne 0 ]]; then
              echo 1>&2 "Stopping cluster after test failure or interruption."
              stop_cluster
       fi
       return ${rc}
}

set -e

trap fixture_cleanup EXIT
trap 'stop_cluster; exit 130' INT TERM HUP

${XRDCP} --version

for ((i = 0; i < ${#host_names[@]}; i++)); do
       ${XRDFS} "${host_roots[i]}" query config version
done

# query some common server configurations

CONFIG_PARAMS=( version role sitename )

for PARAM in ${CONFIG_PARAMS[@]}; do
       for ((i = 0; i < ${#host_names[@]}; i++)); do
              ${XRDFS} "${host_roots[i]}" query config ${PARAM}
       done
done

# some extra query commands that don't make any changes
${XRDFS} ${HOST_METAMAN} stat /
${XRDFS} ${HOST_METAMAN} statvfs /
${XRDFS} ${HOST_METAMAN} spaceinfo /

mkdir -p ${LCLDATADIR}

assert_xrdmapc_json() {
       expected_json="{\"name\":\"localhost:${XRD_PORT_METAMAN}\",\"type\":\"manager\",\"managers\":[{\"name\":\"localhost:${XRD_PORT_MAN1}\",\"type\":\"manager\",\"servers\":[{\"name\":\"localhost:${XRD_PORT_SRV1}\"},{\"name\":\"localhost:${XRD_PORT_SRV2}\"}]},{\"name\":\"localhost:${XRD_PORT_MAN2}\",\"type\":\"manager\",\"servers\":[{\"name\":\"localhost:${XRD_PORT_SRV3}\"},{\"name\":\"localhost:${XRD_PORT_SRV4}\"}]}]}"
       actual_json="$(${XRDMAPC} --format json "localhost:${XRD_PORT_METAMAN}")"

       if [[ "${actual_json}" != "${expected_json}" ]]; then
               echo 1>&2 "$(basename $0): error: xrdmapc JSON output does not match expected topology"
               echo "Expected: ${expected_json}"
               echo "Actual:   ${actual_json}"
               exit 1
       fi

       # Print cluster topology in string format
       ${XRDMAPC} "localhost:${XRD_PORT_METAMAN}"

}

assert_xrdmapc_json

# create local files with random contents using OpenSSL

for ((i = 0; i < ${#host_names[@]}; i++)); do
       ${OPENSSL} rand -out "${LCLDATADIR}/${host_names[i]}.ref" $((1024 * ($RANDOM + 1)))
done

# upload local files to the servers in parallel
HTTP_SUFFIX=(".ref_http" ".ref_%23_http")
HTTP_XRD_SUFFIX=(".ref_http" ".ref_#_http")

for ((i = 0; i < ${#host_names[@]}; i++)); do
       host="${host_names[i]}"
       ${XRDCP} ${LCLDATADIR}/${host}.ref ${host_roots[i]}/${RMTDATADIR}/${host}.ref

       for suffix in "${HTTP_SUFFIX[@]}"; do
              ${CURL} -v -L ${host_https[i]}/${RMTDATADIR}/${host}${suffix} -T ${LCLDATADIR}/${host}.ref
       done

done

# list uploaded files, then download them to check for corruption

for ((i = 0; i < ${#host_names[@]}; i++)); do
       ${XRDFS} "${host_roots[i]}" ls -l ${RMTDATADIR}
done

for ((i = 0; i < ${#host_names[@]}; i++)); do
       host="${host_names[i]}"
       ${XRDCP} ${host_roots[i]}/${RMTDATADIR}/${host}.ref ${LCLDATADIR}/${host}.dat
       count=0

       for suffix in "${HTTP_SUFFIX[@]}"; do
               ${CURL} -v -L ${host_https[i]}/${RMTDATADIR}/${host}${suffix} -o ${LCLDATADIR}/${host}.dat_http${count}
               count=$((count + 1))
       done
done

# check that all checksums for downloaded files match

for ((i = 0; i < ${#host_names[@]}; i++)); do
       host="${host_names[i]}"
       # check the CRC32 checksums
       REF32C=$(${CRC32C} < ${LCLDATADIR}/${host}.ref | cut -d' '  -f1)
       NEW32C=$(${CRC32C} < ${LCLDATADIR}/${host}.dat | cut -d' '  -f1)
       SRV32C=$(${XRDFS} ${host_roots[i]} query checksum ${RMTDATADIR}/${host}.ref?cks.type=crc32c | cut -d' ' -f2)

       if [[ "${NEW32C}" != "${REF32C}" || "${SRV32C}" != "${REF32C}" ]]; then
               echo "${host}:  crc32c: reference: ${REF32C}, server: ${SRV32C}, downloaded: ${REF32C}"
               echo 1>&2 "$(basename $0): error: crc32 checksum check failed for file: ${host}.dat"
               exit 1
       fi

       # check the HTTP files
       count=0

       for suffix in "${HTTP_SUFFIX[@]}"; do
               HTTP_SRV32C=$(${XRDFS} ${host_roots[i]} query checksum ${RMTDATADIR}/${host}${HTTP_XRD_SUFFIX[count]}?cks.type=crc32c | cut -d' ' -f2)
               HTTP_NEW32C=$(${CRC32C} < ${LCLDATADIR}/${host}.dat_http${count} | cut -d' '  -f1)

               if [[ "${HTTP_NEW32C}" != "${REF32C}" || "${HTTP_SRV32C}" != "${REF32C}" ]]; then
                       echo "${host}:  crc32c: reference: ${REF32C}, server: ${HTTP_SRV32C}, downloaded: ${HTTP_NEW32C}"
                       echo 1>&2 "$(basename $0): error: crc32 checksum check failed for file: ${host}${HTTP_SUFFIX[$count]}"
                       exit 1
               fi

               count=$((count + 1))
       done

       # check the ADLER32 checksums
       REFA32=$(${ADLER32} < ${LCLDATADIR}/${host}.ref | cut -d' '  -f1)
       NEWA32=$(${ADLER32} < ${LCLDATADIR}/${host}.dat | cut -d' '  -f1)
       SRVA32=$(${XRDFS} ${host_roots[i]} query checksum ${RMTDATADIR}/${host}.ref?cks.type=adler32 | cut -d' ' -f2)

       if [[ "${NEWA32}" != "${REFA32}" || "${SRVA32}" != "${REFA32}" ]]; then
               echo "${host}: adler32: reference: ${NEWA32}, server: ${SRVA32}, downloaded: ${NEWA32}"
               echo 1>&2 "$(basename $0): error: adler32 checksum check failed for file: ${host}.dat"
               exit 1
       fi

       # check the HTTP files
       count=0

       for suffix in "${HTTP_SUFFIX[@]}"; do
               HTTP_SRVA32=$(${XRDFS} ${host_roots[i]} query checksum ${RMTDATADIR}/${host}${HTTP_XRD_SUFFIX[count]}?cks.type=adler32 | cut -d' ' -f2)
               HTTP_NEWA32=$(${ADLER32} < ${LCLDATADIR}/${host}.dat_http${count} | cut -d' '  -f1)

               if [[ "${HTTP_NEWA32}" != "${REFA32}" || "${HTTP_SRVA32}" != "${REFA32}" ]]; then
                       echo "${host}:  adler32: reference: ${REFA32}, server: ${HTTP_SRVA32}, downloaded: ${HTTP_NEWA32}"
                       echo 1>&2 "$(basename $0): error: adler32 checksum check failed for file: ${host}${HTTP_SUFFIX[$count]}"
                       exit 1
               fi

               count=$((count + 1))

       done
done

# Renaming operation on meta manager fail with 501
# Not sure why this is the expected behaviour
# https://github.com/xrootd/xrootd/blob/8ac19b1d2b74521acff9ed0200052a2e373092cc/src/XrdHttp/XrdHttpReq.cc#L1746-L1752

move_src_names=(metaman man1 srv1)
move_src_codes=(501 201 201)

# Upload files
for ((i = 0; i < ${#move_src_names[@]}; i++)); do
    src="${move_src_names[i]}"
    src_idx=$(get_index_for_host "$src")
    curl -s -S -L -v -T "${LCLDATADIR}/srv1.ref" \
        "${host_https[$src_idx]}/${RMTDATADIR}/old_file_$src"
done

# Perform MOVE and check response
for ((i = 0; i < ${#move_src_names[@]}; i++)); do
    src="${move_src_names[i]}"
    src_idx=$(get_index_for_host "$src")
    expected_code="${move_src_codes[i]}"
    response_code=$(curl -s -v -S -L -o /dev/null -w "%{http_code}" -X MOVE \
        -H "Destination: ${host_https[$src_idx]}/${RMTDATADIR}/new_file_$src" \
        "${host_https[$src_idx]}/${RMTDATADIR}/old_file_$src")

    if [[ "$response_code" != "$expected_code" ]]; then
        echo "Assertion failed for '$src': expected $expected_code, got $response_code"
        exit 1
    else
        echo "Success: '$src' returned $response_code as expected."
    fi
done

for ((i = 0; i < ${#host_names[@]}; i++)); do
       host="${host_names[i]}"
       ${XRDFS} ${HOST_METAMAN} rm ${RMTDATADIR}/${host}.ref &
       rm ${LCLDATADIR}/${host}.dat &
       count=0

       for suffix in "${HTTP_SUFFIX[@]}"; do
               ${XRDFS} ${HOST_METAMAN} rm "${RMTDATADIR}/${host}${HTTP_XRD_SUFFIX[$count]}" &
               rm ${LCLDATADIR}/${host}.dat_http${count} &
               count=$((count + 1))
       done
done
wait

# Additional cleanup for move operation files
for ((i = 0; i < ${#move_src_names[@]}; i++)); do
    src="${move_src_names[i]}"
    if [[ "$src" == "man1" || "$src" == "srv1" ]]; then
        ${XRDFS} "${HOST_METAMAN}" rm "${RMTDATADIR}/new_file_${src}" &
    else
        ${XRDFS} "${HOST_METAMAN}" rm "${RMTDATADIR}/old_file_${src}" &
    fi
done

# Remove local file once after the loop
rm -f "${LCLDATADIR}/srv1.ref" &
wait

${XRDFS} ${HOST_METAMAN} rmdir ${RMTDATADIR}

echo "ALL TESTS PASSED"
exit 0
