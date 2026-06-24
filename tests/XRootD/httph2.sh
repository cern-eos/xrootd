#!/usr/bin/env bash

function setup_httph2() {
	require_commands curl
}

function test_httph2() {
	export HTTPS_HOST="https://localhost:${XRD_PORT}"
	export CURL_CA="${PWD}/../tls/ca.pem"
	local tmpdir body out code alphabet

	tmpdir=$(mktemp -d "${LOCAL_DIR}/httph2-XXXXXX")
	body="${tmpdir}/body.txt"
	echo "http2 parser test payload" > "${body}"

	echo "Testing HTTPS with ALPN h2"

	assert curl --http2 --cacert "${CURL_CA}" -s -T "${body}" \
		"${HTTPS_HOST}/h2-upload.txt"
	out="${tmpdir}/download.out"
	assert curl --http2 --cacert "${CURL_CA}" -s -o "${out}" \
		"${HTTPS_HOST}/h2-upload.txt"
	assert diff -u "${body}" "${out}"

	code=$(curl --http2 --cacert "${CURL_CA}" -s -o /dev/null -w '%{http_code}' \
		-I "${HTTPS_HOST}/h2-upload.txt")
	assert_eq 200 "${code}" "HEAD over HTTP/2 should return 200"

	# Sequential GETs (separate connections; same-connection reuse is not yet covered)
	code1=$(curl --http2 --cacert "${CURL_CA}" -s -o /dev/null -w '%{http_code}' \
		"${HTTPS_HOST}/h2-upload.txt")
	code2=$(curl --http2 --cacert "${CURL_CA}" -s -o /dev/null -w '%{http_code}' \
		"${HTTPS_HOST}/h2-missing.txt")
	assert_eq 200 "${code1}" "HTTP/2 GET should return 200"
	assert_eq 404 "${code2}" "HTTP/2 GET for missing file should return 404"

	alphabet="${tmpdir}/alphabet.txt"
	printf 'abcdefghijklmnopqrstuvwxyz' > "${alphabet}"
	assert curl --http2 --cacert "${CURL_CA}" -s -T "${alphabet}" \
		"${HTTPS_HOST}/h2-chunked.txt"
	assert curl --http2 --cacert "${CURL_CA}" -s -o "${out}" \
		"${HTTPS_HOST}/h2-chunked.txt"
	assert diff -u "${alphabet}" "${out}"

	code=$(curl --http2 --cacert "${CURL_CA}" -s -o /dev/null -w '%{http_code}' \
		-X DELETE "${HTTPS_HOST}/h2-upload.txt")
	assert_eq 200 "${code}" "DELETE over HTTP/2 should return 200"
}
