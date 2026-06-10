#!/usr/bin/env bash

export KRB5CCNAME=${BINARY_DIR}/tests/krb5/krb5cc
export KRB5_CONFIG=${BINARY_DIR}/tests/krb5/krb5.conf

function setup_httpkrb5() {
	require_commands kinit curl
	assert kinit -p xrootd@XROOTD.ORG <<< xrootd
	assert klist -e
}

function test_httpkrb5() {
	export HTTPS_HOST="https://localhost:${XRD_PORT}"
	export CURL_CA="${SOURCE_DIR}/../tls/ca.pem"

	echo
	echo "client: XRootD $(xrdcp --version 2>&1)"
	echo

	TMPDIR=$(mktemp -d "${LOCAL_DIR}/test-XXXXXX")
	TESTFILE="${TMPDIR}/krb5test.txt"
	echo "kerberos over https" > "${TESTFILE}"

	# Upload with curl using SPNEGO (Negotiate) authentication
	assert curl --negotiate -u : \
		--cacert "${CURL_CA}" \
		-T "${TESTFILE}" \
		"${HTTPS_HOST}/krb5test.txt"

	# Download and verify contents
	DOWNLOAD="${TMPDIR}/krb5test.out"
	assert curl --negotiate -u : \
		--cacert "${CURL_CA}" \
		-o "${DOWNLOAD}" \
		"${HTTPS_HOST}/krb5test.txt"

	assert diff -u "${TESTFILE}" "${DOWNLOAD}"

	# Unauthenticated request must be rejected
	HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
		--cacert "${CURL_CA}" \
		"${HTTPS_HOST}/krb5test.txt")
	assert_eq 401 "${HTTP_CODE}" "unauthenticated GET should return 401"

	# HEAD request with Kerberos auth
	HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' \
		--negotiate -u : \
		--cacert "${CURL_CA}" \
		-I "${HTTPS_HOST}/krb5test.txt")
	assert_eq 200 "${HTTP_CODE}" "authenticated HEAD should return 200"

	# Clean up remote file
	assert curl --negotiate -u : \
		--cacert "${CURL_CA}" \
		-X DELETE \
		"${HTTPS_HOST}/krb5test.txt"
}
