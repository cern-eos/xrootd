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
	# TLS fixture writes certs into the build tree, not the source tree.
	export CURL_CA="${BINARY_DIR}/tests/tls/ca.pem"

	echo
	echo "client: XRootD $(xrdcp --version 2>&1)"
	echo

	TMPDIR=$(mktemp -d "${LOCAL_DIR}/test-XXXXXX")
	TESTFILE="${TMPDIR}/krb5test.txt"
	echo "kerberos over https" > "${TESTFILE}"

	# Upload with curl using SPNEGO (Negotiate) authentication.
	# Disable Expect: 100-continue: curl 7.61 (Alma 8) will otherwise
	# complete a Negotiate PUT without sending the body, so GET is empty.
	# Do not pass -f: older curl treats the 401 Negotiate challenge as a
	# hard failure and never sends the AP-REQ.
	HTTP_CODE=$(curl --negotiate -u : -s -o /dev/null -w '%{http_code}' \
		-H 'Expect:' --cacert "${CURL_CA}" \
		-T "${TESTFILE}" \
		"${HTTPS_HOST}/krb5test.txt")
	if [[ "${HTTP_CODE}" != "201" && "${HTTP_CODE}" != "200" ]]; then
		curl --negotiate -u : -v -H 'Expect:' --cacert "${CURL_CA}" \
			-T "${TESTFILE}" \
			"${HTTPS_HOST}/krb5test.txt" || true
		error "authenticated PUT should return 201/200, got ${HTTP_CODE}"
	fi

	# Download and verify contents
	DOWNLOAD="${TMPDIR}/krb5test.out"
	HTTP_CODE=$(curl --negotiate -u : -s -o "${DOWNLOAD}" -w '%{http_code}' \
		--cacert "${CURL_CA}" \
		"${HTTPS_HOST}/krb5test.txt")
	assert_eq 200 "${HTTP_CODE}" "authenticated GET should return 200"

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
