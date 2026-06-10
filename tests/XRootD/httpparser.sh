#!/usr/bin/env bash

function setup_httpparser() {
	require_commands openssl curl
	openssl rand -base64 -out macaroons-secret 64
}

function teardown_httpparser() {
	rm -f macaroons-secret
}

function test_httpparser() {
	export HTTP_HOST="${HOST/root:/http:}"
	local tmpdir
	tmpdir=$(mktemp -d "${LOCAL_DIR}/httpparser-XXXXXX")
	local body="${tmpdir}/body.txt"
	echo "llhttp parser test payload" > "${body}"

	echo "Testing default http.parser=llhttp"

	assert curl -s -T "${body}" "${HTTP_HOST}/llhttp-upload.txt"
	local out="${tmpdir}/download.out"
	assert curl -s -o "${out}" "${HTTP_HOST}/llhttp-upload.txt"
	assert diff -u "${body}" "${out}"

	local code
	code=$(curl -s -o /dev/null -w '%{http_code}' -I "${HTTP_HOST}/llhttp-upload.txt")
	assert_eq 200 "${code}" "HEAD should return 200"

	code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "${HTTP_HOST}/llhttp-upload.txt")
	assert_eq 200 "${code}" "DELETE should return 200"

	code=$(curl -s -o /dev/null -w '%{http_code}' "${HTTP_HOST}/llhttp-missing.txt")
	assert_eq 404 "${code}" "missing object should return 404"
}
