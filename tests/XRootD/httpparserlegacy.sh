#!/usr/bin/env bash

function setup_httpparserlegacy() {
	require_commands openssl curl
	openssl rand -base64 -out macaroons-secret 64
}

function teardown_httpparserlegacy() {
	rm -f macaroons-secret
}

function test_httpparserlegacy() {
	export HTTP_HOST="${HOST/root:/http:}"
	local tmpdir body out code
	tmpdir=$(mktemp -d "${LOCAL_DIR}/httpparserlegacy-XXXXXX")
	body="${tmpdir}/body.txt"
	echo "legacy parser test payload" > "${body}"

	echo "Testing http.parser=legacy"

	assert curl -s -T "${body}" "${HTTP_HOST}/legacy-upload.txt"
	out="${tmpdir}/download.out"
	assert curl -s -o "${out}" "${HTTP_HOST}/legacy-upload.txt"
	assert diff -u "${body}" "${out}"

	code=$(curl -s -o /dev/null -w '%{http_code}' -I "${HTTP_HOST}/legacy-upload.txt")
	assert_eq 200 "${code}" "HEAD should return 200"

	code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "${HTTP_HOST}/legacy-upload.txt")
	assert_eq 200 "${code}" "DELETE should return 200"
}
