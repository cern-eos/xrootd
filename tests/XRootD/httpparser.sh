#!/usr/bin/env bash

function setup_httpparser() {
	require_commands openssl curl
	openssl rand -base64 -out "${NAME}/macaroons-secret" 64
}

function teardown_httpparser() {
	rm -f "${NAME}/macaroons-secret"
}

function test_httpparser() {
	export HTTP_HOST="${HOST/root:/http:}"
	local tmpdir body out code alphabet
	tmpdir=$(mktemp -d "${LOCAL_DIR}/httpparser-XXXXXX")
	body="${tmpdir}/body.txt"
	echo "llhttp parser test payload" > "${body}"

	echo "Testing http.parser=llhttp"

	assert curl -s -T "${body}" "${HTTP_HOST}/llhttp-upload.txt"
	out="${tmpdir}/download.out"
	assert curl -s -o "${out}" "${HTTP_HOST}/llhttp-upload.txt"
	assert diff -u "${body}" "${out}"

	code=$(curl -s -o /dev/null -w '%{http_code}' -I "${HTTP_HOST}/llhttp-upload.txt")
	assert_eq 200 "${code}" "HEAD should return 200"

	# Sequential keep-alive requests on one connection
	read -r code1 code2 <<< "$(curl -s -H 'Connection: Keep-Alive' \
		-o /dev/null -w '%{http_code} ' "${HTTP_HOST}/llhttp-upload.txt" \
		--next -o /dev/null -w '%{http_code}' "${HTTP_HOST}/llhttp-missing.txt")"
	assert_eq 200 "${code1}" "keep-alive first GET should return 200"
	assert_eq 404 "${code2}" "keep-alive second GET should return 404"

	code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "${HTTP_HOST}/llhttp-upload.txt")
	assert_eq 200 "${code}" "DELETE should return 200"

	code=$(curl -s -o /dev/null -w '%{http_code}' "${HTTP_HOST}/llhttp-missing.txt")
	assert_eq 404 "${code}" "missing object should return 404"

	# OPTIONS preflight
	code=$(curl -s -o /dev/null -w '%{http_code}' -X OPTIONS "${HTTP_HOST}/llhttp-upload.txt")
	assert_eq 200 "${code}" "OPTIONS should return 200"

	# Chunked upload
	alphabet="${tmpdir}/alphabet.txt"
	printf 'abcdefghijklmnopqrstuvwxyz' > "${alphabet}"
	assert curl -s -H 'Transfer-Encoding: chunked' \
		-T "${alphabet}" "${HTTP_HOST}/llhttp-chunked.txt"
	assert curl -s -o "${out}" "${HTTP_HOST}/llhttp-chunked.txt"
	assert diff -u "${alphabet}" "${out}"

	# Malformed request line must not crash the server
	code=$(printf 'NOTHTTP\r\n\r\n' | curl -s -o /dev/null -w '%{http_code}' \
		--http0.9 -X GET "${HTTP_HOST}/" --data-binary @- || echo 000)
	assert_ne 000 "${code}" "malformed request should get an HTTP response"
}
