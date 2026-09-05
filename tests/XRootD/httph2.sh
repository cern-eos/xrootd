#!/usr/bin/env bash

function setup_httph2() {
	require_commands curl openssl
}

function test_httph2() {
	export HTTPS_HOST="https://localhost:${XRD_PORT}"
	export CURL_CA="${PWD}/../tls/ca.pem"
	local tmpdir body out code alphabet outputFilePath
	local alphabetadler32 alphabetcrc32c alphabetmd5sumb64 alphabetadlerb64
	local code1 code2 expectedDigest receivedDigest expectedHeader receivedHeader
	local expectedContentRange contentRange expectedBody receivedBody
	local expectedContentLength receivedContentLength HTTP_CODE HTTP_CONTENTS
	local expectedDelimiters receivedDelimiters

	h2() {
		curl --http2 --cacert "${CURL_CA}" "$@"
	}

	# HTTP/2 responses lowercase field names; compare headers case-insensitively.
	assert_ieq() {
		local exp recv msg
		exp=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')
		recv=$(printf '%s' "$2" | tr '[:upper:]' '[:lower:]')
		msg=$3
		[[ "${exp}" == "${recv}" ]] || error "${msg}: expected $1 but received $2"
	}

	tmpdir=$(mktemp -d "${LOCAL_DIR}/httph2-XXXXXX")
	body="${tmpdir}/body.txt"
	echo "http2 parser test payload" > "${body}"
	out="${tmpdir}/download.out"
	outputFilePath="${tmpdir}/output.txt"

	echo "Testing HTTPS with ALPN h2"

	assert h2 -s -T "${body}" "${HTTPS_HOST}/h2-upload.txt"
	assert h2 -s -o "${out}" "${HTTPS_HOST}/h2-upload.txt"
	assert diff -u "${body}" "${out}"

	code=$(h2 -s -o /dev/null -w '%{http_code}' -I "${HTTPS_HOST}/h2-upload.txt")
	assert_eq 200 "${code}" "HEAD over HTTP/2 should return 200"

	read -r code1 code2 <<< "$(h2 -s \
		-o /dev/null -w '%{http_code} ' "${HTTPS_HOST}/h2-upload.txt" \
		--next --http2 --cacert "${CURL_CA}" \
		-o /dev/null -w '%{http_code}' "${HTTPS_HOST}/h2-missing.txt")"
	assert_eq 200 "${code1}" "HTTP/2 first GET on same connection should return 200"
	assert_eq 404 "${code2}" "HTTP/2 second GET on same connection should return 404"

	alphabet="${tmpdir}/alphabet.txt"
	printf 'abcdefghijklmnopqrstuvwxyz' > "${alphabet}"
	assert h2 -s -T "${alphabet}" "${HTTPS_HOST}/h2-chunked.txt"
	assert h2 -s -o "${out}" "${HTTPS_HOST}/h2-chunked.txt"
	assert diff -u "${alphabet}" "${out}"

	code=$(h2 -s -o /dev/null -w '%{http_code}' \
		-X DELETE "${HTTPS_HOST}/h2-upload.txt")
	assert_eq 200 "${code}" "DELETE over HTTP/2 should return 200"

	echo "Testing streamed PUT (2MiB)"
	dd if=/dev/urandom of="${tmpdir}/big.bin" bs=1024 count=2048 status=none
	assert h2 -s -T "${tmpdir}/big.bin" "${HTTPS_HOST}/h2-big.bin"
	assert h2 -s -o "${tmpdir}/big.out" "${HTTPS_HOST}/h2-big.bin"
	assert cmp "${tmpdir}/big.bin" "${tmpdir}/big.out"

	echo "Testing range GET"
	printf 'abcdefghijklmnopqrstuvw987' > "${alphabet}"
	assert h2 -s -T "${alphabet}" "${HTTPS_HOST}/h2-alphabet.txt"
	h2 -s -H 'range: bytes=0-3,24-26' "${HTTPS_HOST}/h2-alphabet.txt" \
		--output - | tr -d '\r' > "${outputFilePath}"
	contentRange=$(grep -i 'Content-range' "${outputFilePath}" | awk 'NR==1')
	expectedContentRange='Content-range: bytes 0-3/26'
	assert_eq "${expectedContentRange}" "${contentRange}" \
		"GET range-request test failed (first Content-range)"
	expectedBody='abcd'
	receivedBody=$(grep -E 'abcd$' "${outputFilePath}")
	assert_eq "${expectedBody}" "${receivedBody}" \
		"GET range-request test failed (first body)"
	contentRange=$(grep -i 'Content-range' "${outputFilePath}" | awk 'NR==2')
	expectedContentRange='Content-range: bytes 24-25/26'
	assert_eq "${expectedContentRange}" "${contentRange}" \
		"GET range-request test failed (second Content-range)"
	expectedBody='87'
	receivedBody=$(grep -E '87' "${outputFilePath}")
	assert_eq "${expectedBody}" "${receivedBody}" \
		"GET range-request test failed (second body)"
	expectedDelimiters=3
	receivedDelimiters=$(grep -c '\-\-123456' "${outputFilePath}")
	assert_eq "${expectedDelimiters}" "${receivedDelimiters}" \
		"GET range-request test failed (boundary delimiters)"

	h2 -s -H 'range: bytes=0-3' -o "${tmpdir}/range-single.out" \
		"${HTTPS_HOST}/h2-alphabet.txt"
	printf 'abcd' > "${tmpdir}/range-single.ref"
	assert diff -u "${tmpdir}/range-single.ref" "${tmpdir}/range-single.out"

	echo "Testing open-once cache with Range GETs on one connection"
	h2 -s -H 'range: bytes=0-12' -o "${tmpdir}/r1" "${HTTPS_HOST}/h2-alphabet.txt" \
		--next --http2 --cacert "${CURL_CA}" \
		-H 'range: bytes=13-25' -o "${tmpdir}/r2" "${HTTPS_HOST}/h2-alphabet.txt"
	cat "${tmpdir}/r1" "${tmpdir}/r2" > "${tmpdir}/r-all"
	assert diff -u "${alphabet}" "${tmpdir}/r-all"
	assert h2 -s -o "${tmpdir}/r-full" "${HTTPS_HOST}/h2-chunked.txt" \
		--next --http2 --cacert "${CURL_CA}" \
		-H 'range: bytes=0-3' -o "${tmpdir}/r-after-switch" \
		"${HTTPS_HOST}/h2-alphabet.txt"
	printf 'abcd' > "${tmpdir}/r-after-switch.ref"
	assert diff -u "${tmpdir}/r-after-switch.ref" "${tmpdir}/r-after-switch"

	alphabetadler32="$(xrdadler32 "${alphabet}" | cut -d' ' -f1)"
	alphabetcrc32c="$(xrdcrc32c -s "${alphabet}")"
	alphabetmd5sumb64='mRykpCtRV62NckS3pmYroQ=='
	alphabetadlerb64='jwQKXQ=='

	echo "Testing HEAD Want-Digest / Want-Repr-Digest"
	h2 -s -I -H 'Want-Digest: adler32' "${HTTPS_HOST}/h2-alphabet.txt" \
		| tr -d '\r' > "${outputFilePath}"
	grep '200' "${outputFilePath}" >/dev/null \
		|| error "HEAD request test failed: Failed to perform HEAD on h2-alphabet.txt"
	expectedDigest="Digest: adler32=${alphabetadler32}"
	receivedDigest=$(grep -i "Digest" "${outputFilePath}")
	assert_ieq "${expectedDigest}" "${receivedDigest}" "HEAD request test failed (adler32)"
	expectedContentLength="Content-Length: $(wc -c < "${alphabet}" | sed 's/^ *//')"
	receivedContentLength=$(grep -i 'Content-Length' "${outputFilePath}")
	assert_ieq "${expectedContentLength}" "${receivedContentLength}" \
		"HEAD request test failed (Content-Length)"

	h2 -s -I -H 'Want-Digest: adler' "${HTTPS_HOST}/h2-alphabet.txt" \
		| tr -d '\r' > "${outputFilePath}"
	expectedDigest="Digest: adler=${alphabetadler32}"
	receivedDigest=$(grep -i "Digest" "${outputFilePath}")
	assert_ieq "${expectedDigest}" "${receivedDigest}" "HEAD request test failed (adler)"

	h2 -s -I -H 'Want-Digest: crc32c' "${HTTPS_HOST}/h2-alphabet.txt" \
		| tr -d '\r' > "${outputFilePath}"
	expectedDigest="Digest: crc32c=${alphabetcrc32c}"
	receivedDigest=$(grep -i "Digest" "${outputFilePath}")
	assert_ieq "${expectedDigest}" "${receivedDigest}" "HEAD request test failed (crc32c)"

	h2 -s -I -H 'Want-Digest: md5' "${HTTPS_HOST}/h2-alphabet.txt" \
		| tr -d '\r' > "${outputFilePath}"
	expectedDigest="Digest: md5=${alphabetmd5sumb64}"
	receivedDigest=$(grep -i "Digest" "${outputFilePath}")
	assert_ieq "${expectedDigest}" "${receivedDigest}" "HEAD request test failed (md5)"

	h2 -s -I -H 'Want-Digest: NotSupported, adler32, crc32c' \
		"${HTTPS_HOST}/h2-alphabet.txt" | tr -d '\r' > "${outputFilePath}"
	expectedDigest="Digest: adler32=${alphabetadler32}"
	receivedDigest=$(grep -i "Digest" "${outputFilePath}")
	assert_ieq "${expectedDigest}" "${receivedDigest}" \
		"HEAD request test failed (digest not supported)"

	h2 -s -I -H 'Want-Repr-Digest: adler=1' "${HTTPS_HOST}/h2-alphabet.txt" \
		| tr -d '\r' > "${outputFilePath}"
	expectedDigest="Repr-Digest: adler=:${alphabetadlerb64}:"
	receivedDigest=$(grep -i "Repr-Digest" "${outputFilePath}")
	assert_ieq "${expectedDigest}" "${receivedDigest}" \
		"HEAD request test failed (Want-Repr-Digest)"

	h2 -s -I -H 'Want-Repr-Digest: adler=1,md5=5' \
		"${HTTPS_HOST}/h2-alphabet.txt" | tr -d '\r' > "${outputFilePath}"
	expectedDigest="Repr-Digest: md5=:${alphabetmd5sumb64}:"
	receivedDigest=$(grep -i "Repr-Digest" "${outputFilePath}")
	assert_ieq "${expectedDigest}" "${receivedDigest}" \
		"HEAD request test failed (Want-Repr-Digest multiple digests requested)"

	h2 -s -I -H 'Want-Digest: adler32' -H 'Want-Repr-Digest: md5=1' \
		"${HTTPS_HOST}/h2-alphabet.txt" | tr -d '\r' > "${outputFilePath}"
	expectedDigest="Digest: adler32=${alphabetadler32}"
	receivedDigest=$(grep -i "Digest" "${outputFilePath}")
	assert_ieq "${expectedDigest}" "${receivedDigest}" \
		"HEAD request test failed (Want-Digest prior to Want-Repr-Digest)"

	h2 -s -I -H 'Want-Repr-Digest: adler32' "${HTTPS_HOST}/h2-alphabet.txt" \
		| tr -d '\r' > "${outputFilePath}"
	receivedDigest=$(grep -c "Digest" "${outputFilePath}")
	assert_eq 0 "${receivedDigest}" \
		"HEAD request test failed (Malformed Want-Repr-Digest)"

	h2 -s -v --raw -H 'Want-Repr-Digest: adler=1' \
		"${HTTPS_HOST}/h2-alphabet.txt" 2>&1 | tr -d '\r' > "${outputFilePath}"
	grep -qi "repr-digest: adler=:${alphabetadlerb64}:" "${outputFilePath}" \
		|| error "GET with Want-Repr-Digest failed"

	h2 -s -v --raw -H 'Want-Digest: adler32' -H 'Want-Repr-Digest: md5=1' \
		"${HTTPS_HOST}/h2-alphabet.txt" 2>&1 | tr -d '\r' > "${outputFilePath}"
	grep -qi "digest: adler32=${alphabetadler32}" "${outputFilePath}" \
		|| error "GET with Want-Digest and Want-Repr-Digest failed"

	echo "Testing directory listing"
	assert h2 -s -T "${alphabet}" "${HTTPS_HOST}/h2-list/testlistings/01.ref"
	HTTP_CODE=$(h2 -s -o "${outputFilePath}" -w '%{http_code}' \
		"${HTTPS_HOST}/h2-list")
	assert_eq 200 "${HTTP_CODE}" "Directory listing should return 200"
	HTTP_CONTENTS=$(h2 -s "${HTTPS_HOST}/h2-list" | tr '"' '\n' | tr '<' '\n' \
		| tr '>' '\n' | grep testlistings/ | wc -l | tr -d ' ')
	assert_eq 2 "${HTTP_CONTENTS}" "Directory listing should include testlistings/"

	echo "Testing static headers"
	h2 -s -X OPTIONS -v --raw "${HTTPS_HOST}/h2-alphabet.txt" 2>&1 \
		| tr -d '\r' > "${outputFilePath}"
	grep -qi 'access-control-allow-origin: \*' "${outputFilePath}" \
		|| error "OPTIONS is missing statically-defined Access-Control-Allow-Origin"
	grep -qi 'test: 1' "${outputFilePath}" \
		|| error "OPTIONS is missing statically-defined Test header"

	h2 -s -v --raw "${HTTPS_HOST}/h2-alphabet.txt" 2>&1 \
		| tr -d '\r' > "${outputFilePath}"
	assert_eq "1" "$(grep -ci 'foo: bar' "${outputFilePath}")" \
		"Incorrect number of 'Foo: Bar' header values"
	assert_eq "1" "$(grep -ci 'foo: baz' "${outputFilePath}")" \
		"Incorrect number of 'Foo: Baz' header values"
	assert_eq "1" "$(grep -ci 'test: 1' "${outputFilePath}")" \
		"Incorrect number of 'Test' header values"

	h2 -I -s --raw "${HTTPS_HOST}/h2-alphabet.txt" 2>&1 \
		| tr -d '\r' > "${outputFilePath}"
	grep -qi 'test: 1' "${outputFilePath}" \
		|| error "HEAD is missing statically-defined Test header"

	echo "Testing CORS"
	h2 -s -D "${tmpdir}/cors-bad.hdr" -o /dev/null \
		-H 'Origin: does_not_exist' "${HTTPS_HOST}/h2-alphabet.txt"
	assert_eq 0 "$(grep -ci 'access-control-allow-origin' "${tmpdir}/cors-bad.hdr")" \
		"CORS should omit Allow-Origin for unknown origin"

	h2 -s -D "${tmpdir}/cors-ok.hdr" -o /dev/null \
		-H 'Origin: https://webserver.bli.bla.blo' \
		"${HTTPS_HOST}/h2-alphabet.txt"
	grep -qi 'access-control-allow-origin: https://webserver.bli.bla.blo' \
		"${tmpdir}/cors-ok.hdr" \
		|| error "CORS should emit Allow-Origin for configured origin"

	echo "Testing HTTP error codes"
	code=$(h2 -s -o /dev/null -w '%{http_code}' "${HTTPS_HOST}/h2-does-not-exist")
	assert_eq 404 "${code}" "GET of missing file should return 404"

	code=$(h2 -s -o /dev/null -w '%{http_code}' \
		-T "${alphabet}" "${HTTPS_HOST}/readonly/file")
	assert_eq 403 "${code}" "PUT to readonly export should return 403"

	code=$(h2 -s -o /dev/null -w '%{http_code}' \
		-T "${alphabet}" "${HTTPS_HOST}/h2-list")
	assert_eq 409 "${code}" "PUT over a directory should return 409"

	echo "Testing multiplexed GETs on one connection"
	local i
	for i in 1 2 3 4; do
		echo "payload ${i}" > "${tmpdir}/p${i}.txt"
		assert h2 -s -T "${tmpdir}/p${i}.txt" "${HTTPS_HOST}/h2-par-${i}.txt"
	done
	assert h2 --parallel --parallel-immediate -s \
		-o "${tmpdir}/g1" "${HTTPS_HOST}/h2-par-1.txt" \
		-o "${tmpdir}/g2" "${HTTPS_HOST}/h2-par-2.txt" \
		-o "${tmpdir}/g3" "${HTTPS_HOST}/h2-par-3.txt" \
		-o "${tmpdir}/g4" "${HTTPS_HOST}/h2-par-4.txt"
	for i in 1 2 3 4; do
		assert diff -u "${tmpdir}/p${i}.txt" "${tmpdir}/g${i}"
	done

	echo "Testing overlapping large parallel GETs"
	local j
	for j in 1 2 3 4; do
		dd if=/dev/urandom of="${tmpdir}/lg${j}.bin" bs=1024 count=512 status=none
		assert h2 -s -T "${tmpdir}/lg${j}.bin" "${HTTPS_HOST}/h2-lg-${j}.bin"
	done
	assert h2 --parallel --parallel-immediate -s \
		-o "${tmpdir}/lg1.out" "${HTTPS_HOST}/h2-lg-1.bin" \
		-o "${tmpdir}/lg2.out" "${HTTPS_HOST}/h2-lg-2.bin" \
		-o "${tmpdir}/lg3.out" "${HTTPS_HOST}/h2-lg-3.bin" \
		-o "${tmpdir}/lg4.out" "${HTTPS_HOST}/h2-lg-4.bin"
	for j in 1 2 3 4; do
		assert cmp "${tmpdir}/lg${j}.bin" "${tmpdir}/lg${j}.out"
	done

	echo "Testing same-connection reuse after a large GET"
	read -r code1 code2 <<< "$(h2 -s \
		-o /dev/null -w '%{http_code} ' "${HTTPS_HOST}/h2-big.bin" \
		--next --http2 --cacert "${CURL_CA}" \
		-o /dev/null -w '%{http_code}' "${HTTPS_HOST}/h2-alphabet.txt")"
	assert_eq 200 "${code1}" "large GET on reused connection should return 200"
	assert_eq 200 "${code2}" "follow-up GET after large response should return 200"

	echo "Testing HTTP/2 trailers (X-Transfer-Status)"
	h2 -s -v -H 'X-Transfer-Status: true' -H 'TE: trailers' \
		-o "${out}" "${HTTPS_HOST}/h2-alphabet.txt" 2> "${tmpdir}/trailer.err"
	assert diff -u "${alphabet}" "${out}"
	grep -qi 'x-transfer-status: 200: OK' "${tmpdir}/trailer.err" \
		|| error "HTTP/2 GET should deliver X-Transfer-Status trailer"

	echo "Testing PROPFIND with request body over HTTP/2"
	code=$(h2 -s -o "${outputFilePath}" -w '%{http_code}' -X PROPFIND \
		-H 'Depth: 0' --data-binary \
		'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/></D:prop></D:propfind>' \
		"${HTTPS_HOST}/h2-alphabet.txt")
	assert_eq 207 "${code}" "PROPFIND with body over HTTP/2 should return 207"
	grep -q 'getcontentlength' "${outputFilePath}" \
		|| error "PROPFIND response should contain getcontentlength"

	echo "Testing RST_STREAM on an active transfer"
	# --max-filesize makes curl cancel the stream; the connection must survive.
	read -r code1 code2 <<< "$(h2 -s --max-filesize 1000 \
		-o /dev/null -w '%{http_code} ' "${HTTPS_HOST}/h2-big.bin" \
		--next --http2 --cacert "${CURL_CA}" \
		-o /dev/null -w '%{http_code}' "${HTTPS_HOST}/h2-alphabet.txt" || true)"
	assert_eq 200 "${code2}" "GET after a cancelled stream on the same connection should return 200"
	assert h2 -s -o "${tmpdir}/big.out" "${HTTPS_HOST}/h2-big.bin"
	assert cmp "${tmpdir}/big.bin" "${tmpdir}/big.out"

	if command -v nghttp >/dev/null 2>&1; then
		echo "Testing flow control with a 64KiB window (nghttp)"
		# GET: server must park the read loop until WINDOW_UPDATE arrives.
		# --no-push: the pushed http.h2push body would also land on stdout.
		nghttp --no-verify-peer --no-push -w 16 -W 16 --timeout=30 \
			"${HTTPS_HOST}/h2-big.bin" > "${tmpdir}/ng-get.out" \
			|| error "nghttp small-window GET failed"
		assert cmp "${tmpdir}/big.bin" "${tmpdir}/ng-get.out"
		grep -q 'HTTP/2 flow control: response window exhausted' \
			"${XROOTD_SERVER_LOGFILE}" \
			|| error "server should have parked the read loop on a 64KiB window"
		# PUT: server must only open the window as the body is consumed.
		nghttp --no-verify-peer -w 16 -W 16 --timeout=30 -n \
			-H ':method: PUT' -d "${tmpdir}/big.bin" \
			"${HTTPS_HOST}/h2-ng-put.bin" \
			|| error "nghttp small-window PUT failed"
		assert h2 -s -o "${tmpdir}/ng-put.out" "${HTTPS_HOST}/h2-ng-put.bin"
		assert cmp "${tmpdir}/big.bin" "${tmpdir}/ng-put.out"
	else
		echo "nghttp not available; skipping small-window flow control checks"
	fi

	echo "Testing HTTP/2 server push"
	echo "pushed payload" > "${tmpdir}/push-target.txt"
	assert h2 --connect-timeout 5 --max-time 15 -s -T "${tmpdir}/push-target.txt" \
		"${HTTPS_HOST}/h2-push-target.txt"
	assert h2 --connect-timeout 5 --max-time 15 -s -o /dev/null \
		"${HTTPS_HOST}/h2-alphabet.txt"
	if command -v nghttp >/dev/null 2>&1; then
		nghttp -n -v --no-verify-peer --timeout=10 \
			"${HTTPS_HOST}/h2-alphabet.txt" > "${tmpdir}/nghttp.out" 2>&1 || true
		grep -qi 'PUSH_PROMISE' "${tmpdir}/nghttp.out" \
			|| error "nghttp should observe PUSH_PROMISE"
		grep -q 'HTTP/2 PUSH_PROMISE' "${XROOTD_SERVER_LOGFILE}" \
			|| error "server should log PUSH_PROMISE for http.h2push"
	elif grep -q 'HTTP/2 PUSH_PROMISE' "${XROOTD_SERVER_LOGFILE}"; then
		echo "PUSH_PROMISE logged (curl may have SETTINGS_ENABLE_PUSH=0)"
	else
		echo "No HTTP/2 client with push enabled; skipping PUSH_PROMISE wire check"
	fi
}
