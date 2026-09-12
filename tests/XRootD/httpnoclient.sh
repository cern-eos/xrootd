#!/usr/bin/env bash

function setup_httpnoclient() {
	require_commands curl

	# Setup the issuer and protected directory
	mkdir -p "${NAME}/xrootd/protected"
	echo 'Hello, World' > "${NAME}/xrootd/protected/hello_world.txt"
}

function test_httpnoclient() {
	export HOST="https://localhost:${XRD_PORT}"

	# tlsclientauth is off; a client cert is neither required nor expected.
	# The previous command passed the server cert as a client cert, which
	# aborted the handshake (curl SSL_ERROR_ZERO_RETURN) before the 403.
	assert curl --cacert ../issuer/tlsca.pem -o /dev/null -w "%{http_code}" "$HOST/protected/hello_world.txt" > "${NAME}/curl_output.txt"
	assert_eq 403 "$(tail -n 1 < "${NAME}/curl_output.txt")"
}
