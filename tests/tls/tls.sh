#!/usr/bin/env bash

set -ex

function setup() {
	rm -rf ca
	mkdir -p ca
	touch ca/db
	echo 1000 > ca/serial
	echo 1000 > ca/serial-crl
	# Create private key and root certificate for the CA
	openssl genrsa -out ca.key 4096
	openssl req -x509 -new -key ca.key -subj '/O=XRootD/CN=XRootD Root CA' -days 30 -out ca.pem
	openssl verify -CAfile ca.pem ca.pem

	# Create private key and certificate for the XRootD server
	openssl genrsa -out host.key 4096
	openssl req -new -key host.key -outform PEM -out host.csr -subj '/CN=localhost'
	openssl ca -batch -config tls.conf -in host.csr -extensions xrootd_crt_ext -out host.pem
	openssl verify -CAfile ca.pem host.pem

	# Create private key and certificate for the XRootD client
	openssl genrsa -out client.key 4096
	openssl req -new -key client.key -out client.csr -subj '/CN=client'
	openssl ca -batch -config tls.conf -in client.csr -extensions xrootd_usr_ext -out client.crt
	openssl verify -CAfile ca.pem client.crt

	# Create a bad certificate which misses the required xrootd_usr_ext extensions
	openssl genrsa -out invalid.key 4096
	openssl req -new -key invalid.key -out invalid.csr -subj '/CN=invalid'
	openssl ca -batch -config tls.conf -in invalid.csr -out invalid.crt

	# Create a revoked certificate and a certificate revocation list
	openssl genrsa -out revoked.key 4096
	openssl req -new -key revoked.key -out revoked.csr -subj '/CN=revoked'
	openssl ca -batch -config tls.conf -in revoked.csr -extensions xrootd_usr_ext -out revoked.crt
	openssl ca -batch -config tls.conf -revoke revoked.crt

	openssl ca -batch -config tls.conf -gencrl -keyfile ca.key -cert ca.pem -out root.crl
	openssl crl -in root.crl -noout -text

	# Create symlinks based on certificate hashes (needed by XRootD TLS initialization)
	openssl rehash .

	# Ensure that revoked certificate fails certificate verification
	openssl verify -CApath . -crl_check -x509_strict revoked.crt && exit 1

  # XRootD client/server expect restricted permissions on CA directory
  chmod 750 .
  chmod 600 ca.key host.key client.key invalid.key revoked.key
  chmod 644 ca.pem host.pem client.crt invalid.crt revoked.crt root.crl
}

function teardown() {
	rm -rf ca ./*.{0,1,r0,r1,crl,crt,crtp,csr,key,pem}
}

[[ $(type -t "$1") == "function" ]] || die "unknown command: $1"
"$@"
