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
	openssl req -new -key ca.key -out ca.csr -subj '/O=XRootD/CN=XRootD Root CA'
	openssl ca -batch -selfsign -config tls.conf -in ca.csr -extensions xrootd_ca_ext -out ca.pem
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
	rehash_certs() {
		rm -f ./*.0 ./*.1 2>/dev/null || true
		# LibreSSL reports success for "openssl rehash" but does nothing.
		if ! openssl version 2>/dev/null | grep -q LibreSSL; then
			if openssl rehash . 2>/dev/null && compgen -G '*.0' >/dev/null; then
				return 0
			fi
		fi
		if openssl certhash . 2>/dev/null; then
			:
		fi
		if [[ -f ca.pem ]]; then
			local hash hash_old
			hash=$(openssl x509 -hash -noout -in ca.pem)
			ln -sf ca.pem "${hash}.0"
			hash_old=$(openssl x509 -subject_hash_old -noout -in ca.pem 2>/dev/null || true)
			if [[ -n "${hash_old}" && ! -e "${hash_old}.0" ]]; then
				ln -sf ca.pem "${hash_old}.0"
			fi
		fi
		if [[ -f root.crl ]]; then
			local hash hash_old
			hash=$(openssl x509 -hash -noout -in ca.pem)
			ln -sf root.crl "${hash}.r0"
			hash_old=$(openssl x509 -subject_hash_old -noout -in ca.pem 2>/dev/null || true)
			if [[ -n "${hash_old}" && ! -e "${hash_old}.r0" ]]; then
				ln -sf root.crl "${hash_old}.r0"
			fi
		fi
		if compgen -G '*.0' >/dev/null; then
			return 0
		fi
		if command -v c_rehash >/dev/null 2>&1; then
			c_rehash .
			return 0
		fi
		# Last resort: create the CA hash link manually.
		local cert hash
		for cert in ca.pem; do
			hash=$(openssl x509 -hash -noout -in "${cert}")
			ln -sf "${cert}" "${hash}.0"
		done
	}
	rehash_certs

	# Ensure that revoked certificate fails certificate verification
	if openssl verify -CApath . -crl_check -x509_strict revoked.crt; then
		exit 1
	fi

  # XRootD client/server expect restricted permissions on CA directory
  chmod 750 .
  chmod 600 ca.key host.key client.key invalid.key revoked.key
  chmod 644 ca.pem host.pem client.crt invalid.crt revoked.crt root.crl
}

function teardown() {
	rm -rf ca ca.csr ./*.{0,1,r0,r1,crl,crt,crtp,csr,key,pem}
}

[[ $(type -t "$1") == "function" ]] || die "unknown command: $1"
"$@"
