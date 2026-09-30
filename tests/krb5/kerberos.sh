#!/usr/bin/env bash

set -e

# Add /usr/sbin to PATH so that kerberos binaries (kdb5_util, krb5kdc
# and kadmin.local) are found when the test is run by a user without
# it.

export PATH="${PATH}:/usr/sbin"

export KRB5CCNAME="${PWD}/krb5cc"
export KRB5_CONFIG="${PWD}/krb5.conf"
export KRB5_KDC_PROFILE="${PWD}/kdc.conf"

function setup() {
	rm -f "${KRB5CCNAME}" krb5.keytab kdc/{db*,*.{log,pem,srl}}

	pushd kdc >/dev/null || exit 1

	# Create certificates for KDC PKINIT preauthentication mechanism
	openssl genrsa -out cakey.pem 2048
	openssl req -key cakey.pem -new -x509 -out cacert.pem -days 30 \
	    -subj '/O=XRootD/CN=XRootD Kerberos CA'

	openssl genrsa -out kdckey.pem 2048
	openssl req -new -out kdc.req -key kdckey.pem \
	    -subj '/O=XRootD/CN=XRootD Kerberos Realm'
	env REALM=XROOTD.ORG openssl x509 -req -in kdc.req \
	    -CAkey cakey.pem -CA cacert.pem -out kdc.pem -days 30 \
	    -extfile extensions.kdc -extensions kdc_cert -CAcreateserial
	rm kdc.req

	openssl genrsa -out clientkey.pem 2048
	openssl req -new -key clientkey.pem -out client.req \
	    -subj '/O=XRootD/CN=XRootD Kerberos Client'
	env REALM=XROOTD.ORG CLIENT=xrootd@XROOTD.ORG \
	    openssl x509 -in client.req -CAkey cakey.pem -CA cacert.pem \
	    -req -extensions client_cert -extfile extensions.client \
	    -days 30 -out client.pem
	rm client.req

	popd >/dev/null || exit 1

	# Create the KDC database
	kdb5_util create -s -r XROOTD.ORG -P xrootd

	# Start the KDC daemons
	krb5kdc -P "${PWD}"/krb5kdc.pid

	# Not really needed, since we use kadmin.local
	# kadmind -P ${PWD}/kadmind.pid

	# Add principals for the server and client to KDC database.
	# HTTP/<container-hostname> covers GSS name canonicalization when
	# curl/GSSAPI does not keep the URL host as "localhost".
	local http_hosts="localhost"
	local hn
	local host_re='^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$'
	for hn in "$(hostname -s 2>/dev/null || true)" "$(hostname 2>/dev/null || true)"; do
		# Host-based principals only accept a DNS-ish instance name.
		# hostname -f is not used here: on CI it can be an Azure FQDN
		# or garbage on stdout, and a failed add_principal aborts setup.
		[[ "${hn}" =~ ${host_re} ]] || continue
		[[ "${hn}" != "localhost" ]] || continue
		case " ${http_hosts} " in
			*" ${hn} "*) ;;
			*) http_hosts="${http_hosts} ${hn}" ;;
		esac
	done

	{
		echo "add_principal -randkey -kvno 1 host/localhost@XROOTD.ORG"
		echo "ktadd -k krb5.keytab host/localhost"
		echo "add_principal -randkey -kvno 1 HTTP/localhost@XROOTD.ORG"
		echo "ktadd -k krb5.keytab HTTP/localhost"
		echo "add_principal xrootd@XROOTD.ORG"
		echo "xrootd"
		echo "xrootd"
	} | kadmin.local -r XROOTD.ORG

	for hn in ${http_hosts}; do
		[[ "${hn}" != "localhost" ]] || continue
		{
			echo "add_principal -randkey -kvno 1 HTTP/${hn}@XROOTD.ORG"
			echo "ktadd -k krb5.keytab HTTP/${hn}"
		} | kadmin.local -r XROOTD.ORG || true
	done

	# Display KDC database entries
	kdb5_util tabdump -o - keyinfo

	# Display contents of server keytab
	klist -kte krb5.keytab
}

function teardown() {
	export PIDFILE=krb5kdc.pid
	if test -s "${PIDFILE}"; then
		PID="$(tr -d '[:space:]' < "${PIDFILE}")"
		if [[ "${PID}" =~ ^[0-9]+$ ]] && kill -0 "${PID}" 2>/dev/null; then
			kill -s TERM "${PID}"
			rm "${PIDFILE}"
		fi
	fi
	# setup may fail before the KDC creates logs; do not fail cleanup.
	tail -n "${MAXLINES:-200}" kdc/*.log 2>/dev/null || true
	rm -f "${KRB5CCNAME}" krb5.keytab kdc/{db*,*.{log,pem,srl}}
}

[[ $(type -t "$1") == "function" ]] || die "unknown command: $1"
"$@"
