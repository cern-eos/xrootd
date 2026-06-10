# Kerberos authentication over HTTPS

XrdHTTP supports Kerberos 5 client authentication over HTTPS using
[SPNEGO](https://datatracker.ietf.org/doc/html/rfc4559) (`WWW-Authenticate:
Negotiate`). This is the same mechanism used by tools such as
[curl](https://curl.se/docs/manpage.html) with `--negotiate`.

The native XRootD protocol continues to use the `XrdSeckrb5` security plugin
(`sec.protocol krb5`). HTTP Kerberos is configured separately via
`http.auth krb5` and authenticates requests before the HTTP bridge login.

## Requirements

Build XRootD with Kerberos and HTTP support enabled:

```bash
cmake .. -DENABLE_KRB5=ON -DENABLE_HTTP=ON
```

The build links against the Kerberos and GSS-API libraries (`krb5`,
`gssapi_krb5`).

On the server:

- HTTPS must be configured (`xrd.tls`, and typically `xrd.tlsca`).
- A keytab containing the HTTP service principal for the host.
- TLS client certificate authentication should usually be disabled when
  using Kerberos (`http.tlsclientauth off`), unless both are intentionally
  required.

On the client:

- A valid Kerberos ticket (for example via `kinit`).
- An HTTP client that supports SPNEGO, such as curl 7.40 or later.

## Service principal

HTTP clients request a ticket for the **`HTTP`** service class, not `host`.
The principal in the keytab and in the server configuration must therefore
look like:

```text
HTTP/hostname@REALM
```

For example:

```text
HTTP/localhost@EXAMPLE.ORG
HTTP/myserver.example.org@EXAMPLE.ORG
```

Create the principal and add it to the keytab with your KDC tools. With MIT
Kerberos:

```bash
kadmin.local -q "addprinc -randkey HTTP/myserver.example.org@EXAMPLE.ORG"
kadmin.local -q "ktadd -k /etc/krb5.keytab HTTP/myserver.example.org"
```

The configuration directive accepts the `<host>` keyword, which is expanded
to the local hostname at startup (same convention as `sec.protocol krb5`):

```text
HTTP/<host>@EXAMPLE.ORG
```

## Server configuration

Minimal example:

```cfg
xrd.protocol https:443 libXrdHttp.so

xrd.tlsca certfile /etc/grid-security/certificates/ca.pem
xrd.tls      /etc/grid-security/hostcert.pem /etc/grid-security/hostkey.pem

http.tlsclientauth off
http.auth krb5 /etc/krb5.keytab HTTP/<host>@EXAMPLE.ORG

all.export /
```

Directive syntax:

```text
http.auth krb5 <keytab> <principal>
```

| Argument     | Description                                      |
|--------------|--------------------------------------------------|
| `keytab`     | Path to the server keytab file                   |
| `principal`  | HTTP service principal (see above)             |

When `http.auth krb5` is enabled:

- Only **HTTPS** connections are accepted for authenticated access. Plain
  HTTP receives `403 Forbidden`.
- Unauthenticated requests receive `401 Unauthorized` with
  `WWW-Authenticate: Negotiate`.
- After a successful handshake, the client identity is stored in
  `SecEntity.name` (username part of the principal) and
  `SecEntity.moninfo` (full principal). `SecEntity.prot` is set to `krb5`.

## Client usage

Obtain a Kerberos ticket, then use curl with `--negotiate`:

```bash
export KRB5CCNAME=/tmp/krb5cc_${UID}   # if needed
kinit alice@EXAMPLE.ORG

curl --negotiate -u : \
  --cacert /path/to/ca.pem \
  https://myserver.example.org:443/path/to/file
```

The `-u :` option is required by curl: it enables Negotiate authentication
without sending HTTP Basic credentials.

Other examples:

```bash
# Upload a file
curl --negotiate -u : \
  --cacert /path/to/ca.pem \
  -T local.dat \
  https://myserver.example.org/data/local.dat

# HEAD request
curl --negotiate -u : \
  --cacert /path/to/ca.pem \
  -I \
  https://myserver.example.org/data/local.dat
```

The hostname in the URL must match the service principal (or its DNS alias
as known to the KDC). curl requests a ticket for `HTTP/hostname@REALM` based
on the URL host.

## Authentication flow

```text
Client                                 Server
  |                                       |
  |  GET /file  (HTTPS, no Authorization) |
  |-------------------------------------->|
  |                                       |
  |  401 WWW-Authenticate: Negotiate      |
  |<--------------------------------------|
  |                                       |
  |  GET /file                            |
  |  Authorization: Negotiate <token>     |
  |-------------------------------------->|
  |                                       |
  |  200 OK  (or further 401 rounds)      |
  |<--------------------------------------|
```

The server uses GSS-API `gss_accept_sec_context` to validate the SPNEGO
token. Multi-round handshakes reuse the same TCP connection; the GSS
context is kept per connection until authentication completes.

## Testing

An integration test is provided under `tests/XRootD/`:

- `httpkrb5.cfg` — server configuration
- `httpkrb5.sh` — curl-based test script

The test requires the Kerberos and TLS test fixtures and runs on Linux CI
(`BUILD_KRB5 AND NOT APPLE`). It verifies upload, download, HEAD, DELETE,
and rejection of unauthenticated requests.

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| `403 Kerberos authentication requires HTTPS` | Request arrived over plain HTTP while `http.auth krb5` is enabled |
| `401` on every request | No valid client ticket; run `kinit` and check `klist` |
| `401` / GSS failure after token sent | Wrong service principal in keytab; ensure `HTTP/host@REALM`, not `host/host@REALM` |
| curl does not send Negotiate | Missing `-u :` or curl built without SPNEGO/GSS support |
| Principal mismatch | URL hostname does not match the `HTTP/` principal in the keytab |

Enable HTTP tracing to inspect the handshake:

```cfg
http.trace auth
```

Server-side Kerberos debug can also be enabled with the usual `XrdSecDEBUG`
environment variable used by `XrdSeckrb5`.
