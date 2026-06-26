# XrdSecoauth2

`XrdSecoauth2` is an XRootD security protocol plugin (`sec.protocol oauth2`) that
authenticates OAuth2 bearer tokens (including OIDC JWTs) over TLS.

This implementation performs OAuth2 bearer JWT validation directly in the
plugin (no SciTokens helper dependency).

## Client token pickup order

The client side `getCredentials()` searches for a token in this order:

1. `BEARER_TOKEN` (raw token value)
2. `BEARER_TOKEN_FILE` (path to token file)
3. `XDG_RUNTIME_DIR/bt_u<uid>`
4. `/tmp/bt_u<uid>`

If no token is found, authentication fails with `ENOPROTOOPT`.

When a token file is used (`*_TOKEN_FILE`, `XDG_RUNTIME_DIR/bt_u<uid>`,
`/tmp/bt_u<uid>`), it must be:

- a regular file (no device/FIFO/etc.),
- owned by the effective client uid,
- accessible by owner only (no group/other permission bits),
- opened with `O_NOFOLLOW` (a symlink at the token path is rejected).

## Server initialization parameters

`XrdSecProtocoloauth2Init()` supports:

- `-maxsz <num>`: maximum token size (default 8192, max 524288)
- `-expiry {ignore|optional|required}`:
  - `ignore` = do not enforce the expiry claim (a present `exp` is not checked)
  - `optional` = enforce only if expiry present
  - `required` = expiry must be present and valid
- `-issuer <url>`: expected `iss` claim value; if `-oidc-config-url` is not
specified, discovery URL defaults to `<issuer>/.well-known/openid-configuration`
- `-audience <value>`: expected token audience (`aud` string or array member);
may be repeated for the current issuer
- `-oidc-config-url <https-url>`: OpenID discovery URL (used to locate JWKS URI)
- `-jwks-url <https-url>`: explicit JWKS endpoint (overrides discovery lookup)
- `-jwks-refresh <seconds>`: JWKS refresh cache interval (default 300)
- `-jwks-cache-file <path>`: optional on-disk JWKS cache file shared across
issuers (disabled by default)
- `-jwks-cache-ttl <seconds>`: TTL for on-disk cached issuer keys
(`0` = use `-jwks-refresh`; default 0)
- `-clock-skew <seconds>`: allowed clock skew for time-based claims (default 60,
max 3600)
- `-identity-claim <claim>`: add claim names (in order) used for user identity;
may be specified multiple times
- `-forced-identity-claim <claim>`: for the current `-issuer`, force identity
extraction to this single claim (no fallback order for that issuer)
  - special case: when set to `email`, the email value is mapped to local
  username via `-email-map` entries or `[email-map]` in `oauth2.cfg`
- `-base-path <path>`: for the current `-issuer`, a single absolute path;
exported as `base_path` on `XrdSecEntity` when defined (a later `-base-path`
replaces any previous value for that issuer)
- `-restricted-path <path>`: for the current `-issuer`, one absolute path; may be
repeated (like `-audience`); exported as a JSON array on `restricted_path` when
at least one path is defined
- `-entity-claim <claim>`: export a JWT claim to `XrdSecEntity` extension
attributes (`Entity.eaAPI`); use `claim=attr.key` or bare `claim` (maps to
`token.<claim>`); may be repeated; replaces the default entity-claim list
- `-role-claim <claim>`: map a JWT string or string-array claim to
`XrdSecEntity.role` (array values are joined with spaces)
- `-grps-claim <claim>`: map a JWT string or string-array claim to
`XrdSecEntity.grps` (array values are joined with spaces)
- `-email-map <email>=<username>`: map a token `email` claim (normalized to
lowercase) to a local username; may be repeated (also available as
`[email-map]` in the INI file)
- `-debug-token`: print decoded JWT header/payload on successful auth (debug only;
contains sensitive information)
- `-show-token-claims`: print selected claims only (`alg`, `kid`, `typ`, `iss`,
`aud`, `sub`, `preferred_username`, `azp`, `iat`, `nbf`, `exp`)
- `-token-cache-max <num>`: max number of cached validated tokens (default 10000;
set `0` to disable caching)
- `-token-cache-noexp-ttl <seconds>`: cache TTL for tokens that do not include
`exp` when `-expiry optional|ignore` (default 60)
- `-config-file <path>`: load INI config from this path (instead of default
`/etc/xrootd/oauth2.cfg`); supports runtime reload of `[issuer ...]` and
`[email-map]` sections on inode/mtime changes

If no inline parameters are supplied on `sec.protocol oauth2`, the plugin
automatically tries to load `/etc/xrootd/oauth2.cfg` (INI-style) and maps keys to
the same options listed above.

`-issuer` starts a new issuer policy block. `-audience`, `-oidc-config-url`, and
`-jwks-url` that follow apply to that issuer until the next `-issuer`.
`-forced-identity-claim`, `-base-path`, and `-restricted-path` are also
issuer-scoped. Repeat `-restricted-path` (or `restricted_path` lines in an
`[issuer ...]` INI section) to configure multiple restricted paths; each issuer
has at most one `base_path`.

Multiple `sec.protparm oauth2` lines in `xrootd.cf` are supported (XRootD joins them
with newlines). List every `sec.protparm oauth2` line **before** `sec.protocol oauth2`,
for example:

```conf
sec.protparm oauth2 -issuer https://auth.cern.ch/auth/realms/cern
sec.protparm oauth2 -audience eos-service -expiry required -show-token-claims
sec.protparm oauth2 -forced-identity-claim email -email-map user@example.org=localuser
sec.protocol oauth2
```

## TLS requirement

`oauth2` rejects non-TLS connections. Both client and server constructors enforce
TLS-only use.

## HTTPS (XrdHttp) authentication

HTTPS bearer authentication is routed through the same `sec.protocol oauth2` plugin
as `root://` (via the XRootD security framework). Configure OAuth2 once with
`sec.protparm oauth2` / `sec.protocol oauth2`, then enable HTTP bearer handling:

```conf
xrootd.seclib libXrdSec.so
xrootd.tls all
http.tlsclientauth off
http.header2cgi Authorization authz
http.oauth2 on

sec.protocol oauth2
```

`http.oauth2` modes:

- `on` or `optional` — validate `Authorization: Bearer <token>` when present
- `require` — reject requests without a valid bearer token (unless mTLS already
set the identity)

`sec.protocol oauth2` (and any `sec.protparm oauth2` lines) must be present in the
configuration; `http.oauth2` only enables bearer-token handling over HTTPS.
When both xrootd and HTTP are enabled in the same process, they share a single
`XrdSecService` instance and OAuth2 initializes once.
Bearer tokens may be sent via the `Authorization` header (with `http.header2cgi`)
or as an `authz` CGI parameter. **Prefer the `Authorization` header.** A token
passed as the `authz` query parameter becomes part of the request URL and may be
exposed in proxy/access logs, monitoring, and `Referer` headers across the stack;
the `Authorization` header avoids URL-level leakage (XRootD itself obfuscates the
token in its own trace logs). Requests carrying more than one `Authorization`
header (including case-variant duplicates) are rejected with `400` as ambiguous.

Failed bearer authentication returns an RFC 6750 `WWW-Authenticate: Bearer`
challenge (with `error="invalid_token"` for an invalid/expired token, or
`error="invalid_request"` for malformed credentials) so clients can detect the
bearer-protected resource and trigger a token refresh.

On HTTP keep-alive connections the same bearer token presented again is accepted
without re-validation only while it has not yet expired — a pure clock
comparison against the token's recorded `exp`, with no crypto, JSON parsing, or
allocation on the hot path. Once that expiry passes (or the token changes) the
token is re-validated, so an expired token can never be reused for the lifetime
of the connection. A changed `Authorization` bearer token triggers
a fresh `XrdSecEntity` and xrootd `Bridge` login, and any identity/attributes
from the previous token are cleared first so stale scopes/groups/paths cannot
leak into the new identity. In `require` mode a valid bearer token must be
present on **every** request (a token validated on an earlier request does not
authorize a later token-less request on the same connection). Client-certificate
identities still take precedence and are not replaced by bearer tokens.

## Server-side identity mapping

After signature and claim validation, the server sets `XrdSecEntity.name` from
claims in this default order:

1. `sub`
2. `username`
3. `upn`

`preferred_username` and `name` are exported as entity attributes by default
(`token.preferred_username`, `token.name`) rather than used for identity.
Use repeated `-identity-claim` options to override the identity order, and
`-entity-claim` to override the default entity-attribute mappings.

Per-issuer override: use `-forced-identity-claim <claim>` after a specific
`-issuer` to force a single claim for that issuer.
If `forced-identity-claim = email` is used, authentication fails unless the
token has an `email` claim and that email is present in `[email-map]`.

## Native entity field mapping (`role`, `grps`)

Use `-role-claim` and `-grps-claim` to copy JWT claims directly onto the native
`XrdSecEntity.role` and `XrdSecEntity.grps` fields (not extension attributes).

Claim values may be a single string or a JSON string array; arrays are joined
with spaces, matching the format used for multi-value entity fields elsewhere in
XRootD.

Examples:

```conf
# Keycloak-style roles claim
sec.protparm oauth2 -role-claim roles

# WLCG IAM groups claim (dotted claim name)
sec.protparm oauth2 -grps-claim wlcg.groups
```

Token fragment:

```json
{
  "sub": "alice",
  "roles": ["admin", "user"],
  "wlcg.groups": ["/wlcg/it", "/wlcg/usatlas"]
}
```

Result on `XrdSecEntity`:

| Field | Value |
| - | - |
| `name` | `alice` |
| `role` | `admin user` |
| `grps` | `/wlcg/it /wlcg/usatlas` |

INI equivalent in `oauth2.cfg`:

```ini
[global]
role-claim = roles
grps-claim = wlcg.groups
```

## Example config snippet

```conf
sec.protocol oauth2 \
  -issuer https://issuer-a.example \
  -audience xrootd \
  -audience xrootd-admin \
  -issuer https://issuer-b.example \
  -audience service-b \
  -expiry required
```

## CERN SSO vs WLCG IAM

CERN operates two related but distinct OIDC ecosystems:

| | CERN SSO (Keycloak) | WLCG IAM (INDIGO IAM) |
| - | --------------------- | ---------------------- |
| Role | Login / identity for CERN apps | VO-scoped tokens for WLCG storage/compute |
| Typical issuer | `https://auth.cern.ch/auth/realms/cern` | VO-specific IAM URL (e.g. `https://wlcg.cloud.cnaf.infn.it/`) |
| Token for XRootD | `id_token` or SSO `access_token` | WLCG-profile **`access_token` JWT** |
| Key claims | `sub`, `email`, `preferred_username` | `scope` (`storage.*`), `wlcg.ver`, `sub` |
| Storage authorization | **No** `storage.*` scopes in SSO tokens | **Yes**, with `XrdAccToken` |

`XrdSecoauth2` accepts **both** issuers when configured. Use CERN SSO for
authentication-only setups. For EOS/XRootD path authorization, clients must
present an **IAM access token** with WLCG storage scopes — a CERN SSO
`id_token` from `xrdtoken CERNOIDC` is not sufficient on its own.

### WLCG IAM server configuration

**`xrootd.cf` (authentication + claim export):**

```conf
xrootd.seclib libXrdSec.so
xrootd.tls all

sec.protparm oauth2 -issuer https://wlcg.cloud.cnaf.infn.it/
sec.protparm oauth2 -audience <iam-client-id>
sec.protparm oauth2 -expiry required
sec.protparm oauth2 -entity-claim scope
sec.protparm oauth2 -entity-claim wlcg.ver
sec.protparm oauth2 -grps-claim wlcg.groups
sec.protocol oauth2

ofs.authorize
ofs.authlib libXrdAccToken.so
acctoken.basepath /eos/user
acctoken.onmissing deny
```

**INI equivalent:** see `src/XrdSecoauth2/configs/wlcg-iam.example.cfg`.

**Client token acquisition:**

```sh
./utils/xrdtoken WLCG \
  --client-id <iam-client-id> \
  --scope "storage.read:/public storage.modify:/alice"
# or: xrdtoken IAM --issuer https://<vo-iam>/ --client-id ... --scope ...
```

The stored JWT should contain `wlcg.ver` and a space-separated `scope` claim
(or a JSON array of scope strings, which is normalized on export).

### Dual-issuer deployments

A single server may configure multiple `[issuer ...]` blocks — for example CERN
SSO for interactive identity checks and a WLCG IAM issuer for storage tokens.
Each issuer may define its own `base_path` and `restricted_path` values.

## Standard CERN SSO configuration

Use this for CERN Keycloak identity only (not WLCG storage scopes):

Inline `xrootd.cf` style:

```conf
sec.protocol oauth2 \
  -issuer https://auth.cern.ch/auth/realms/cern \
  -audience public-client \
  -expiry required \
  -show-token-claims
```

INI fallback (`/etc/xrootd/oauth2.cfg`) equivalent:

```ini
[global]
expiry = required
show-token-claims = true
# issuer configuration
```

INI `oauth2.cfg` equivalent using the CERN issuer and default mapping:

```ini
[issuer "https://auth.cern.ch/auth/realms/cern"]
audience = public-client
```

## Standard Google configuration

Example `oauth2.cfg` using Google issuer + email mapping:

```ini
[issuer "https://accounts.google.com"]
audience = foo.apps.googleusercontent.com
forced-identity-claim = email

[email-map]
foo.bar@gmail.com = foo
```

Google app setup (Google Cloud Console):

1. Create/select a project.
2. Configure OAuth consent screen.
3. Create OAuth client credentials:
  - HTTPfor `xrdtoken ... GOOGLE` default flow (`--flow device`), use a client type
   that supports device authorization and provide both `client_id` and
   `client_secret` to `xrdtoken`.
  - for `--flow pkce`, use a Desktop app client id.
4. Copy client id/secret and use them in `xrdtoken` options (or env vars).

Quickstart (Google):

```sh
# 0) Minimal server config (example: /etc/xrootd/xrootd.cf)
cat > /etc/xrootd/xrootd.cf <<'EOF'
###########################################################
xrootd.seclib libXrdSec.so
all.role server
sec.protocol oauth2
xrootd.tls all
xrd.tlsca certdir /etc/grid-security/certificates
xrd.tls /etc/grid-security/xrd/xrdcert.pem /etc/grid-security/xrd/xrdkey.pem
EOF

# 0b) Start the server (foreground example)
xrootd -c /etc/xrootd/xrootd.cf -R xrootd

# 1) Create a token (Google device flow)
./utils/xrdtoken create ./build/google.token GOOGLE \
  --client-id "<google-oauth-client-id>" \
  --client-secret "<google-oauth-client-secret>"

# 2) Inspect token claims
./utils/xrdtoken show ./build/google.token

# 3) Try access with xrdfs
BEARER_TOKEN_FILE="$PWD/build/google.token" \
LD_LIBRARY_PATH=/usr/local/lib64 \
/usr/local/bin/xrdfs localhost:2000 stat /tmp/
```

## INI fallback file (`/etc/xrootd/oauth2.cfg`)

When `sec.protocol oauth2` has no trailing parameters, this file is required and
loaded at plugin init time. If the file is missing, initialization fails.

You can override this path with:

```conf
sec.protocol oauth2 -config-file /path/to/oauth2.cfg
```

The JWKS on-disk cache file is opened with `O_NOFOLLOW` and validated (owner ==
process euid, not group/other writable) on the same descriptor it is read from,
so a symlinked or swapped cache file is rejected rather than trusted to supply
signing keys.

When file-backed config is used (default path or `-config-file`), the plugin
checks inode/mtime changes at authentication time and reloads only:

- `[issuer "..."]` blocks (issuer/audience/OIDC+JWKS URLs/forced-identity-claim/
base_path/restricted_path)
- `[email-map]`

`[global]` keys are intentionally **not** reloaded and remain fixed from startup.

```ini
[global]
maxsz = 8192
expiry = required
jwks-refresh = 300
jwks-cache-file = /var/lib/xrootd/oidc-jwks-cache.ini
jwks-cache-ttl = 600
clock-skew = 60
identity-claim = preferred_username,sub
show-token-claims = true
token-cache-max = 10000
token-cache-noexp-ttl = 60

[issuer "https://issuer-a.example"]
audience = xrootd,xrootd-admin
forced-identity-claim = preferred_username
base_path = /tree1/
restricted_path = /public/
restricted_path = /shared/
# Optional if you want to override discovery default:
# oidc-config-url = https://issuer-a.example/.well-known/openid-configuration
# jwks-url = https://issuer-a.example/protocol/openid-connect/certs

[issuer "https://issuer-b.example"]
audience = service-b

[email-map]
alice@example.org = alice
bob@example.org = bobby
```

Supported sections are `[global]` and `[issuer "<issuer-url>"]` (or `[issuer]`
with `issuer = <url>` inside the section), plus `[email-map]` for
`forced-identity-claim = email`. Boolean keys accept `true/false`, `yes/no`,
`on/off`, `1/0`.

Security checks for `/etc/xrootd/oauth2.cfg`:

- must be a regular file,
- must be owned by the effective uid of the running xrootd process,
- must not be writable by group or others,
- is opened with `O_NOFOLLOW`, so a symlink at the config path is rejected.

When `jwks-cache-file` is configured, cached keys are stored per issuer in that
file and reused across refresh/restart cycles until TTL expiry.

## Security considerations

- **Audience binding is only enforced when configured.** If an issuer has no
`-audience` (or `audience =`) entry, the `aud` claim is **not** checked and
any signed, unexpired token from that trusted issuer is accepted regardless of
its audience. This means a token minted for a different relying party at the
same issuer would be accepted by this server. Configure at least one
`-audience` per issuer to bind tokens to this service. When one or more
audiences are set, a token is accepted only if its `aud` (string or array
member) matches one of them, and a token lacking an `aud` claim is rejected.
A startup/reload **warning** is logged for any issuer configured without an
audience.
- **Minimum RSA key size.** JWKS signing keys smaller than 2048 bits are
rejected, even if served by a configured issuer's JWKS endpoint.
- **Identity sanitization.** The resolved identity (the value mapped to
`XrdSecEntity.name`) is rejected if it contains control characters, whitespace,
path separators (`/`, `\`), or is `.`/`..`. Such a token fails closed rather
than placing an unsafe value into the authorization identity.
- **JWKS forced-refresh rate limiting.** A signature failure only triggers an
out-of-band JWKS re-fetch when the token's `kid` is unknown to the current
keyset, and such forced refreshes are rate-limited per issuer. A bad-signature
token for a known key is rejected without any network activity, preventing
refresh-amplification denial of service against this server and the issuer.
- **Negative validation cache.** Recently failed tokens are remembered for a
short TTL (hashed, never stored in raw form) so repeated identical invalid
tokens are rejected cheaply.

## Notes

- Protocol id on the wire is `oauth2`.
- Accepted JWT signature algorithm is currently `RS256`; tokens using any other
`alg` (including `none` or HMAC variants) or with no `alg` are rejected.
- OpenID Connect discovery and JWKS endpoints must use `https://`.
- Server-side token cache is enabled by default and keyed by the SHA-256 hash
of the token (the raw token value is never used as a map key).
- Client-side debug logging can be enabled by setting the `XrdSecDEBUG`
environment variable to `1`, `on`, `yes`, `true`, or `enabled`; it
logs which token source/file the client selected.

## Local helper scripts

For quick manual testing, helper scripts are available in `utils/`:

- `utils/xrdtoken create [tokenfile] [CERN|CERNOIDC|GOOGLE|GITHUB|IAM|WLCG]`:
creates token (`CERN` uses OAuth2 access token flow, `CERNOIDC` requests OIDC
scopes and requires/stores `id_token`, `GOOGLE` defaults to device flow, `GITHUB`
stores an opaque OAuth `access_token`, `IAM`/`WLCG` request WLCG storage
scopes and store the IAM `access_token` JWT).
- Shortcuts: `utils/xrdtoken CERNOIDC [tokenfile]`, `utils/xrdtoken WLCG ...`,
`utils/xrdtoken IAM ...` (IAM and WLCG are aliases).
- `utils/xrdtoken show [tokenfile]`: decodes and prints JWT header/payload.
- If `<tokenfile>` is omitted, default is `${XDG_RUNTIME_DIR}/bt_u<uid>`
(or `/tmp/bt_u<uid>` when `XDG_RUNTIME_DIR` is unset).
- For `GOOGLE`, provide `--client-id` (or set `GOOGLE_OAUTH_CLIENT_ID`).
- For `GOOGLE` default device flow, also provide `--client-secret` (or set
`GOOGLE_OAUTH_CLIENT_SECRET`).
- `GOOGLE` flow can be selected via `--flow device|pkce` (`device` default).
- `--client-secret` is optional for PKCE but may be required for some client
types/endpoints; env fallback: `GOOGLE_OAUTH_CLIENT_SECRET`.
- For `GOOGLE`, `xrdtoken` stores `id_token` when present (JWT), otherwise
falls back to `access_token`.
- For `GITHUB`, provide `--client-id` (or set `GITHUB_OAUTH_CLIENT_ID`). Device
flow does not need a client secret; enable **Device flow** in the OAuth app
settings. `GITHUB` supports `--flow device|pkce` (`device` default); PKCE
requires `--client-secret` (or `GITHUB_OAUTH_CLIENT_SECRET`).
- Device flow prints a prefilled verification URL when possible; GitHub prints
the verification page URL and a separate user code to enter manually.
- For `IAM`/`WLCG`, provide `--client-id` (or `WLCG_IAM_CLIENT_ID`) and a
required `--scope` with WLCG storage scopes. Override the IAM issuer with
`--issuer` (or `WLCG_IAM_ISSUER`; default CNAF test instance).

Example:

```sh
./utils/xrdtoken create
./utils/xrdtoken CERNOIDC
./utils/xrdtoken WLCG \
  --client-id "<iam-client-id>" \
  --scope "storage.read:/public storage.modify:/alice"
./utils/xrdtoken IAM \
  --issuer https://wlcg.cloud.cnaf.infn.it/ \
  --client-id "<iam-client-id>" \
  --scope "storage.read:/"
./utils/xrdtoken create GOOGLE \
  --client-id "<google-oauth-client-id>" \
  --client-secret "<google-oauth-client-secret>"
./utils/xrdtoken create GOOGLE --flow pkce \
  --client-id "<google-oauth-client-id>"
./utils/xrdtoken create GITHUB \
  --client-id "<github-oauth-client-id>"
./utils/xrdtoken show 
```

