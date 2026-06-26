# XrdAccToken

`XrdAccToken` is an XRootD **authorization** plugin (`ofs.authlib`) that enforces
JWT scope claims on storage paths. It works together with `XrdSecoauth2`, which
handles **authentication** and JWT validation.

The plugin does not parse or verify tokens itself. After `XrdSecoauth2` has
authenticated a client, selected JWT claims are available as extended attributes
on the `XrdSecEntity`. `XrdAccToken` reads those attributes and decides whether
the requested operation on a path is permitted.

```
Client ──► XrdSecoauth2 (authenticate, validate JWT)
              │
              ▼
         XrdSecEntity
         • name        (identity)
         • token.scope (authorization scopes)
         • token.wlcg.ver (profile marker, if present)
         • base_path   (optional issuer namespace, from oauth2)
         • restricted_path (optional JSON path allow-list, from oauth2)
              │
              ▼
         XrdAccToken (authorize path + operation)
```

This separation keeps token validation in one place and allows authorization
logic to be swapped independently via `ofs.authlib`.

---

## Features

- **WLCG Token Profile** — `storage.read:/path`, `storage.create:/path`,
  `storage.modify:/path`, `storage.stage:/path`, `storage.poll:/path`
- **SciTokens** — `read:/path`, `write:/path`
- **Automatic profile detection** — WLCG vs SciTokens is chosen from the
  `wlcg.ver` claim
- **Subtree path matching** — scopes authorize an entire directory subtree
- **Site base path** — optional `acctoken.basepath` prefix applied to all scope
  paths; when the entity also carries oauth2 `base_path`, the configured base
  path is applied first and the entity value is appended
  (e.g. site `/atlas` + entity `/eos` + scope `/data` → `/atlas/eos/data`)
- **Restricted paths** — when oauth2 exports `restricted_path` as a JSON array,
  the request path must fall under at least one listed path or access is denied
- **Plugin chaining** — can wrap another `ofs.authlib` (e.g. `libXrdAcc.so`)
- **HTTP support** — works with `root://` and HTTPS when oauth2 is configured

Non-storage scopes (`compute.*`, `queue`, `execute`, …) are ignored.

---

## Supported token profiles

### WLCG

WLCG tokens are identified by a non-empty `wlcg.ver` claim (stored on the
entity as `token.wlcg.ver` by default). When this claim is present, scopes use
the WLCG format:

```json
{
  "iss": "https://iam.example.org",
  "sub": "alice",
  "aud": "https://storage.example.org",
  "wlcg.ver": "1.0",
  "scope": "storage.read:/datasets storage.modify:/scratch storage.stage:/archive"
}
```

This grants:

- read access under `/datasets`
- full write access under `/scratch`
- tape staging operations under `/archive` (no read implied)

### SciTokens

When `wlcg.ver` is absent or empty, scopes are interpreted as SciTokens:

```json
{
  "iss": "https://issuer.example.org",
  "sub": "248289761001",
  "aud": "storage.example.org",
  "scope": "read:/datasets write:/scratch"
}
```

| Profile | Detected when | Scope format |
| ------- | ------------- | ------------ |
| WLCG | `wlcg.ver` is non-empty | `storage.<capability>:<path>` |
| SciTokens | `wlcg.ver` missing or empty | `read:<path>`, `write:<path>` |

---

## XrdSecoauth2 configuration

`XrdAccToken` requires `XrdSecoauth2` (`sec.protocol oauth2`) to authenticate
clients and export JWT claims onto the `XrdSecEntity`.

### Required: export claims with `-entity-claim`

Use `-entity-claim` to copy JWT claims into entity attributes after successful
authentication. Without this, `XrdAccToken` has no scope information to
evaluate.

| JWT claim | Default entity attribute | Required for |
| --------- | ------------------------ | ------------ |
| `scope` | `token.scope` | all tokens |
| `wlcg.ver` | `token.wlcg.ver` | WLCG profile detection |

The default mapping is `token.<claim>`. To use a custom attribute key, specify
`claim=attrkey`:

```conf
-entity-claim scope=token.scope
-entity-claim wlcg.ver=token.wlcg.ver
```

### Minimal `xrootd.cf` example

```conf
xrootd.seclib libXrdSec.so
xrootd.tls all

# OAuth2 authentication — export claims needed by XrdAccToken
sec.protparm oauth2 -issuer https://iam.example.org
sec.protparm oauth2 -audience https://storage.example.org
sec.protparm oauth2 -expiry required
sec.protparm oauth2 -entity-claim scope
sec.protparm oauth2 -entity-claim wlcg.ver
sec.protocol oauth2

# Authorization
ofs.authorize
ofs.authlib libXrdAccToken.so
```

For SciTokens-only deployments, `wlcg.ver` export is optional — only
`-entity-claim scope` is required.

The `scope` claim may be a space-separated string or a JSON array of strings;
`XrdSecoauth2` normalizes arrays to a space-separated `token.scope` value.

### CERN SSO vs WLCG IAM

| Use case | Issuer | Client token |
| -------- | ------ | ------------ |
| CERN identity only | `auth.cern.ch` (Keycloak) | `xrdtoken CERNOIDC` (`id_token`) |
| WLCG storage paths | VO IAM issuer | `xrdtoken WLCG --scope "storage.read:/..."` |

See [`src/XrdSecoauth2/README.md`](../XrdSecoauth2/README.md) and
[`src/XrdSecoauth2/configs/wlcg-iam.example.cfg`](../XrdSecoauth2/configs/wlcg-iam.example.cfg)
for a full IAM example with `XrdAccToken`.

### Optional: issuer `base_path` and `restricted_path`

When configured per issuer in `sec.protocol oauth2` or `oauth2.cfg`, oauth2 exports
these attributes automatically (no `-entity-claim` needed):

| Entity attribute | Source | Used by `XrdAccToken` as |
| ---------------- | ------ | ------------------------ |
| `base_path` | issuer `-base-path` / `base_path =` | second prefix after `acctoken.basepath` |
| `restricted_path` | issuer `-restricted-path` / repeated `restricted_path =` | JSON array allow-list for the request path |

Example issuer block:

```ini
[issuer "https://iam.example.org"]
audience = https://storage.example.org
base_path = /eos
restricted_path = /public/
restricted_path = /shared/
```

With `acctoken.basepath /atlas` and scope `storage.read:/data`, the effective
scope path is `/atlas/eos/data`. A request for `/atlas/eos/public/file` is
allowed only if `/public` is listed in `restricted_path`.

### Equivalent `oauth2.cfg` (INI)

When using a config file instead of inline parameters:

```ini
[global]
expiry = required
entity-claim = scope
entity-claim = wlcg.ver

[issuer "https://iam.example.org"]
audience = https://storage.example.org
```

With inline `sec.protparm oauth2` lines, list all parameters **before**
`sec.protocol oauth2`. See the [XrdSecoauth2 README](../XrdSecoauth2/README.md)
for full oauth2 options (issuers, audiences, JWKS, identity mapping, HTTP setup).

### HTTPS clients

For HTTPS access with the same oauth2 configuration:

```conf
http.tlsclientauth off
http.header2cgi Authorization authz
http.oauth2 on

sec.protparm oauth2 -issuer https://iam.example.org
sec.protparm oauth2 -audience https://storage.example.org
sec.protparm oauth2 -entity-claim scope
sec.protparm oauth2 -entity-claim wlcg.ver
sec.protocol oauth2
```

---

## XrdAccToken configuration

### `xrootd.cf` directives

```conf
acctoken.trace info
acctoken.basepath /atlas
acctoken.scopeattr token.scope
acctoken.wlcgverattr token.wlcg.ver
acctoken.onmissing passthrough
```

| Directive | Default | Description |
| --------- | ------- | ----------- |
| `acctoken.trace` | `error,warning` | Logging: `all`, `error`, `warning`, `info`, `debug`, `none` |
| `acctoken.basepath` | *(empty)* | Site prefix prepended to every scope path |
| `acctoken.scopeattr` | `token.scope` | Entity attribute containing the space-separated `scope` claim |
| `acctoken.wlcgverattr` | `token.wlcg.ver` | Entity attribute for the `wlcg.ver` claim |
| `acctoken.onmissing` | `passthrough` | Behavior when no scope attribute is present (see below) |

### `ofs.authlib` parameters

Parameters on the `ofs.authlib` line override defaults:

```conf
ofs.authlib libXrdAccToken.so base_path=/atlas,scope_attr=token.scope,wlcg_ver_attr=token.wlcg.ver,onmissing=deny
```

| Parameter | Description |
| --------- | ----------- |
| `base_path` | Same as `acctoken.basepath` |
| `scope_attr` | Same as `acctoken.scopeattr` |
| `wlcg_ver_attr` | Same as `acctoken.wlcgverattr` |
| `onmissing` | Same as `acctoken.onmissing` |

### `onmissing` behavior

When the entity has no scope attribute (e.g. a non-oauth2 session, or oauth2
configured without `-entity-claim scope`):

| Value | Behavior |
| ----- | -------- |
| `passthrough` | Delegate to the chained authorization plugin (default) |
| `allow` | Grant the requested operation |
| `deny` | Deny the request |

When a scope attribute is present but does not authorize the operation, the
request is always denied.

### Base path example

A token with `storage.read:/data` and site configuration
`acctoken.basepath /atlas` authorizes reads under `/atlas/data`, not `/data`.

---

## Scope-to-operation mapping

### WLCG scopes

| Scope | Permitted operations |
| ----- | -------------------- |
| `storage.read:<path>` | read, readdir, stat |
| `storage.create:<path>` | exclusive create, mkdir, rename, exclusive insert, stat |
| `storage.modify:<path>` | create, mkdir, rename, insert, update, chmod, stat, delete |
| `storage.stage:<path>` | stage (bring online, pin, release) |
| `storage.poll:<path>` | poll (query staging status) |

`storage.stage` and `storage.poll` are independent: neither implies read or
the other.

### SciTokens scopes

| Scope | Permitted operations |
| ----- | -------------------- |
| `read:<path>` | read, readdir, stat |
| `write:<path>` | create, mkdir, rename, insert, update, chmod, stat, delete |

`write` does not imply `read`; use both scopes on the same path for full access.

### Path semantics

Scopes authorize a **subtree** rooted at the given path:

```
storage.read:/data  →  /data/file1, /data/subdir/file2  ✓
                       /other                            ✗
```

`stat` and `mkdir` on parent directories of an authorized path are also
permitted, consistent with WLCG storage guidance.

---

## Plugin chaining

`XrdAccToken` can wrap another authorization plugin. Token-based scope
authorization is evaluated first; if it does not grant access, behavior depends
on `acctoken.onmissing`:

```conf
ofs.authorize
ofs.authlib libXrdAccToken.so
ofs.authlib++ libXrdAcc.so
```

With `onmissing passthrough`, requests without token scopes fall through to
`libXrdAcc.so` (e.g. for local Unix users or secondary policies).

---

## Complete example

### WLCG storage with tape staging

**`xrootd.cf`:**

```conf
xrootd.seclib libXrdSec.so
xrootd.tls all
xrd.tlsca certdir /etc/grid-security/certificates

sec.protparm oauth2 -issuer https://iam.example.org
sec.protparm oauth2 -audience eos-storage
sec.protparm oauth2 -expiry required
sec.protparm oauth2 -entity-claim scope
sec.protparm oauth2 -entity-claim wlcg.ver
sec.protocol oauth2

acctoken.basepath /eos/user
acctoken.trace info
acctoken.onmissing deny

ofs.authorize
ofs.authlib libXrdAccToken.so
```

**Token:**

```json
{
  "wlcg.ver": "1.0",
  "scope": "storage.read:/public storage.modify:/alice storage.stage:/tape"
}
```

**Effective authorization** (with `basepath /eos/user`):

| Token scope | Effective path | Access |
| ----------- | -------------- | ------ |
| `storage.read:/public` | `/eos/user/public/**` | read |
| `storage.modify:/alice` | `/eos/user/alice/**` | read + write |
| `storage.stage:/tape` | `/eos/user/tape/**` | stage only |

### SciTokens analysis workflow

**Token** (no `wlcg.ver`):

```json
{
  "scope": "read:/datasets write:/scratch"
}
```

Grants read-only access to `/datasets/**` and full write access to
`/scratch/**`.

---

## Comparison with XrdSciTokens

| | XrdAccToken | XrdSciTokens |
| - | ----------- | ------------ |
| JWT validation | via XrdSecoauth2 | built-in (libscitokens) |
| Configuration | `sec.protocol oauth2` + `ofs.authlib` | `ofs.authlib` + `scitokens.cfg` |
| WLCG scopes | yes (`wlcg.ver` detection) | via scitokens-cpp |
| SciTokens scopes | yes (default when no `wlcg.ver`) | yes |
| Tape staging scopes | yes (WLCG only) | partial |
| Issuer/JWKS config | in oauth2 plugin | in scitokens.cfg |

`XrdAccToken` is suited for deployments that already use `XrdSecoauth2` and want
authorization driven directly from entity attributes, without a separate
SciTokens configuration file.

---

## Building

The plugin is built as part of the XRootD server (`libXrdAccToken-<version>.so`).
It has no external dependencies beyond the core XRootD libraries.

Unit tests (with `-DENABLE_TESTS=ON`):

```sh
cmake --build . --target xrdacctoken-unit-tests
./bin/xrdacctoken-unit-tests
```
