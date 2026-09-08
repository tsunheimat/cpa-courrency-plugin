# CPA Account Concurrency Plugin

A native CLIProxyAPI plugin that provides cache-aware, per-account hard in-flight
concurrency admission for CPA Pro accounts.

The plugin uses the official CLIProxyAPI dynamic-library plugin ABI and request
lifecycle hooks. It limits upstream calls per selected CPA account, returns
structured local capacity errors before provider I/O, and provides an
authenticated Management Center view of account usage.

## Features

- Per-account hard in-flight concurrency limits.
- Local authority for a single CPA process.
- Optional Redis authority for multiple CPA processes sharing the same account
  pool.
- Cache-aware warm/general capacity handling.
- Stable selected-auth identity and failover-safe lease lifecycle.
- Bounded admission wait; no unbounded internal request queue.
- Fail-closed behavior when the configured authority is unavailable or a lease
  cannot be renewed safely.
- Authenticated Management Center usage view with explicit unavailable-auth
  filtering.
- An **All available accounts** summary calculated only from displayed rows.
- No credential JSON, provider token, password, authority key, or Management key
  is returned by the plugin API or rendered in the usage table.

## Compatibility

- Host: CLIProxyAPI/CPA with the native plugin ABI and request-lifecycle support.
- Plugin ID: `cpa-account-concurrency`.
- Current release: `v0.1.7`.
- Published binary target in this repository: **Linux amd64**.

Other operating systems and architectures are not published by the current
release. Do not install the Linux `.so` on another platform.

## Configuration

Enable the global plugin system and configure the plugin in the normal
CLIProxyAPI configuration:

```yaml
plugins:
  enabled: true
  dir: plugins
  configs:
    cpa-account-concurrency:
      enabled: true
      admission-enforcing: true
      priority: 100
      max_concurrency: 2
      warm_reserved_slots: 0 # 0 selects the recommended formula
      wait_timeout: 50ms
      authority: local
      redis_prefix: cpa:concurrency
      # Required when authority is redis:
      # redis_addr: 127.0.0.1:6379
      # redis_password: example-value
      # redis_db: 0
```

`max_concurrency` must be at least one. For a hard limit `C`, the recommended
warm reservation is `W=0` for `C<=1`; otherwise it is
`min(C-1, max(1, ceil(0.20*C)))`. General capacity is `G=C-W`.

Set `authority: local` for one CPA process. Use `authority: redis` only when
all participating CPA processes point at the same Redis authority and use a
consistent `redis_prefix`. Keep Redis credentials in the service secret
configuration; never commit them to this repository.

`admission-enforcing: true` is intentional. It makes plugin loading,
registration, incompatible host schemas, callback failures, and authority
failures terminate the request with the typed local 503 error instead of
silently bypassing the configured hard limit.

## Admission and failover behavior

The plugin acquires a lease only after CPA supplies the selected auth ID.
Repeated post-auth callbacks for the same request are idempotent. When CPA
retries or fails over to another account, the previous lease is released before
the new lease is acquired, so leases do not stack across accounts.

`request.complete` releases the final lease for successful requests, provider
failures, stream termination, cancellation, timeout, client disconnect,
rejection, and plugin errors. Admission waits at most `wait_timeout`.

A local capacity rejection is returned before executor/provider I/O:

```http
HTTP/1.1 503 Service Unavailable
Retry-After: 1
Content-Type: application/json
```

```json
{
  "error": {
    "type": "account_concurrency_limit",
    "code": "account_concurrency_limit",
    "message": "account concurrency limit reached",
    "retryable": true
  }
}
```

Authority failures use the separate
`account_concurrency_authority_unavailable` code. They are not provider 429
responses and should not be treated as provider health or breaker failures.

Redis leases use bounded expiry and heartbeat renewal. Uncertain authority
operations fail closed; the plugin does not assume that a lost lease was safely
released.

## Management observability

The usage view reads the authenticated CPA route:

```text
/v0/management/plugins/cpa-account-concurrency/usage
```

It uses CPA's stock `host.auth.list` callback as the account index and filters an
entry only when explicit metadata marks it disabled, unavailable,
unauthorized, authentication-failed, or inside a future retry window. Account
names and filenames are not used as availability heuristics, so an available
account whose name contains `401` remains visible.

The view displays idle available accounts as `0 / limit` and `0 / reserved`.
The table remains:

- `Account`
- `Total (in-flight / limit)`
- `Warm reserved (in-flight / reserved)`

The **All available accounts** summary sums only the rows shown in that table.
Filtered or unavailable accounts do not contribute to the totals.

The UI asks for the CPA Management key in its Settings area and stores it only
in browser-local storage scoped to the current origin. The plugin does not
request or store auth JSON, provider tokens, passwords, or the Management key.

## Install the published release

Download the assets from the public GitHub Release:

<https://github.com/tsunheimat/cpa-courrency-plugin/releases/tag/v0.1.7>

The Linux amd64 release contains:

```text
cpa-account-concurrency_0.1.7_linux_amd64.zip
checksums.txt
```

Verify the archive before installation:

```bash
sha256sum --check checksums.txt
unzip -t cpa-account-concurrency_0.1.7_linux_amd64.zip
```

The archive contains exactly this root-level library:

```text
cpa-account-concurrency.so
```

For manual installation, place the versioned library under the CLIProxyAPI
plugin directory for the target platform:

```text
plugins/linux/amd64/cpa-account-concurrency-v0.1.7.so
```

Restart or reload CLIProxyAPI according to its normal plugin lifecycle after
installation. The official CLIProxyAPI plugin store can install the same
release after the registry entry is accepted.

## Build and release provenance

The CLIProxyAPI SDK is a public, versioned module dependency pinned in
`go.mod`, with its checksums committed in `go.sum`. The project does not use a
local SDK checkout or a `replace` directive.

To run the same checks and Linux amd64 build used for release `v0.1.7`:

```bash
go mod tidy
git diff --exit-code -- go.mod go.sum
scripts/build-release.sh 0.1.7
(cd dist && sha256sum --check checksums.txt)
```

The tag-triggered GitHub Actions workflow performs these checks from a clean
checkout, uploads the resulting files as workflow evidence, and publishes them
to the matching GitHub Release. See [`RELEASE.md`](RELEASE.md) for the complete
process. The plugin does not require a CLIProxyAPI source modification.

## Official plugin store

This repository is prepared for submission to the official CLIProxyAPI plugin
registry:

<https://github.com/router-for-me/CLIProxyAPI-Plugins-Store>

The official store maintains registry metadata only. Plugin binaries,
`checksums.txt`, and release notes remain in this repository. Store inclusion is
controlled by the upstream registry review and should not be assumed until the
corresponding registry pull request is merged.

## Security and trust

Native plugins run in-process with CLIProxyAPI and must be treated as trusted
code. Review the source and release checksum before installation. Do not commit
provider credentials, Redis passwords, Management keys, cookies, or API tokens
to this repository or to a release asset.

## License

MIT. See the repository license notice for the applicable terms.
