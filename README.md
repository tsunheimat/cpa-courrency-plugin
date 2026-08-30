# CPA account concurrency plugin

This plugin adds sub2api-like protection for CPA Pro accounts. It is a C ABI
dynamic-library plugin and uses the CPA request-interceptor and request-lifecycle
hooks. The protected resource is the stable CPA auth ID. IDs are canonicalized
by trimming surrounding whitespace (case and interior characters remain
significant), then hashed before they are used as authority keys and never
returned in errors or logs. Duplicate scheduler candidates collapse after
this canonicalization.

## Configuration

```yaml
plugins:
  enabled: true
  configs:
    cpa-account-concurrency:
      enabled: true
      priority: 100
      max_concurrency: 2
      warm_reserved_slots: 0 # 0 selects the recommended formula
      wait_timeout: 50ms
      authority: local
      redis_prefix: cpa:concurrency
      # redis_addr: 127.0.0.1:6379 # required for authority: redis
```

`max_concurrency` is required to be at least one. The recommended reservation
for a hard limit `C` is `W=0` for `C<=1`, otherwise
`min(C-1,max(1,ceil(0.20*C)))`; general capacity is `G=C-W`. Warm/strict
affinity requests can consume warm or general capacity. Cold requests (including
an unverified `prompt_cache_key`-style hint) can consume only general capacity.

Admission waits at most `wait_timeout`; there is no unbounded internal queue.
The scheduler chooses the least-loaded currently available candidate, with
stable candidate-order tie breaking. A strict affinity hint (`pinned_auth_id`,
`session_auth_id`, or `affinity_auth_id`) is retained when that account is
available. `selected_auth_id` is selection state published by CPA on every
attempt and is never sufficient for warm classification. A host-provided
`cache_auth_id` is warm only when paired with `cache_verified: true`; the normal
CPA session-affinity selector publishes that pair only on a validated cache hit.
On a retry/failover, the binding must match the selected auth or the attempt is
cold and uses general capacity.

## Management observability

The CPA concurrency management view reads the existing authenticated
`/v0/management/plugins/cpa-account-concurrency/usage` route and uses CPA's
stock `host.auth.list` callback to list every currently available account and
enrich each hashed usage bucket with redacted account metadata. Idle available
accounts show `0 / limit` usage, while active buckets remain visible if label
metadata is stale or incomplete. Labels prefer the auth email, then the CPA auth
JSON filename/name, and finally the existing hashed account key. Each account
shows its own `in_flight / limit` value (for example, `1 / 2`). The callback is
bounded; timeout, callback error, malformed data, or an empty/unusable list
leaves usage counts available, marks the snapshot stale with an
`account labels unavailable` error, and uses the hashed key as the truthful
label fallback. This enrichment path never participates in admission. A
timed-out callback remains the single in-flight worker; shutdown, reload, and
reinitialization fence and join it before the host API is released or the
plugin can be unloaded. The live UI accepts a CPA Management key in its
Settings area, stores it only in browser-local storage scoped to the current
origin, and sends it as `X-Management-Key` on usage requests. The plugin never
requests or stores auth JSON, tokens, passwords, or the browser's management
key, and the browser makes no separate auth-files request.

## Authority modes

`local` is process-local and is the safe default for one CPA instance. `redis`
uses atomic Lua `EVAL` scripts over RESP2 and requires `redis_addr` (with
optional `redis_password` and `redis_db`). If a distributed authority is
unavailable, the plugin fails closed with HTTP 503 and does not call the
provider. Redis leases carry a bounded 30-second expiry and are renewed by a
10-second heartbeat while the request is live. Acquire, expiry reclamation,
renewal, and release are atomic; release is idempotent. If renewal or any
authority operation is uncertain, new admissions fail closed until the plugin
is reconfigured. A failed renewal fences that request locally; uncertainty is
not cleared by reconfigure while its lease is still tracked, preventing a
live, unrenewed request from being oversold to another lease.

## Lifecycle and failover

The lease is acquired only after CPA supplies the selected auth ID. Repeated
post-auth hooks for the same request/account are idempotent. When CPA retries or
fails over to another account, the prior lease is released before the new one
is acquired, so leases never stack. `request.complete` releases the final lease
exactly once for success, provider failure, stream EOF, WebSocket/turn end,
timeout, cancellation, client disconnect, rejection, and plugin errors.

The admission response is a direct response before executor/provider I/O:

```http
HTTP/1.1 503 Service Unavailable
Retry-After: 1
Content-Type: application/json
```

```json
{"error":{"type":"account_concurrency_limit","code":"account_concurrency_limit","message":"account concurrency limit reached","retryable":true}}
```

Authority failures use `account_concurrency_authority_unavailable` with the same
HTTP status and retryable metadata. These local CPA errors are intentionally
distinct from provider 429, provider health, performance-score, and breaker
failures. Newapi should treat `account_concurrency_limit` as eligible for
bounded channel-level failover, without classifying it as provider 429.

CPA host compatibility: schema version 2 or newer is required for terminal
`request.complete` callbacks. This task's host/SDK contract adds
`request_interceptor_enforces_admission` to registration capabilities. When it
is true, interceptor RPC/process errors, fusing, unavailable callbacks, or an
incompatible schema terminate the request with typed
`account_concurrency_authority_unavailable` (HTTP 503); ordinary interceptors
retain the historical fail-open behavior. The extension also adds `AuthID` and
`AuthProvider` to post-auth interceptor requests and exposes plugin RPC error
`Code`, `Class`, `Retryable`, and `StatusCode` methods. Hosts must preserve those
typed fields across the plugin boundary; newapi must not parse human-readable
messages.

Set the host-side plugin instance option `admission-enforcing: true` for this
plugin. That explicit requirement keeps the provider path fail-closed while the
plugin is loading or if registration fails; leave it unset for ordinary
non-enforcement plugins.

Redis lease records are deliberately fail-closed across partitions. If renewal
is lost, the plugin marks the account fenced in Redis; an acquire from any CPA
instance returns `account_concurrency_authority_unavailable` (and an expired
lease is fenced when observed) until the stale lease is explicitly released.
Redis keys are therefore not reclaimed automatically: a crashed holder may
require operational cleanup, which is the availability trade-off required to
preserve the hard cap without relying on a local process flag.

## Build and test

From this directory:

```bash
/usr/local/go/bin/go test ./...
/usr/local/go/bin/go build -buildmode=c-shared -o cpa-account-concurrency.so .
```

Use the platform-specific shared-library suffix expected by CPA. The artifact
filename must match the plugin ID (`cpa-account-concurrency`).
