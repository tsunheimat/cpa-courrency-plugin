# CPA account concurrency plugin

This plugin adds sub2api-like protection for CPA Pro accounts. It is a C ABI
dynamic-library plugin and uses the CPA request-interceptor and request-lifecycle
hooks. The protected resource is the stable CPA auth ID; IDs are hashed before
they are used as authority keys and are never returned in errors or logs.

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
`selected_auth_id`, `session_auth_id`, `affinity_auth_id`, or a host-provided
`cache_auth_id`) is retained when that account is available. A cache hint is not
treated as verified warm affinity unless the host also supplies a verified
binding.

## Authority modes

`local` is process-local and is the safe default for one CPA instance. `redis`
uses atomic Lua `EVAL` scripts over RESP2 and requires `redis_addr` (with
optional `redis_password` and `redis_db`). If a distributed authority is
unavailable, the plugin fails closed with HTTP 503 and does not call the
provider.

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
`request.complete` callbacks. This task's host extension adds `AuthID` and
`AuthProvider` to post-auth interceptor requests and exposes plugin RPC error
`Code`, `Class`, `Retryable`, and `StatusCode` methods. Hosts must preserve those
typed fields across the plugin boundary; newapi must not parse human-readable
messages.

## Build and test

From this directory:

```bash
/usr/local/go/bin/go test ./...
/usr/local/go/bin/go build -buildmode=c-shared -o cpa-account-concurrency.so .
```

Use the platform-specific shared-library suffix expected by CPA. The artifact
filename must match the plugin ID (`cpa-account-concurrency`).
