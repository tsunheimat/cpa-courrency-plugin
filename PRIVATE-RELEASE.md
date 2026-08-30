# Private CPA account concurrency plugin

This repository is a **private** CPA plugin distribution repository. It is not
published to the official or public plugin marketplace.

## Registry

`registry.json` is a CPA plugin-store registry (`schema_version: 1`). The entry
uses `github-release`, so CPA obtains the release metadata and release assets
from this private GitHub repository. The repository and release API requests
must be authenticated by CPA with a GitHub token supplied through its secret
management/environment; no token belongs in this repository or in the registry.

## Release asset contract

For version `0.1.3`, publish these assets on GitHub Release tag `v0.1.3`:

```text
cpa-account-concurrency_0.1.3_linux_amd64.zip
checksums.txt
```

Version `0.1.3` includes the bounded stock CPA auth metadata observability repair
from commit `6d0c24c2dfc9c153cba747456197dc96f13ad7c4`: management label lookup
times out and falls back to hashed keys without affecting admission, and disk
fallback entries map through their auth JSON filename when no ID is supplied.
Timed-out metadata workers remain single-flight and are fenced and joined by
shutdown, reload, and reinitialization before the host API can be released.
It also removes the browser-side `/v0/management/auth-files` fetch. Version
`0.1.3` retains the stock CPA compatibility repair from commit
`198e407979e55ef30bd33b61a99a53c430e9878d`: the plugin resolves the selected
account from stock CPA `Metadata["selected_auth_id"]` without requiring any
CPA/CLIProxyAPI host source change. The account-level hard in-flight admission,
fail-closed identity handling, lifecycle release, authenticated read-only usage
UI, private registry/store model, and single-CPA `authority: local` configuration
remain unchanged. The UI's Settings area stores the CPA Management key only in
browser-local storage for the current origin, sends it via `X-Management-Key`,
and provides explicit save/update and clear actions; no key is stored by the
plugin or host.

The ZIP must contain the dynamic library at its root using this name:

```text
cpa-account-concurrency.so
```

CPA verifies the SHA-256 entry in `checksums.txt` before installing the
library. The registry itself is fetched from the repository's raw `main`
branch URL:

```text
https://raw.githubusercontent.com/tokenrouter-tech/cpa-courrency-plugin/main/registry.json
```

## CPA configuration

Configure the private registry and GitHub authentication without putting the
token value in config. The `token-env` value is only an environment-variable
name:

```yaml
plugins:
  enabled: true
  store-sources:
    - https://raw.githubusercontent.com/tokenrouter-tech/cpa-courrency-plugin/main/registry.json
  store-auth:
    - match: https://raw.githubusercontent.com/tokenrouter-tech/cpa-courrency-plugin/
      apply-to: [registry]
      type: github-token
      token-env: CPA_PLUGIN_GITHUB_TOKEN
    - match: https://api.github.com/repos/tokenrouter-tech/cpa-courrency-plugin/releases/
      apply-to: [metadata, artifact]
      type: github-token
      token-env: CPA_PLUGIN_GITHUB_TOKEN
  configs:
    cpa-account-concurrency:
      enabled: true
      admission-enforcing: true
      priority: 100
      max_concurrency: 2
      warm_reserved_slots: 0
      wait_timeout: 50ms
      authority: local
      redis_prefix: cpa:concurrency
```

The same host configuration is also provided as JSON in
`cpa-private-config.example.json`. CPA parses this JSON through its normal
configuration parser; set `CPA_PLUGIN_GITHUB_TOKEN` in the CPA service
environment/secret manager, not in this file.

For multiple CPA processes sharing the same Pro account, use `authority:
redis` and supply the Redis settings through the plugin configuration/secret
management. Keep the CPA GitHub token separate from Redis credentials.

## Local build

Use the repository's available Go toolchain:

```bash
GOOS=linux GOARCH=amd64 \
  go build -trimpath -buildmode=c-shared \
  -o cpa-account-concurrency.so .

zip -X cpa-account-concurrency_0.1.3_linux_amd64.zip \
  cpa-account-concurrency.so
sha256sum cpa-account-concurrency_0.1.3_linux_amd64.zip > checksums.txt
```

The checked-in `registry.json` is metadata only; source and release assets are
private GitHub content.
