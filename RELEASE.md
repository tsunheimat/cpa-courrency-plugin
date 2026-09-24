# Release process

Releases are built from public, tagged source by GitHub Actions. The module
dependency on CLIProxyAPI is pinned in `go.mod`, and `go.sum` records its
published checksums; no local SDK checkout or module replacement is used.

## Create a release

1. Update the version in `main.go`, `registry.json`, and `README.md`.
2. Run the clean-build checks locally:

   ```bash
   go mod tidy
   git diff --exit-code -- go.mod go.sum
   go test ./...
   go vet ./...
   GOOS=linux GOARCH=amd64 scripts/build-release.sh 0.1.8
   (cd dist && sha256sum --check checksums.txt)
   ```

3. Commit the release source and push it to `main`.
4. Create and push the matching tag, for example:

   ```bash
   git tag -s v0.1.8 -m "Release v0.1.8"
   git push origin v0.1.8
   ```

The tag-triggered `Release` workflow reruns tests and vetting, then builds the
five CPA Plugin Store required targets on native GitHub-hosted runners:
Linux amd64/arm64, Darwin amd64/arm64, and Windows amd64. A final Linux job
packages each platform library into its store-compatible ZIP, creates one
combined `checksums.txt`, uploads the workflow evidence, and publishes all
release assets. The Actions used by the
workflow are pinned to full commit SHAs.

## Release assets

For version `X.Y.Z`, the release contains:

```text
cpa-account-concurrency_X.Y.Z_linux_amd64.zip
cpa-account-concurrency_X.Y.Z_linux_arm64.zip
cpa-account-concurrency_X.Y.Z_darwin_amd64.zip
cpa-account-concurrency_X.Y.Z_darwin_arm64.zip
cpa-account-concurrency_X.Y.Z_windows_amd64.zip
checksums.txt
```

Each ZIP contains exactly one root-level dynamic library. Linux archives contain
`cpa-account-concurrency.so`, Darwin archives contain
`cpa-account-concurrency.dylib`, and Windows archives contain
`cpa-account-concurrency.dll`.
