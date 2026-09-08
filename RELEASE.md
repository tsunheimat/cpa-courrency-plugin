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
   scripts/build-release.sh 0.1.7
   (cd dist && sha256sum --check checksums.txt)
   ```

3. Commit the release source and push it to `main`.
4. Create and push the matching tag, for example:

   ```bash
   git tag -s v0.1.7 -m "Release v0.1.7"
   git push origin v0.1.7
   ```

The tag-triggered `Release` workflow reruns tests and vetting, builds the Linux
amd64 shared library, creates `checksums.txt`, uploads the workflow artifact,
and publishes both files to the GitHub Release. The Actions used by the
workflow are pinned to full commit SHAs.

## Release assets

For version `X.Y.Z`, the release contains:

```text
cpa-account-concurrency_X.Y.Z_linux_amd64.zip
checksums.txt
```

The ZIP contains exactly one root-level file:

```text
cpa-account-concurrency.so
```
