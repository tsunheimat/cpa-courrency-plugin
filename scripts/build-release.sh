#!/usr/bin/env bash
set -euo pipefail

version="${1:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 X.Y.Z" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

registered_version="$(sed -n 's/.*Version: "\([0-9][0-9.]*\)".*/\1/p' main.go | head -n 1)"
if [[ "$registered_version" != "$version" ]]; then
  echo "main.go registers version $registered_version, expected $version" >&2
  exit 1
fi
if ! grep -Fq "\"version\": \"$version\"" registry.json; then
  echo "registry.json does not declare version $version" >&2
  exit 1
fi
if ! grep -Fq "Current release: \`v$version\`" README.md; then
  echo "README.md does not declare release v$version" >&2
  exit 1
fi

target_goos="${GOOS:-$(go env GOOS)}"
target_goarch="${GOARCH:-$(go env GOARCH)}"

case "$target_goos/$target_goarch" in
  linux/amd64|linux/arm64|darwin/amd64|darwin/arm64|windows/amd64) ;;
  *)
    echo "unsupported plugin-store target: $target_goos/$target_goarch" >&2
    exit 2
    ;;
esac

case "$target_goos" in
  linux) library="cpa-account-concurrency.so" ;;
  darwin) library="cpa-account-concurrency.dylib" ;;
  windows) library="cpa-account-concurrency.dll" ;;
esac

artifact_dir="${ARTIFACT_DIR:-dist}"
archive="cpa-account-concurrency_${version}_${target_goos}_${target_goarch}.zip"

mkdir -p "$artifact_dir"
rm -f "$artifact_dir/$archive" "$artifact_dir/$library" "$artifact_dir/checksums.txt"

go mod download
go mod verify
go test ./...
go vet ./...

CGO_ENABLED=1 GOOS="$target_goos" GOARCH="$target_goarch" \
  go build -trimpath -buildmode=c-shared -ldflags='-buildid=' \
  -o "$artifact_dir/$library" .

(
  cd "$artifact_dir"
  zip -X -q -9 "$archive" "$library"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$archive" > checksums.txt
  else
    shasum -a 256 "$archive" > checksums.txt
  fi
  test "$(unzip -Z1 "$archive")" = "$library"
)

rm -f "$artifact_dir/$library"
echo "created $artifact_dir/$archive and $artifact_dir/checksums.txt"
