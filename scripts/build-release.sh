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

artifact_dir="${ARTIFACT_DIR:-dist}"
archive="cpa-account-concurrency_${version}_linux_amd64.zip"
library="cpa-account-concurrency.so"

mkdir -p "$artifact_dir"
rm -f "$artifact_dir/$archive" "$artifact_dir/$library" "$artifact_dir/checksums.txt"

go mod download
go mod verify
go test ./...
go vet ./...

CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
  go build -trimpath -buildmode=c-shared -ldflags='-buildid=' \
  -o "$artifact_dir/$library" .

source_date_epoch="${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct)}"
touch -d "@$source_date_epoch" "$artifact_dir/$library"

(
  cd "$artifact_dir"
  zip -X -q -9 "$archive" "$library"
  sha256sum "$archive" > checksums.txt
  test "$(unzip -Z1 "$archive")" = "$library"
)

rm -f "$artifact_dir/$library"
echo "created $artifact_dir/$archive and $artifact_dir/checksums.txt"
