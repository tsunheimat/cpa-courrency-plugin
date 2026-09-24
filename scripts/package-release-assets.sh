#!/usr/bin/env bash
set -euo pipefail

version="${1:-}"
staging_dir="${2:-staging}"
artifact_dir="${3:-dist}"

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: $0 X.Y.Z [staging-dir] [artifact-dir]" >&2
  exit 2
fi

mkdir -p "$artifact_dir"
rm -f "$artifact_dir"/cpa-account-concurrency_*.zip "$artifact_dir/checksums.txt"

package_dir="$(mktemp -d)"
trap 'rm -rf "$package_dir"' EXIT

package_target() {
  local goos="$1"
  local goarch="$2"
  local ext="$3"
  local library="cpa-account-concurrency.${ext}"
  local source="${staging_dir}/cpa-account-concurrency-${goos}-${goarch}/${library}"
  local archive="${artifact_dir}/cpa-account-concurrency_${version}_${goos}_${goarch}.zip"

  if [[ ! -s "$source" ]]; then
    echo "missing platform library: $source" >&2
    exit 1
  fi

  rm -rf "$package_dir"/*
  cp "$source" "$package_dir/$library"
  (
    cd "$package_dir"
    zip -X -q -9 "$archive" "$library"
  )

  if [[ "$(unzip -Z1 "$archive")" != "$library" ]]; then
    echo "archive $archive must contain exactly one root-level $library" >&2
    exit 1
  fi
}

package_target linux amd64 so
package_target linux arm64 so
package_target darwin amd64 dylib
package_target darwin arm64 dylib
package_target windows amd64 dll

zip_count="$(find "$artifact_dir" -maxdepth 1 -type f -name 'cpa-account-concurrency_*.zip' | wc -l | tr -d ' ')"
if [[ "$zip_count" != "5" ]]; then
  echo "expected 5 release ZIPs, found $zip_count" >&2
  exit 1
fi

(
  cd "$artifact_dir"
  sha256sum cpa-account-concurrency_*.zip > checksums.txt
  sha256sum --check checksums.txt
)

echo "created 5 platform archives and $artifact_dir/checksums.txt"
