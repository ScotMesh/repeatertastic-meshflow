#!/usr/bin/env bash
# Build the RepeaterTastic plugin bundle: scripts/bundle.sh <version> [out.zip]
# Cross-compiles for 64-bit and 32-bit Raspberry Pi OS and x86-64, then zips the binaries with
# plugin.yaml (version filled in) and the logo.
set -euo pipefail
cd "$(dirname "$0")/.."
version=${1:?usage: scripts/bundle.sh <version> [out.zip]}
out=$(realpath -m "${2:-dist/repeatertastic-meshflow-$version.zip}")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$(dirname "$out")"
for arch in arm64 arm amd64; do
  echo "building linux/$arch"
  GOOS=linux GOARCH=$arch GOARM=6 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$version" \
    -o "$work/bin/repeatertastic-meshflow-linux-$arch" ./cmd/repeatertastic-meshflow
done
sed "s/^version: .*/version: ${version#v}/" cmd/repeatertastic-meshflow/plugin.yaml > "$work/plugin.yaml"
cp assets/logo.png LICENSE NOTICE.md "$work/"
rm -f "$out"
( cd "$work" && zip -qr -X "$out" . )
echo "wrote $out"
