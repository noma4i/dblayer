#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
generated="$root/swift/Sources/ConsumerCheck/Generated.swift"
trap 'rm -f "$generated"' EXIT
bash "$root/scripts/gen_consumer_corpus.sh" --swift > "$generated"
swift build --package-path "$root/swift" --target ConsumerCheck
