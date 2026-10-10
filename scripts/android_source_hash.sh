#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd -- "$root"
git ls-files -z core/src core/Cargo.toml Cargo.lock | xargs -0 shasum -a 256 | shasum -a 256 | cut -d' ' -f1
