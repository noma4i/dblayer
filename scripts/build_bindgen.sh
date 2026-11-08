#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cargo build --manifest-path "$root/Cargo.toml" -p dblayer-bindgen --bin uniffi-bindgen
