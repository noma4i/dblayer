#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd -- "$root"
mkdir -p target/bindgen-bin
lockf -k target/bindgen.lock bash -c 'cargo build -p dblayer-bindgen --bin uniffi-bindgen && cp target/debug/uniffi-bindgen target/bindgen-bin/uniffi-bindgen.$$ && mv -f target/bindgen-bin/uniffi-bindgen.$$ target/bindgen-bin/uniffi-bindgen'
