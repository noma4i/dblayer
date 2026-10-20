#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
mkdir -p "$root/target/bindgen"
python3 - "$root" <<'PY'
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
version = re.search(r'(?m)^name = "uniffi"\nversion = "([^"]+)"$', (root / "Cargo.lock").read_text()).group(1)
manifest = '\n'.join([
    '[workspace]', '[package]', 'name = "dblayer-bindgen"', 'version = "0.0.0"', 'edition = "2021"',
    '[dependencies]', f'uniffi = {{ version = "={version}", features = ["cli"] }}',
    '[[bin]]', 'name = "uniffi-bindgen"', f'path = {json.dumps(str(root / "core/src/bin/uniffi-bindgen.rs"))}', '',
])
destination = root / "target/bindgen/Cargo.toml"
if not destination.exists() or destination.read_text() != manifest:
    destination.write_text(manifest)
(destination.parent / "Cargo.lock").write_bytes((root / "Cargo.lock").read_bytes())
PY
cargo build --manifest-path "$root/target/bindgen/Cargo.toml" --target-dir "$root/target" --bin uniffi-bindgen
