#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"

reclaim_target() {
  local target="${CARGO_TARGET_DIR:-$root/target}"
  if pgrep -f cargo >/dev/null; then
    echo "reclaim_target: cargo is running, nothing removed" >&2
    return 1
  fi
  echo "target before: $(du -sk "$target" | cut -f1) KiB"
  cargo clean --manifest-path "$root/Cargo.toml" -p dblayer -p dblayer-gen
  echo "target after: $(du -sk "$target" | cut -f1) KiB"
}

reclaim_target
