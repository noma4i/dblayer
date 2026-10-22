#!/usr/bin/env bash
# Usage: build-slot -- sim-run --exclusive <name> -- bash scripts/check_cloudkit_host.sh @UDID@
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
udid="${1:?usage: check_cloudkit_host.sh <simulator udid>}"
host="$root/swift/CloudHost"
derived="$root/.orca-artifacts/DD.noindex"
bundle="com.yupi.dblayer.cloudhost"
expected="CLOUDHOST account=signedOut push=needsAuthorization"
log="$(mktemp -t cloudkit-host)"

(cd -- "$host" && /opt/homebrew/bin/xcodegen generate --spec project.yml --quiet)

FORCE_REBUILD=1 xcodebuild test \
  -project "$host/CloudHost.xcodeproj" -scheme CloudHost \
  -destination "id=$udid" -derivedDataPath "$derived" \
  -allowProvisioningUpdates DEVELOPMENT_TEAM=34ZTAW54JD -quiet

app="$(find "$derived/Build/Products" -name CloudHost.app -path '*iphonesimulator*' | head -1)"
entitlements="$(find "$derived/Build/Intermediates.noindex" -name 'CloudHost.app-Simulated.xcent' | head -1)"
rg -q 'iCloud.com.yupi' "$entitlements" || { echo "FAIL: container entitlement missing"; exit 1; }
xcrun simctl install "$udid" "$app"
SIMCTL_CHILD_CLOUDHOST_EXIT=1 xcrun simctl launch --console --terminate-running-process "$udid" "$bundle" > "$log" 2>&1 || true
xcrun simctl uninstall "$udid" "$bundle"
rg "CLOUDHOST" "$log"
rg -q "^$expected\$" "$log" || { echo "FAIL: expected '$expected'"; exit 1; }
echo "PASS: $expected"
