#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
cd "$root"
fail() { printf 'release: %s\n' "$*" >&2; exit 1; }
[ "$#" -ge 1 ] && [ "$#" -le 2 ] || fail 'usage: release.sh <version> [--publish]'
version="$1"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail 'version must be x.y.z'
publish="${2:-}"
[ -z "$publish" ] || [ "$publish" = --publish ] || fail 'usage: release.sh <version> [--publish]'
tmp=""
lock_owned=0
lockdir="$root/target/release.lock"
cleanup() {
  [ -z "$tmp" ] || /bin/rm -rf "$tmp"
  [ "$lock_owned" != 1 ] || /bin/rm -rf "$lockdir"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$root/target"
if ! mkdir "$lockdir" 2>/dev/null; then
  holder="$(cat "$lockdir/holder" 2>/dev/null || true)"
  holder_pid="${holder%% *}"
  if [ -n "$holder_pid" ] && ! kill -0 "$holder_pid" 2>/dev/null; then
    /bin/rm -rf "$lockdir"
    mkdir "$lockdir" 2>/dev/null || fail "another release run holds $lockdir"
  else
    fail "another release run holds $lockdir: ${holder:-unknown holder}"
  fi
fi
lock_owned=1
printf '%s %s\n' "$$" "$version" > "$lockdir/holder"
[ -z "$(git status --porcelain -- . ':(exclude)swift/Sources/DBlayer/dblayer.swift')" ] || fail 'working tree is not clean'
[ "$(git branch --show-current)" = master ] || fail 'branch must be master'
package_files=$'Package.swift\nswift/Sources/DBlayer/dblayer.swift'
check_package_commit() {
  local commit="$1" parent="$2"
  [ "$(git rev-parse "$commit^" 2>/dev/null)" = "$parent" ] || fail "package commit $commit is not directly on source commit $parent"
  local changed
  changed="$(git diff-tree --no-commit-id --name-only -r "$commit")"
  { [ -n "$changed" ] && [ -z "$(printf '%s\n' "$changed" | grep -vxF "$package_files" || true)" ]; } || fail "package commit $commit changes files other than Package.swift and dblayer.swift"
}
package_sha=""
source_sha="$(git rev-parse HEAD)"
if [ "$(git log -1 --format=%s)" = "Package release $version" ]; then
  package_sha="$source_sha"
  source_sha="$(git rev-parse "$package_sha^")" || fail 'package commit has no parent'
  check_package_commit "$package_sha" "$source_sha"
fi
run_id="$(date +%s)-$$-$RANDOM"
export RELEASE_SOURCE_SHA="$source_sha" RELEASE_RUN_ID="$run_id"
require_source() {
  local head expected="${package_sha:-$source_sha}"
  head="$(git rev-parse HEAD)"
  [ "$head" = "$expected" ] || fail "source commit changed during the run: recorded $expected, HEAD is $head"
  [ -z "$(git status --porcelain -- . ':(exclude)swift/Sources/DBlayer/dblayer.swift')" ] || fail 'working tree changed during the run'
}
previous_tag="$(git describe --tags --abbrev=0 --exclude "$version")" || fail 'previous tag not found'
subjects="$(git log --format=%s "$previous_tag"..HEAD)" || fail 'cannot read commits since previous tag'
[ "$(printf '%s\n' "$subjects" | grep -Fxc "Release $version" || true)" = 1 ] || fail "expected exactly one Release $version commit since $previous_tag"
[ "$(sed -n '/^\[package\]/,/^\[/s/^version = "\([^"]*\)"$/\1/p' core/Cargo.toml)" = "$version" ] || fail 'core version does not match'
grep -Fq "## $version - " CHANGELOG.md || fail 'changelog heading missing'
manifest=core/src/ffi/selftest/manifest.tsv
[ -s "$manifest" ] || fail "sequence manifest missing: $manifest (the lifecycle sequences must be on master before a release)"
for check in check_docs check_invariants check_zoo; do bash "scripts/$check.sh" || fail "$check failed"; done
# The app suites run when the release touches the library boundary or its build and test inputs. RELEASE_CONSUMERS=always forces them. No setting skips them.
consumer_boundary='^(core/src/(lifecycle|ffi|session|sync|store)/|gen/|swift/|kotlin/|Cargo\.toml$|scripts/(build_|generate_|release\.sh|check_sequences|check_consumers))'
consumers=required
if [ "${RELEASE_CONSUMERS:-}" != always ]; then
  changed="$(git diff --name-only "$previous_tag"..HEAD)" || fail 'cannot list files changed since the previous tag'
  grep -Eq "$consumer_boundary" <<< "$changed" || consumers=not-required
fi
bash scripts/build_ios.sh
bash scripts/build_kotlin.sh --abis
cargo build --release --manifest-path Cargo.toml -p dblayer-gen --target aarch64-apple-darwin --target x86_64-apple-darwin

stage="$root/target/release-$version"
tmp="$(mktemp -d)"
attachments=(DBlayerFFI.xcframework.zip dblayer.swift dblayer-android.zip dblayer-gen-macos-universal)
archives=(DBlayerFFI.xcframework.zip dblayer-android.zip)
if [ -d "$stage" ]; then
  for file in "${attachments[@]}" SHA256SUMS; do [ -s "$stage/$file" ] || fail "incomplete existing output: $stage/$file"; done
  (cd "$stage" && shasum -a 256 -c SHA256SUMS)
else
  mkdir -p "$stage"
  (cd swift && zip -qry "$stage/DBlayerFFI.xcframework.zip" DBlayerFFI.xcframework)
  cp swift/Sources/DBlayer/dblayer.swift "$stage/dblayer.swift"
  (cd kotlin && zip -q "$stage/dblayer-android.zip" src/main/jniLibs/{arm64-v8a,armeabi-v7a,x86_64,x86}/libdblayer.so src/main/kotlin/com/noma4i/dblayer/dblayer.kt)
  lipo -create target/{aarch64-apple-darwin,x86_64-apple-darwin}/release/dblayer-gen -output "$stage/dblayer-gen-macos-universal"
  chmod +x "$stage/dblayer-gen-macos-universal"
  (cd "$stage" && shasum -a 256 "${attachments[@]}" > SHA256SUMS)
fi
for archive in "${archives[@]}"; do mkdir -p "$tmp/$archive"; unzip -q "$stage/$archive" -d "$tmp/$archive"; done
awk -v heading="## $version - " 'index($0, heading)==1 {found=1; next} found && /^## / {exit} found {print}' CHANGELOG.md > "$stage/NOTES.md"
check_archs() {
  local file="$1" expected="$2" actual
  lipo -info "$file"
  actual="$(lipo -archs "$file" | tr ' ' '\n' | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//')"
  [ "$actual" = "$expected" ] || fail "unexpected architectures in $file: $actual"
}
check_archs "$tmp/DBlayerFFI.xcframework.zip/DBlayerFFI.xcframework/macos-arm64_x86_64/libdblayer.a" 'arm64 x86_64'
check_archs "$tmp/DBlayerFFI.xcframework.zip/DBlayerFFI.xcframework/ios-arm64_x86_64-simulator/libdblayer.a" 'arm64 x86_64'
check_archs "$tmp/DBlayerFFI.xcframework.zip/DBlayerFFI.xcframework/ios-arm64/libdblayer.a" 'arm64'
check_archs "$stage/dblayer-gen-macos-universal" 'arm64 x86_64'
for archive in "${archives[@]}"; do unzip -l "$stage/$archive"; done
for abi in arm64-v8a armeabi-v7a x86_64 x86; do [ -f "$tmp/dblayer-android.zip/src/main/jniLibs/$abi/libdblayer.so" ] || fail "missing Android ABI $abi"; done
[ -f "$tmp/dblayer-android.zip/src/main/kotlin/com/noma4i/dblayer/dblayer.kt" ] || fail 'missing Kotlin bindings'

gradle="${GRADLE:-$(ls -d "$HOME"/.gradle/wrapper/dists/gradle-9.*-bin/*/gradle-9.*/bin/gradle 2>/dev/null | sort -V | tail -1)}"
[ -x "$gradle" ] || fail 'Gradle 9 not found: set GRADLE'
staging="$tmp/maven-staging"
mkdir -p "$staging"
"$gradle" -p kotlin -PreleaseVersion="$version" -PstagingRepo="$staging" publishReleasePublicationToStagingRepository
artifact_dir="$staging/com/noma4i/dblayer/$version"
for file in dblayer-$version.aar dblayer-$version.pom dblayer-$version.module; do
  [ -s "$artifact_dir/$file" ] || fail "missing Maven artifact $file"
done
mkdir "$tmp/maven-aar"
(cd "$tmp/maven-aar" && unzip -q "$artifact_dir/dblayer-$version.aar")
for abi in arm64-v8a armeabi-v7a x86_64 x86; do [ -s "$tmp/maven-aar/jni/$abi/libdblayer.so" ] || fail "missing Maven ABI $abi"; done
mkdir "$tmp/maven-classes"
(cd "$tmp/maven-classes" && unzip -q "$tmp/maven-aar/classes.jar")
[ -n "$(find "$tmp/maven-classes/com/noma4i/dblayer" -name '*.class' -print -quit 2>/dev/null)" ] || fail 'missing Kotlin classes in Maven AAR'

checksum="$(swift package compute-checksum "$stage/DBlayerFFI.xcframework.zip")"
write_package() {
  cat <<EOF
// swift-tools-version: 6.0

import PackageDescription

let package = Package(
  name: "DBlayer",
  platforms: [.iOS(.v17), .macOS(.v14)],
  products: [
    .library(name: "DBlayer", targets: ["DBlayer"]),
    .library(name: "DBlayerTesting", targets: ["DBlayerTesting"])
  ],
  targets: [
    .binaryTarget(
      name: "dblayerFFI",
      url: "https://github.com/noma4i/dblayer/releases/download/$version/DBlayerFFI.xcframework.zip",
      checksum: "$checksum"
    ),
    .target(
      name: "DBlayer",
      dependencies: ["dblayerFFI"],
      path: "swift/Sources/DBlayer",
      swiftSettings: [.swiftLanguageMode(.v5)]
    ),
    .target(
      name: "DBlayerTesting",
      dependencies: ["DBlayer"],
      path: "swift/Sources/DBlayerTesting",
      swiftSettings: [.swiftLanguageMode(.v5)]
    )
  ]
)
EOF
}
write_package > "$tmp/Package.swift"

/bin/rm -f "$stage/sequences.tsv" "$stage/consumers.tsv" "$stage/RECEIPT.json"
bash scripts/check_sequences.sh "$stage" --manifest "$manifest" || fail 'sequence suites are red or incomplete: no tag'
if [ "$consumers" = required ]; then
  bash scripts/check_consumers.sh "$stage" --version "$version" --maven "$artifact_dir" --package-swift "$tmp/Package.swift" || fail 'app suites are red or incomplete: no tag'
fi
require_source
python3 -I - "$stage" "$version" "$manifest" "$consumers" "$source_sha" "$run_id" <<'RECEIPT' || fail 'lane reports are missing or incomplete: no tag'
import hashlib, json, sys
from pathlib import Path

stage, version, manifest, consumers, commit, run_id = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]
def refuse(message):
    sys.exit(f"release: {message}")
def rows(name):
    path = stage / name
    if not path.is_file() or path.stat().st_size == 0:
        refuse(f"{name} is missing or empty")
    return [line.split("\t") for line in path.read_text().splitlines()]
def lanes(name, required, width):
    result = {}
    for row in rows(name):
        if len(row) < width or row[0] in result:
            refuse(f"{name} has a malformed or repeated row: {row}")
        if row[width - 2] != commit or row[width - 1] != run_id:
            refuse(f"{name} row {row[0]} belongs to source {row[width - 2]} run {row[width - 1]}, expected source {commit} run {run_id}")
        result[row[0]] = row
    if sorted(result) != sorted(required):
        refuse(f"{name} lanes are {sorted(result)}, expected {sorted(required)}")
    return result
listed_by_lane = {}
for line in Path(manifest).read_text().splitlines()[2:]:
    cells = line.split("\t")
    if len(cells) > 3:
        listed_by_lane.setdefault(cells[2], set()).add(cells[3])
sequences = {}
for lane, row in lanes("sequences.tsv", ["core", "swift", "kotlin"], 7).items():
    try:
        listed, passed = int(row[1]), int(row[2])
    except ValueError:
        refuse(f"sequences.tsv lane {lane} has non-numeric counts: {row}")
    expected = len(listed_by_lane.get(lane, ()))
    if row[3] != "0" or listed == 0 or listed != expected or passed != listed:
        refuse(f"sequences lane {lane} is incomplete: rc={row[3]} listed={listed} passed={passed} manifest={expected}")
    sequences[lane] = {"listed": listed, "passed": passed, "rc": row[3]}
consumer_lanes = {}
if consumers == "required":
    for lane, row in lanes("consumers.tsv", ["ios", "android"], 5).items():
        if row[1] != "0":
            refuse(f"consumers lane {lane} is red: rc={row[1]}")
        consumer_lanes[lane] = {"rc": row[1]}
receipt = {
    "version": version,
    "source_commit": commit,
    "manifest_sha256": hashlib.sha256(Path(manifest).read_bytes()).hexdigest(),
    "artifacts_sha256": {line.split("  ", 1)[1]: line.split("  ", 1)[0] for line in (stage / "SHA256SUMS").read_text().splitlines()},
    "sequences": sequences,
    "consumers": consumers,
    "consumer_lanes": consumer_lanes,
}
(stage / "RECEIPT.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
RECEIPT
if [ "$publish" = --publish ]; then
  require_source
  package_commits="$(git log --format=%s "$previous_tag"..HEAD | grep -Fxc "Package release $version" || true)"
  if [ -z "$package_sha" ] && [ "$package_commits" = 0 ]; then
    cp "$tmp/Package.swift" Package.swift
    git add Package.swift
    cp "$stage/dblayer.swift" swift/Sources/DBlayer/dblayer.swift
    git add -f swift/Sources/DBlayer/dblayer.swift
    git commit -m "Package release $version"
    package_sha="$(git rev-parse HEAD)"
    check_package_commit "$package_sha" "$source_sha"
  elif [ -n "$package_sha" ] && [ "$package_commits" = 1 ]; then
    git show HEAD:Package.swift > "$tmp/committed-Package.swift"
    git show HEAD:swift/Sources/DBlayer/dblayer.swift > "$tmp/committed-dblayer.swift"
    cmp -s "$tmp/Package.swift" "$tmp/committed-Package.swift" || fail 'existing Package.swift differs from the release package'
    cmp -s "$stage/dblayer.swift" "$tmp/committed-dblayer.swift" || fail 'existing dblayer.swift differs from the release package'
  else
    fail "unexpected Package release $version commit state"
  fi
  require_source
  git tag "$version" "$package_sha" 2>/dev/null || {
    [ "$(git rev-parse "$version^{commit}")" = "$package_sha" ] || fail "tag $version already points elsewhere"
  }
  git show "$version":swift/Sources/DBlayer/dblayer.swift > "$tmp/tagged-dblayer.swift"
  cmp -s "$stage/dblayer.swift" "$tmp/tagged-dblayer.swift" || fail 'tagged dblayer.swift differs from the release package'
else
  printf '%s\n' 'Package.swift to be written:'
  cat "$tmp/Package.swift"
  mkdir -p "$tmp/package/swift/Sources/DBlayer" "$tmp/package/swift/Sources/DBlayerTesting"
  cp "$tmp/Package.swift" "$tmp/package/Package.swift"
  cp swift/Sources/DBlayer/*.swift "$tmp/package/swift/Sources/DBlayer/"
  cp "$stage/dblayer.swift" "$tmp/package/swift/Sources/DBlayer/dblayer.swift"
  cp swift/Sources/DBlayerTesting/*.swift "$tmp/package/swift/Sources/DBlayerTesting/"
  swift package --package-path "$tmp/package" dump-package > "$tmp/package.json"
fi

printf 'release artifacts: %s\n' "$stage"

push_maven_branch() {
  local remote="$1" checkout="$tmp/maven-branch" remote_branch
  remote_branch="$(git ls-remote --heads "$remote" maven | awk 'NR == 1 {print $1}')"
  if [ -n "$remote_branch" ]; then
    git clone --branch maven --single-branch "$remote" "$checkout"
  else
    mkdir "$checkout"
    git -C "$checkout" init -b maven
    git -C "$checkout" remote add origin "$remote"
  fi
  if [ -d "$checkout/com/noma4i/dblayer/$version" ]; then
    diff -qr "$artifact_dir" "$checkout/com/noma4i/dblayer/$version" || fail "Maven $version already exists with different files"
    return
  fi
  mkdir -p "$checkout/com/noma4i/dblayer"
  cp -R "$artifact_dir" "$checkout/com/noma4i/dblayer/$version"
  git -C "$checkout" add "com/noma4i/dblayer/$version"
  git -C "$checkout" commit -m "Maven $version"
  git -C "$checkout" push -u origin maven
}

if [ "$publish" = --publish ]; then
  git push origin master
  git push origin "$version"
  push_maven_branch "$(git remote get-url origin)"
  files=(); for file in "${attachments[@]}"; do files+=("$stage/$file"); done
  if ! gh release view "$version" >/dev/null 2>&1; then
    gh release create "$version" --title "$version" --notes-file "$stage/NOTES.md" --latest "${files[@]}"
  fi
  gh release view "$version" --json assets --jq '.assets[].name' > "$tmp/assets"
  for file in "${attachments[@]}"; do grep -Fxq "$file" "$tmp/assets" || fail "missing published attachment: $file"; done
  mkdir "$tmp/download"
  gh release download "$version" --dir "$tmp/download"
  (cd "$tmp/download" && shasum -a 256 -c "$stage/SHA256SUMS")
fi
