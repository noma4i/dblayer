#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$root"
fail() { printf 'release: %s\n' "$*" >&2; exit 1; }
[ "$#" -ge 1 ] && [ "$#" -le 2 ] || fail 'usage: release.sh <version> [--publish]'
version="$1"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail 'version must be x.y.z'
publish="${2:-}"
[ -z "$publish" ] || [ "$publish" = --publish ] || fail 'usage: release.sh <version> [--publish]'
[ -z "$(git status --porcelain)" ] || fail 'working tree is not clean'
[ "$(git branch --show-current)" = master ] || fail 'branch must be master'
previous_tag="$(git describe --tags --abbrev=0)" || fail 'previous tag not found'
subjects="$(git log --format=%s "$previous_tag"..HEAD)" || fail 'cannot read commits since previous tag'
[ "$(printf '%s\n' "$subjects" | grep -Fxc "Release $version" || true)" = 1 ] || fail "expected exactly one Release $version commit since $previous_tag"
[ "$(sed -n '/^\[package\]/,/^\[/s/^version = "\([^"]*\)"$/\1/p' core/Cargo.toml)" = "$version" ] || fail 'core version does not match'
grep -Fq "## $version - " CHANGELOG.md || fail 'changelog heading missing'
for check in check_docs check_invariants check_zoo; do bash "scripts/$check.sh" || fail "$check failed"; done
bash scripts/build_ios.sh
bash scripts/build_kotlin.sh --abis
bash scripts/build_kotlin.sh --jvm
for target in aarch64-apple-darwin x86_64-apple-darwin; do
  cargo build --release --manifest-path Cargo.toml -p dblayer-gen --target "$target"
done

stage="$root/target/release-$version"
[ ! -e "$stage" ] || fail "output already exists: $stage"
mkdir -p "$stage"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
(cd swift && zip -qry "$stage/DBlayerFFI.xcframework.zip" DBlayerFFI.xcframework)
cp swift/Sources/DBlayer/dblayer.swift "$stage/dblayer.swift"
(cd kotlin && zip -q "$stage/dblayer-android.zip" src/main/jniLibs/{arm64-v8a,armeabi-v7a,x86_64,x86}/libdblayer.so src/main/kotlin/com/noma4i/dblayer/dblayer.kt)
lipo -create target/{aarch64-apple-darwin,x86_64-apple-darwin}/release/dblayer-gen -output "$stage/dblayer-gen-macos-universal"
chmod +x "$stage/dblayer-gen-macos-universal"
cp target/jvm-build/libdblayer.dylib "$stage/libdblayer-macos-universal.dylib"
attachments=(DBlayerFFI.xcframework.zip dblayer.swift dblayer-android.zip dblayer-gen-macos-universal libdblayer-macos-universal.dylib)
manifest="MANIFEST-$version.txt"
archives=(DBlayerFFI.xcframework.zip dblayer-android.zip)
for archive in "${archives[@]}"; do mkdir -p "$tmp/$archive"; unzip -q "$stage/$archive" -d "$tmp/$archive"; done
{
  printf 'tag %s\ncommit %s\n' "$version" "$(git rev-parse HEAD)"
  for file in "${attachments[@]}"; do printf '%s %s %s\n' "$(shasum -a 256 "$stage/$file" | cut -d ' ' -f1)" "$(wc -c < "$stage/$file" | tr -d ' ')" "$file"; done
  for archive in "${archives[@]}"; do
    while IFS= read -r -d '' file; do printf '%s %s %s:%s\n' "$(shasum -a 256 "$file" | cut -d ' ' -f1)" "$(wc -c < "$file" | tr -d ' ')" "$archive" "${file#"$tmp/$archive/"}"; done < <(find "$tmp/$archive" -type f -print0 | LC_ALL=C sort -z)
  done
} > "$stage/$manifest"
(cd "$stage" && shasum -a 256 "${attachments[@]}" "$manifest" > SHA256SUMS)
checksum="$(swift package compute-checksum "$stage/DBlayerFFI.xcframework.zip")"
awk -v heading="## $version - " 'index($0, heading)==1 {found=1; next} found && /^## / {exit} found {print}' CHANGELOG.md > "$stage/NOTES.md"
cat >> "$stage/NOTES.md" <<NOTES

## Compiled parts

- DBlayerFFI.xcframework.zip: Apple libraries. Place in swift/DBlayerFFI.xcframework. SwiftPM checksum: $checksum
- dblayer.swift: Swift bindings. Place in swift/Sources/DBlayer/dblayer.swift.
- dblayer-android.zip: four Android ABI libraries and Kotlin bindings. Extract into the Kotlin module.
- dblayer-gen-macos-universal: release model generator for macOS arm64 and x86_64. Install as dblayer-gen and run to generate models.
- libdblayer-macos-universal.dylib: JVM library for macOS arm64 and x86_64. Place on the JVM native library path.
- $manifest: tag, commit, sizes and hashes. Use for inventory.
- SHA256SUMS: attachment hashes. Check with \`shasum -a 256 -c SHA256SUMS\`.
NOTES
(cd "$stage" && shasum -a 256 -c SHA256SUMS)
check_archs() {
  local file="$1" expected="$2" actual
  lipo -info "$file"
  actual="$(lipo -archs "$file" | tr ' ' '\n' | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//')"
  [ "$actual" = "$expected" ] || fail "unexpected architectures in $file: $actual"
}
check_archs "$tmp/DBlayerFFI.xcframework.zip/DBlayerFFI.xcframework/macos-arm64_x86_64/libdblayer.a" 'arm64 x86_64'
check_archs "$tmp/DBlayerFFI.xcframework.zip/DBlayerFFI.xcframework/ios-arm64_x86_64-simulator/libdblayer.a" 'arm64 x86_64'
check_archs "$tmp/DBlayerFFI.xcframework.zip/DBlayerFFI.xcframework/ios-arm64/libdblayer.a" 'arm64'
check_archs "$stage/libdblayer-macos-universal.dylib" 'arm64 x86_64'
check_archs "$stage/dblayer-gen-macos-universal" 'arm64 x86_64'
for archive in "${archives[@]}"; do unzip -l "$stage/$archive"; done
for abi in arm64-v8a armeabi-v7a x86_64 x86; do [ -f "$tmp/dblayer-android.zip/src/main/jniLibs/$abi/libdblayer.so" ] || fail "missing Android ABI $abi"; done
[ -f "$tmp/dblayer-android.zip/src/main/kotlin/com/noma4i/dblayer/dblayer.kt" ] || fail 'missing Kotlin bindings'
printf 'release artifacts: %s\n' "$stage"
if [ "$publish" = --publish ]; then
  git tag -a "$version" -m "$version"
  git push origin master
  git push origin "$version"
  files=(); for file in "${attachments[@]}" "$manifest" SHA256SUMS; do files+=("$stage/$file"); done
  gh release create "$version" --repo noma4i/dblayer --title "$version" --notes-file "$stage/NOTES.md" --latest "${files[@]}"
  gh release view "$version" --repo noma4i/dblayer --json assets --jq '.assets[].name' > "$tmp/assets"
  for file in "${attachments[@]}" "$manifest" SHA256SUMS; do grep -Fxq "$file" "$tmp/assets" || fail "missing published attachment: $file"; done
  mkdir "$tmp/download"
  gh release download "$version" --repo noma4i/dblayer --dir "$tmp/download"
  (cd "$tmp/download" && shasum -a 256 -c SHA256SUMS)
fi
