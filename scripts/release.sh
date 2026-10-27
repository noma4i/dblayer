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
[ -z "$(git status --porcelain)" ] || fail 'working tree is not clean'
[ "$(git branch --show-current)" = master ] || fail 'branch must be master'
previous_tag="$(git describe --tags --abbrev=0 --exclude "$version")" || fail 'previous tag not found'
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
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
attachments=(DBlayerFFI.xcframework.zip dblayer.swift dblayer-android.zip dblayer-gen-macos-universal libdblayer-macos-universal.dylib)
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
  cp target/jvm-build/libdblayer.dylib "$stage/libdblayer-macos-universal.dylib"
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
check_archs "$stage/libdblayer-macos-universal.dylib" 'arm64 x86_64'
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
if [ "$publish" = --publish ]; then
  package_commits="$(git log --format=%s "$previous_tag"..HEAD | grep -Fxc "Package release $version" || true)"
  if [ "$package_commits" = 0 ]; then
    cp "$tmp/Package.swift" Package.swift
    git add Package.swift
    git add -f swift/Sources/DBlayer/dblayer.swift
    git commit -m "Package release $version"
  elif [ "$package_commits" = 1 ] && [ "$(git log -1 --format=%s)" = "Package release $version" ]; then
    git show HEAD:Package.swift > "$tmp/committed-Package.swift"
    git show HEAD:swift/Sources/DBlayer/dblayer.swift > "$tmp/committed-dblayer.swift"
    cmp -s "$tmp/Package.swift" "$tmp/committed-Package.swift" || fail 'existing Package.swift differs from the release package'
    cmp -s "$stage/dblayer.swift" "$tmp/committed-dblayer.swift" || fail 'existing dblayer.swift differs from the release package'
  else
    fail "unexpected Package release $version commit state"
  fi
  git tag -a "$version" -m "$version" 2>/dev/null || {
    [ "$(git rev-parse "$version^{commit}")" = "$(git rev-parse HEAD)" ] || fail "tag $version already points elsewhere"
  }
else
  printf '%s\n' 'Package.swift to be written:'
  cat "$tmp/Package.swift"
  mkdir -p "$tmp/package/swift/Sources/DBlayer" "$tmp/package/swift/Sources/DBlayerTesting"
  cp "$tmp/Package.swift" "$tmp/package/Package.swift"
  cp swift/Sources/DBlayer/*.swift "$tmp/package/swift/Sources/DBlayer/"
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
