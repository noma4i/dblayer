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
  holder_state=unknown
  if [ -n "$holder_pid" ]; then
    if kill -0 "$holder_pid" 2>/dev/null; then holder_state=alive; else holder_state=dead; fi
  fi
  fail "another release run holds $lockdir: holder pid ${holder_pid:-none}, ${holder_state}; remove the directory by hand only when no release run is alive"
fi
lock_owned=1
printf '%s %s\n' "$$" "$version" > "$lockdir/holder"
[ -z "$(git status --porcelain -- . ':(exclude)swift/Sources/DBlayer/dblayer.swift')" ] || fail 'working tree is not clean'
[ "$(git branch --show-current)" = master ] || fail 'branch must be master'
check_package_commit() {
  local commit="$1" parent="$2"
  [ "$(git rev-list --parents -n 1 "$commit" 2>/dev/null)" = "$commit $parent" ] || fail "package commit $commit is not directly on source commit $parent"
  local changed
  changed="$(git diff-tree --no-commit-id --name-only -r "$commit" | LC_ALL=C sort)"
  [ "$changed" = "$(printf 'Package.swift\nswift/Sources/DBlayer/dblayer.swift')" ] || fail "package commit $commit must change exactly Package.swift and dblayer.swift"
}
package_sha=""
head_sha="$(git rev-parse HEAD)"
source_sha="$head_sha"
point_subject="Point the package manifest at $version"
[ "$(git log -1 --format=%s)" != "$point_subject" ] || source_sha="$(git rev-parse HEAD^)"
[ "$(git log -1 --format=%s "$source_sha")" = "Release $version" ] || fail "HEAD must be Release $version"
[ -z "$(git ls-files -- swift/Sources/DBlayer/dblayer.swift)" ] || fail 'generated Swift binding must not be tracked on master'
run_id="$(date +%s)-$$-$RANDOM"
export RELEASE_SOURCE_SHA="$source_sha" RELEASE_RUN_ID="$run_id"
require_source() {
  local head expected="$head_sha"
  head="$(git rev-parse HEAD)"
  [ "$head" = "$expected" ] || fail "source commit changed during the run: recorded $expected, HEAD is $head"
  [ -z "$(git status --porcelain -- . ':(exclude)swift/Sources/DBlayer/dblayer.swift')" ] || fail 'working tree changed during the run'
}
previous_tag=""
while IFS= read -r candidate; do
  [[ "$candidate" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
  [ "$candidate" != "$version" ] || continue
  [ "$(printf '%s\n%s\n' "$candidate" "$version" | sort -V | head -1)" = "$candidate" ] || continue
  if git merge-base --is-ancestor "$candidate" "$source_sha" 2>/dev/null; then
    previous_tag="$candidate"
    break
  fi
  parent="$(git rev-parse "$candidate^" 2>/dev/null)" || continue
  if [ "$(git log -1 --format=%s "$candidate")" = "Package release $candidate" ] && [ "$(git log -1 --format=%s "$parent")" = "Release $candidate" ] && git merge-base --is-ancestor "$parent" "$source_sha" 2>/dev/null; then
    previous_tag="$candidate"
    break
  fi
done < <(git tag --list '[0-9]*' --sort=-v:refname)
[ -n "$previous_tag" ] || fail 'previous tag not found'
current_manifest="$(git rev-parse -q --verify "$source_sha:Package.swift" || true)"
previous_manifest="$(git rev-parse -q --verify "$previous_tag:Package.swift" || true)"
[ -n "$current_manifest" ] && [ "$current_manifest" = "$previous_manifest" ] || fail "Package.swift on master differs from the manifest of $previous_tag: master must carry the last published manifest (commit 'Point the package manifest at $previous_tag' first)"
subjects="$(git log --format=%s "$previous_tag"..HEAD)" || fail 'cannot read commits since previous tag'
[ "$(printf '%s\n' "$subjects" | grep -Fxc "Release $version" || true)" = 1 ] || fail "expected exactly one Release $version commit since $previous_tag"
[ "$(sed -n '/^\[package\]/,/^\[/s/^version = "\([^"]*\)"$/\1/p' core/Cargo.toml)" = "$version" ] || fail 'core version does not match'
grep -Fq "## $version - " CHANGELOG.md || fail 'changelog heading missing'
manifest=core/src/ffi/selftest/manifest.tsv
[ -s "$manifest" ] || fail "sequence manifest missing: $manifest (the lifecycle sequences must be on master before a release)"
for check in check_docs check_zoo; do bash "scripts/$check.sh" || fail "$check failed"; done
bash scripts/check_legacy.sh check || fail "check_legacy failed"
# The app suites run when the release touches the library boundary or its build and test inputs. RELEASE_CONSUMERS=always forces them. CONSUMER_LANES=none skips them only by the word of the owner for one release; the receipt records consumers = waived-by-owner.
consumer_boundary='^(core/src/|gen/|swift/|kotlin/|Cargo\.toml$|scripts/(build_|generate_|release\.sh|check_sequences|check_consumers))'
consumers=required
consumer_lanes="${CONSUMER_LANES:-ios android}"
[ "$consumer_lanes" = none ] && { consumers=waived-by-owner; consumer_lanes=""; }
for lane in $consumer_lanes; do case "$lane" in ios | android) ;; *) fail "CONSUMER_LANES names an unknown lane: $lane" ;; esac; done
if [ "$consumers" = required ] && [ "${RELEASE_CONSUMERS:-}" != always ]; then
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
# An existing stage is reused only for the source commit that built it: another commit moves it aside and builds again.
if [ -d "$stage" ] && [ "$(cat "$stage/SOURCE" 2> /dev/null)" != "$source_sha" ]; then
  mv "$stage" "$stage.stale-$(cat "$stage/SOURCE" 2> /dev/null || echo unknown)-$run_id" || fail "cannot move the stale stage aside: $stage"
fi
if [ -d "$stage" ]; then
  for file in "${attachments[@]}" SHA256SUMS; do [ -s "$stage/$file" ] || fail "incomplete existing output: $stage/$file"; done
  (cd "$stage" && shasum -a 256 -c SHA256SUMS)
else
  mkdir -p "$stage"
  printf '%s\n' "$source_sha" > "$stage/SOURCE"
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
[ -s "$artifact_dir/dblayer-$version-test-fixtures.aar" ] || fail 'missing Maven test-fixtures AAR'
mkdir "$tmp/fixtures-aar" "$tmp/fixtures-classes"
(cd "$tmp/fixtures-aar" && unzip -q "$artifact_dir/dblayer-$version-test-fixtures.aar") || fail 'corrupt Maven test-fixtures AAR'
[ -s "$tmp/fixtures-aar/classes.jar" ] || fail 'missing classes.jar in Maven test-fixtures AAR'
(cd "$tmp/fixtures-classes" && unzip -q "$tmp/fixtures-aar/classes.jar") || fail 'corrupt classes.jar in Maven test-fixtures AAR'
for class in DBlayerTestingApi MockTransport; do [ -s "$tmp/fixtures-classes/com/noma4i/dblayer/$class.class" ] || fail "missing $class.class in Maven test-fixtures AAR"; done
/bin/rm -rf "$stage/maven"
mkdir -p "$stage/maven/com/noma4i/dblayer"
cp -R "$artifact_dir" "$stage/maven/com/noma4i/dblayer/$version" || fail 'cannot save the Maven staging copy'

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

planned_apps=""
[ "$consumers" != required ] || planned_apps="$consumer_lanes"
receipt_args=(--stage "$stage" --commit "$source_sha" --run-id "$run_id" --maven "$artifact_dir" --package-swift "$tmp/Package.swift" --consumer-lanes "$planned_apps" --plan "$tmp/plan.json")
# A lane runs only when its real inputs differ from the last green receipt (check_release_receipt.py); unknown inputs stop the release before any run.
python3 -I scripts/check_release_receipt.py plan "${receipt_args[@]}" > "$tmp/plan.tsv" || fail 'check inputs are unknown or unreadable: no tag'
cat "$tmp/plan.tsv"
/bin/rm -f "$stage/sequences.tsv" "$stage/consumers.tsv"
seq_lanes=() app_lanes=()
while IFS=$'\t' read -r verdict lane _; do
  [ "$verdict" = run ] || continue
  case "$lane" in core | swift | kotlin) seq_lanes+=(--lane "$lane") ;; *) app_lanes+=(--lane "$lane") ;; esac
done < "$tmp/plan.tsv"
[ "${#seq_lanes[@]}" = 0 ] || bash scripts/check_sequences.sh "$stage" --manifest "$manifest" "${seq_lanes[@]}" || fail 'sequence suites are red or incomplete: no tag'
[ "${#app_lanes[@]}" = 0 ] || bash scripts/check_consumers.sh "$stage" --version "$version" --maven "$artifact_dir" --package-swift "$tmp/Package.swift" "${app_lanes[@]}" || fail 'app suites are red or incomplete: no tag'
require_source
python3 -I scripts/check_release_receipt.py write "${receipt_args[@]}" --version "$version" --consumers "$consumers" --manifest "$manifest" || fail 'lane reports are missing or incomplete: no tag'
if [ "$publish" = --publish ]; then
  require_source
  git diff --cached --quiet || fail 'index is not empty before packaging'
  if git rev-parse --verify -q "refs/tags/$version^{commit}" > "$tmp/tag-sha"; then
    package_sha="$(cat "$tmp/tag-sha")"
  elif [ -s "$stage/PACKAGE_COMMIT" ]; then
    package_sha="$(cat "$stage/PACKAGE_COMMIT")"
    git cat-file -e "$package_sha^{commit}" || fail 'saved package commit is missing'
  else
    package_index="$tmp/package-index"
    GIT_INDEX_FILE="$package_index" git read-tree "$source_sha"
    manifest_blob="$(git hash-object -w "$tmp/Package.swift")"
    binding_blob="$(git hash-object -w "$stage/dblayer.swift")"
    GIT_INDEX_FILE="$package_index" git update-index --add --cacheinfo "100644,$manifest_blob,Package.swift" --cacheinfo "100644,$binding_blob,swift/Sources/DBlayer/dblayer.swift"
    package_tree="$(GIT_INDEX_FILE="$package_index" git write-tree)"
    package_sha="$(printf 'Package release %s\n' "$version" | git commit-tree "$package_tree" -p "$source_sha")"
  fi
  check_package_commit "$package_sha" "$source_sha"
  [ "$(git log -1 --format=%s "$package_sha")" = "Package release $version" ] || fail 'package commit has an unexpected subject'
  git show "$package_sha":Package.swift > "$tmp/committed-Package.swift"
  git show "$package_sha":swift/Sources/DBlayer/dblayer.swift > "$tmp/committed-dblayer.swift"
  cmp -s "$tmp/Package.swift" "$tmp/committed-Package.swift" || fail 'existing Package.swift differs from the release package'
  bash scripts/check_artifact_provenance.sh --bindings "$stage/dblayer.swift" "$tmp/committed-dblayer.swift" || fail 'existing dblayer.swift differs from the release package'
  printf '%s\n' "$package_sha" > "$tmp/PACKAGE_COMMIT"
  mv "$tmp/PACKAGE_COMMIT" "$stage/PACKAGE_COMMIT"
  require_source
  git tag "$version" "$package_sha" 2>/dev/null || {
    [ "$(git rev-parse "$version^{commit}")" = "$package_sha" ] || fail "tag $version already points elsewhere"
  }
  git show "$version":swift/Sources/DBlayer/dblayer.swift > "$tmp/tagged-dblayer.swift"
  bash scripts/check_artifact_provenance.sh --bindings "$stage/dblayer.swift" "$tmp/tagged-dblayer.swift" || fail 'tagged dblayer.swift differs from the release package'
  require_source
  if git rev-parse --verify -q "refs/tags/release-$version^{commit}" > "$tmp/marker-sha"; then
    [ "$(cat "$tmp/marker-sha")" = "$source_sha" ] || fail "tag release-$version already points elsewhere"
  else
    git tag "release-$version" "$source_sha"
  fi
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
  [ "$head_sha" != "$source_sha" ] || git push origin "$source_sha:refs/heads/master"
  git push origin "$package_sha:refs/tags/$version"
  git push origin "$source_sha:refs/tags/release-$version"
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
  if [ "$head_sha" = "$source_sha" ]; then
    require_source
    cp "$tmp/Package.swift" Package.swift
    git add Package.swift
    git commit -q -m "$point_subject" -- Package.swift
    head_sha="$(git rev-parse HEAD)"
  fi
  [ "$(git rev-list --parents -n 1 "$head_sha")" = "$head_sha $source_sha" ] || fail "manifest commit $head_sha is not directly on source commit $source_sha"
  [ "$(git diff-tree --no-commit-id --name-only -r "$head_sha")" = Package.swift ] || fail "manifest commit $head_sha must change exactly Package.swift"
  git show "$head_sha:Package.swift" | cmp -s - "$tmp/Package.swift" || fail 'manifest commit differs from the release package'
  git push origin "$head_sha:refs/heads/master"
fi
