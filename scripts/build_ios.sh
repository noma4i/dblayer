#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
bridge="$root/swift"
build="$root/target/ios-build"
targets=(aarch64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim)
[ "$#" -le 1 ] || { echo "usage: build_ios.sh [--check | --device]" >&2; exit 2; }
case "${1:-}" in
  --check) targets=(aarch64-apple-darwin aarch64-apple-ios-sim) ;;
  --device) targets=(aarch64-apple-ios) ;;
  "") ;;
  *) echo "usage: build_ios.sh [--check | --device]" >&2; exit 2 ;;
esac
if [ "${DBLAYER_BUILD_TARGETS_ONLY:-0}" = 1 ]; then
  printf '%s\n' "${targets[@]}"
  exit 0
fi

export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}" RUST_TEST_THREADS="${RUST_TEST_THREADS:-$(nproc 2>/dev/null || sysctl -n hw.ncpu)}"
export IPHONEOS_DEPLOYMENT_TARGET=17.0

stamp="$build/SOURCE_HASH"
outputs=("$bridge/DBlayerFFI.xcframework" "$bridge/Sources/DBlayer/dblayer.swift")
for target in "${targets[@]}"; do outputs+=("$root/target/$target/release/libdblayer.a"); done
if bash "$root/scripts/native_source_hash.sh" --fresh "$stamp" ios "${targets[@]}" -- "${outputs[@]}"; then
  exit 0
fi
hash="$(bash "$root/scripts/native_source_hash.sh" ios "${targets[@]}")"

for target in "${targets[@]}"; do
  cargo build --release --manifest-path "$root/core/Cargo.toml" --target "$target"
done

bash "$root/scripts/build_bindgen.sh"
rm -rf "$build" "$bridge/DBlayerFFI.xcframework"
mkdir -p "$build/headers" "$bridge/Sources/DBlayer"

(cd -- "$root/core" && "$root/target/debug/uniffi-bindgen" generate \
  --library "$root/target/${targets[0]}/release/libdblayer.a" --language swift --out-dir "$build/swift")

cp "$build/swift/dblayerFFI.h" "$build/headers/"
cp "$build/swift/dblayerFFI.modulemap" "$build/headers/module.modulemap"
cp "$build/swift/dblayer.swift" "$bridge/Sources/DBlayer/dblayer.swift"

generated="$bridge/Sources/DBlayer/dblayer.swift"
task_line='    let task = Task {'
free_line='        free: uniffiForeignFutureDroppedCallback'
[ "$(grep -cxF -- "$task_line" "$generated")" = 2 ] || { echo "build_ios.sh: expected exactly 2 foreign future Task lines in dblayer.swift, the UniFFI template changed" >&2; exit 1; }
[ "$(grep -cxF -- "$free_line" "$generated")" = 2 ] || { echo "build_ios.sh: expected exactly 2 dropped callback lines in dblayer.swift, the UniFFI template changed" >&2; exit 1; }
awk -v task_line="$task_line" -v free_line="$free_line" '
  $0 == task_line { print "    let (gateStream, gate) = AsyncStream<Void>.makeStream()"; print; print "        for await _ in gateStream { break }"; next }
  after_free { after_free = 0; if ($0 == "    )") { print; print "    gate.yield()"; print "    gate.finish()"; next } }
  { print }
  $0 == free_line { after_free = 1 }' "$generated" > "$generated.gated"
mv "$generated.gated" "$generated"
[ "$(grep -cxF -- '    gate.finish()' "$generated")" = 2 ] || { echo "build_ios.sh: the foreign future gate was not inserted twice into dblayer.swift" >&2; exit 1; }

helper='private func uniffiTraitInterfaceCallAsyncWithError<T, E>('
dropped_param='    droppedCallback: UnsafeMutablePointer<UniffiForeignFutureDroppedCallbackStruct>'
success='        handleSuccess(callResult)'
last_argument='                droppedCallback: uniffiOutDroppedCallback'
[ "$(grep -cxF -- "$helper" "$generated")" = 1 ] || { echo "build_ios.sh: expected exactly 1 uniffiTraitInterfaceCallAsyncWithError helper in dblayer.swift, the UniFFI template changed" >&2; exit 1; }
[ "$(grep -cxF -- "$success" "$generated")" = 2 ] || { echo "build_ios.sh: expected exactly 2 handleSuccess(callResult) lines in dblayer.swift, the UniFFI template changed" >&2; exit 1; }
awk -v helper="$helper" -v dropped_param="$dropped_param" -v success="$success" -v last_argument="$last_argument" '
  /^private func / { inside = ($0 == helper) }
  /^        [A-Za-z]+: \{ \($/ { handed = ($0 == "        openCable: { (") ? "cableClose" : (($0 == "        subscribe: { (") ? "unsubscribe" : "") }
  inside && $0 == dropped_param { print dropped_param ","; print "    giveBack: @escaping (T) async -> () = { _ in }"; next }
  inside && $0 == success { print "        if Task.isCancelled { await giveBack(callResult) }"; print; inside = 0; next }
  handed != "" && $0 == "            uniffiTraitInterfaceCallAsyncWithError(" { print "            let uniffiReturnTarget = try? FfiConverterTypeTransport.handleMap.get(handle: uniffiHandle)" }
  handed == "unsubscribe" && $0 == last_argument { print last_argument ","; print "                giveBack: { issued in uniffiReturnTarget?.unsubscribe(token: issued) }"; handed = ""; next }
  handed == "cableClose" && $0 == last_argument { print last_argument ","; print "                giveBack: { issued in await uniffiReturnTarget?.cableClose(token: issued) }"; handed = ""; next }
  { print }' "$generated" > "$generated.returned"
mv "$generated.returned" "$generated"
[ "$(grep -cF -- '    giveBack: @escaping (T) async -> () = { _ in }' "$generated")" = 1 ] || { echo "build_ios.sh: the giveBack parameter was not inserted once into dblayer.swift" >&2; exit 1; }
[ "$(grep -cF -- 'if Task.isCancelled { await giveBack(callResult) }' "$generated")" = 1 ] || { echo "build_ios.sh: the giveBack call was not inserted once into dblayer.swift" >&2; exit 1; }
[ "$(grep -cF -- '                giveBack: { issued in' "$generated")" = 2 ] && [ "$(grep -cF -- 'let uniffiReturnTarget = try? FfiConverterTypeTransport.handleMap.get(handle: uniffiHandle)' "$generated")" = 2 ] || { echo "build_ios.sh: giveBack was not passed to exactly 2 callbacks (subscribe, openCable) in dblayer.swift" >&2; exit 1; }

arguments=()
for target in "${targets[@]}"; do
  arguments+=(-library "$root/target/$target/release/libdblayer.a" -headers "$build/headers")
done
xcodebuild -create-xcframework "${arguments[@]}" -output "$bridge/DBlayerFFI.xcframework"
printf '%s\n' "$hash" > "$stamp"
echo "xcframework: $bridge/DBlayerFFI.xcframework"
