#!/usr/bin/env bash
set -euo pipefail

name="generate_kotlin_bindings.sh"
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
absolute() { printf '%s/%s' "$(cd -- "$(dirname -- "$1")" && pwd -P)" "$(basename -- "$1")"; }
usage="usage: $name --bindgen <uniffi-bindgen> --library <libdblayer.a|.so|.dylib> --config <uniffi.toml> --out <directory>"
bindgen="" library="" config="" out=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --bindgen | --library | --config | --out)
      [ "$#" -ge 2 ] || { echo "$name: $1 needs a value" >&2; exit 1; }
      case "$1" in
        --bindgen) bindgen="$2" ;;
        --library) library="$2" ;;
        --config) config="$2" ;;
        --out) out="$2" ;;
      esac
      shift 2 ;;
    *) echo "$name: unknown argument $1" >&2; echo "$usage" >&2; exit 1 ;;
  esac
done
[ -n "$bindgen" ] || { echo "$name: --bindgen is required" >&2; exit 1; }
[ -n "$library" ] || { echo "$name: --library is required" >&2; exit 1; }
[ -n "$config" ] || { echo "$name: --config is required" >&2; exit 1; }
[ -n "$out" ] || { echo "$name: --out is required" >&2; exit 1; }
[ -f "$bindgen" ] && [ -x "$bindgen" ] || { echo "$name: --bindgen is not an executable file: $bindgen" >&2; exit 1; }
[ -f "$library" ] || { echo "$name: --library is not a file: $library" >&2; exit 1; }
[ -f "$config" ] || { echo "$name: --config is not a file: $config" >&2; exit 1; }
[ -d "$out" ] || { echo "$name: --out is not a directory: $out" >&2; exit 1; }

global="$(mktemp)"
trap '/bin/rm -f "$global"' EXIT
sed 's/^\[/[crates.dblayer./' "$config" > "$global"
(cd -- "$root/core" && "$(absolute "$bindgen")" generate --library "$(absolute "$library")" --config "$global" --language kotlin --no-format --out-dir "$(cd -- "$out" && pwd -P)")

generated="$out/com/noma4i/dblayer/dblayer.kt"
[ -f "$generated" ] || { echo "$name: $generated was not generated" >&2; exit 1; }
marker='        Native.register(UniffiLib::class.java, findLibraryName(componentName = "dblayer"))'
[ "$(grep -cxF -- "$marker" "$generated")" = 1 ] || { echo "$name: expected exactly 1 UniffiLib Native.register line in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
awk -v marker="$marker" '$0 == marker { print "        ProcessOwner.claim()" } { print }' "$generated" > "$generated.claimed"
mv "$generated.claimed" "$generated"
[ "$(grep -cxF -- '        ProcessOwner.claim()' "$generated")" = 1 ] || { echo "$name: ProcessOwner.claim() was not inserted into dblayer.kt" >&2; exit 1; }

launch='    val job = GlobalScope.launch coroutineBlock@ {'
publish='    uniffiOutDroppedCallback.uniffiSetValue(UniffiForeignFutureDroppedCallbackStruct(handle, uniffiForeignFutureDroppedCallbackImpl))'
[ "$(grep -cxF -- "$launch" "$generated")" = 2 ] || { echo "$name: expected exactly 2 GlobalScope.launch lines in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
[ "$(grep -cxF -- "$publish" "$generated")" = 2 ] || { echo "$name: expected exactly 2 uniffiOutDroppedCallback.uniffiSetValue lines in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
awk -v launch="$launch" -v publish="$publish" '
  $0 == launch { print "    val job = GlobalScope.launch(start = kotlinx.coroutines.CoroutineStart.LAZY) coroutineBlock@ {"; next }
  $0 == publish { print; print "    uniffiOutDroppedCallback.write()"; print "    uniffiOutDroppedCallback.setAutoWrite(false)"; print "    job.start()"; next }
  { print }' "$generated" > "$generated.published"
mv "$generated.published" "$generated"
[ "$(grep -cxF -- '    job.start()' "$generated")" = 2 ] || { echo "$name: job.start() was not inserted twice into dblayer.kt" >&2; exit 1; }
[ "$(grep -cxF -- '    uniffiOutDroppedCallback.setAutoWrite(false)' "$generated")" = 2 ] || { echo "$name: setAutoWrite(false) was not inserted twice into dblayer.kt" >&2; exit 1; }

wake='        uniffiContinuationHandleMap.remove(data).resume(pollResult)'
[ "$(grep -cxF -- "$wake" "$generated")" = 1 ] || { echo "$name: expected exactly 1 continuation resume line in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
awk -v wake="$wake" '$0 == wake { print "        RuntimeLane.run { uniffiContinuationHandleMap.remove(data).resume(pollResult) }"; next } { print }' "$generated" > "$generated.wake"
mv "$generated.wake" "$generated"
[ "$(grep -cxF -- '        RuntimeLane.run { uniffiContinuationHandleMap.remove(data).resume(pollResult) }' "$generated")" = 1 ] || { echo "$name: the deferred continuation resume was not written to dblayer.kt" >&2; exit 1; }

complete_pattern='`uniffiFutureCallback`: UniffiForeignFutureComplete'
alloc='    external fun ffi_dblayer_rustbuffer_alloc(`size`: Long,uniffi_out_err: UniffiRustCallStatus, '
[ "$(grep -cF -- "$complete_pattern" "$generated")" = 18 ] || { echo "$name: expected exactly 18 UniffiForeignFutureComplete parameters in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
[ "$(grep -cE -- '`uniffiFutureCallback`: UniffiForeignFutureComplete(RustBuffer|Void),' "$generated")" = 18 ] || { echo "$name: dblayer.kt uses a UniffiForeignFutureComplete type without a scalar completion in core/src/ffi/foreign_future.rs" >&2; exit 1; }
[ "$(grep -cxF -- "$alloc" "$generated")" = 1 ] || { echo "$name: expected exactly 1 ffi_dblayer_rustbuffer_alloc declaration in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
awk -v alloc="$alloc" '
  $0 == alloc {
    print "    external fun dblayer_complete_foreign_future_buffer(`callback`: Pointer?,`handle`: Long,`returnValue`: RustBuffer.ByValue,`code`: Byte,`errorBuf`: RustBuffer.ByValue,): Unit"
    print "    external fun dblayer_complete_foreign_future_unit(`callback`: Pointer?,`handle`: Long,`code`: Byte,`errorBuf`: RustBuffer.ByValue,): Unit"
  }
  /`uniffiFutureCallback`: UniffiForeignFutureCompleteRustBuffer,/ { sub(/`uniffiFutureCallback`: UniffiForeignFutureCompleteRustBuffer,/, "`uniffiFuturePointer`: Pointer?,"); kind = "RustBuffer" }
  /`uniffiFutureCallback`: UniffiForeignFutureCompleteVoid,/ { sub(/`uniffiFutureCallback`: UniffiForeignFutureCompleteVoid,/, "`uniffiFuturePointer`: Pointer?,"); kind = "Void" }
  { print }
  /^        override fun callback\(.*`uniffiFuturePointer`/ { print "            val uniffiFutureCallback = UniffiForeignFutureAnswer" kind "(uniffiFuturePointer)" }
  { if ($0 !~ /uniffiFuturePointer/) kind = "" }' "$generated" > "$generated.scalar"
mv "$generated.scalar" "$generated"
cat >> "$generated" <<'KOTLIN'

internal class UniffiForeignFutureAnswerRustBuffer(private val pointer: Pointer?) {
    fun callback(`callbackData`: Long,`result`: UniffiForeignFutureResultRustBuffer.UniffiByValue,) {
        UniffiLib.dblayer_complete_foreign_future_buffer(pointer, callbackData, result.`returnValue`, result.`callStatus`.code, result.`callStatus`.error_buf)
    }
}

internal class UniffiForeignFutureAnswerVoid(private val pointer: Pointer?) {
    fun callback(`callbackData`: Long,`result`: UniffiForeignFutureResultVoid.UniffiByValue,) {
        UniffiLib.dblayer_complete_foreign_future_unit(pointer, callbackData, result.`callStatus`.code, result.`callStatus`.error_buf)
    }
}
KOTLIN
[ "$(grep -c '`uniffiFuturePointer`: Pointer?,' "$generated")" = 18 ] || { echo "$name: 18 uniffiFuturePointer parameters were not written to dblayer.kt" >&2; exit 1; }
[ "$(grep -c '^            val uniffiFutureCallback = UniffiForeignFutureAnswer' "$generated")" = 12 ] || { echo "$name: 12 UniffiForeignFutureAnswer values were not inserted into dblayer.kt" >&2; exit 1; }
[ "$(grep -c 'external fun dblayer_complete_foreign_future_' "$generated")" = 2 ] || { echo "$name: the scalar completion declarations were not inserted into dblayer.kt" >&2; exit 1; }
