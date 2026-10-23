#!/usr/bin/env bash
set -euo pipefail

name="generate_kotlin_bindings.sh"
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
trap 'rm -f "$global"' EXIT
sed 's/^\[/[crates.dblayer./' "$config" > "$global"
"$bindgen" generate --library "$library" --config "$global" --language kotlin --no-format --out-dir "$out"

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

helper='internal inline fun<T, reified E: Throwable> uniffiTraitInterfaceCallAsyncWithError('
dropped_param='    uniffiOutDroppedCallback: UniffiForeignFutureDroppedCallbackStruct,'
success='        handleSuccess(callResult)'
last_argument='                uniffiOutDroppedCallback'
[ "$(grep -cxF -- "$helper" "$generated")" = 1 ] || { echo "$name: expected exactly 1 uniffiTraitInterfaceCallAsyncWithError helper in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
[ "$(grep -cxF -- "$success" "$generated")" = 2 ] || { echo "$name: expected exactly 2 handleSuccess(callResult) lines in dblayer.kt, the UniFFI template changed" >&2; exit 1; }
awk -v helper="$helper" -v dropped_param="$dropped_param" -v success="$success" -v last_argument="$last_argument" '
  /^internal inline fun/ { inside = ($0 == helper) }
  /^    internal object `/ { handed = ($0 ~ /^    internal object `subscribe`:/) ? "unsubscribe" : (($0 ~ /^    internal object `openCable`:/) ? "cableClose" : "") }
  inside && $0 == dropped_param { print; print "    crossinline giveBack: suspend (T) -> Unit = {},"; next }
  inside && $0 == success {
    print "        if (coroutineContext[kotlinx.coroutines.Job]!!.isCancelled) {"
    print "            kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) { giveBack(callResult) }"
    print "        }"
    print; inside = 0; next
  }
  handed != "" && $0 == last_argument { print $0 ","; print "                giveBack = { issued -> uniffiObj.`" handed "`(issued) }"; next }
  { print }' "$generated" > "$generated.returned"
mv "$generated.returned" "$generated"
[ "$(grep -cF -- 'crossinline giveBack: suspend (T) -> Unit = {},' "$generated")" = 1 ] || { echo "$name: the giveBack parameter was not inserted once into dblayer.kt" >&2; exit 1; }
[ "$(grep -cF -- 'kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) { giveBack(callResult) }' "$generated")" = 1 ] || { echo "$name: the giveBack call was not inserted once into dblayer.kt" >&2; exit 1; }
[ "$(grep -cF -- 'giveBack = { issued -> uniffiObj.' "$generated")" = 2 ] || { echo "$name: giveBack was not passed to exactly 2 callbacks (subscribe, openCable) in dblayer.kt" >&2; exit 1; }
