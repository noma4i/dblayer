//! The edits the repository makes to the UniFFI output of each platform. Every edit states how many places it changes and fails when the template differs.
use std::fmt::Write as _;

#[cfg(test)]
mod tests;

const SWIFT_RUNTIME: &str = include_str!("swift_runtime.inc");
const SWIFT_ASYNC: &str = include_str!("swift_async.inc");
const KOTLIN_RUNTIME: &str = include_str!("kotlin_runtime.inc");

fn count_lines(source: &str, line: &str) -> usize { source.lines().filter(|candidate| *candidate == line).count() }

fn expect_lines(source: &str, line: &str, expected: usize) -> Result<(), String> {
    let found = count_lines(source, line);
    if found == expected { Ok(()) } else { Err(format!("expected {expected} lines `{line}`, found {found}: the UniFFI template changed")) }
}

pub fn swift(source: &str) -> Result<String, String> {
    let (prefix, tail) = source.split_once("fileprivate func uniffiRustCallAsync<F, T>(").ok_or("missing UniFFI Swift async function")?;
    let (_, suffix) = tail.split_once("// Callback handlers for an async calls.").ok_or("missing UniFFI Swift async callback")?;
    let source = format!("{prefix}{SWIFT_RUNTIME}{SWIFT_ASYNC}// Callback handlers for an async calls.{suffix}");
    if source.matches("fatalError(\"Cancellation not supported yet\")").count() != 1 { return Err("unexpected UniFFI Swift cancellation status".into()); }
    let source = source.replace("fatalError(\"Cancellation not supported yet\")", "throw CancellationError()");
    let source = cancellable_calls(&source)?;
    foreign_future_gate(&source)
}

fn cancellable_calls(source: &str) -> Result<String, String> {
    let mut output = String::new();
    let mut cancellable = false;
    let mut cancellations = 0;
    for line in source.split_inclusive('\n') {
        if ["uniffi_dblayer_fn_method_database_session_call(", "uniffi_dblayer_fn_func_fetch_server_config("].iter().any(|call| line.contains(call)) { cancellable = true; }
        if let Some((indent, result)) = line.split_once("pollFunc: ffi_dblayer_rust_future_poll_") {
            if cancellable {
                let _ = write!(output, "{indent}cancelFunc: ffi_dblayer_rust_future_cancel_{result}");
                cancellations += 1;
            }
            cancellable = false;
        }
        output.push_str(line);
    }
    if cancellations != 2 { return Err("expected two cancellable encrypted Swift calls".into()); }
    Ok(output)
}

fn foreign_future_gate(source: &str) -> Result<String, String> {
    let task = "    let task = Task {";
    let free = "        free: uniffiForeignFutureDroppedCallback";
    expect_lines(source, task, 2)?;
    expect_lines(source, free, 2)?;
    let mut output = String::new();
    let mut after_free = false;
    for line in source.lines() {
        if line == task {
            output.push_str("    let (gateStream, gate) = AsyncStream<Void>.makeStream()\n");
            output.push_str(line);
            output.push_str("\n        for await _ in gateStream { break }\n");
            continue;
        }
        if after_free {
            after_free = false;
            if line == "    )" {
                output.push_str(line);
                output.push_str("\n    gate.yield()\n    gate.finish()\n");
                continue;
            }
        }
        output.push_str(line);
        output.push('\n');
        if line == free { after_free = true; }
    }
    expect_lines(&output, "    gate.finish()", 2)?;
    Ok(output)
}

pub fn kotlin(source: &str) -> Result<String, String> {
    let marker = "        Native.register(UniffiLib::class.java, findLibraryName(componentName = \"dblayer\"))";
    expect_lines(source, marker, 1)?;
    let launch = "    val job = GlobalScope.launch coroutineBlock@ {";
    let publish = "    uniffiOutDroppedCallback.uniffiSetValue(UniffiForeignFutureDroppedCallbackStruct(handle, uniffiForeignFutureDroppedCallbackImpl))";
    expect_lines(source, launch, 2)?;
    expect_lines(source, publish, 2)?;
    let wake = "        uniffiContinuationHandleMap.remove(data).resume(pollResult)";
    expect_lines(source, wake, 1)?;
    let mut output = String::new();
    for line in source.lines() {
        if line == marker {
            output.push_str("        ProcessOwner.claim()\n");
        } else if line == launch {
            output.push_str("    val job = GlobalScope.launch(start = kotlinx.coroutines.CoroutineStart.LAZY) coroutineBlock@ {\n");
            continue;
        } else if line == publish {
            output.push_str(line);
            output.push_str("\n    uniffiOutDroppedCallback.write()\n    uniffiOutDroppedCallback.setAutoWrite(false)\n    job.start()\n");
            continue;
        } else if line == wake {
            output.push_str("        RuntimeLane.enqueue { uniffiContinuationHandleMap.remove(data).resume(pollResult) }\n");
            continue;
        }
        output.push_str(line);
        output.push('\n');
    }
    let output = scalar_completions(&output)?;
    Ok(format!("{output}{KOTLIN_RUNTIME}"))
}

fn scalar_completions(source: &str) -> Result<String, String> {
    let pattern = "`uniffiFutureCallback`: UniffiForeignFutureComplete";
    let alloc = "    external fun ffi_dblayer_rustbuffer_alloc(`size`: Long,uniffi_out_err: UniffiRustCallStatus, ";
    if source.matches(pattern).count() != 8 { return Err("expected exactly 8 UniffiForeignFutureComplete parameters in dblayer.kt, the UniFFI template changed".into()); }
    let buffer = "`uniffiFutureCallback`: UniffiForeignFutureCompleteRustBuffer,";
    let unit = "`uniffiFutureCallback`: UniffiForeignFutureCompleteVoid,";
    if source.matches(buffer).count() + source.matches(unit).count() != 8 { return Err("dblayer.kt uses a UniffiForeignFutureComplete type without a scalar completion in core/src/ffi/foreign_future/mod.rs".into()); }
    expect_lines(source, alloc, 1)?;
    let pointer = "`uniffiFuturePointer`: Pointer?,";
    let mut output = String::new();
    let mut kind = "";
    for line in source.lines() {
        let mut line = line.to_string();
        if line == alloc {
            output.push_str("    external fun dblayer_complete_foreign_future_buffer(`callback`: Pointer?,`handle`: Long,`returnValue`: RustBuffer.ByValue,`code`: Byte,`errorBuf`: RustBuffer.ByValue,): Unit\n");
            output.push_str("    external fun dblayer_complete_foreign_future_unit(`callback`: Pointer?,`handle`: Long,`code`: Byte,`errorBuf`: RustBuffer.ByValue,): Unit\n");
        }
        if line.contains(buffer) { line = line.replacen(buffer, pointer, 1); kind = "RustBuffer"; }
        if line.contains(unit) { line = line.replacen(unit, pointer, 1); kind = "Void"; }
        output.push_str(&line);
        output.push('\n');
        if line.starts_with("        override fun callback(") && line.contains("`uniffiFuturePointer`") {
            let _ = writeln!(output, "            val uniffiFutureCallback = UniffiForeignFutureAnswer{kind}(uniffiFuturePointer)");
        }
        if !line.contains("uniffiFuturePointer") { kind = ""; }
    }
    output.push_str(ANSWERS);
    if output.matches(pointer).count() != 8 { return Err("8 uniffiFuturePointer parameters were not written to dblayer.kt".into()); }
    if output.lines().filter(|line| line.starts_with("            val uniffiFutureCallback = UniffiForeignFutureAnswer")).count() != 4 { return Err("4 UniffiForeignFutureAnswer values were not inserted into dblayer.kt".into()); }
    if output.matches("external fun dblayer_complete_foreign_future_").count() != 2 { return Err("the scalar completion declarations were not inserted into dblayer.kt".into()); }
    Ok(output)
}

const ANSWERS: &str = "
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
";
