use super::*;

const GATE_TEMPLATE: &str = "    let task = Task {\n    }\n    let handle = foo(\n        free: uniffiForeignFutureDroppedCallback\n    )\n";

fn swift_template() -> String {
    let calls = "    uniffi_dblayer_fn_method_database_session_call(\n        pollFunc: ffi_dblayer_rust_future_poll_rust_buffer,\n    uniffi_dblayer_fn_func_fetch_server_config(\n        pollFunc: ffi_dblayer_rust_future_poll_rust_buffer,\n";
    format!("head\nfileprivate func uniffiRustCallAsync<F, T>(\n    old\n// Callback handlers for an async calls.\n{calls}fatalError(\"Cancellation not supported yet\")\n{GATE_TEMPLATE}{GATE_TEMPLATE}")
}

fn kotlin_template() -> String {
    let mut text = String::from("        Native.register(UniffiLib::class.java, findLibraryName(componentName = \"dblayer\"))\n");
    for _ in 0..2 {
        text.push_str("    val job = GlobalScope.launch coroutineBlock@ {\n    uniffiOutDroppedCallback.uniffiSetValue(UniffiForeignFutureDroppedCallbackStruct(handle, uniffiForeignFutureDroppedCallbackImpl))\n");
    }
    text.push_str("        uniffiContinuationHandleMap.remove(data).resume(pollResult)\n    external fun ffi_dblayer_rustbuffer_alloc(`size`: Long,uniffi_out_err: UniffiRustCallStatus, \n");
    for kind in ["RustBuffer", "RustBuffer", "Void", "Void"] {
        text.push_str(&format!("    external fun ffi(`uniffiFutureCallback`: UniffiForeignFutureComplete{kind},)\n        override fun callback(`uniffiFutureCallback`: UniffiForeignFutureComplete{kind},)\n"));
    }
    text
}

#[test]
fn bridge_prop_generated_swift_gate_and_runtime_come_from_the_generator() {
    let patched = swift(&swift_template()).unwrap();
    assert_eq!(count_lines(&patched, "    gate.finish()"), 2);
    assert_eq!(count_lines(&patched, "final class RustFutureHandle: @unchecked Sendable {"), 1);
    assert_eq!(patched.matches("cancelFunc: ffi_dblayer_rust_future_cancel_rust_buffer").count(), 2);
    assert!(patched.contains("throw CancellationError()"));
}

#[test]
fn bridge_prop_generated_kotlin_patches_name_every_place_they_change() {
    let patched = kotlin(&kotlin_template()).unwrap();
    assert_eq!(count_lines(&patched, "        ProcessOwner.claim()"), 1);
    assert_eq!(count_lines(&patched, "    job.start()"), 2);
    assert_eq!(patched.matches("            val uniffiFutureCallback = UniffiForeignFutureAnswer").count(), 4);
    assert_eq!(count_lines(&patched, "internal object RuntimeLane {"), 1);
    assert_eq!(count_lines(&patched, "internal object ProcessOwner {"), 1);
}

#[test]
fn bridge_prop_generated_a_changed_template_is_refused() {
    let drifted = kotlin_template().replacen("        Native.register(UniffiLib::class.java, findLibraryName(componentName = \"dblayer\"))\n", "", 1);
    assert!(kotlin(&drifted).unwrap_err().contains("the UniFFI template changed"));
    let drifted = swift_template().replacen("    let task = Task {\n", "", 1);
    assert!(swift(&drifted).unwrap_err().contains("the UniFFI template changed"));
}
