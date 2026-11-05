fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    uniffi::uniffi_bindgen_main();
    if args.get(1).map(String::as_str) != Some("generate") || !args.windows(2).any(|pair| matches!(pair[0].as_str(), "--language" | "-l") && pair[1] == "swift") { return Ok(()); }
    let output = args.windows(2).find(|pair| matches!(pair[0].as_str(), "--out-dir" | "-o")).ok_or("Swift generation requires --out-dir")?;
    let file = std::path::Path::new(&output[1]).join("dblayer.swift");
    let source = std::fs::read_to_string(&file)?;
    let (prefix, tail) = source.split_once("fileprivate func uniffiRustCallAsync<F, T>(").ok_or("missing UniFFI Swift async function")?;
    let (_, suffix) = tail.split_once("// Callback handlers for an async calls.").ok_or("missing UniFFI Swift async callback")?;
    let source = format!("{prefix}{}// Callback handlers for an async calls.{suffix}", include_str!("swift_async.inc"));
    if source.matches("fatalError(\"Cancellation not supported yet\")").count() != 1 { return Err("unexpected UniFFI Swift cancellation status".into()); }
    let source = source.replace("fatalError(\"Cancellation not supported yet\")", "throw CancellationError()");
    let mut output = String::new();
    let mut cancellable = false;
    let mut cancellations = 0;
    for line in source.split_inclusive('\n') {
        if ["uniffi_dblayer_fn_method_database_session_call(", "uniffi_dblayer_fn_func_fetch_server_config("].iter().any(|call| line.contains(call)) { cancellable = true; }
        if let Some((indent, result)) = line.split_once("pollFunc: ffi_dblayer_rust_future_poll_") {
            if cancellable {
                output.push_str(&format!("{indent}cancelFunc: ffi_dblayer_rust_future_cancel_{result}"));
                cancellations += 1;
            }
            cancellable = false;
        }
        output.push_str(line);
    }
    if cancellations != 2 { return Err("expected two cancellable encrypted Swift calls".into()); }
    std::fs::write(file, output)?;
    Ok(())
}
