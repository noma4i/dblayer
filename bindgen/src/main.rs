fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    uniffi::uniffi_bindgen_main();
    if args.get(1).map(String::as_str) != Some("generate") { return Ok(()); }
    let language = args.windows(2).find(|pair| matches!(pair[0].as_str(), "--language" | "-l")).map(|pair| pair[1].as_str());
    let output = args.windows(2).find(|pair| matches!(pair[0].as_str(), "--out-dir" | "-o")).ok_or("generation requires --out-dir")?;
    let out = std::path::Path::new(&output[1]);
    let (file, patch): (_, fn(&str) -> Result<String, String>) = match language {
        Some("swift") => (out.join("dblayer.swift"), dblayer_bindgen::patch::swift),
        Some("kotlin") => (out.join("com/noma4i/dblayer/dblayer.kt"), dblayer_bindgen::patch::kotlin),
        _ => return Ok(()),
    };
    let source = std::fs::read_to_string(&file)?;
    std::fs::write(&file, patch(&source)?)?;
    Ok(())
}
