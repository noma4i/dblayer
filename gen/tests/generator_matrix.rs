//! C4: every DSL key x every fixture unit x platform. A unit is the top-level fixtures together, each directory of fixtures together, and the models of the app. Swift output is parsed with `swiftc -parse`. Kotlin output is generated, and `kotlinc` is absent on this host, so its compile cells are unclassified. Every DSL key must occur in a fixture, else the generator branch has no fixture row (red). The result goes to `.orca-artifacts/matrix/C4-generator.tsv`.
use std::path::{Path, PathBuf};
use std::process::Command;

use dblayer_gen::{emit_kotlin, emit_swift, parse_files, SchemaTypes};

fn files(directory: &Path, found: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(directory).unwrap().map(|entry| entry.unwrap().path()).collect();
    entries.sort();
    for path in entries {
        if path.is_dir() { files(&path, found); } else if path.extension().is_some_and(|extension| extension == "dbl") { found.push(path); }
    }
}

fn units(root: &Path) -> Vec<(String, Vec<PathBuf>)> {
    let mut all = Vec::new();
    files(root, &mut all);
    let mut by_directory: std::collections::BTreeMap<PathBuf, Vec<PathBuf>> = Default::default();
    for path in all { by_directory.entry(path.parent().unwrap().to_path_buf()).or_default().push(path); }
    by_directory.into_iter().map(|(directory, list)| (directory.strip_prefix(root).unwrap().to_string_lossy().into_owned(), list)).collect()
}

#[test]
fn generator_matrix_over_every_fixture_unit() {
    let root = std::env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from).unwrap_or_else(|| std::env::current_dir().unwrap()).join("../fixtures");
    let mut rows: Vec<(String, String, String, String)> = Vec::new();
    let mut sources = String::new();
    for (name, list) in units(&root) {
        for path in &list { sources.push_str(&std::fs::read_to_string(path).unwrap()); sources.push('\n'); }
        let label = if name.is_empty() { "fixtures".to_string() } else { format!("fixtures/{name}") };
        let models = match parse_files(&list) {
            Ok(models) => models,
            Err(errors) if errors.iter().any(|error| { let text = error.to_string(); text.contains("names the schema") || text.contains("require --schema") }) => { rows.push((label, "parse".into(), "Unclassified".into(), "the unit needs a GraphQL schema (--schema): the cell is driven by the schema_cli test, not here".into())); continue; }
            Err(errors) => { rows.push((label, "parse".into(), "Red".into(), format!("{} errors, first: {:?}", errors.len(), errors.first().map(|error| error.to_string()).unwrap_or_default().chars().take(120).collect::<String>()))); continue; }
        };
        rows.push((label.clone(), "parse".into(), "Green".into(), format!("{} models", models.len())));
        let swift = emit_swift(&models, &[], &SchemaTypes::default());
        let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
        rows.push((label.clone(), "kotlin generates".into(), if kotlin.is_empty() { "Red" } else { "Green" }.into(), format!("{} bytes", kotlin.len())));
        rows.push((label.clone(), "kotlin compiles".into(), "Unclassified".into(), "kotlinc is not installed on this host".into()));
        let target = std::env::temp_dir().join(format!("c4-{}.swift", label.replace('/', "-")));
        std::fs::write(&target, &swift).unwrap();
        match Command::new("swiftc").arg("-parse").arg(&target).output() {
            Ok(output) if output.status.success() => rows.push((label.clone(), "swift parses".into(), "Green".into(), format!("{} bytes", swift.len()))),
            Ok(output) => rows.push((label.clone(), "swift parses".into(), "Red".into(), String::from_utf8_lossy(&output.stderr).lines().find(|line| line.contains("error")).unwrap_or("").chars().take(160).collect())),
            Err(error) => rows.push((label.clone(), "swift parses".into(), "Unclassified".into(), error.to_string())),
        }
        let _ = std::fs::remove_file(&target);
    }
    for key in ["preload", "subscribe", "root:", "removed_by", "copies_receiver_to", "on_failure", "members_complete", "default:", "null:"] {
        let count = sources.matches(key).count();
        rows.push(("DSL key".into(), key.into(), if count > 0 { "Green" } else { "Red" }.into(), format!("{count} occurrences in the fixtures")));
    }
    let output = root.join("../.orca-artifacts/matrix");
    std::fs::create_dir_all(&output).unwrap();
    let mut tsv = String::from("path\tkind\tverdict\tdetail\n");
    for (path, kind, verdict, detail) in &rows { tsv.push_str(&format!("{path}\t{kind}\t{verdict}\t{detail}\n")); }
    std::fs::write(output.join("C4-generator.tsv"), tsv).unwrap();
    let count = |wanted: &str| rows.iter().filter(|row| row.2 == wanted).count();
    println!("matrix C4-generator: rows {} green {} red {} unclassified {};", rows.len(), count("Green"), count("Red"), count("Unclassified"));
    let red: Vec<String> = rows.iter().filter(|row| row.2 == "Red").map(|row| format!("{} {}: {}", row.0, row.1, row.3)).collect();
    assert!(red.is_empty(), "red cells:\n{}", red.join("\n"));
}
