//! C4: every DSL key x every fixture unit x platform. A unit is the `.dbl` files of one fixture directory, the models and operations of the consumer app together, with its GraphQL schema where the unit has one. Swift output is parsed with `swiftc -parse`. Kotlin output is compiled by the Gradle module `kotlin/consumer-check` (property `matrixKotlin`). Every DSL key must occur in a fixture, else the generator branch has no fixture row (red). The result goes to `target/matrix/C4-generator.tsv`.
use std::path::{Path, PathBuf};
use std::process::Command;

fn files(directory: &Path, found: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(directory).unwrap().map(|entry| entry.unwrap().path()).collect();
    entries.sort();
    for path in entries {
        if path.is_dir() { files(&path, found); } else if path.extension().is_some_and(|extension| extension == "dbl") { found.push(path); }
    }
}

struct Unit {
    label: String,
    files: Vec<PathBuf>,
    schema: Option<PathBuf>,
}

fn units(root: &Path) -> Vec<Unit> {
    let mut all = Vec::new();
    files(root, &mut all);
    let app = root.join("consumer/app/schemas");
    let operations = root.join("operations");
    let mut by_directory: std::collections::BTreeMap<PathBuf, Vec<PathBuf>> = Default::default();
    for path in all {
        let directory = if path.starts_with(&app) { app.clone() } else { path.parent().unwrap().to_path_buf() };
        by_directory.entry(directory).or_default().push(path);
    }
    by_directory.into_iter().map(|(directory, files)| {
        let schema = if directory == app { Some(root.join("consumer/app/graphql/schema.graphql")) } else if directory == operations { Some(operations.join("schema.graphql")) } else { None };
        let name = directory.strip_prefix(root).unwrap().to_string_lossy().into_owned();
        Unit { label: if name.is_empty() { "fixtures".into() } else { format!("fixtures/{name}") }, files, schema }
    }).collect()
}

fn generate(unit: &Unit, platform: &str) -> Result<String, String> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dblayer-gen"));
    if let Some(schema) = &unit.schema { command.arg("--schema").arg(schema); }
    let output = command.arg(platform).args(&unit.files).output().map_err(|error| error.to_string())?;
    if output.status.success() { Ok(String::from_utf8_lossy(&output.stdout).into_owned()) } else { Err(String::from_utf8_lossy(&output.stderr).lines().next().unwrap_or("").chars().take(160).collect()) }
}

fn gradle() -> Option<PathBuf> {
    let dists = PathBuf::from(std::env::var_os("HOME")?).join(".gradle/wrapper/dists");
    let mut found: Vec<PathBuf> = std::fs::read_dir(dists).ok()?.flatten().filter(|entry| entry.file_name().to_string_lossy().starts_with("gradle-9.")).flat_map(|entry| std::fs::read_dir(entry.path()).into_iter().flatten().flatten()).flat_map(|hash| std::fs::read_dir(hash.path()).into_iter().flatten().flatten()).map(|home| home.path().join("bin/gradle")).filter(|path| path.exists()).collect();
    found.sort();
    found.pop()
}

#[test]
fn generator_matrix_over_every_fixture_unit() {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from).unwrap_or_else(|| std::env::current_dir().unwrap());
    let repository = manifest.join("..");
    let root = repository.join("fixtures");
    let scratch = repository.join("target/matrix/kotlin-src");
    let mut rows: Vec<(String, String, String, String)> = Vec::new();
    let mut sources = String::new();
    for unit in units(&root) {
        for path in &unit.files { sources.push_str(&std::fs::read_to_string(path).unwrap()); sources.push('\n'); }
        let label = unit.label.clone();
        let (swift, kotlin) = (generate(&unit, "swift"), generate(&unit, "kotlin"));
        match (&swift, &kotlin) {
            (Ok(_), Ok(_)) => rows.push((label.clone(), "generate".into(), "Green".into(), format!("{} files{}", unit.files.len(), if unit.schema.is_some() { " with schema" } else { "" }))),
            (Err(error), _) | (_, Err(error)) => { rows.push((label.clone(), "generate".into(), "Red".into(), error.clone())); continue; }
        }
        let swift = swift.unwrap();
        let target = std::env::temp_dir().join(format!("c4-{}.swift", label.replace('/', "-")));
        std::fs::write(&target, &swift).unwrap();
        match Command::new("swiftc").arg("-parse").arg(&target).output() {
            Ok(output) if output.status.success() => rows.push((label.clone(), "swift parses".into(), "Green".into(), format!("{} bytes", swift.len()))),
            Ok(output) => rows.push((label.clone(), "swift parses".into(), "Red".into(), String::from_utf8_lossy(&output.stderr).lines().find(|line| line.contains("error")).unwrap_or("").chars().take(160).collect())),
            Err(error) => rows.push((label.clone(), "swift parses".into(), "Unclassified".into(), error.to_string())),
        }
        let _ = std::fs::remove_file(&target);
        let directory = scratch.join(label.replace('/', "-"));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("Generated.kt"), kotlin.unwrap()).unwrap();
        let verdict = match gradle() {
            None => ("Unclassified", "no Gradle distribution under ~/.gradle/wrapper/dists".to_string()),
            Some(gradle) => match Command::new("timeout").args(["--kill-after=10", "300"]).arg(gradle).arg("-p").arg(repository.join("kotlin")).arg(":consumer-check:compileDebugKotlin").arg(format!("-PmatrixKotlin={}", directory.display())).args(["-Pkotlin.incremental=false", "--console=plain", "--no-watch-fs", "--quiet"]).output() {
                Ok(output) if output.status.success() => ("Green", "compileDebugKotlin".to_string()),
                Ok(output) => ("Red", String::from_utf8_lossy(&output.stderr).lines().find(|line| line.starts_with("e: ")).unwrap_or("gradle failed").chars().take(200).collect()),
                Err(error) => ("Unclassified", error.to_string()),
            },
        };
        rows.push((label, "kotlin compiles".into(), verdict.0.into(), verdict.1));
    }
    for key in ["preload", "subscribe", "root:", "removed_by", "copies_receiver_to", "on_failure", "members_complete", "default:", "null:"] {
        let count = sources.matches(key).count();
        rows.push(("DSL key".into(), key.into(), if count > 0 { "Green" } else { "Red" }.into(), format!("{count} occurrences in the fixtures")));
    }
    let output = repository.join("target/matrix");
    std::fs::create_dir_all(&output).unwrap();
    let mut tsv = String::from("path\tkind\tverdict\tdetail\n");
    for (path, kind, verdict, detail) in &rows { tsv.push_str(&format!("{path}\t{kind}\t{verdict}\t{detail}\n")); }
    std::fs::write(output.join("C4-generator.tsv"), tsv).unwrap();
    let count = |wanted: &str| rows.iter().filter(|row| row.2 == wanted).count();
    println!("matrix C4-generator: rows {} green {} red {} unclassified {};", rows.len(), count("Green"), count("Red"), count("Unclassified"));
    let red: Vec<String> = rows.iter().filter(|row| row.2 == "Red").map(|row| format!("{} {}: {}", row.0, row.1, row.3)).collect();
    assert!(red.is_empty(), "red cells:\n{}", red.join("\n"));
}
