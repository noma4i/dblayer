//! C9: static checks of what a run cannot see, read from the source of the core: an exported field that no code reads, an atomic in the bridge (owner state is written in `step` only), a timer or a spawn outside `task.rs` and the runtime.
use std::path::{Path, PathBuf};

use crate::test_matrix::{crate_dir, Matrix};

fn sources(directory: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
            if matches!(name.as_str(), "tests" | "selftest" | "spec" | "check" | "bin") { continue; }
            sources(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let text = path.to_string_lossy();
            if !text.ends_with("_tests.rs") && !text.ends_with("test_wait.rs") && !text.ends_with("test_matrix.rs") { found.push(path); }
        }
    }
}

fn production() -> Vec<(String, String)> {
    let root = crate_dir().join("src");
    let mut found = Vec::new();
    sources(&root, &mut found);
    found.into_iter().filter_map(|path| Some((path.strip_prefix(&root).ok()?.to_string_lossy().into_owned(), std::fs::read_to_string(&path).ok()?))).collect()
}

/// The `pub` fields of the records the bridge exports.
fn exported_fields(files: &[(String, String)]) -> Vec<(String, String, String)> {
    let mut fields = Vec::new();
    for (path, text) in files {
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains("uniffi::Record") { continue; }
            let Some(head) = lines[index..].iter().take(4).find(|line| line.trim_start().starts_with("pub struct ")) else { continue };
            let name: String = head.trim_start()["pub struct ".len()..].chars().take_while(|character| character.is_alphanumeric() || *character == '_').collect();
            const INPUTS: [&str; 10] = ["Request", "Config", "Source", "Item", "Command", "Spec", "Target", "Ports", "Batch", "Declaration"];
            if !INPUTS.iter().any(|suffix| name.ends_with(suffix)) { continue; }
            if head.contains('}') { continue; }
            let start = index + lines[index..].iter().position(|line| line.contains(" {")).unwrap_or(0) + 1;
            for field in lines[start..].iter().take_while(|line| line.trim() != "}" && !line.trim_start().starts_with("pub struct ") && !line.trim_start().starts_with("fn ")) {
                if let Some(rest) = field.trim().strip_prefix("pub ") {
                    if let Some((field_name, _)) = rest.split_once(':') { fields.push((path.clone(), name.clone(), field_name.trim().to_string())); }
                }
            }
        }
    }
    fields
}

/// The module files gated by `#[cfg(test)]` at their `mod` declaration in a `mod.rs` or `lib.rs`: test-only support the static scan excludes the same way it excludes `tests/`.
fn test_gated_modules(files: &[(String, String)]) -> std::collections::HashSet<String> {
    let mut gated = std::collections::HashSet::new();
    for (path, text) in files {
        if !path.ends_with("mod.rs") && !path.ends_with("lib.rs") { continue; }
        let parent = path.rsplit_once('/').map_or("", |(directory, _)| directory);
        let lines: Vec<&str> = text.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if line.trim() != "#[cfg(test)]" { continue; }
            let Some(next) = lines.get(index + 1) else { continue };
            let declaration = next.trim().trim_start_matches("pub(crate) ").trim_start_matches("pub(super) ").trim_start_matches("pub ");
            let Some(rest) = declaration.strip_prefix("mod ") else { continue };
            let name: String = rest.chars().take_while(|character| character.is_alphanumeric() || *character == '_').collect();
            if name.is_empty() { continue; }
            gated.insert(if parent.is_empty() { name } else { format!("{parent}/{name}") });
        }
    }
    gated
}

fn is_test_gated(path: &str, gated: &std::collections::HashSet<String>) -> bool {
    let stem = path.strip_suffix(".rs").unwrap_or(path);
    let stem = stem.strip_suffix("/mod").unwrap_or(stem);
    gated.contains(stem) || gated.iter().any(|module| stem.starts_with(&format!("{module}/")))
}

// bridge.md: HttpRequest and PutTarget flow to the platform Transport (send/put contract, timeouts line); ServerConfig is the result of fetch_server_config returned to the app.
const PLATFORM_OR_APP_READ: [(&str, &[&str]); 3] = [
    ("HttpRequest", &["method", "connect_ms", "request_ms"]),
    ("PutTarget", &["connect_ms", "request_ms"]),
    ("ServerConfig", &[]),
];

#[test]
fn matrix_static_checks() {
    let mut matrix = Matrix::new("C9-static");
    let files = production();
    if files.len() < 100 { matrix.unclassified("core/src", "-", "-", &format!("the scan found {} files", files.len())); }
    let gated = test_gated_modules(&files);

    // an exported field that no code reads, unless the platform or the app reads it (bridge.md)
    for (path, record, field) in exported_fields(&files) {
        if let Some((_, exempt)) = PLATFORM_OR_APP_READ.iter().find(|(name, _)| *name == record.as_str()) {
            if exempt.is_empty() || exempt.contains(&field.as_str()) {
                matrix.check(&format!("{record}.{field}"), "exported field read by the platform or the app", &path, "exempt", "exempt");
                continue;
            }
        }
        let access = format!(".{field}");
        let read = files.iter().any(|(_, text)| text.match_indices(&access).any(|(at, found)| {
            let next = text[at + found.len()..].chars().next();
            let writes = text[at + found.len()..].trim_start().starts_with("= ") && !text[at + found.len()..].trim_start().starts_with("== ");
            next.is_none_or(|next| !(next.is_alphanumeric() || next == '_')) && !writes
        })) || files.iter().any(|(_, text)| text.contains(&format!("{record} {{")) && text.contains(&format!("{field}, ")) && text.contains(&format!("let {record} {{")))
            || files.iter().any(|(_, text)| text.contains(&format!("{field}: _")) || text.contains(&format!("{{ {field}, ")) || text.contains(&format!(", {field} }}")) || text.contains(&format!(", {field},")));
        matrix.check(&format!("{record}.{field}"), "exported field is read by the core", &path, "read", if read { "read" } else { "no read of the field in the core" });
    }

    // atomics in the bridge: the state of an owner is written in `step` only
    for (path, text) in files.iter().filter(|(path, _)| path.starts_with("ffi/") && !is_test_gated(path, &gated)) {
        let count = text.matches("Atomic").count();
        matrix.check(path, "no atomics in the bridge", "count of `Atomic`", "0", &count.to_string());
    }

    // spawns and timers only through task.rs and the runtime
    for (path, text) in &files {
        if path == "task.rs" || path.starts_with("runtime/") || is_test_gated(path, &gated) { continue; }
        let (runtime, time, thread) = ("tokio", "time", "thread");
        let needles = [format!("{runtime}::spawn("), format!("std::{thread}::spawn("), format!("{runtime}::{time}::{}(", "sleep"), format!("{runtime}::{time}::{}(", "timeout"), format!("{runtime}::{time}::{}(", "interval"), format!("{thread}::{}(", "sleep")];
        for needle in needles.iter().map(String::as_str) {
            let count = text.lines().filter(|line| !line.trim_start().starts_with("//") && line.contains(needle)).count();
            matrix.check(path, &format!("no `{needle}` outside task.rs and the runtime"), "count", "0", &count.to_string());
        }
    }

    // the removed-symbol table lives in the local machine spec directory: the coordinator runs the rg when that directory is next to the crate, skipped when it is not
    if crate_dir().join("../specs").is_dir() {
        matrix.check("removed-symbol table", "specs directory present", "rg of the symbols the spec removed", "run by the coordinator", "run by the coordinator");
    } else {
        matrix.check("removed-symbol table", "spec directory absent", "the machine spec directory is local only, not next to the crate", "skipped", "skipped");
    }
    matrix.finish();
}
