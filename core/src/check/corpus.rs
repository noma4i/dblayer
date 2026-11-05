use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use super::value::{Step, Trace};

/// Parses a corpus file: line 1 is `{"trace":"<name>"}`, every next non-empty line is one step.
pub fn parse(text: &str) -> Result<Trace, String> {
    let mut lines = text.lines().enumerate().filter(|(_, line)| !line.trim().is_empty());
    let (_, header) = lines.next().ok_or("the corpus file is empty")?;
    let header: Value = serde_json::from_str(header).map_err(|error| format!("line 1: {error}"))?;
    let name = header.get("trace").and_then(Value::as_str).ok_or("line 1: the header needs a trace name")?.to_string();
    let mut steps = Vec::new();
    for (index, line) in lines {
        let step: Step = serde_json::from_str(line).map_err(|error| format!("line {}: {error}", index + 1))?;
        steps.push(step);
    }
    Ok(Trace { name, steps })
}

pub fn render(trace: &Trace) -> String {
    let mut text = json!({ "trace": trace.name }).to_string();
    text.push('\n');
    for step in &trace.steps {
        text.push_str(&serde_json::to_string(step).unwrap_or_default());
        text.push('\n');
    }
    text
}

pub fn read(path: &Path) -> Result<Trace, String> {
    parse(&std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?)
}

/// Writes `<dir>/<trace name>.jsonl` and returns the path.
pub fn write(dir: &Path, trace: &Trace) -> Result<PathBuf, String> {
    std::fs::create_dir_all(dir).map_err(|error| error.to_string())?;
    let path = dir.join(format!("{}.jsonl", trace.name));
    std::fs::write(&path, render(trace)).map_err(|error| error.to_string())?;
    Ok(path)
}

/// Every `*.jsonl` trace of a corpus directory, sorted by file name.
pub fn read_dir(dir: &Path) -> Result<Vec<Trace>, String> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir).map_err(|error| format!("{}: {error}", dir.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|extension| extension == "jsonl"))
        .collect();
    paths.sort();
    paths.iter().map(|path| read(path)).collect()
}

/// The corpus directory shared with the Swift and Kotlin runners.
pub fn shared_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}
