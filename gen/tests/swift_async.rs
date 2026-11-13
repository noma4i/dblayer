use std::path::PathBuf;
use std::process::Command;

fn fixtures(pattern_dir: &str) -> Vec<PathBuf> {
    let directory = PathBuf::from(".").join("../fixtures").join(pattern_dir);
    let mut files: Vec<PathBuf> = std::fs::read_dir(directory).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    files.sort();
    files
}

fn generate(arguments: &[&str], sources: &[PathBuf]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_dblayer-gen")).args(arguments).args(sources).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn every_generated_swift_entry_to_an_owner_runs_on_the_actor_of_the_caller() {
    let schema = PathBuf::from(".").join("../fixtures/operations/schema.graphql");
    let models = generate(&["swift"], &fixtures(""));
    let operations = generate(&["--schema", schema.to_str().unwrap(), "swift"], &fixtures("operations"));
    for (label, source) in [("models", models), ("operations", operations)] {
        let entries: Vec<&str> = source.lines().filter(|line| line.contains(") async throws") && line.trim_start().starts_with(['p', '@', 'n'])).collect();
        assert!(entries.len() > 10, "{label}: the fixtures declare async entries, found {}", entries.len());
        let local: Vec<&&str> = entries.iter().filter(|line| !line.contains("@concurrent") && !line.contains("nonisolated(nonsending)")).collect();
        assert!(local.is_empty(), "{label}: async entries run on neither the actor of the caller nor off it by declaration: {local:?}");
        let leaves: Vec<&&str> = entries.iter().filter(|line| line.contains("@concurrent") && !line.contains("Edge(")).collect();
        assert!(leaves.is_empty(), "{label}: async entries that reach an owner leave the actor of the caller before the library takes the place: {leaves:?}");
    }
}
