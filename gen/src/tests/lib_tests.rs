use std::fs;
use std::path::PathBuf;

use super::*;

fn fixture_paths() -> Vec<PathBuf> {
    let root = crate::package_dir().join("../fixtures");
    let mut paths: Vec<PathBuf> = fs::read_dir(root).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    paths.sort();
    paths
}

#[test]
fn fixtures_parse_into_models_that_are_valid_for_the_core() {
    let models = parse_files(&fixture_paths()).unwrap();
    assert!(dblayer::schema::validate_models(&models, &[]).is_empty());
}

#[test]
fn kotlin_models_are_immutable() {
    let root = crate::package_dir().join("../fixtures");
    let operation_root = root.join("operations");
    let mut operation_paths: Vec<PathBuf> = fs::read_dir(&operation_root).unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    operation_paths.sort();
    for declarations in [compile_files(&fixture_paths(), None, &[]).unwrap(),
        compile_files(&operation_paths, Some(&operation_root.join("schema.graphql")), &[operation_root.join("mutations"), operation_root.join("queries")]).unwrap()] {
        let kotlin = emit_kotlin(&declarations.models, &declarations.operations, &declarations.inputs);
        let mut annotations = Vec::new();
        let mut classes = 0;
        for line in kotlin.lines().map(str::trim) {
            assert!(!line.split(|character: char| !character.is_alphanumeric() && character != '_').any(|token| token == "var")
                || line.starts_with("var built_ = Query("), "Mutable model field: {line}");
            for mutable in ["MutableList<", "MutableMap<", "MutableSet<", ".add(", ".remove(", ".clear("] {
                assert!(!line.contains(mutable), "Mutable model: {line}");
            }
            if line.starts_with('@') { annotations.push(line); continue; }
            if line.starts_with("data class ") || line.starts_with("sealed class ") || line.starts_with("sealed interface ") {
                assert!(annotations.contains(&"@Immutable"), "Missing @Immutable: {line}");
                classes += 1;
            }
            annotations.clear();
        }
        assert!(classes > 0);
    }
}

#[test]
fn unknown_model_across_files_is_reported_per_file() {
    let directory = tempfile::tempdir().unwrap();
    let only_moment = directory.path().join("moment.dbl");
    fs::copy(crate::package_dir().join("../fixtures/moment.dbl"), &only_moment).unwrap();
    let errors = parse_files(std::slice::from_ref(&only_moment)).unwrap_err();
    assert!(errors.iter().all(|error| error.file == only_moment.display().to_string()));
    let source = fs::read_to_string(&only_moment).unwrap();
    let declarations = source.lines().enumerate().filter(|(_, line)| line.trim_start().starts_with("belongs_to") || line.trim_start().starts_with("has_many"))
        .map(|(line, _)| line + 1).collect::<Vec<_>>();
    assert_eq!(errors.iter().map(|error| error.error.line).collect::<Vec<_>>(), declarations);
}

