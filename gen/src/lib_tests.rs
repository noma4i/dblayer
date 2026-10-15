use std::fs;
use std::path::PathBuf;

use super::*;

fn fixture_paths() -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures");
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
fn unknown_model_across_files_is_reported_per_file() {
    let directory = tempfile::tempdir().unwrap();
    let only_moment = directory.path().join("moment.dbl");
    fs::copy(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures/moment.dbl"), &only_moment).unwrap();
    let errors = parse_files(std::slice::from_ref(&only_moment)).unwrap_err();
    assert!(errors.iter().all(|error| error.file == only_moment.display().to_string()));
    let source = fs::read_to_string(&only_moment).unwrap();
    let declarations = source.lines().enumerate().filter(|(_, line)| line.trim_start().starts_with("belongs_to") || line.trim_start().starts_with("has_many"))
        .map(|(line, _)| line + 1).collect::<Vec<_>>();
    assert_eq!(errors.iter().map(|error| error.error.line).collect::<Vec<_>>(), declarations);
}

#[test]
fn missing_file_is_an_error() {
    let errors = parse_files(&["/nonexistent/none.dbl"]).unwrap_err();
    assert_eq!(errors[0].file, "/nonexistent/none.dbl");
}

#[test]
fn file_error_prints_file_and_line() {
    let error = FileError { file: "a.dbl".into(), error: ParseError::new(4, 2, "boom") };
    assert_eq!(error.to_string(), "a.dbl:4: boom");
}
