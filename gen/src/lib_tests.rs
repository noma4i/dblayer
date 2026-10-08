use std::fs;
use std::path::PathBuf;

use super::*;

fn fixture_paths() -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures");
    let mut paths: Vec<PathBuf> = fs::read_dir(root).unwrap().map(|entry| entry.unwrap().path()).collect();
    paths.sort();
    paths
}

#[test]
fn fixtures_parse_and_validate_together() {
    let models = parse_files(&fixture_paths()).unwrap();
    let summary: Vec<(&str, usize, usize, usize)> = models
        .iter()
        .map(|model| (model.key.as_str(), model.fields.len(), model.associations.len(), model.relations.len()))
        .collect();
    assert_eq!(
        summary,
        vec![("Chat", 2, 1, 1), ("Message", 4, 1, 2), ("Moment", 6, 2, 3), ("User", 3, 0, 0), ("Vibe", 3, 0, 0)]
    );
}

#[test]
fn fixtures_are_valid_for_the_core() {
    let models = parse_files(&fixture_paths()).unwrap();
    assert!(dblayer::schema::validate_models(&models, &[]).is_empty());
}

#[test]
fn unknown_model_across_files_is_reported_per_file() {
    let directory = tempfile::tempdir().unwrap();
    let only_moment = directory.path().join("moment.dbl");
    fs::copy(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures/moment.dbl"), &only_moment).unwrap();
    let errors = parse_files(&[only_moment.clone()]).unwrap_err();
    assert!(errors.iter().all(|error| error.file == only_moment.display().to_string()));
    assert_eq!(errors.iter().map(|error| error.error.line).collect::<Vec<_>>(), vec![13, 14]);
}

#[test]
fn parse_models_accepts_a_single_source_and_keeps_two_models() {
    let source = "model A do\n  schema do\n    string :x\n  end\nend\n# between\nmodel B do\n  schema do\n    int :y\n  end\nend\n";
    assert_eq!(parse_models(source).unwrap().len(), 2);
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
