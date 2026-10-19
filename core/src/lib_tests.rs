use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") && !path.to_string_lossy().ends_with("_tests.rs") {
            found.push(path);
        }
    }
}

fn mentions(word: &str) -> Vec<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    files
        .into_iter()
        .filter(|path| fs::read_to_string(path).unwrap().contains(word))
        .map(|path| path.strip_prefix(&root).unwrap().display().to_string())
        .collect()
}

#[test]
fn inv_fresh_09_a_freshness_window_is_a_duration_and_no_policy_table_resolves_names() {
    for word in ["FreshnessTable", "FreshnessPolicy", "UnknownFreshnessPolicy"] {
        assert_eq!(mentions(word), Vec::<String>::new(), "{word} is still defined or used");
    }
}

#[test]
fn user_agent_preserves_application_products() {
    let token = format!("DBlayer/{}", env!("CARGO_PKG_VERSION"));
    for (application, expected) in [(None, token.clone()), (Some(String::new()), token.clone()), (Some("Example/3".into()), format!("Example/3 {token}"))] {
        assert_eq!(crate::ffi::user_agent(application), expected);
    }
}
