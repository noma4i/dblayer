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

fn declared_freshness(dir: &Path, found: &mut Vec<(String, String)>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            declared_freshness(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "dbl") {
            for line in fs::read_to_string(&path).unwrap().lines().filter(|line| line.trim_start().starts_with("freshness")) {
                for part in line.trim_start().trim_start_matches("freshness").split(',') {
                    let (key, value) = part.split_once(':').unwrap_or((part, ""));
                    found.push((format!("{}: {}", path.display(), key.trim()), value.trim().to_string()));
                }
            }
        }
    }
}

#[test]
fn inv_fresh_09_a_freshness_window_is_a_duration_and_no_policy_table_resolves_names() {
    for word in ["FreshnessTable", "FreshnessPolicy", "UnknownFreshnessPolicy"] {
        assert_eq!(mentions(word), Vec::<String>::new(), "{word} is still defined or used");
    }
    for name in ["default", "short", "5minutes", "expires_in"] {
        assert!(crate::pacing::parse_duration(name).is_none(), "{name} resolved as a freshness policy");
    }
    let mut declared = Vec::new();
    declared_freshness(&Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("fixtures"), &mut declared);
    assert!(declared.len() >= 2, "no freshness declarations found: {declared:?}");
    for (place, value) in declared {
        assert!(crate::pacing::parse_duration(&value).is_some(), "{place} is not a duration: {value:?}");
    }
}

#[test]
fn user_agent_preserves_application_products() {
    let token = format!("DBlayer/{}", env!("CARGO_PKG_VERSION"));
    for (application, expected) in [(None, token.clone()), (Some(String::new()), token.clone()), (Some("Example/3".into()), format!("Example/3 {token}"))] {
        assert_eq!(crate::ffi::user_agent(application), expected);
    }
}
