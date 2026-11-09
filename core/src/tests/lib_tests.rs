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
    let root = crate::test_wait::package_dir().join("src");
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
        assert!(crate::runtime::parse_duration(name).is_none(), "{name} resolved as a freshness policy");
    }
    let mut declared = Vec::new();
    declared_freshness(&crate::test_wait::package_dir().join("..").join("fixtures"), &mut declared);
    assert!(declared.len() >= 2, "no freshness declarations found: {declared:?}");
    for (place, value) in declared {
        assert!(crate::runtime::parse_duration(&value).is_some(), "{place} is not a duration: {value:?}");
    }
}

#[test]
fn inv_time_01_only_the_clock_module_and_the_stress_rigs_read_the_wall_clock() {
    let allowed = ["runtime/threaded.rs", "runtime/published.rs", "spec/rig.rs", "owners/read/rig.rs", "runtime/clock.rs", "runtime/test_clock.rs", "bin/stress.rs", "bin/rowbench.rs", "sync/stress/mod.rs", "sync/stress/watchdog.rs", "test_wait.rs"];
    for word in ["SystemTime::now", "Instant::now", "OffsetDateTime::now"] {
        let strays: Vec<String> = mentions(word).into_iter().filter(|file| !allowed.contains(&file.as_str())).collect();
        assert_eq!(strays, Vec::<String>::new(), "{word} reads the wall clock outside the clock module");
    }
    let defaults: Vec<String> = mentions("SystemClock").into_iter().filter(|file| !file.starts_with("spec/") && !["runtime/clock.rs", "runtime/mod.rs", "store/mod.rs", "db/ephemeral.rs", "ffi/database.rs", "ffi/encrypted.rs"].contains(&file.as_str())).collect();
    assert_eq!(defaults, Vec::<String>::new(), "a path builds its own system clock instead of taking the one given at open");
}

#[test]
fn inv_zoo_01_a_public_type_name_is_defined_once_in_core() {
    let root = crate::test_wait::package_dir().join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let mut places: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for path in files.into_iter().filter(|path| !path.starts_with(root.join("spec"))) {
        for line in fs::read_to_string(&path).unwrap().lines() {
            let line = line.trim_start();
            let Some(rest) = ["pub struct ", "pub enum ", "pub trait ", "pub type "].iter().find_map(|prefix| line.strip_prefix(prefix)) else { continue };
            let name: String = rest.chars().take_while(|character| character.is_alphanumeric()).collect();
            if name.chars().next().is_some_and(char::is_uppercase) {
                places.entry(name).or_default().push(path.strip_prefix(&root).unwrap().display().to_string());
            }
        }
    }
    let allowed: Vec<String> = fs::read_to_string(crate::test_wait::package_dir().join("..").join("scripts").join("zoo_allowlist.txt"))
        .unwrap()
        .lines()
        .filter_map(|line| line.split('#').next()?.trim().strip_prefix("type:").map(str::to_string))
        .collect();
    let duplicated: Vec<_> = places.iter().filter(|(name, found)| found.len() > 1 && !allowed.contains(name)).collect();
    assert!(duplicated.is_empty(), "public types defined in several places: {duplicated:?}");
}

#[test]
fn user_agent_preserves_application_products() {
    let token = format!("DBlayer/{}", env!("CARGO_PKG_VERSION"));
    for (application, expected) in [(None, token.clone()), (Some(String::new()), token.clone()), (Some("Example/3".into()), format!("Example/3 {token}"))] {
        assert_eq!(crate::ffi::user_agent(application), expected);
    }
}

#[test]
fn rt_prop_no_owner_delay() {
    let owners = crate::test_wait::package_dir().join("src").join("owners");
    let mut files = Vec::new();
    rust_files(&owners, &mut files);
    let mut strays: Vec<String> = Vec::new();
    for path in &files {
        let text = fs::read_to_string(path).unwrap();
        let name = path.strip_prefix(&owners).unwrap().display().to_string();
        for needle in [".delay(", "thread_rng", "StdRng", "SmallRng", "gen_range", "OsRng", "fastrand", "process_seed"] {
            if text.contains(needle) {
                strays.push(format!("{name}: {needle}"));
            }
        }
    }
    strays.sort();
    assert_eq!(strays, Vec::<String>::new(), "an owner computes a retry delay or draws randomness instead of sending Effect::Backoff{{policy, attempt}}");
}

#[test]
fn xfer_source_one_representation() {
    let root = crate::test_wait::package_dir().join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let mut strays: Vec<String> = Vec::new();
    for path in &files {
        let name = path.strip_prefix(&root).unwrap().display().to_string();
        if name == "ffi/upload_wire.rs" {
            continue;
        }
        let lines: Vec<String> = fs::read_to_string(path).unwrap().lines().map(str::to_string).collect();
        for (index, line) in lines.iter().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("path: Option<String>") || trimmed.starts_with("path: String") {
                let window = lines[index..(index + 5).min(lines.len())].join("\n");
                if window.contains("name: Option<String>") && window.contains("mime: Option<String>") {
                    strays.push(name.clone());
                }
            }
        }
    }
    strays.sort();
    strays.dedup();
    assert_eq!(strays, Vec::<String>::new(), "the upload source triple (path, name, mime) is declared outside ffi/upload_wire.rs; the source is one WireSource value to the owner (D-199)");
}
