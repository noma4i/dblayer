use std::path::{Path, PathBuf};

const MANIFEST: &str = include_str!("manifest.tsv");
const S1_TOKENS: [&str; 7] = ["logout()", "session_reset()", "login_ios(", "login_android(", ".exit(", "Act::Logout", "Transition::Logout"];

struct Row {
    id: String,
    lane: String,
    test: String,
    s1: String,
}

fn rows() -> Vec<Row> {
    let mut lines = MANIFEST.lines();
    assert_eq!(lines.next(), Some("version\t1"), "the manifest names its version first");
    assert_eq!(lines.next(), Some("id\taliases\tlane\ttest\tstate\ts1\tprovenance"));
    lines.map(|line| {
        let cells: Vec<&str> = line.split('\t').collect();
        assert_eq!(cells.len(), 7, "a manifest row has 7 cells: {line}");
        Row { id: cells[0].into(), lane: cells[2].into(), test: cells[3].into(), s1: cells[5].into() }
    }).collect()
}

fn read_all(dir: &Path, extension: &str) -> String {
    let mut text = String::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path: PathBuf = entry.unwrap().path();
            if path.is_dir() { pending.push(path) } else if path.extension().is_some_and(|found| found == extension) { text.push_str(&std::fs::read_to_string(&path).unwrap()); }
        }
    }
    text
}

fn name_of(test: &str) -> &str { test.rsplit('.').next().unwrap() }

/// The manifest is the fixed list of the sequences for the three lanes: every C01 to C19 has a core row, every row names a test that exists in its lane, and every sequence test of the selftest files is listed.
#[test]
fn the_manifest_lists_every_sequence_and_each_listed_test_exists() {
    let rows = rows();
    for number in 1..=19 {
        let id = format!("C{number:02}");
        assert!(rows.iter().any(|row| row.id == id && row.lane == "core"), "{id} has no core row");
    }
    let root = crate::test_wait::package_dir().join("..");
    let (core, swift, kotlin) = (read_all(&root.join("core/src"), "rs"), read_all(&root.join("swift/Tests"), "swift"), read_all(&root.join("kotlin/src/test"), "kt"));
    for row in &rows {
        let (text, keyword) = match row.lane.as_str() {
            "core" => (&core, "fn"),
            "swift" => (&swift, "func"),
            "kotlin" => (&kotlin, "fun"),
            other => panic!("{} names the lane {other}", row.id),
        };
        assert!(text.contains(&format!("{keyword} {}(", name_of(&row.test))), "{} lists {} which does not exist in the {} lane", row.id, row.test, row.lane);
    }
    let core_only = ["B09", "B11", "B13", "B15"];
    for number in 1..=15 {
        let id = format!("B{number:02}");
        for lane in ["core", "swift", "kotlin"] {
            let present = rows.iter().any(|row| row.id == id && row.lane == lane);
            let required = if id == "B14" { lane == "kotlin" } else { lane == "core" || !core_only.contains(&id.as_str()) };
            assert!(present || !required, "{id} has no {lane} row: a bridge sequence is proved on every lane its public API reaches");
        }
    }
    for id in core_only {
        assert!(rows.iter().any(|row| row.id == id && row.lane == "core" && row.s1.starts_with("core-only:")), "{id} stays in the core lane only and its row must name the core hook it needs");
    }
    let directory = root.join("core/src/ffi/selftest");
    for file in ["app", "bridge", "calls", "effect_rows", "harness", "machine", "recovery", "windows"] {
        let source = std::fs::read_to_string(directory.join(format!("{file}_tests.rs"))).unwrap();
        for (at, _) in source.match_indices("#[tokio::test") {
            let name = source[at..].split("fn ").nth(1).and_then(|rest| rest.split('(').next()).unwrap();
            assert!(rows.iter().any(|row| row.lane == "core" && row.test == name), "{file}: {name} is not in the manifest");
        }
    }
}

/// A sequence whose steps the 6.17.0 decisions change (the data logout leaves, the exit is the session reset) says so in the manifest instead of being fitted to the 6.16.1 behavior.
#[test]
fn a_selftest_that_uses_the_removed_logout_or_the_old_exit_is_marked_as_changing_in_s1() {
    let rows = rows();
    let directory = crate::test_wait::package_dir().join("src/ffi/selftest");
    for file in ["app", "calls", "harness", "machine", "recovery", "windows"] {
        let source = std::fs::read_to_string(directory.join(format!("{file}_tests.rs"))).unwrap();
        let starts: Vec<usize> = source.match_indices("#[tokio::test").map(|(at, _)| at).collect();
        for (index, start) in starts.iter().enumerate() {
            let end = starts.get(index + 1).copied().unwrap_or(source.len());
            let body = &source[*start..end];
            let name = body.split("fn ").nth(1).and_then(|rest| rest.split('(').next()).unwrap();
            if !S1_TOKENS.iter().any(|token| body.contains(token)) { continue; }
            assert!(rows.iter().filter(|row| row.lane == "core" && row.test == name).all(|row| row.s1.starts_with("S1:")), "{file}: {name} uses a lifecycle call that S1 changes and the manifest does not say so");
        }
    }
}
