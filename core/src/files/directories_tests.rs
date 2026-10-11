use super::*;
use std::fs;

fn beside_database() -> (tempfile::TempDir, FileDirectories) {
    let root = tempfile::tempdir().unwrap();
    let directories = FileDirectories::beside(&root.path().join("42.sqlite"));
    (root, directories)
}

#[test]
fn write_stores_bytes_under_the_id_and_leaves_no_partial_file() {
    let (_root, directories) = beside_database();
    let path = directories.write("f1", b"hello").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"hello");
    assert_eq!(directories.read("f1").unwrap(), b"hello");
    assert_eq!(directories.directory_entries(), vec!["f1"]);
}

#[test]
fn directory_sits_next_to_the_database_file() {
    let (root, directories) = beside_database();
    let path = directories.write("f1", b"x").unwrap();
    assert_eq!(path, root.path().join("42.sqlite.files").join("f1"));
}

#[test]
fn memory_database_has_no_directory_and_cleans_its_scratch_cache_on_drop() {
    let directories = FileDirectories::scratch();
    assert!(!directories.has_directory());
    let path = directories.materialize("f1", "clip.mp4", b"video").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"video");
    assert!(path.ends_with("f1/clip.mp4"));
    drop(directories);
    assert!(!path.exists());
}

#[test]
fn materialize_is_stable_and_strips_path_parts_from_the_name() {
    let (_root, directories) = beside_database();
    let first = directories.materialize("f1", "../../evil.png", b"png").unwrap();
    let second = directories.materialize("f1", "../../evil.png", b"png").unwrap();
    assert_eq!(first, second);
    assert!(first.ends_with("f1/evil.png"));
    let blank = directories.materialize("f2", "", b"x").unwrap();
    assert!(blank.ends_with("f2/file"));
}

#[test]
fn discard_removes_directory_files_and_cache_entries() {
    let (_root, directories) = beside_database();
    directories.write("f1", b"a").unwrap();
    let cached = directories.materialize("f1", "a.bin", b"a").unwrap();
    directories.write("f2", b"b").unwrap();
    directories.discard(&["f1".to_string(), "missing".to_string()]);
    assert_eq!(directories.directory_entries(), vec!["f2"]);
    assert!(!cached.exists());
    assert!(directories.cache_entries().is_empty());
}

#[test]
fn entries_lists_nothing_before_the_directories_exist() {
    let (_root, directories) = beside_database();
    assert!(directories.directory_entries().is_empty());
    assert!(directories.cache_entries().is_empty());
}
