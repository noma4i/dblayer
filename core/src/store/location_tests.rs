use super::*;
use std::path::Path;

#[test]
fn database_path_ends_with_directory_and_user_file() {
    let location = StoreLocation::new(Path::new("/base"));
    let path = location.database_path("42").unwrap();
    assert!(path.ends_with("DBlayer/42.sqlite"));
}

#[test]
fn database_path_rejects_ids_that_are_not_file_names() {
    let location = StoreLocation::new(Path::new("/base"));
    for bad in ["", "..", ".", "a/b", "a\\b"] {
        assert!(location.database_path(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn prepare_directory_creates_subdirectory_and_can_run_twice() {
    let base = tempfile::tempdir().unwrap();
    let location = StoreLocation::new(base.path());
    let first = location.prepare_directory().unwrap();
    let second = location.prepare_directory().unwrap();
    assert_eq!(first, second);
    assert!(first.is_dir());
    assert_eq!(first, location.directory());
}

#[test]
fn two_users_get_two_files() {
    let location = StoreLocation::new(Path::new("/base"));
    assert_ne!(location.database_path("a").unwrap(), location.database_path("b").unwrap());
}
