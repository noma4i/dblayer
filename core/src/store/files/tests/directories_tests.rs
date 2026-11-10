use super::*;
use std::fs;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;
use crate::test_wait::join_within;

type FileIo = HashMap<String, Vec<(&'static str, ThreadId)>>;
static FILE_IO: OnceLock<Mutex<FileIo>> = OnceLock::new();

pub(in super::super) fn record_io(id: &str, operation: &'static str) {
    if let Some(events) = FILE_IO.get() {
        if let Some(events) = events.lock().unwrap().get_mut(id) { events.push((operation, std::thread::current().id())); }
    }
}


fn beside_database() -> (tempfile::TempDir, FileDirectories) {
    let root = tempfile::tempdir().unwrap();
    let directories = FileDirectories::beside(&root.path().join("42.sqlite"));
    (root, directories)
}

#[test]
fn a_write_keeps_the_bytes_under_the_id_and_leaves_no_partial_file() {
    let (_root, directories) = beside_database();
    let path = directories.write("f1", b"hello").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"hello");
    assert_eq!(fs::read(directories.path("f1").unwrap()).unwrap(), b"hello");
    assert_eq!(directories.directory_entries().unwrap(), vec!["f1"]);
}

#[test]
fn directory_sits_next_to_the_database_file() {
    let (root, directories) = beside_database();
    let path = directories.write("f1", b"x").unwrap();
    assert_eq!(path, root.path().join("42.sqlite.files").join("f1"));
}

#[test]
fn memory_database_keeps_files_in_a_scratch_directory_and_removes_it_on_drop() {
    let directories = FileDirectories::scratch();
    assert!(directories.has_directory());
    let path = directories.materialize("f1", "clip.mp4", b"video").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"video");
    assert!(path.ends_with("f1"));
    drop(directories);
    assert!(!path.exists());
}

#[test]
fn materialize_is_stable_for_each_id() {
    let (_root, directories) = beside_database();
    let first = directories.materialize("f1", "../../evil.png", b"png").unwrap();
    let second = directories.materialize("f1", "../../evil.png", b"png").unwrap();
    assert_eq!(first, second);
    assert!(first.ends_with("cache/f1"));
    let blank = directories.materialize("f2", "", b"x").unwrap();
    assert!(blank.ends_with("cache/f2"));
}

#[test]
fn discard_removes_directory_files_and_cache_entries() {
    let (_root, directories) = beside_database();
    directories.write("f1", b"a").unwrap();
    let cached = directories.materialize("f1", "a.bin", b"a").unwrap();
    directories.write("f2", b"b").unwrap();
    directories.discard(&["f1".to_string(), "missing".to_string()]).unwrap();
    assert_eq!(directories.directory_entries().unwrap(), vec!["f2"]);
    assert!(!cached.exists());
    assert!(directories.cache_entries().unwrap().is_empty());
}

#[test]
fn entries_lists_nothing_before_the_directories_exist() {
    let (_root, directories) = beside_database();
    assert!(directories.directory_entries().unwrap().is_empty());
    assert!(directories.cache_entries().unwrap().is_empty());
}

#[test]
fn concurrent_materializations_of_one_file_all_succeed_with_the_same_bytes() {
    let (_root, directories) = beside_database();
    let directories = std::sync::Arc::new(directories);
    for round in 0..40 {
        let id = format!("f{round}");
        let barrier = std::sync::Arc::new(crate::test_wait::Rendezvous::new(6));
        let workers: Vec<_> = (0..6).map(|_| {
            let (directories, barrier, id) = (directories.clone(), barrier.clone(), id.clone());
            std::thread::spawn(move || { barrier.meet(); directories.materialize(&id, "clip.mp4", b"abcdef") })
        }).collect();
        for worker in workers {
            let path = join_within(worker).unwrap();
            assert_eq!(fs::read(path).unwrap(), b"abcdef", "round {round}");
        }
    }
}

#[test]
fn mem_upload_copy_is_chunked() {
    const CHUNK: usize = 1024 * 1024;
    const SOURCE_BYTES: u64 = 32 * CHUNK as u64;
    let (root, directories) = beside_database();
    let source = root.path().join("source");
    let source_file = std::fs::File::create(&source).unwrap();
    source_file.set_len(SOURCE_BYTES).unwrap();
    drop(source_file);
    let (result, peak) = crate::tests::peak_tests::measure_peak(|| {
        directories.copy_source("upload", &source).unwrap()
    });
    match result {
        super::super::directories::CopiedSource::Ready { size, .. } => assert_eq!(size, SOURCE_BYTES),
        super::super::directories::CopiedSource::Unreadable(error) => panic!("{error}"),
    }
    assert!(peak < 2 * CHUNK, "a source of 32 MiB is copied with one chunk and the table limit in memory: peak={peak}");
}

#[test]
fn mem_file_attach_is_chunked() {
    const CHUNK: usize = 1024 * 1024;
    const SIZE: u64 = 32 * CHUNK as u64;
    let (root, directories) = beside_database();
    let source = root.path().join("source");
    let file = fs::File::create(&source).unwrap();
    file.set_len(SIZE).unwrap();
    drop(file);
    let (copy, peak) = crate::tests::peak_tests::measure_peak(|| {
        crate::store::files::copy_attachment(&directories, &source).unwrap()
    });
    assert_eq!(copy.size, SIZE);
    assert!(peak < 2 * CHUNK, "copy peak is {peak}");
    let partial = directories.path(&copy.id).unwrap().with_extension("partial");
    let copied = fs::read(partial).unwrap();
    assert_eq!(copied.len() as u64, SIZE);
    assert_eq!(copy.sha256, crate::store::files::sha256_hex(&copied));
}
