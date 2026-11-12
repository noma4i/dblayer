//! Crash images of a running database: the file and its write-ahead log cut at a commit boundary, with the `<database>.files` directory. A killed process leaves one of these.

use std::future::Future;
use std::path::{Path, PathBuf};

use super::rig_tests::bound;

pub(super) fn copy_tree(from: &Path, to: &Path) {
    if !from.exists() { return; }
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() { copy_tree(&entry.path(), &to.join(entry.file_name())); } else { std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap(); }
    }
}

pub(super) fn files_of(database: &Path) -> PathBuf { PathBuf::from(format!("{}.files", database.display())) }

fn wal_of(database: &Path) -> PathBuf { PathBuf::from(format!("{}-wal", database.display())) }

/// The file and the log of a live database, copied while its process runs.
pub(super) struct Capture {
    directory: tempfile::TempDir,
    base: PathBuf,
    wal: Vec<u8>,
    cuts: Vec<usize>,
}

impl Capture {
    /// The state a scenario starts from: the file of the live database as it is now.
    pub(super) fn begin(live: &Path) -> Base {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("base.sqlite");
        std::fs::copy(live, &base).unwrap();
        Base { directory, base }
    }

    /// Every cut of the log at a commit, the empty log first: each is the file a process killed at that commit leaves.
    pub(super) fn cuts(&self) -> usize { self.cuts.len() }

    fn image(&self, index: usize, files: &Path) -> PathBuf {
        let image = self.directory.path().join(format!("image-{index}.sqlite"));
        std::fs::copy(&self.base, &image).unwrap();
        std::fs::write(wal_of(&image), &self.wal[..self.cuts[index]]).unwrap();
        copy_tree(files, &files_of(&image));
        image
    }
}

pub(super) struct Base {
    directory: tempfile::TempDir,
    base: PathBuf,
}

impl Base {
    /// The log of the live database now, with the files directory as it is now.
    pub(super) fn end(self, live: &Path) -> (Capture, PathBuf) {
        let wal = std::fs::read(wal_of(live)).unwrap_or_default();
        let files = self.directory.path().join("files");
        copy_tree(&files_of(live), &files);
        let mut cuts = vec![0];
        if wal.len() >= 32 {
            let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
            let frame = 24 + if page_size == 1 { 65536 } else { page_size };
            cuts.extend((32..wal.len()).step_by(frame).filter(|offset| u32::from_be_bytes(wal[offset + 4..offset + 8].try_into().unwrap()) != 0).map(|offset| offset + frame));
        }
        (Capture { directory: self.directory, base: self.base, wal, cuts }, files)
    }
}

/// Runs the check on every image of the capture, all at once; the results come back in the order of the cuts.
pub(super) async fn over_images<T: Send + 'static, F, Fut>(capture: &Capture, files: &Path, check: F) -> Vec<T> where F: Fn(usize, PathBuf) -> Fut, Fut: Future<Output = T> + Send + 'static {
    let runs: Vec<_> = (0..capture.cuts()).map(|index| crate::task::spawn(check(index, capture.image(index, files)))).collect();
    let mut results = Vec::new();
    for run in runs { results.push(bound(run).await.unwrap()); }
    results
}

/// A fresh process on the image: its own server double, the credential the killed process had saved.
pub(super) async fn reopen(image: &Path, secret: &str, operations: &[(&str, &str)]) -> (super::upload_stand::Stand, std::sync::Arc<crate::ffi::Database>) {
    let mut fresh = super::upload_stand::Stand::new();
    fresh.rig.credentials = std::sync::Arc::new(crate::ffi::tests::session_guards_tests::WatchedHeld::default());
    crate::ffi::CredentialStore::set(fresh.rig.credentials.as_ref(), secret.to_string()).unwrap();
    let database = fresh.open(Some(image), operations).await;
    (fresh, database)
}

/// The operations the image saved: (all, with a saved server answer).
pub(super) fn saved_work(image: &Path) -> (i64, i64) {
    let connection = rusqlite::Connection::open(image).unwrap();
    let count = |table: &str| connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row.get::<_, i64>(0)).unwrap();
    (count("op"), count("encrypted_answer"))
}

/// The length of the operation queue as the reader sees it now.
pub(super) async fn queue_now(database: &crate::ffi::Database) -> usize {
    let stream = database.subscribe_values(database.session_epoch(), crate::ffi::ReadRequest::Operations);
    match bound(stream.pull_next()).await {
        Some(crate::ffi::ReadValue::Operations { items, .. }) => items.len(),
        other => panic!("the operation queue ended: {other:?}"),
    }
}

/// Waits for the queue event that shows it empty.
pub(super) async fn drained(database: &crate::ffi::Database) {
    let stream = database.subscribe_values(database.session_epoch(), crate::ffi::ReadRequest::Operations);
    loop {
        match bound(stream.pull_next()).await {
            Some(crate::ffi::ReadValue::Operations { items, .. }) if items.is_empty() => return,
            Some(crate::ffi::ReadValue::Operations { .. }) => {}
            other => panic!("the operation queue ended: {other:?}"),
        }
    }
}
