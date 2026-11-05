use std::io::Write;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::thread::ThreadId;

use super::*;

type Reads = HashMap<PathBuf, Vec<(&'static str, ThreadId)>>;
static READS: OnceLock<Mutex<Reads>> = OnceLock::new();

pub(super) fn record_read(path: &Path, operation: &'static str) {
    if let Some(reads) = READS.get() {
        if let Some(events) = reads.lock().unwrap().get_mut(path) {
            events.push((operation, std::thread::current().id()));
        }
    }
}

fn file_with(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(bytes).unwrap();
    file
}

#[tokio::test]
async fn ranges_preserve_offsets_and_reject_truncation() {
    let file = file_with(b"0123456789");
    let cancel = crate::transport::CancelToken::new();
    assert_eq!(file_len(file.path(), &cancel).await.unwrap(), 10);
    let directory = tempfile::tempdir().unwrap();
    let copied = |name: &str, offset, length| {
        let target = directory.path().join(name);
        let cancel = cancel.clone();
        let source = file.path().to_path_buf();
        async move { copy_range(&source, offset, length, &target, &cancel).await.map(|()| std::fs::read(&target).unwrap()) }
    };
    assert_eq!(copied("a", 3, 4).await.unwrap(), b"3456");
    assert_eq!(copied("b", 8, 2).await.unwrap(), b"89");
    assert!(copied("c", 8, 5).await.is_err());
    assert!(!directory.path().join("c").exists() && !directory.path().join("c.partial").exists());
}

#[tokio::test]
async fn checksum_is_base64_md5() {
    let file = file_with(b"hello");
    assert_eq!(md5_base64(file.path(), &crate::transport::CancelToken::new()).await.unwrap(), "XUFAKrxLKna5cZ2REBfFkg==");
}
