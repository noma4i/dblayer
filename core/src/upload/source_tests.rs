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

#[tokio::test]
async fn upload_file_reads_do_not_run_on_the_async_executor() {
    use crate::sync::rig::*;
    use crate::sync::{OperationDef, SyncConfig};
    use crate::transport::{MockTransport, Reply};
    use serde_json::json;
    let directory = tempfile::tempdir().unwrap();
    let rig = rig_on(crate::db::Db::open(&directory.path().join("db.sqlite"), specs(), None).unwrap(), MockTransport::new(), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    put(&rig, "moments", moment("m1", "file"));
    rig.sync.register(OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    rig.transport.enqueue("CreateBlob", Reply::data(json!({"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed","blobId":"blob"}})));
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment":{"id":"m1","text":"file"}})));
    let bytes = vec![73; crate::files::TABLE_LIMIT + 1];
    let source = file_with(&bytes);
    let input = UploadFile::new(source.path().to_str().unwrap());
    let row = crate::identity::RowId::new("m1").unwrap();
    let (seq, _, _, _) = (rig.sync.inner.begin_upload(&rig.sync.inner.db.admit().unwrap(), rig.db.generation(), "moments", &row, "upload", &input, UploadState::queued())).unwrap();
    let id = rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT file_id FROM op_item WHERE seq = ?1", [seq], |row| row.get::<_, String>(0))?)).unwrap();
    let path = rig.db.shared.files.path(&id).unwrap();
    READS.get_or_init(Default::default).lock().unwrap().insert(path.clone(), Vec::new());
    let executor = std::thread::current().id();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.db.admit().unwrap())).await.unwrap();
    rig.sync.wait_uploads().await;
    let reads = READS.get().unwrap().lock().unwrap().remove(&path).unwrap();
    assert_eq!(reads.iter().map(|(operation, _)| *operation).collect::<Vec<_>>(), ["metadata", "checksum"]);
    assert!(reads.iter().all(|(_, thread)| *thread != executor), "file reads used the executor {executor:?}: {reads:?}");
    assert_eq!(rig.transport.put_calls()[0].body, bytes);
    assert!(rig.db.outbox().unwrap().is_empty());
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
