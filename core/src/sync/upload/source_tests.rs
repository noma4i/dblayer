use serde_json::json;

use crate::db::Db;
use crate::sync::rig::*;
use crate::sync::{OperationDef, SyncConfig};
use crate::transport::{MockTransport, Reply};
use crate::upload::{UploadBatchInput, UploadItem, UploadSteps};

use super::upload_tests::{blob, created, input, rid};

#[tokio::test(start_paused = true)]
async fn local_models_reject_uploads_without_creating_rows_or_files() {
    let directory = tempfile::tempdir().unwrap();
    let mut model = crate::schema::ModelSpec::new("moments", vec![
        crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id),
        crate::schema::FieldSpec::scalar("text", crate::value::ScalarKind::Str),
    ]);
    model.storage = crate::schema::Storage::Local;
    let rig = rig_on(Db::open(&directory.path().join("db.sqlite"), vec![model], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.set_online(false);
    let source = super::upload_tests::file(&vec![73; crate::files::TABLE_LIMIT + 1]);
    let result = rig.sync.upload_create("moments", "upload", params(json!({"id":"local-upload","text":"rejected"})), input(&source)).await;
    assert!(result.is_err());
    assert!(rig.db.find("moments", &rid("local-upload")).unwrap().is_none());
    assert!(rig.db.shared.files.directory_entries().unwrap().is_empty());
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[test]
fn reset_rejects_upload_admission_waiting_for_a_blocking_thread() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().max_blocking_threads(1).build().unwrap();
    runtime.block_on(async {
        let rig = super::upload_tests::upload_rig(super::upload_tests::small());
        rig.sync.set_online(false);
        let source = super::upload_tests::file(b"source");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let occupied = tokio::task::spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let mut upload = Box::pin(rig.sync.upload_create("moments", "upload", params(json!({"id":"old-upload","text":"old account"})), input(&source)));
        let pending = matches!(upload.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Pending);
        rig.db.reset_all().unwrap();
        release_tx.send(()).unwrap();
        occupied.await.unwrap();
        let result = upload.await;
        assert!(pending);
        assert!(result.is_err(), "pre-reset upload was admitted: {result:?}");
        assert!(rig.db.find("moments", &rid("old-upload")).unwrap().is_none());
        assert!(rig.db.outbox().unwrap().is_empty());
    });
}

#[tokio::test(start_paused = true)]
async fn refused_admission_removes_only_its_prepared_files() {
    for mode in ["existing", "create", "batch"] {
        let directory = tempfile::tempdir().unwrap();
        let mut models = specs();
        models.push(super::batch_tests::spec());
        let rig = rig_on(Db::open(&directory.path().join("db.sqlite"), models, None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.set_online(false);
        rig.db.shared.files.write("kept", b"keep").unwrap();
        rig.db.execute_for_test("CREATE TRIGGER refuse BEFORE INSERT ON op BEGIN SELECT RAISE(ABORT, 'refused admission'); END");
        let source = super::upload_tests::file(&vec![73; crate::files::TABLE_LIMIT + 1]);
        assert!(admit(&rig, mode, &source).await.unwrap_err().to_string().contains("refused admission"), "{mode}");
        assert_eq!(rig.db.shared.files.directory_entries().unwrap(), vec!["kept"], "{mode}");
        assert_eq!(rig.db.shared.files.read("kept").unwrap(), b"keep");
        assert!(rig.db.outbox().unwrap().is_empty(), "{mode}");
    }
}

async fn admit(rig: &Rig, mode: &str, source: &tempfile::NamedTempFile) -> Result<usize, crate::sync::SyncError> {
    Ok(match mode {
        "existing" => {
            put(rig, "moments", moment("m1", "existing"));
            rig.sync.upload("moments", &rid("m1"), "upload", input(source)).await?;
            1
        }
        "create" => {
            rig.sync.upload_create("moments", "upload", params(json!({"text":"created"})), input(source)).await?;
            1
        }
        _ => {
            let items = (0..2).map(|position| UploadItem {
                values: params(json!({"clientKey":format!("key-{position}"),"size":2})),
                path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
            }).collect();
            rig.sync.upload_create_batch("Asset", items, UploadBatchInput {
                operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default(),
            }).await?;
            2
        }
    })
}

#[tokio::test(start_paused = true)]
async fn admitted_large_files_live_on_disk_and_resume_without_the_source() {
    for mode in ["existing", "create", "batch"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let bytes = vec![73; crate::files::TABLE_LIMIT + 1];
        let source = super::upload_tests::file(&bytes);
        let source_path = source.path().to_path_buf();
        let mut models = specs();
        models.push(super::batch_tests::spec());
        let rig = rig_on(Db::open(&path, models.clone(), None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.set_online(false);
        let count = admit(&rig, mode, &source).await.unwrap();
        let files = rig.db.shared.read(&[], |reader| {
            let mut statement = reader.connection().prepare("SELECT file.id, file.location, file.size, file.data IS NULL FROM op_item JOIN file ON file.id = op_item.file_id ORDER BY op_item.seq, op_item.position")?;
            let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?, row.get::<_, bool>(3)?)))?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        }).unwrap();
        assert_eq!(files.len(), count, "{mode}");
        for (id, location, size, no_blob) in files {
            assert_eq!((location.as_str(), size, no_blob), ("disk", bytes.len() as i64, true), "{mode}");
            assert_eq!(std::fs::read(rig.db.shared.files.path(&id).unwrap()).unwrap(), bytes, "{mode}");
        }
        source.close().unwrap();
        let mut tasks = std::mem::take(&mut super::locked(&rig.sync.inner.uploads).tasks);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        drop(rig);
        let rig = rig_on(Db::open(&path, models, None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
        rig.sync.register(OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
        for _ in 0..count { rig.transport.enqueue("CreateBlob", blob()); }
        rig.transport.enqueue("CreateMoment", created("m9"));
        rig.transport.enqueue("CreateAssets", Reply::data(json!({"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":2},{"id":"a1","clientKey":"key-1","size":2}]}})));
        rig.sync.flush().await.unwrap();
        rig.sync.wait_uploads().await;
        let puts = rig.transport.put_calls();
        assert_eq!(puts.len(), count, "{mode}");
        assert!(puts.iter().all(|put| put.body == bytes), "{mode}");
        assert!(rig.db.outbox().unwrap().is_empty(), "{mode}");
        assert!(!source_path.exists());
    }
}
