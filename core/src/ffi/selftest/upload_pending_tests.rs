use super::rig_tests::bound;
use super::upload_batch_shift_tests::{blob, OPERATIONS};
use super::upload_stand::Stand;
use crate::ffi::upload_wire::{WireUploadBatch, WireUploadItem, WireUploadSource};

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_pending_no_io() {
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    let epoch = database.session_epoch();
    let reads = database.db.files().source_reads();
    let rounds = database.life.rounds_needed_count();
    let requests = stand.script.names().len();
    let items = (0..2).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":2,"position":{index}}}"#), source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() }).collect();
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    let stream = database.subscribe_values(epoch, crate::ffi::ReadRequest::List { query: crate::ffi::QuerySpec::json(r#"{"model":"Asset"}"#) });
    let snapshot = bound(stream.pull_next()).await.unwrap();
    assert!(matches!(snapshot, crate::ffi::ReadValue::Snapshot { ref rows, revision, .. } if revision == receipt.items[0].receipt.revision && rows.len() == 2));
    let status = database.subscribe_values(epoch, crate::ffi::ReadRequest::Blob { model: "Asset".into(), id: receipt.items[0].optimistic_value.id.clone(), action: "create_files".into() });
    assert!(matches!(bound(status.pull_next()).await, Some(crate::ffi::ReadValue::Status { upload, .. }) if upload.status == crate::ffi::WireUploadStatus::Waiting && upload.wait_reason.as_deref() == Some("source_pending")));
    let life = database.life_handle();
    bound(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
    assert_eq!(database.db.files().source_reads() - reads, 0);
    assert_eq!(database.life.rounds_needed_count() - rounds, 0);
    assert_eq!(stand.script.names().len() - requests, 0);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"source").unwrap();
    stand.script.data("CreateBlob", &blob(0));
    let source = WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None, max_bytes: None };
    bound(database.supply_source(epoch, "Asset".into(), receipt.items[0].optimistic_value.id.clone(), "create_files".into(), source)).await.unwrap();
    crate::test_wait::settled(&database, "the supplied file reaches the server", || stand.script.count("CreateBlob") > 0).await;
    let life = database.life_handle();
    bound(tokio::task::spawn_blocking(move || life.barrier())).await.unwrap();
    assert!(database.db.files().source_reads() > reads);
    assert!(database.life.rounds_needed_count() > rounds);
    assert!(stand.script.names().len() > requests);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_source_rejects_unknown_row_fields() {
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    let items = (0..2).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":2,"position":{index}}}"#), source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() }).collect();
    let epoch = database.session_epoch();
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"data").unwrap();
    let id = receipt.items[0].optimistic_value.id.clone();
    let source = WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: r#"{"unknown":1}"#.into(), variables_json: "{}".into(), prepare: None, max_bytes: None };
    assert!(matches!(bound(database.supply_source(epoch, "Asset".into(), id, "create_files".into(), source)).await, Err(crate::ffi::DbError::Invalid { .. })));
    assert_eq!(stand.script.count("CreateBlob"), 0);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn transfers_use_the_row_id_when_batches_share_a_correlation_key() {
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    database.set_online(false).await.unwrap();
    let epoch = database.session_epoch();
    let items = || (0..2).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":2,"position":{index}}}"#), source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() }).collect();
    let batch = || WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() };
    let first = bound(database.upload_create_batch(epoch, "Asset".into(), items(), batch())).await.unwrap();
    let second = bound(database.upload_create_batch(epoch, "Asset".into(), items(), batch())).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"data").unwrap();
    let source = || WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None, max_bytes: None };
    let key = "key-0".to_string();
    assert!(matches!(bound(database.supply_source(epoch, "Asset".into(), key.clone(), "create_files".into(), source())).await, Err(crate::ffi::DbError::NotFound { .. })));
    assert!(matches!(bound(database.fail_source(epoch, "Asset".into(), key.clone(), "create_files".into(), "failed".into())).await, Err(crate::ffi::DbError::NotFound { .. })));
    assert!(matches!(bound(database.retry(epoch, "Asset".into(), key.clone(), Some("create_files".into()), None)).await, Err(crate::ffi::DbError::NotFound { .. })));
    assert!(matches!(bound(database.discard(epoch, "Asset".into(), key.clone(), Some("create_files".into()))).await, Err(crate::ffi::DbError::NotFound { .. })));
    assert!(matches!(bound(database.cancel_upload(epoch, "Asset".into(), key, "create_files".into())).await, Err(crate::ffi::DbError::NotFound { .. })));
    let first_id = first.items[0].optimistic_value.id.clone();
    let second_id = second.items[0].optimistic_value.id.clone();
    bound(database.supply_source(epoch, "Asset".into(), first_id.clone(), "create_files".into(), source())).await.unwrap();
    let other = database.peek_row(epoch, "Asset".into(), second_id).await.unwrap().unwrap();
    assert_eq!(other.uploads[0].upload.wait_reason.as_deref(), Some("source_pending"));
    assert_ne!(first_id, first.items[0].item.value);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_source_values_reach_the_row_and_rollback() {
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    let items = (0..2).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":2,"position":{index}}}"#), source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() }).collect();
    let epoch = database.session_epoch();
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"data").unwrap();
    let id = receipt.items[0].optimistic_value.id.clone();
    let source = WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: r#"{"size":42}"#.into(), variables_json: "{}".into(), prepare: None, max_bytes: None };
    database.set_online(false).await.unwrap();
    bound(database.supply_source(epoch, "Asset".into(), id.clone(), "create_files".into(), source)).await.unwrap();
    let row = database.db.read(&["Asset"], |reader| reader.point("Asset", &id)).unwrap().unwrap();
    assert_eq!(row.get("size"), Some(&serde_json::json!(42)));
    bound(database.discard(epoch, "Asset".into(), id.clone(), Some("create_files".into()))).await.unwrap();
    assert!(database.db.read(&["Asset"], |reader| reader.point("Asset", &id)).unwrap().is_none());
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_prop_pending_terminates() {
    for order in [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
        let preparer = super::upload_stand::ScriptedPreparer::new(crate::ffi::ProbeOutcome::Failed { message: "unused".into() }, b"", &[]);
        let stand = Stand::new().with_preparer(preparer.clone());
        let database = stand.open(None, &OPERATIONS).await;
        for index in 0..3 { stand.script.data("CreateBlob", &blob(index)); }
        stand.script.data("CreateAssets", r#"{"data":{"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":3,"position":0},{"id":"a1","clientKey":"key-1","size":3,"position":1},{"id":"a2","clientKey":"key-2","size":3,"position":2}]}}}"#);
        let directory = tempfile::tempdir().unwrap();
        let paths: Vec<_> = (0..3).map(|index| {
            let path = directory.path().join(format!("{index}.jpg"));
            std::fs::write(&path, format!("image-{index}")).unwrap();
            path
        }).collect();
        let items = (0..3).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":3,"position":{index}}}"#), source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() }).collect();
        let epoch = database.session_epoch();
        let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
        for index in order {
            let id = receipt.items[index].optimistic_value.id.clone();
            let source = WireUploadSource { path: paths[index].to_str().unwrap().into(), name: None, mime: None, values_json: "{}".into(), variables_json: "{}".into(), prepare: None, max_bytes: None };
            bound(database.supply_source(epoch, "Asset".into(), id, "create_files".into(), source)).await.unwrap();
        }
        for item in receipt.items {
            assert!(bound(database.confirmed(item.receipt, false)).await.is_ok());
        }
        let sent = stand.script.variables("CreateAssets");
        assert_eq!(sent.len(), 1, "{order:?}");
        let keys: Vec<_> = sent[0]["items"].as_array().unwrap().iter().map(|item| item["clientKey"].as_str().unwrap()).collect();
        assert_eq!(keys, ["key-0", "key-1", "key-2"], "{order:?}");
        assert_eq!(preparer.calls(), 0, "{order:?}: a source without settings never reaches the platform preparer");
        database.close_now();
    }
}

/// Backlog 225: the values a supplied source gives a row of a batch hold until the batch is answered; each confirmed row shows the server value, whatever the order in which the sources came two at a time.
#[tokio::test(flavor = "multi_thread")]
async fn supplied_values_of_a_batch_give_way_to_the_answer() {
    let stand = Stand::new();
    let database = stand.open(None, &OPERATIONS).await;
    let epoch = database.session_epoch();
    let items = (0..4).map(|index| WireUploadItem { values_json: format!(r#"{{"clientKey":"key-{index}","size":3,"position":{index}}}"#), source: crate::ffi::upload_wire::WireSource::Pending, variables_json: "{}".into() }).collect();
    let receipt = bound(database.upload_create_batch(epoch, "Asset".into(), items, WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    for index in 0..4 { stand.script.data("CreateBlob", &blob(index)); }
    let answered: Vec<String> = (0..4).map(|index| format!(r#"{{"id":"a{index}","clientKey":"key-{index}","size":7,"position":{index}}}"#)).collect();
    stand.script.data("CreateAssets", &format!(r#"{{"data":{{"createAssets":{{"assets":[{}]}}}}}}"#, answered.join(",")));
    let directory = tempfile::tempdir().unwrap();
    let supply = |index: usize| {
        let path = directory.path().join(format!("{index}.jpg"));
        std::fs::write(&path, format!("image-{index}")).unwrap();
        let source = WireUploadSource { path: path.to_str().unwrap().into(), name: None, mime: None, values_json: format!(r#"{{"size":{}}}"#, 40 + index), variables_json: "{}".into(), prepare: None, max_bytes: None };
        database.supply_source(epoch, "Asset".into(), receipt.items[index].optimistic_value.id.clone(), "create_files".into(), source)
    };
    for pair in [[0, 1], [2, 3]] {
        let (first, second) = tokio::join!(bound(supply(pair[0])), bound(supply(pair[1])));
        first.unwrap();
        second.unwrap();
    }
    let mut ids = Vec::new();
    for item in &receipt.items { ids.push(bound(database.confirmed(item.receipt.clone(), false)).await.unwrap().value.map(|row| row.id)); }
    let sizes: Vec<(Option<String>, Option<serde_json::Value>)> = ids.iter().map(|id| (id.clone(), id.as_ref().and_then(|id| database.db.read(&["Asset"], |reader| reader.point("Asset", id)).unwrap()).and_then(|row| row.get("size").cloned()))).collect();
    let expected: Vec<(Option<String>, Option<serde_json::Value>)> = (0..4).map(|index| (Some(format!("a{index}")), Some(serde_json::json!(7)))).collect();
    assert_eq!(sizes, expected, "every confirmed row has the id and the size of the answer");
    database.close_now();
}
