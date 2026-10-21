use std::sync::Arc;

use serde_json::json;

use crate::db::Db;
use crate::pacing::ManualClock;
use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, RootShape};
use crate::sync::{Sync, SyncConfig};
use crate::syncstate::SyncState;
use crate::transport::MockTransport;
use crate::upload::{UploadBatchInput, UploadItem, UploadSteps};
use crate::value::ScalarKind;

#[tokio::test(start_paused = true)]
async fn batch_sends_ordered_items_once_and_replaces_by_key() {
    for early in [false, true] {
    let mut asset = spec();
    asset.actions[1].lands.push(crate::schema::ActionLanding { model: "Entry".into(), root: "createAssets.entry".into() });
    let mut entry = ModelSpec::new("Entry", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut recent = crate::schema::RelationSpec::global("recent");
    recent.joins_events = true;
    recent.remote = Some(super::super::rig::remote("Entries", crate::schema::RemoteShape::List { coverage: crate::schema::Coverage::Complete }, "5.minutes"));
    let scope = recent.scope_key("Entry", &Default::default()).unwrap();
    entry.relations.push(recent);
    let db = Db::open_in_memory(vec![asset, entry], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
    sync.register(crate::sync::OperationDef::new("Entries", "query x { x }", "entries"));
    transport.enqueue("Entries", crate::transport::Reply::data(json!({"entries":[{"id":"e0"}]})));
    sync.load("Entry", "recent", &Default::default(), Default::default()).await.unwrap();
    sync.set_online(false);
    sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    let directory = tempfile::tempdir().unwrap();
    let mut items = Vec::new();
    for position in 0..2 {
        let source = directory.path().join(format!("{position}.png"));
        std::fs::write(&source, b"file").unwrap();
        items.push(UploadItem { values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default() });
    }
    transport.respond_with(|request| {
        if request.operation_name != "CreateBlob" { return None; }
        let position = request.variables["filename"].as_str().unwrap().trim_end_matches(".png");
        Some(crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":format!("signed-{position}"),"blobId":format!("blob-{position}")}})))
    });
    transport.enqueue("CreateAssets", crate::transport::Reply::data(json!({"createAssets":{"assets":[{"id":"a1","clientKey":"key-1","size":2},{"id":"a0","clientKey":"key-0","size":2}],"entry":{"id":"e1"}}})).after(std::time::Duration::from_secs(2)));
    let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
    let ids: Vec<_> = sync.upload_create_batch("Asset", items, request).await.unwrap().iter().map(|row| row.id.clone()).collect();
    let changes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = changes.clone();
    let _subscription = db.observe(crate::query::Query::new("Asset"), move |change| sink.lock().unwrap().push((change.size, change.diff.replaced.len()))).unwrap();
    db.settle();
    sync.set_online(true);
    super::super::rig::settle().await;
    assert_eq!(transport.call_count("CreateAssets"), 1);
    if early {
        let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Event, db.generation());
        for position in 0..2 {
            envelope.entity_ops.push(crate::commit::EntityOp::Upsert { model: crate::identity::ModelKey::new("Asset"), id: crate::identity::RowId::new(&format!("a{position}")).unwrap(), row: json!({"id":format!("a{position}"),"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone() });
        }
        assert_eq!(db.land(envelope).unwrap().replaced.len(), 2);
        db.settle();
    }
    sync.wait_uploads().await;
    db.settle();
    let seen = changes.lock().unwrap();
    assert!(seen.iter().all(|(count, _)| *count == 2), "{seen:?}");
    assert_eq!(seen.iter().map(|(_, replaced)| replaced).sum::<usize>(), 2);
    let members = db.shared.store.scope_members("Entry", scope.as_str()).unwrap();
    assert_eq!(members.into_iter().map(|(id, _)| id).collect::<Vec<_>>(), ["e0", "e1"]);
    let calls: Vec<_> = transport.calls().into_iter().filter(|call| call.operation_name == "CreateAssets").collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].variables["items"], json!([{"clientKey":"key-0","signedId":"signed-0"},{"clientKey":"key-1","signedId":"signed-1"}]));
    for (position, temp) in ids.iter().enumerate() {
        assert!(db.find("Asset", temp).unwrap().is_none());
        assert_eq!(sync.confirmed("Asset", temp).await.unwrap().as_str(), format!("a{position}"));
    }
    assert!(db.outbox().unwrap().is_empty());
    }
}

fn spec() -> ModelSpec {
    let mut model = ModelSpec::new("Asset", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("clientKey", ScalarKind::Str), FieldSpec::scalar("size", ScalarKind::Int)]);
    let mut single = ActionDeclaration::new("create_file", RootShape::Upsert);
    single.upload = true;
    single.mutation = Some("CreateAsset".into());
    single.correlate = vec!["clientKey".into()];
    let mut batch = single.clone();
    batch.name = "create_files".into();
    batch.mutation = Some("CreateAssets".into());
    batch.batch = Some(serde_json::from_value(json!({"min":2,"max":10,"items":"items","single":"create_file","input":{"clientKey":"clientKey"}})).unwrap());
    model.actions = vec![single, batch];
    model
}

#[tokio::test]
async fn discard_while_the_final_mutation_waits_offline_uses_the_single_operation() {
    use crate::transport::{GraphQLRequest, PutRequest, PutResponse, Response, Subscription, Transport, TransportError};
    struct PauseAfterTransfer {
        transport: Arc<MockTransport>,
        online: std::sync::Mutex<Option<crate::transport::Connectivity>>,
    }
    #[async_trait::async_trait]
    impl Transport for PauseAfterTransfer {
        async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> { self.transport.query(request).await }
        async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> { self.transport.mutate(request).await }
        async fn subscribe(&self, request: GraphQLRequest) -> Result<Subscription, TransportError> { self.transport.subscribe(request).await }
        async fn put(&self, request: PutRequest) -> Result<PutResponse, TransportError> {
            let result = self.transport.put(request).await;
            if self.transport.put_calls().len() == 2 { self.online.lock().unwrap().as_ref().unwrap().set(false); }
            result
        }
    }
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let paused = Arc::new(PauseAfterTransfer { transport: transport.clone(), online: Default::default() });
    let sync = Sync::new(db.clone(), paused.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
    *paused.online.lock().unwrap() = Some(sync.inner.connectivity.clone());
    sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(crate::sync::OperationDef::new("CreateAsset", "mutation x { x }", "createAsset.asset"));
    sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}})));
    transport.enqueue("CreateAsset", crate::transport::Reply::data(json!({"createAsset":{"asset":{"id":"a1","clientKey":"key-1","size":2}}})));
    transport.enqueue("CreateAssets", crate::transport::Reply::data(json!({"createAssets":{"assets":[{"id":"a1","clientKey":"key-1","size":2}]}})));
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    let items = (0..2).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
    let rows = sync.upload_create_batch("Asset", items, request).await.unwrap();
    super::super::rig::settle().await;
    assert_eq!(transport.put_calls().len(), 2);
    assert_eq!(transport.call_count("CreateAssets"), 0);
    sync.discard("Asset", &rows[0].id).await.unwrap();
    sync.set_online(true);
    sync.wait_uploads().await;
    assert_eq!(transport.call_count("CreateAssets"), 0);
    assert_eq!(transport.call_count("CreateAsset"), 1);
    assert_eq!(transport.put_calls().len(), 2);
    assert_eq!(db.find("Asset", &crate::identity::RowId::new("a1").unwrap()).unwrap().unwrap().data["size"], 2);
}

#[tokio::test]
async fn one_failed_file_does_not_fail_a_transferred_sibling() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
    sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}})));
    let items = ["missing.png", source.path().to_str().unwrap()].into_iter().enumerate().map(|(position, path)| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: path.into(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
    let ids: Vec<_> = sync.upload_create_batch("Asset", items, request).await.unwrap().iter().map(|row| row.id.clone()).collect();
    sync.wait_uploads().await;
    assert_eq!(db.find("Asset", &ids[0]).unwrap().unwrap().sync.state, SyncState::Failed);
    assert_eq!(db.find("Asset", &ids[1]).unwrap().unwrap().sync.state, SyncState::Sending);
    assert_eq!(transport.put_calls().len(), 1);
    assert_eq!(transport.call_count("CreateAssets"), 0);
    let settled = tokio::time::timeout(std::time::Duration::from_millis(100), sync.settled("Asset", &ids[0])).await;
    assert!(matches!(settled, Ok(Err(_))), "{settled:?}");
    sync.register(crate::sync::OperationDef::new("CreateAsset", "mutation x { x }", "createAsset.asset"));
    transport.enqueue("CreateAsset", crate::transport::Reply::data(json!({"createAsset":{"asset":{"id":"a1","clientKey":"key-1","size":2}}})));
    sync.discard("Asset", &ids[0]).await.unwrap();
    assert!(db.find("Asset", &ids[0]).unwrap().is_none());
    assert!(db.find("Asset", &ids[1]).unwrap().is_some());
    sync.wait_uploads().await;
    assert_eq!(transport.call_count("CreateAsset"), 1);
    assert_eq!(transport.put_calls().len(), 1);
    assert!(db.outbox().unwrap().is_empty());
}

#[tokio::test]
async fn batch_rejects_invalid_count_or_keys_without_writing() {
    for (count, key) in [(1, "unique"), (11, "unique"), (2, ""), (2, "duplicate")] {
        let db = Db::open_in_memory(vec![spec()], None).unwrap();
        let transport = Arc::new(MockTransport::new());
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
        let items = (0..count).map(|position| UploadItem {
            values: json!({"clientKey":if key == "unique" { format!("key-{position}") } else { key.to_string() },"size":count}).as_object().unwrap().clone(),
            path: "missing.png".into(), name: None, mime: None, variables: Default::default(),
        }).collect();
        let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
        assert!(sync.upload_create_batch("Asset", items, request).await.is_err());
        assert!(db.outbox().unwrap().is_empty());
        assert!(transport.calls().is_empty());
    }
}

#[tokio::test]
async fn cancelling_one_offline_item_keeps_the_other_item_and_both_local_rows() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
    sync.set_online(false);
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    let items = (0..2).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
    let ids: Vec<_> = sync.upload_create_batch("Asset", items, request).await.unwrap().iter().map(|row| row.id.clone()).collect();
    assert!(sync.cancel_upload("Asset", &ids[0]).await.unwrap());
    assert!(db.find("Asset", &ids[0]).unwrap().unwrap().upload().is_none());
    assert!(db.find("Asset", &ids[1]).unwrap().unwrap().upload().is_some());
    assert!(transport.calls().is_empty());
}

#[tokio::test]
async fn retry_of_one_failed_item_preserves_the_completed_transfer() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
    sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    transport.enqueue("CreateBlob", crate::transport::Reply::fail(crate::transport::TransportError::Http { status: 400, body: "rejected".into() }));
    for _ in 0..2 {
        transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}})));
    }
    transport.enqueue("CreateAssets", crate::transport::Reply::data(json!({"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":2},{"id":"a1","clientKey":"key-1","size":2}]}})));
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    let items = (0..2).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
    let ids: Vec<_> = sync.upload_create_batch("Asset", items, request).await.unwrap().iter().map(|row| row.id.clone()).collect();
    sync.wait_uploads().await;
    let failed = ids.iter().find(|id| db.find("Asset", id).unwrap().unwrap().sync.state == SyncState::Failed).unwrap();
    assert_eq!(transport.put_calls().len(), 1);
    assert_eq!(db.retry("Asset", failed).unwrap(), 1);
    sync.flush().await.unwrap();
    sync.wait_uploads().await;
    assert_eq!(transport.put_calls().len(), 2);
    assert_eq!(transport.call_count("CreateAssets"), 1);
    assert!(db.outbox().unwrap().is_empty());
}

#[tokio::test]
async fn restart_preserves_files_and_unknown_delivery_keeps_the_request_or_discards_every_member() {
    for scenario in ["offline", "retry", "discard"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let mut model = spec();
        model.actions[1].client_mutation_id = true;
        model.actions[1].wrap = Some("input".into());
        let db = Db::open(&path, vec![model.clone()], None).unwrap();
        let transport = Arc::new(MockTransport::new());
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
        sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
        sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
        sync.set_online(scenario != "offline");
        for _ in 0..2 { transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}))); }
        if scenario != "offline" { transport.enqueue("CreateAssets", crate::transport::Reply::fail(crate::transport::TransportError::Interrupted("lost answer".into()))); }
        let source = directory.path().join("image.png");
        std::fs::write(&source, b"image").unwrap();
        let items = (0..2).map(|position| UploadItem {
            values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
        }).collect();
        let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
        let ids: Vec<_> = sync.upload_create_batch("Asset", items, request).await.unwrap().iter().map(|row| row.id.clone()).collect();
        std::fs::remove_file(source).unwrap();
        if scenario != "offline" {
            sync.wait_uploads().await;
            for id in &ids { assert_eq!(db.find("Asset", id).unwrap().unwrap().sync.state, SyncState::DeliveryUnknown); }
        }
        let mut tasks = std::mem::take(&mut super::locked(&sync.inner.uploads).tasks);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        drop(sync);
        drop(db);
        let db = Db::open(&path, vec![model], None).unwrap();
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
        sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
        sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
        transport.respond_with(|request| (request.operation_name == "CreateAssets").then(|| crate::transport::Reply::data(json!({"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":2},{"id":"a1","clientKey":"key-1","size":2}]}}))));
        if scenario == "discard" {
            db.discard("Asset", &ids[0]).unwrap();
            for id in &ids { assert!(db.find("Asset", id).unwrap().is_none()); }
            assert!(db.outbox().unwrap().is_empty());
            assert_eq!(transport.call_count("CreateAssets"), 1);
            continue;
        }
        sync.flush().await.unwrap();
        if scenario == "retry" {
            assert_eq!(transport.call_count("CreateAssets"), 1);
            sync.retry("Asset", &ids[1]).await.unwrap();
        }
        sync.wait_uploads().await;
        assert!(db.outbox().unwrap().is_empty(), "{scenario}: {:?}, {:?}", db.outbox().unwrap(), transport.calls());
        assert_eq!(transport.put_calls().len(), 2);
        let calls: Vec<_> = transport.calls().into_iter().filter(|call| call.operation_name == "CreateAssets").collect();
        assert_eq!(calls.len(), if scenario == "retry" { 2 } else { 1 });
        if scenario == "retry" { assert_eq!(calls[0].variables, calls[1].variables); }
    }
}

#[tokio::test]
async fn batch_creates_all_rows_and_owns_each_file_before_returning() {
    for count in [2, 10] {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let sync = Sync::new(db.clone(), Arc::new(MockTransport::new()), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
    sync.set_online(false);
    let directory = tempfile::tempdir().unwrap();
    let mut items = Vec::new();
    for position in 0..count {
        let source = directory.path().join(format!("{position}.png"));
        std::fs::write(&source, vec![position as u8; position + 1]).unwrap();
        items.push(UploadItem { values: json!({"clientKey":format!("key-{position}"),"size":3}).as_object().unwrap().clone(), path: source.to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default() });
    }
    let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
    let ids: Vec<_> = sync.upload_create_batch("Asset", items, request).await.unwrap().iter().map(|row| row.id.clone()).collect();
    drop(directory);
    assert_eq!(ids.len(), count);
    for id in &ids {
        assert!(id.is_temp());
        let row = db.find("Asset", id).unwrap().unwrap();
        assert_eq!(row.sync.state, SyncState::Sending);
        assert!(row.upload().is_some());
    }
    assert_eq!(db.outbox().unwrap().len(), 1);
    db.shared.read(&[], |reader| {
        assert_eq!(reader.connection().query_row("SELECT COUNT(*) FROM op_item JOIN file ON file.id = op_item.file_id", [], |row| row.get::<_, i64>(0))?, count as i64);
        Ok(())
    }).unwrap();
    }
}

#[tokio::test]
async fn invalid_batch_answers_preserve_all_temporary_records() {
    for answer in [
        json!([{"id":"a0","clientKey":"key-0","size":2}]),
        json!([{"id":"a0","clientKey":"key-0","size":2},{"id":"a1","clientKey":"key-0","size":2}]),
        json!([{"id":"a0","clientKey":"key-0","size":2},{"id":"a1","clientKey":"foreign","size":2}]),
        json!([{"id":"a0","clientKey":"key-0","size":2},{"id":"a0","clientKey":"key-1","size":2}]),
    ] {
        let db = Db::open_in_memory(vec![spec()], None).unwrap();
        let transport = Arc::new(MockTransport::new());
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig::default());
        sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
        sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
        transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}})));
        transport.enqueue("CreateAssets", crate::transport::Reply::data(json!({"createAssets":{"assets":answer}})));
        let source = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(source.path(), b"image").unwrap();
        let items = (0..2).map(|position| UploadItem {
            values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
        }).collect();
        let request = UploadBatchInput { operation: "create_files".into(), steps: UploadSteps::new("CreateAssets").single("CreateBlob"), variables: Default::default() };
        let ids: Vec<_> = sync.upload_create_batch("Asset", items, request).await.unwrap().iter().map(|row| row.id.clone()).collect();
        sync.wait_uploads().await;
        for id in &ids {
            assert_eq!(db.find("Asset", id).unwrap().unwrap().sync.state, SyncState::DeliveryUnknown, "{answer}");
        }
        assert!(db.find("Asset", &crate::identity::RowId::new("a0").unwrap()).unwrap().is_none(), "{answer}");
    }
}
