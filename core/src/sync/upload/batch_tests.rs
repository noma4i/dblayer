use std::sync::Arc;

use serde_json::json;

use crate::db::Db;
use crate::pacing::ManualClock;
use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, RootShape};
use crate::sync::{Sync, SyncConfig};
use crate::syncstate::SyncState;
use crate::transport::MockTransport;
use crate::upload::{UploadBatchInput, UploadItem, UploadOperations};
use crate::value::ScalarKind;
use crate::test_wait::within;

fn register_declared(sync: &Sync) {
    for (name, root) in [("CreateBlob", "createBlob"), ("Init", "initMultipart"), ("Presign", "presignParts"), ("Complete", "completeMultipart"), ("Abort", "abortMultipart")] {
        sync.register(crate::sync::OperationDef::new(name, "mutation x { x }", root));
    }
}

#[tokio::test]
async fn batch_renumber_preserves_order_and_publishes_complete_rows() {
    for (removed, cancel, declared, minimum, base, nested) in [
        (1, false, true, 2, 0, false),
        (0, false, true, 2, 1, false),
        (3, false, true, 2, 0, false),
        (1, true, true, 2, 0, false),
        (1, false, false, 2, 0, false),
        (1, false, true, 4, 0, false),
        (1, false, true, 2, 1, true),
    ] {
        let mut model = spec();
        model.fields.push(FieldSpec::scalar("position", ScalarKind::Int));
        let mut object = FieldSpec::object("object");
        object.fields = vec![FieldSpec::scalar("position", ScalarKind::Int), FieldSpec::scalar("size", ScalarKind::Int), FieldSpec::scalar("label", ScalarKind::Str)];
        model.fields.push(object);
        let mut batch = serde_json::to_value(model.actions[1].batch.as_ref().unwrap()).unwrap();
        batch["min"] = json!(minimum);
        if declared {
            batch["renumber"] = json!({"position":if nested { "object.position" } else { "position" },"size":if nested { "object.size" } else { "size" },"position_from":base});
        }
        model.actions[1].batch = Some(serde_json::from_value(batch).unwrap());
        let db = Db::open_in_memory(vec![model], None).unwrap();
        let sync = Sync::new(db.clone(), Arc::new(MockTransport::new()), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
        register_declared(&sync);
        sync.set_online(false);
        let source = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(source.path(), b"file").unwrap();
        let items = (0..4).map(|position| UploadItem {
            values: json!({"clientKey":format!("key-{position}"),"position":position + base,"size":4,"object":{"position":position + base,"size":4,"label":"keep"}}).as_object().unwrap().clone(),
            path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
        }).collect();
        let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
        let rows = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.into_iter().map(|item| item.optimistic_value).collect::<Vec<_>>();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let _subscription = db.observe(crate::query::Query::new("Asset"), move |change| sink.lock().unwrap().push(change.rows.clone())).unwrap();
        db.await_revision(db.revision_seq().unwrap());
        seen.lock().unwrap().clear();
        if cancel { assert!(crate::sync::rig::app(&sync, sync.cancel_upload(&sync.inner.db.admit().unwrap(), "Asset", &rows[removed].id, "create_files")).await.unwrap()); }
        else { assert_eq!(db.discard(&db.admit().unwrap(), "Asset", &rows[removed].id, None).unwrap(), 1); }
        db.await_revision(db.revision_seq().unwrap());
        let remaining: Vec<_> = rows.iter().enumerate().filter(|(index, _)| *index != removed).collect();
        for (index, (original, row)) in remaining.iter().enumerate() {
            let row = db.find("Asset", &row.id).unwrap().unwrap();
            let values = if nested { row.data["object"].as_object().unwrap() } else { &row.data };
            let changes = declared && minimum <= 3;
            assert_eq!(values["position"], json!(if changes { index + base } else { original + base }), "removed={removed}, cancel={cancel}, nested={nested}");
            assert_eq!(values["size"], json!(if changes { 3 } else { 4 }));
            assert_eq!(row.data["object"]["label"], "keep");
        }
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty());
        if !cancel && declared && minimum <= 3 {
            assert_eq!(seen.len(), 1, "membership and renumber must publish together");
            assert_eq!(seen[0].len(), 3);
            for row in &seen[0] {
                let values = if nested { row.data["object"].as_object().unwrap() } else { &row.data };
                assert_eq!(values["size"], 3);
            }
        }
    }
}

#[tokio::test]
async fn batch_renumber_delivery_unknown_discards_every_member_without_rewriting() {
    let mut model = spec();
    model.fields.push(FieldSpec::scalar("position", ScalarKind::Int));
    let mut declaration = serde_json::to_value(model.actions[1].batch.as_ref().unwrap()).unwrap();
    declaration["renumber"] = json!({"position":"position","size":"size","position_from":0});
    model.actions[1].batch = Some(serde_json::from_value(declaration).unwrap());
    let db = Db::open_in_memory(vec![model], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
    sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}})));
    transport.enqueue("CreateAssets", crate::transport::Reply::fail(crate::transport::TransportError::Interrupted("lost answer".into())));
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"file").unwrap();
    let items = (0..4).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"position":position,"size":4}).as_object().unwrap().clone(),
        path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let rows = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.into_iter().map(|item| item.optimistic_value).collect::<Vec<_>>();
    sync.wait_uploads().await;
    assert_eq!(transport.call_count("CreateAssets"), 1);
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = seen.clone();
    let _subscription = db.observe(crate::query::Query::new("Asset"), move |change| sink.lock().unwrap().push(change.rows.clone())).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    seen.lock().unwrap().clear();
    for (position, row) in rows.iter().enumerate() {
        let stored = db.find("Asset", &row.id).unwrap().unwrap();
        assert_eq!(stored.sync.state, SyncState::DeliveryUnknown);
        assert_eq!(stored.data["position"], position);
        assert_eq!(stored.data["size"], 4);
    }
    assert_eq!(db.discard(&db.admit().unwrap(), "Asset", &rows[1].id, None).unwrap(), 1);
    db.await_revision(db.revision_seq().unwrap());
    assert!(rows.iter().all(|row| db.find("Asset", &row.id).unwrap().is_none()));
    assert!(seen.lock().unwrap().iter().all(Vec::is_empty));
}

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
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
    sync.register(crate::sync::OperationDef::new("Entries", "query x { x }", "entries"));
    transport.enqueue("Entries", crate::transport::Reply::data(json!({"entries":[{"id":"e0"}]})));
    sync.load(&sync.inner.db.admit().unwrap(), "Entry", "recent", &Default::default(), Default::default()).await.unwrap();
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
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let receipt = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap();
    let ids: Vec<_> = receipt.items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    let changes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = changes.clone();
    let _subscription = db.observe(crate::query::Query::new("Asset"), move |change| sink.lock().unwrap().push((change.size, change.diff.replaced.len()))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    sync.set_online(true);
    transport.await_calls("CreateAssets", 1).await;
    assert_eq!(transport.call_count("CreateAssets"), 1);
    if early {
        let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Event, db.generation());
        for position in 0..2 {
            envelope.entity_ops.push(crate::commit::EntityOp::Upsert { model: crate::identity::ModelKey::new("Asset"), id: crate::identity::RowId::new(&format!("a{position}")).unwrap(), row: json!({"id":format!("a{position}"),"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone() });
        }
        assert_eq!(db.land(&db.admit().unwrap(), envelope).unwrap().replaced.len(), 2);
        db.await_revision(db.revision_seq().unwrap());
    }
    sync.wait_uploads().await;
    db.await_revision(db.revision_seq().unwrap());
    {
        let seen = changes.lock().unwrap();
        assert!(seen.iter().all(|(count, _)| *count == 2), "{seen:?}");
        assert_eq!(seen.iter().map(|(_, replaced)| replaced).sum::<usize>(), 2);
    }
    let members = db.shared.store.scope_members("Entry", scope.as_str()).unwrap();
    assert_eq!(members.into_iter().map(|(id, _)| id).collect::<Vec<_>>(), ["e0", "e1"]);
    let calls: Vec<_> = transport.calls().into_iter().filter(|call| call.operation_name == "CreateAssets").collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].variables["items"], json!([{"clientKey":"key-0","signedId":"signed-0"},{"clientKey":"key-1","signedId":"signed-1"}]));
    let outcome = within(receipt.outcome()).await.unwrap();
    assert_eq!(outcome.result, crate::sync::SendResult::Committed);
    for (position, (temp, item)) in ids.iter().zip(&outcome.items).enumerate() {
        assert!(db.find("Asset", temp).unwrap().is_none());
        let crate::sync::ItemOutcome::Confirmed { item, value } = item else { panic!("item was not confirmed") };
        assert_eq!(item.0, format!("key-{position}"));
        assert_eq!(value.id.as_str(), format!("a{position}"));
    }
    assert!(db.outbox().unwrap().is_empty());
    }
}

pub(super) fn spec() -> ModelSpec {
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
    use crate::transport::{GraphQLRequest, PutRequest, PutResponse, Response, ServerSubscription, Transport, TransportError};
    struct PauseAfterTransfer {
        transport: Arc<MockTransport>,
        online: std::sync::Mutex<Option<Arc<crate::transport::Connectivity>>>,
    }
    #[async_trait::async_trait]
    impl Transport for PauseAfterTransfer {
        async fn query(&self, _admitted: &crate::lifecycle::Admitted, request: GraphQLRequest) -> Result<Response, TransportError> { self.transport.query(_admitted, request).await }
        async fn mutate(&self, _admitted: &crate::lifecycle::Admitted, request: GraphQLRequest) -> Result<Response, TransportError> { self.transport.mutate(_admitted, request).await }
        async fn subscribe(&self, _admitted: &crate::lifecycle::Admitted, request: GraphQLRequest) -> Result<ServerSubscription, TransportError> { self.transport.subscribe(_admitted, request).await }
        async fn put(&self, _admitted: &crate::lifecycle::Admitted, request: PutRequest) -> Result<PutResponse, TransportError> {
            let result = self.transport.put(_admitted, request).await;
            if self.transport.put_calls().len() == 2 { self.online.lock().unwrap().as_ref().unwrap().set_online(false); }
            result
        }
    }
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let paused = Arc::new(PauseAfterTransfer { transport: transport.clone(), online: Default::default() });
    let sync = Sync::new(db.clone(), paused.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
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
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let rows = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.into_iter().map(|item| item.optimistic_value).collect::<Vec<_>>();
    transport.await_put_calls(2).await;
    assert_eq!(transport.put_calls().len(), 2);
    assert_eq!(transport.call_count("CreateAssets"), 0);
    crate::sync::rig::app(&sync, sync.discard(&sync.inner.db.admit().unwrap(), "Asset", &rows[0].id, None)).await.unwrap();
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
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
    sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}})));
    let items = ["missing.png", source.path().to_str().unwrap()].into_iter().enumerate().map(|(position, path)| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: path.into(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    sync.wait_uploads().await;
    assert_eq!(db.find("Asset", &ids[0]).unwrap().unwrap().sync.state, SyncState::Failed);
    assert_eq!(db.find("Asset", &ids[1]).unwrap().unwrap().sync.state, SyncState::Sending);
    assert_eq!(transport.put_calls().len(), 1);
    assert_eq!(transport.call_count("CreateAssets"), 0);
    let settled = tokio::time::timeout(std::time::Duration::from_millis(100), sync.settled(&sync.inner.db.admit().unwrap(), "Asset", &ids[0], None)).await;
    assert!(matches!(settled, Ok(Err(_))), "{settled:?}");
    sync.register(crate::sync::OperationDef::new("CreateAsset", "mutation x { x }", "createAsset.asset"));
    transport.enqueue("CreateAsset", crate::transport::Reply::data(json!({"createAsset":{"asset":{"id":"a1","clientKey":"key-1","size":2}}})));
    crate::sync::rig::app(&sync, sync.discard(&sync.inner.db.admit().unwrap(), "Asset", &ids[0], None)).await.unwrap();
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
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
        register_declared(&sync);
        let items = (0..count).map(|position| UploadItem {
            values: json!({"clientKey":if key == "unique" { format!("key-{position}") } else { key.to_string() },"size":count}).as_object().unwrap().clone(),
            path: "missing.png".into(), name: None, mime: None, variables: Default::default(),
        }).collect();
        let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
        assert!(crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.is_err());
        assert!(db.outbox().unwrap().is_empty());
        assert!(transport.calls().is_empty());
    }
}

#[tokio::test]
async fn cancelling_one_offline_item_keeps_the_other_item_and_both_local_rows() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
    sync.set_online(false);
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    let items = (0..2).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    assert!(crate::sync::rig::app(&sync, sync.cancel_upload(&sync.inner.db.admit().unwrap(), "Asset", &ids[0], "create_files")).await.unwrap());
    assert!(db.find("Asset", &ids[0]).unwrap().unwrap().sync.uploads.is_empty());
    assert!(db.find("Asset", &ids[1]).unwrap().unwrap().sync.uploads.contains_key("create_files"));
    assert!(transport.calls().is_empty());
}

#[tokio::test]
async fn retry_of_one_failed_item_preserves_the_completed_transfer() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
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
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    sync.wait_uploads().await;
    let failed = ids.iter().find(|id| db.find("Asset", id).unwrap().unwrap().sync.state == SyncState::Failed).unwrap();
    assert_eq!(transport.put_calls().len(), 1);
    assert_eq!(db.retry(&db.admit().unwrap(), "Asset", failed, None).unwrap(), 1);
    crate::sync::rig::app(&sync, sync.flush(&sync.inner.db.admit().unwrap())).await.unwrap();
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
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
        register_declared(&sync);
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
        let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
        let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
        std::fs::remove_file(source).unwrap();
        if scenario != "offline" {
            sync.wait_uploads().await;
            for id in &ids { assert_eq!(db.find("Asset", id).unwrap().unwrap().sync.state, SyncState::DeliveryUnknown); }
        }
        let mut tasks = std::mem::take(&mut super::locked(&sync.inner.uploads).tasks);
        tasks.abort_all();
        while within(tasks.join_next()).await.is_some() {}
        drop(sync);
        drop(db);
        let db = Db::open(&path, vec![model], None).unwrap();
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
        register_declared(&sync);
        sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
        sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
        transport.respond_with(|request| (request.operation_name == "CreateAssets").then(|| crate::transport::Reply::data(json!({"createAssets":{"assets":[{"id":"a0","clientKey":"key-0","size":2},{"id":"a1","clientKey":"key-1","size":2}]}}))));
        if scenario == "discard" {
            db.discard(&db.admit().unwrap(), "Asset", &ids[0], None).unwrap();
            for id in &ids { assert!(db.find("Asset", id).unwrap().is_none()); }
            assert!(db.outbox().unwrap().is_empty());
            assert_eq!(transport.call_count("CreateAssets"), 1);
            continue;
        }
        crate::sync::rig::app(&sync, sync.flush(&sync.inner.db.admit().unwrap())).await.unwrap();
        if scenario == "retry" {
            assert_eq!(transport.call_count("CreateAssets"), 1);
            crate::sync::rig::app(&sync, sync.retry(&sync.inner.db.admit().unwrap(), "Asset", &ids[1], None)).await.unwrap();
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
    let sync = Sync::new(db.clone(), Arc::new(MockTransport::new()), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
    sync.set_online(false);
    let directory = tempfile::tempdir().unwrap();
    let mut items = Vec::new();
    for position in 0..count {
        let source = directory.path().join(format!("{position}.png"));
        std::fs::write(&source, vec![position as u8; position + 1]).unwrap();
        items.push(UploadItem { values: json!({"clientKey":format!("key-{position}"),"size":3}).as_object().unwrap().clone(), path: source.to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default() });
    }
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    drop(directory);
    assert_eq!(ids.len(), count);
    for id in &ids {
        assert!(id.is_temp());
        let row = db.find("Asset", id).unwrap().unwrap();
        assert_eq!(row.sync.state, SyncState::Sending);
        assert!(row.sync.uploads.contains_key("create_files"));
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
        let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
        register_declared(&sync);
        sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
        sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
        transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}})));
        transport.enqueue("CreateAssets", crate::transport::Reply::data(json!({"createAssets":{"assets":answer}})));
        let source = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(source.path(), b"image").unwrap();
        let items = (0..2).map(|position| UploadItem {
            values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(), path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
        }).collect();
        let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
        let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
        sync.wait_uploads().await;
        for id in &ids {
            assert_eq!(db.find("Asset", id).unwrap().unwrap().sync.state, SyncState::DeliveryUnknown, "{answer}");
        }
        assert!(db.find("Asset", &crate::identity::RowId::new("a0").unwrap()).unwrap().is_none(), "{answer}");
    }
}

#[tokio::test]
async fn discarding_one_batch_member_keeps_its_multipart_abort_after_the_item_is_gone() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let sync = Sync::new(db.clone(), Arc::new(MockTransport::new()), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob").multipart("Init", "Presign", "Complete", "Abort")), ..SyncConfig::default() });
    register_declared(&sync);
    sync.set_online(false);
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    let items = (0..2).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(),
        path: source.path().to_string_lossy().into_owned(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    db.shared.store.lock().connection.execute("UPDATE op_item SET upload_id = 'u1', storage_key = 'k1' WHERE row_id = ?1", [ids[0].as_str()]).unwrap();
    assert_eq!(db.discard(&db.admit().unwrap(), "Asset", &ids[0], None).unwrap(), 1);
    let orphans = db.shared.store.upload_orphans().unwrap();
    assert_eq!(orphans.iter().map(|(_, key, upload, abort)| (key.as_str(), upload.as_str(), abort.as_str())).collect::<Vec<_>>(), [("k1", "u1", "Abort")]);
}

async fn failed_batch(whole_operation: bool) -> (Db, Sync, Arc<ManualClock>, Vec<crate::identity::RowId>) {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let clock = Arc::new(ManualClock::new(std::time::Duration::ZERO));
    let sync = Sync::new(db.clone(), transport.clone(), clock.clone(), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
    sync.register(crate::sync::OperationDef::new("CreateBlob", "mutation x { x }", "createBlob"));
    sync.register(crate::sync::OperationDef::new("CreateAssets", "mutation x { x }", "createAssets.assets"));
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"image").unwrap();
    for _ in 0..2 { transport.enqueue("CreateBlob", crate::transport::Reply::data(json!({"createBlob":{"url":"https://storage.test/file","headers":{},"signedId":"signed","blobId":"blob"}}))); }
    if whole_operation { transport.enqueue("CreateAssets", crate::transport::Reply::graphql_error("denied", "FORBIDDEN")); }
    let paths = ["missing.png", source.path().to_str().unwrap()];
    let items = (0..2).map(|position| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(),
        path: if whole_operation { source.path().to_str().unwrap() } else { paths[position] }.into(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    sync.wait_uploads().await;
    crate::sync::rig::app(&sync, sync.flush(&sync.inner.db.admit().unwrap())).await.ok();
    sync.wait_uploads().await;
    assert_eq!(db.find("Asset", &ids[0]).unwrap().unwrap().sync.state, SyncState::Failed, "whole_operation={whole_operation}");
    (db, sync, clock, ids)
}

#[tokio::test]
async fn failed_batch_work_older_than_seven_days_is_removed_per_item_and_whole_but_siblings_stay() {
    for (whole_operation, removed) in [(false, [true, false]), (true, [true, true])] {
        let (db, sync, clock, ids) = failed_batch(whole_operation).await;
        clock.advance(std::time::Duration::from_secs(7 * 24 * 3600 - 3600));
        crate::sync::rig::app(&sync, sync.inner.expire_uploads(&sync.inner.db.admit().unwrap())).await.unwrap();
        assert!(ids.iter().all(|id| db.find("Asset", id).unwrap().is_some()), "whole_operation={whole_operation}: not yet due");
        clock.advance(std::time::Duration::from_secs(2 * 3600));
        crate::sync::rig::app(&sync, sync.inner.expire_uploads(&sync.inner.db.admit().unwrap())).await.unwrap();
        let gone: Vec<bool> = ids.iter().map(|id| db.find("Asset", id).unwrap().is_none()).collect();
        assert_eq!(gone, removed, "whole_operation={whole_operation}");
    }
}

#[tokio::test]
async fn a_batch_item_retried_after_it_was_selected_is_not_expired() {
    let (db, sync, clock, ids) = failed_batch(false).await;
    clock.advance(std::time::Duration::from_secs(8 * 24 * 3600));
    let due = sync.inner.now_ms() - 7 * 24 * 3600 * 1000;
    let stale = db.failed_upload_candidates(due).unwrap();
    assert_eq!(stale.len(), 1);
    db.retry(&db.admit().unwrap(), "Asset", &ids[0], None).unwrap();
    assert_eq!(db.expire_candidates(&db.admit().unwrap(), stale, due, &[]).unwrap(), 0);
    assert!(db.find("Asset", &ids[0]).unwrap().is_some());
}

#[tokio::test]
async fn a_batch_item_that_fails_while_its_request_is_pending_is_aborted_once_the_request_resolves() {
    let db = Db::open_in_memory(vec![spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let config = SyncConfig { upload: crate::upload::UploadConfig { multipart_threshold: 10, part_size: 5, ..Default::default() }, uploads: Some(UploadOperations::empty().create_blob("CreateBlob").multipart("Init", "Presign", "Complete", "Abort")), ..SyncConfig::default() };
    let sync = Sync::new(db.clone(), transport.clone(), Arc::new(ManualClock::new(std::time::Duration::ZERO)), config);
    register_declared(&sync);
    for (name, root) in [("CreateAssets", "createAssets.assets"), ("CreateBlob", "createBlob"), ("Init", "initMultipart"), ("Presign", "presignParts"), ("Complete", "completeMultipart"), ("Abort", "abortMultipart")] {
        sync.register(crate::sync::OperationDef::new(name, "mutation x { x }", root));
    }
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"0123456789ab").unwrap();
    let items = [source.path().to_str().unwrap(), "missing.png"].into_iter().enumerate().map(|(position, path)| UploadItem {
        values: json!({"clientKey":format!("key-{position}"),"size":2}).as_object().unwrap().clone(),
        path: path.into(), name: None, mime: None, variables: Default::default(),
    }).collect();
    let request = UploadBatchInput { operation: "create_files".into(), variables: Default::default() };
    transport.enqueue("Init", crate::transport::Reply::data(json!({"initMultipart": {"key": "k1", "uploadId": "u1"}})));
    transport.enqueue("Init", crate::transport::Reply::data(json!({"initMultipart": {"key": "k2", "uploadId": "u2"}})));
    let pending = db.clone();
    transport.respond_with(move |call| {
        if call.operation_name != "Presign" { return None; }
        let seq: i64 = pending.shared.store.lock().connection.query_row("SELECT seq FROM op", [], |row| row.get(0)).unwrap();
        let position = 0;
        pending.shared.store.lock().connection.execute("INSERT OR REPLACE INTO encrypted_pending (id, credential_digest, request_id, response_key, sequence, path, body, owner_seq, item_position, stage) VALUES (1, 'd', 'r', 'k', 1, '/graphql', '{}', ?1, ?2, 'presign')", rusqlite::params![seq, position]).unwrap();
        Some(crate::transport::Reply::graphql_error("denied", "FORBIDDEN"))
    });
    let ids: Vec<_> = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, request)).await.unwrap().items.iter().map(|item| item.optimistic_value.id.clone()).collect();
    sync.wait_uploads().await;
    let failed: Vec<_> = crate::operation::read_upload_items(&db.shared.store.lock().connection).unwrap().into_iter().filter(|(_, state)| state.phase == crate::upload::UploadStatus::Failed && state.upload_id.is_some()).collect();
    assert_eq!(failed.len(), 1, "one item failed while its request was pending: {ids:?}");
    let ((seq, position), state) = failed.first().unwrap();
    let (seq, position) = (*seq, *position);
    assert!(state.upload_id.is_some());
    assert_eq!(transport.call_count("Abort"), 0, "the pending request may still complete");
    transport.enqueue("Abort", crate::transport::Reply::data(json!({"abortMultipart": {"ok": true}})));
    db.shared.store.lock().connection.execute("DELETE FROM encrypted_pending", []).unwrap();
    let owner = crate::transport::RequestOwner { seq, item_position: position, stage: "presign".into() };
    let claim = sync.inner.claim_upload_resume(&sync.inner.db.admit().unwrap(), &owner).await.unwrap().expect("the failed item is still the owner of the stored step");
    sync.inner.resume_upload(claim, Err(crate::transport::TransportError::Graphql { messages: vec!["denied".into()], extensions: None })).await;
    sync.wait_uploads().await;
    assert_eq!(transport.call_count("Abort"), 1, "the multipart must be aborted once its pending request resolved");
}

#[tokio::test]
async fn discarding_one_item_of_a_rejected_batch_operation_removes_only_that_item() {
    let (db, _sync, _clock, ids) = failed_batch(true).await;
    assert_eq!(db.discard(&db.admit().unwrap(), "Asset", &ids[0], None).unwrap(), 1);
    assert!(db.find("Asset", &ids[0]).unwrap().is_none());
    assert!(db.find("Asset", &ids[1]).unwrap().is_some());
}

#[tokio::test]
async fn an_unreadable_candidate_is_reported_by_expiry_and_expires_once_readable() {
    let (db, sync, clock, ids) = failed_batch(false).await;
    clock.advance(std::time::Duration::from_secs(8 * 24 * 3600));
    db.shared.store.lock().connection.execute_batch("ALTER TABLE op_part RENAME TO op_part_hidden").unwrap();
    assert!(crate::sync::rig::app(&sync, sync.inner.expire_uploads(&sync.inner.db.admit().unwrap())).await.is_err(), "a read failure must not look like a skipped candidate");
    assert!(db.find("Asset", &ids[0]).unwrap().is_some());
    db.shared.store.lock().connection.execute_batch("ALTER TABLE op_part_hidden RENAME TO op_part").unwrap();
    crate::sync::rig::app(&sync, sync.inner.expire_uploads(&sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(db.find("Asset", &ids[0]).unwrap().is_none());
}

#[tokio::test]
async fn upload_create_rejects_unknown_and_mistyped_nested_values_on_single_and_batch() {
    let mut model = spec();
    let mut media = FieldSpec::object("media");
    media.app_type = Some("Media".into());
    media.fields = vec![FieldSpec::scalar("kind", ScalarKind::Str).optional(), FieldSpec::scalar("width", ScalarKind::Int).optional()];
    model.fields.push(media.optional());
    let db = Db::open_in_memory(vec![model], None).unwrap();
    let sync = Sync::new(db.clone(), Arc::new(MockTransport::new()), Arc::new(ManualClock::new(std::time::Duration::ZERO)), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    register_declared(&sync);
    sync.set_online(false);
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"file").unwrap();
    let path = source.path().to_string_lossy().into_owned();
    for (media, expected) in [(json!({"kind": "IMAGE", "id": "x"}), "store: model Asset declares no field media.id"), (json!({"width": "wide"}), "store: value of field media.width does not fit its type")] {
        let values = json!({"clientKey": "key-0", "size": 4, "media": media}).as_object().unwrap().clone();
        let single = crate::sync::rig::app(&sync, sync.upload_create(&sync.inner.db.admit().unwrap(), "Asset", "create_file", values.clone(), crate::upload::UploadFile::new(&path))).await.unwrap_err();
        assert_eq!(single.to_string(), expected);
        let items = (0..2).map(|position| UploadItem {
            values: json!({"clientKey": format!("key-{position}"), "size": 4, "media": media}).as_object().unwrap().clone(),
            path: path.clone(), name: None, mime: None, variables: Default::default(),
        }).collect();
        let batch = crate::sync::rig::app(&sync, sync.upload_create_batch(&sync.inner.db.admit().unwrap(), "Asset", items, UploadBatchInput { operation: "create_files".into(), variables: Default::default() })).await.unwrap_err();
        assert_eq!(batch.to_string(), expected);
    }
    assert!(db.outbox().unwrap().is_empty());
}
