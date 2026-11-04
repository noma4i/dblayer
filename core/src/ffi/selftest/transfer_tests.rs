use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::Notify;

use crate::ffi::{ByteProgress, CableLease, Config, Database, DbError, EncryptedSocketEvent, PutReply, SubscriptionSink, Transport, TransportRequest};
use crate::ffi::upload_wire::WireUploadRequest;
use crate::upload::{UploadFile, UploadOperations};
use super::rig_tests::named_bound;

#[derive(Default)]
struct TransferTransport {
    calls: Mutex<Vec<TransportRequest>>,
    presign: Notify,
    abort: Notify,
}

#[async_trait]
impl Transport for TransferTransport {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no socket".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> {
        let name = request.operation_name.clone();
        self.calls.lock().unwrap().push(request);
        match name.as_str() {
            "InitMultipart" => Ok(r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#.into()),
            "PresignParts" => { self.presign.notify_one(); std::future::pending().await },
            "AbortMultipart" => { self.abort.notify_one(); Ok(r#"{"data":{"abortMultipart":{"ok":true}}}"#.into()) },
            _ => panic!("unexpected operation {name}"),
        }
    }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

async fn open(path: &std::path::Path, transport: Arc<dyn Transport>) -> Arc<Database> {
    let uploads = UploadOperations::empty().multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart");
    let db = Database::open(Config { path: Some(path.to_string_lossy().into()), data_version: None, cache_rows: 8 }, "[]".into(), Some(transport), None, None, None, Some(uploads), None).await.unwrap();
    db.register_operations(r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"#.into()).unwrap();
    db
}

#[derive(Default)]
struct MultipartTransport {
    calls: Mutex<Vec<String>>,
    puts: Mutex<Vec<String>>,
    identities: Mutex<Vec<crate::transport::TransferIdentity>>,
    paths: Mutex<Vec<(String, std::path::PathBuf)>>,
}

#[derive(Default)]
struct BlobStatus {
    expired: Notify,
}

impl crate::ffi::ValueSink for BlobStatus {
    fn delivered(&self, delivery: crate::ffi::StreamDelivery) {
        if let crate::ffi::StreamValue::Status { blob } = delivery.value {
            if blob.state == crate::upload::BlobState::Expired { self.expired.notify_one(); }
        }
    }
}

struct RefusedBlob;

#[async_trait]
impl Transport for RefusedBlob {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no socket".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { Err(DbError::Http { status: 422, detail: "rejected".into() }) }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

#[derive(Default)]
struct PhotoRefusal { identities: Mutex<Vec<crate::transport::TransferIdentity>> }

#[async_trait]
impl Transport for PhotoRefusal {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no socket".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> {
        match request.operation_name.as_str() {
            "CreateBlob" => Ok(r#"{"data":{"createBlob":{"url":"https://storage/file","headers":{},"signedId":"signed","blobId":"blob"}}}"#.into()),
            "MakeMoment" => Err(DbError::Http { status: 422, detail: "rejected".into() }),
            name => panic!("unexpected operation {name}"),
        }
    }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, identity: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        self.identities.lock().unwrap().push(identity);
        Ok(PutReply { status: 200, etag: None })
    }
}

struct HeldEncrypted {
    server: Arc<crate::transport::encrypted::fake_server_tests::FakeServer>,
    entered: Notify,
    lost: Notify,
    release: tokio::sync::Semaphore,
    routes: Mutex<Vec<(String, crate::transport::NetworkPurpose)>>,
    hold_once: std::sync::atomic::AtomicBool,
    lose: bool,
}

#[async_trait]
impl crate::ffi::EncryptedHttp for HeldEncrypted {
    async fn post(&self, path: String, body: String, purpose: crate::transport::NetworkPurpose) -> Result<crate::ffi::EncryptedReply, DbError> {
        use crate::transport::HttpExchange;
        let request = crate::transport::encrypted::fake_server_tests::decrypt_request(&body).unwrap();
        let content = &request["body"];
        let operation = content["operationName"].as_str().unwrap_or_default();
        self.routes.lock().unwrap().push((operation.to_string(), purpose));
        let held = content["operationName"] == "CreateBlob" && content["variables"]["filename"] == "1.jpg" && self.hold_once.swap(false, std::sync::atomic::Ordering::SeqCst);
        if held {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        if operation == "CreateBlob" {
            let name = content["variables"]["filename"].as_str().unwrap();
            self.server.reply_with(serde_json::json!({"createBlob":{"url":"https://storage/file","headers":{},"signedId":format!("signed-{name}"),"blobId":format!("blob-{name}")}}));
        } else if operation == "CreateAssets" {
            self.server.reply_with(serde_json::json!({"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":2,"position":1},{"id":"a1","clientKey":"key-1","size":2,"position":0}]}}));
        }
        if held && self.lose { self.server.lose_next_answer(); }
        let reply = self.server.post(&path, body, purpose).await;
        if held && self.lose { self.lost.notify_one(); }
        let reply = reply.map_err(crate::ffi::encrypted_tests::failure)?;
        Ok(crate::ffi::EncryptedReply { status: reply.status, body: reply.body })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shifting_a_batch_during_a_held_encrypted_request_keeps_the_file_owner() {
    use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
    for lose in [false, true] {
        let rig = crate::ffi::session_guards_tests::Rig::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let http = Arc::new(HeldEncrypted { server: rig.server.clone(), entered: Notify::new(), lost: Notify::new(), release: tokio::sync::Semaphore::new(0), routes: Mutex::new(Vec::new()), hold_once: std::sync::atomic::AtomicBool::new(true), lose });
        let transfer = Arc::new(PhotoRefusal::default());
        let open = || Database::open(Config { path: Some(path.to_string_lossy().into()), data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![album_model()]).unwrap(), Some(transfer.clone()), Some(crate::ffi::EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() }), Some(rig.credentials.clone()), Some(http.clone()), Some(UploadOperations::empty().create_blob("CreateBlob")), None);
        let db = open().await.unwrap();
        let mut definitions = Vec::new();
        for (name, root) in [("CreateBlob", "createBlob"), ("CreateAssets", "createAssets.assets"), ("CreateAsset", "createAsset.asset")] {
            let field = root.split('.').next().unwrap();
            let document = format!("mutation {name} {{ {field} {{ id }} }}");
            rig.server.register(&document);
            definitions.push(serde_json::json!({"name":name,"document":document,"rootPath":root}));
        }
        db.register_operations(serde_json::to_string(&definitions).unwrap()).unwrap();
        named_bound("sign in", db.session_call("social".into(), crate::ffi::session_guards_tests::SOCIAL_BODY.into())).await.unwrap();
        let items = (0..3).map(|index| {
            let file = directory.path().join(format!("{index}.jpg"));
            std::fs::write(&file, format!("image-{index}")).unwrap();
            crate::ffi::upload_wire::WireUploadItem { values_json: serde_json::json!({"clientKey":format!("key-{index}"),"size":3,"position":index}).to_string(), path: file.to_string_lossy().into(), name: Some(format!("{index}.jpg")), mime: None, variables_json: "{}".into() }
        }).collect();
        let epoch = db.session_epoch();
        let rows = named_bound("upload batch", db.upload_create_batch(epoch, "Asset".into(), items, crate::ffi::upload_wire::WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
        named_bound("second file request", http.entered.notified()).await;
        named_bound("discard first file", db.discard(epoch, "Asset".into(), rows[0].id.clone(), Some("create_files".into()))).await.unwrap();
        if lose { db.set_online(false).unwrap(); }
        http.release.add_permits(1);
        let db = if lose {
            named_bound("lost answer", http.lost.notified()).await;
            named_bound("close before resume", db.close_database()).await.unwrap();
            drop(db);
            let reopened = open().await.unwrap();
            reopened.register_operations(serde_json::to_string(&definitions).unwrap()).unwrap();
            named_bound("resume session", reopened.session_resume()).await.unwrap();
            reopened
        } else { db };
        let epoch = db.session_epoch();
        assert_eq!(named_bound("second confirmation", db.confirmed(epoch, "Asset".into(), rows[1].id.clone())).await.unwrap(), "a1", "lose={lose}");
        assert_eq!(named_bound("third confirmation", db.confirmed(epoch, "Asset".into(), rows[2].id.clone())).await.unwrap(), "a2", "lose={lose}");
        let calls = rig.server.executed();
        assert_eq!(calls.iter().filter(|call| call["operationName"] == "CreateBlob" && call["variables"]["filename"] == "1.jpg").count(), 1);
        let attempts = rig.server.seen();
        assert_eq!(attempts.iter().filter(|call| call["body"]["operationName"] == "CreateBlob" && call["body"]["variables"]["filename"] == "1.jpg").count(), if lose { 2 } else { 1 });
        assert!(http.routes.lock().unwrap().iter().filter(|(operation, _)| operation == "CreateBlob").all(|(_, purpose)| *purpose == crate::transport::NetworkPurpose::Transfer));
        let identities = transfer.identities.lock().unwrap();
        let second = identities.iter().find(|identity| identity.item_id == "key-1").unwrap();
        let third = identities.iter().find(|identity| identity.item_id == "key-2").unwrap();
        assert_eq!(second.operation_id, third.operation_id);
        assert_eq!(second.step, crate::transport::TransferStep::Put);
        assert_eq!(third.step, crate::transport::TransferStep::Put);
        drop(identities);
        let final_call = calls.iter().find(|call| call["operationName"] == "CreateAssets").unwrap();
        let items = final_call["variables"]["items"].as_array().unwrap();
        assert_eq!(items.iter().map(|item| item["clientKey"].as_str().unwrap()).collect::<Vec<_>>(), vec!["key-1", "key-2"]);
        assert_eq!(items.iter().map(|item| item["signedId"].as_str().unwrap()).collect::<Vec<_>>(), vec!["signed-1.jpg", "signed-2.jpg"]);
        named_bound("close database", db.close_database()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_http_refusal_of_a_photo_keeps_the_failed_record_until_discard() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("photo");
    std::fs::write(&source, b"image").unwrap();
    let models = serde_json::to_string(&vec![crate::ffi::sealed_errors_tests::moment_spec()]).unwrap();
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, models, Some(Arc::new(PhotoRefusal::default())), None, None, None, Some(UploadOperations::empty().create_blob("CreateBlob")), None).await.unwrap();
    db.register_operations(r#"[{"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text } }"},{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"}]"#.into()).unwrap();
    let epoch = db.session_epoch();
    let receipt = named_bound("queue photo", db.send(crate::ffi::CommandRequest { epoch, model: "Moment".into(), values_json: r#"{"text":"group"}"#.into(), args_json: "{}".into(), upload: Some(crate::ffi::command::CommandUpload { action: "upload".into(), request: WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())) }) })).await.unwrap();
    let id = receipt.row.id;
    assert!(named_bound("refused photo", db.settled(epoch, "Moment".into(), id.clone(), None)).await.is_err());
    let failed = db.read_row(epoch, "Moment".into(), id.clone()).await.unwrap().unwrap();
    assert_eq!(failed.sync.state, crate::ffi::WireSyncState::Failed);
    named_bound("discard", db.discard(epoch, "Moment".into(), id.clone(), None)).await.unwrap();
    assert!(db.read_row(epoch, "Moment".into(), id).await.unwrap().is_none());
    named_bound("close database", db.close_database()).await.unwrap();
}

struct AlbumTransport {
    calls: Mutex<Vec<TransportRequest>>,
    entered: Notify,
    release: tokio::sync::Semaphore,
    fail_first: bool,
    single: bool,
}

impl Default for AlbumTransport {
    fn default() -> Self {
        Self { calls: Mutex::new(Vec::new()), entered: Notify::new(), release: tokio::sync::Semaphore::new(0), fail_first: false, single: false }
    }
}

#[async_trait]
impl Transport for AlbumTransport {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no socket".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> {
        let name = request.operation_name.clone();
        let count = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(request);
            calls.iter().filter(|call| call.operation_name == name).count()
        };
        match name.as_str() {
            "CreateBlob" => {
                if count == 1 && self.fail_first { return Ok(r#"{"errors":[{"message":"no storage","extensions":{"code":"FORBIDDEN"}}]}"#.into()); }
                if count == 1 { self.entered.notify_one(); self.release.acquire().await.unwrap().forget(); }
                Ok(format!(r#"{{"data":{{"createBlob":{{"url":"https://storage/file","headers":{{}},"signedId":"signed-{count}","blobId":"b{count}"}}}}}}"#))
            }
            "CreateAssets" if self.fail_first => Ok(r#"{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":3,"position":2},{"id":"a1","clientKey":"key-1","size":3,"position":1},{"id":"a0","clientKey":"key-0","size":3,"position":0}]}}}"#.into()),
            "CreateAsset" if self.single => Ok(r#"{"data":{"createAsset":{"asset":{"id":"a1","clientKey":"key-1","size":2,"position":1}}}}"#.into()),
            "CreateAssets" => Ok(r#"{"data":{"createAssets":{"assets":[{"id":"a2","clientKey":"key-2","size":2,"position":1},{"id":"a0","clientKey":"key-0","size":2,"position":0}]}}}"#.into()),
            _ => panic!("unexpected operation {name}"),
        }
    }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { Ok(PutReply { status: 200, etag: None }) }
}

fn album_model() -> crate::schema::ModelSpec {
    use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, RootShape};
    use crate::value::ScalarKind;
    let mut model = ModelSpec::new("Asset", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("clientKey", ScalarKind::Str), FieldSpec::scalar("size", ScalarKind::Int), FieldSpec::scalar("position", ScalarKind::Int)]);
    let mut single = ActionDeclaration::new("create_file", RootShape::Upsert);
    single.upload = true;
    single.mutation = Some("CreateAsset".into());
    single.correlate = vec!["clientKey".into()];
    let mut batch = single.clone();
    batch.name = "create_files".into();
    batch.mutation = Some("CreateAssets".into());
    batch.batch = Some(serde_json::from_value(serde_json::json!({"min":2,"max":10,"items":"items","single":"create_file","input":{"clientKey":"clientKey"},"renumber":{"position":"position","size":"size","position_from":0}})).unwrap());
    model.actions = vec![single, batch];
    model
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_middle_file_keeps_the_other_files_in_order() {
    let directory = tempfile::tempdir().unwrap();
    let transport = Arc::new(AlbumTransport::default());
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![album_model()]).unwrap(), Some(transport.clone()), None, None, None, Some(UploadOperations::empty().create_blob("CreateBlob")), None).await.unwrap();
    db.register_operations(r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }","rootPath":"createAssets.assets"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }","rootPath":"createAsset.asset"}]"#.into()).unwrap();
    let items = (0..3).map(|index| {
        let file = directory.path().join(format!("{index}.jpg"));
        std::fs::write(&file, format!("image-{index}")).unwrap();
        crate::ffi::upload_wire::WireUploadItem { values_json: serde_json::json!({"clientKey":format!("key-{index}"),"size":3,"position":index}).to_string(), path: file.to_string_lossy().into(), name: None, mime: None, variables_json: "{}".into() }
    }).collect();
    let epoch = db.session_epoch();
    let rows = named_bound("upload batch", db.upload_create_batch(epoch, "Asset".into(), items, crate::ffi::upload_wire::WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    named_bound("first file", transport.entered.notified()).await;
    assert!(named_bound("cancel middle", db.cancel_upload(epoch, "Asset".into(), rows[1].id.clone(), "create_files".into())).await.unwrap());
    for (index, position) in [(0, 0), (2, 1)] {
        let row = db.read_row(epoch, "Asset".into(), rows[index].id.clone()).await.unwrap().unwrap();
        assert_eq!(row.cells[1], crate::ffi::Cell::Int { value: 2 });
        assert_eq!(row.cells[2], crate::ffi::Cell::Int { value: position });
    }
    transport.release.add_permits(1);
    assert_eq!(named_bound("first confirmation", db.confirmed(epoch, "Asset".into(), rows[0].id.clone())).await.unwrap(), "a0");
    assert_eq!(named_bound("last confirmation", db.confirmed(epoch, "Asset".into(), rows[2].id.clone())).await.unwrap(), "a2");
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|call| call.operation_name == "CreateBlob").count(), 2);
    let mut filenames: Vec<String> = calls.iter().filter(|call| call.operation_name == "CreateBlob").map(|call| serde_json::from_str::<serde_json::Value>(&call.variables_json).unwrap()["filename"].as_str().unwrap().to_string()).collect();
    filenames.sort();
    assert_eq!(filenames, vec!["0.jpg", "2.jpg"]);
    assert_eq!(calls.iter().filter(|call| call.operation_name == "CreateAssets").count(), 1);
    let sent = calls.iter().find(|call| call.operation_name == "CreateAssets").unwrap();
    let values: serde_json::Value = serde_json::from_str(&sent.variables_json).unwrap();
    assert_eq!(values["items"].as_array().unwrap().iter().map(|item| item["clientKey"].as_str().unwrap()).collect::<Vec<_>>(), vec!["key-0", "key-2"]);
    drop(calls);
    named_bound("close database", db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn retrying_a_refused_file_keeps_the_other_album_files() {
    let directory = tempfile::tempdir().unwrap();
    let transport = Arc::new(AlbumTransport { fail_first: true, ..AlbumTransport::default() });
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![album_model()]).unwrap(), Some(transport.clone()), None, None, None, Some(UploadOperations::empty().create_blob("CreateBlob")), None).await.unwrap();
    db.register_operations(r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }","rootPath":"createAssets.assets"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }","rootPath":"createAsset.asset"}]"#.into()).unwrap();
    let items = (0..3).map(|index| {
        let file = directory.path().join(format!("{index}.jpg"));
        std::fs::write(&file, format!("image-{index}")).unwrap();
        crate::ffi::upload_wire::WireUploadItem { values_json: serde_json::json!({"clientKey":format!("key-{index}"),"size":3,"position":index}).to_string(), path: file.to_string_lossy().into(), name: None, mime: None, variables_json: "{}".into() }
    }).collect();
    let epoch = db.session_epoch();
    let rows = named_bound("upload batch", db.upload_create_batch(epoch, "Asset".into(), items, crate::ffi::upload_wire::WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    assert!(named_bound("refused file", db.settled(epoch, "Asset".into(), rows[0].id.clone(), Some("create_files".into()))).await.is_err());
    assert_eq!(named_bound("retry file", db.retry(epoch, "Asset".into(), rows[0].id.clone(), Some("create_files".into()))).await.unwrap(), 1);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(named_bound("confirmed file", db.confirmed(epoch, "Asset".into(), row.id.clone())).await.unwrap(), format!("a{index}"));
    }
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|call| call.operation_name == "CreateBlob").count(), 4);
    let mut filenames: Vec<String> = calls.iter().filter(|call| call.operation_name == "CreateBlob").map(|call| serde_json::from_str::<serde_json::Value>(&call.variables_json).unwrap()["filename"].as_str().unwrap().to_string()).collect();
    filenames.sort();
    assert_eq!(filenames, vec!["0.jpg", "0.jpg", "1.jpg", "2.jpg"]);
    assert_eq!(calls.iter().filter(|call| call.operation_name == "CreateAssets").count(), 1);
    let sent = calls.iter().find(|call| call.operation_name == "CreateAssets").unwrap();
    let values: serde_json::Value = serde_json::from_str(&sent.variables_json).unwrap();
    assert_eq!(values["items"].as_array().unwrap().iter().map(|item| item["clientKey"].as_str().unwrap()).collect::<Vec<_>>(), vec!["key-0", "key-1", "key-2"]);
    drop(calls);
    named_bound("close database", db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn discarding_a_refused_file_sends_the_remaining_file_as_single() {
    let directory = tempfile::tempdir().unwrap();
    let transport = Arc::new(AlbumTransport { fail_first: true, single: true, ..AlbumTransport::default() });
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![album_model()]).unwrap(), Some(transport.clone()), None, None, None, Some(UploadOperations::empty().create_blob("CreateBlob")), None).await.unwrap();
    db.register_operations(r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey size position } } }","rootPath":"createAssets.assets"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey size position } } }","rootPath":"createAsset.asset"}]"#.into()).unwrap();
    let items = (0..2).map(|index| {
        let file = directory.path().join(format!("{index}.jpg"));
        std::fs::write(&file, format!("image-{index}")).unwrap();
        crate::ffi::upload_wire::WireUploadItem { values_json: serde_json::json!({"clientKey":format!("key-{index}"),"size":2,"position":index}).to_string(), path: file.to_string_lossy().into(), name: None, mime: None, variables_json: "{}".into() }
    }).collect();
    let epoch = db.session_epoch();
    let rows = named_bound("upload batch", db.upload_create_batch(epoch, "Asset".into(), items, crate::ffi::upload_wire::WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    assert!(named_bound("refused file", db.settled(epoch, "Asset".into(), rows[0].id.clone(), Some("create_files".into()))).await.is_err());
    named_bound("discard file", db.discard(epoch, "Asset".into(), rows[0].id.clone(), Some("create_files".into()))).await.unwrap();
    assert_eq!(named_bound("confirmed file", db.confirmed(epoch, "Asset".into(), rows[1].id.clone())).await.unwrap(), "a1");
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|call| call.operation_name == "CreateBlob").count(), 2);
    assert!(calls.iter().all(|call| call.operation_name != "CreateAssets"));
    assert_eq!(calls.iter().filter(|call| call.operation_name == "CreateAsset").count(), 1);
    let single = calls.iter().find(|call| call.operation_name == "CreateAsset").unwrap();
    let values: serde_json::Value = serde_json::from_str(&single.variables_json).unwrap();
    assert_eq!(values["clientKey"], "key-1");
    drop(calls);
    named_bound("close database", db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_blob_expires_when_the_network_returns_after_seven_days() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::write(&source, b"video").unwrap();
    let clock = crate::ffi::TestClock::new(1_000_000);
    let uploads = UploadOperations::empty().create_blob("CreateBlob");
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, "[]".into(), Some(Arc::new(RefusedBlob)), Some(uploads), clock.clone(), None, None, None, None).await.unwrap();
    db.register_operations(r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"}]"#.into()).unwrap();
    let epoch = db.session_epoch();
    let blob = named_bound("upload", db.clone().upload_blob(epoch, "video".into(), "CreateBlob".into(), WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())))).await.unwrap();
    assert!(named_bound("failed upload", db.clone().wait_blob(epoch, blob.id.clone())).await.is_err());
    let status = Arc::new(BlobStatus::default());
    let stream = db.subscribe(epoch, crate::ffi::StreamRequest::Blob { id: blob.id.clone() }, status.clone());
    db.set_online(false).unwrap();
    clock.advance(7 * 24 * 60 * 60 * 1000);
    db.set_online(true).unwrap();
    named_bound("expired status", status.expired.notified()).await;
    assert_eq!(db.clone().blob(epoch, blob.id).await.unwrap().state, crate::upload::BlobState::Expired);
    db.cancel_stream(stream);
    named_bound("close database", db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn reopening_after_seven_days_expires_a_failed_blob() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    let path = directory.path().join("db.sqlite");
    std::fs::write(&source, b"video").unwrap();
    let clock = crate::ffi::TestClock::new(1_000_000);
    let uploads = UploadOperations::empty().create_blob("CreateBlob");
    let config = || Config { path: Some(path.to_string_lossy().into()), data_version: None, cache_rows: 8 };
    let open = || Database::open_with_clock(config(), "[]".into(), Some(Arc::new(RefusedBlob)), Some(uploads.clone()), clock.clone(), None, None, None, None);
    let operations = r#"[{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { signedId } }"}]"#;
    let db = open().await.unwrap();
    db.register_operations(operations.into()).unwrap();
    let epoch = db.session_epoch();
    let blob = named_bound("upload", db.clone().upload_blob(epoch, "video".into(), "CreateBlob".into(), WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())))).await.unwrap();
    assert!(named_bound("failed upload", db.clone().wait_blob(epoch, blob.id.clone())).await.is_err());
    named_bound("close database", db.close_database()).await.unwrap();
    drop(db);
    clock.advance(7 * 24 * 60 * 60 * 1000);
    let db = open().await.unwrap();
    db.register_operations(operations.into()).unwrap();
    let epoch = db.session_epoch();
    let status = Arc::new(BlobStatus::default());
    let stream = db.subscribe(epoch, crate::ffi::StreamRequest::Blob { id: blob.id.clone() }, status.clone());
    named_bound("expired status", status.expired.notified()).await;
    assert_eq!(db.clone().blob(epoch, blob.id).await.unwrap().state, crate::upload::BlobState::Expired);
    db.cancel_stream(stream);
    named_bound("close reopened database", db.close_database()).await.unwrap();
}

#[async_trait]
impl Transport for MultipartTransport {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no socket".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push(request.operation_name.clone());
        match request.operation_name.as_str() {
            "InitMultipart" => Ok(r#"{"data":{"initMultipart":{"key":"key","uploadId":"upload"}}}"#.into()),
            "PresignParts" => Ok(r#"{"data":{"presignParts":{"parts":[{"partNumber":1,"url":"https://storage/1"},{"partNumber":2,"url":"https://storage/2"},{"partNumber":3,"url":"https://storage/3"}]}}}"#.into()),
            "CompleteMultipart" if calls.iter().filter(|name| *name == "CompleteMultipart").count() == 1 => Err(DbError::Http { status: 422, detail: "rejected".into() }),
            "CompleteMultipart" => Ok(r#"{"data":{"completeMultipart":{"signedId":"signed","blobId":"blob"}}}"#.into()),
            name => panic!("unexpected operation {name}"),
        }
    }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, url: String, _: HashMap<String, String>, file: crate::transport::PutFile, identity: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        self.puts.lock().unwrap().push(url);
        self.paths.lock().unwrap().push((identity.item_id.clone(), std::fs::canonicalize(file.path).unwrap()));
        self.identities.lock().unwrap().push(identity);
        Ok(PutReply { status: 200, etag: Some("etag".into()) })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn multipart_batch_accepts_a_correlation_key_with_path_characters() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::write(&source, vec![0; 11 * 1024 * 1024]).unwrap();
    let transport = Arc::new(MultipartTransport::default());
    let uploads = UploadOperations::empty().multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart");
    let db = Database::open(Config { path: Some(directory.path().join("db.sqlite").to_string_lossy().into()), data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![album_model()]).unwrap(), Some(transport.clone()), None, None, None, Some(uploads), None).await.unwrap();
    db.register_operations(r#"[{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"},{"name":"CreateAssets","document":"mutation CreateAssets { createAssets { assets { id clientKey } } }"},{"name":"CreateAsset","document":"mutation CreateAsset { createAsset { asset { id clientKey } } }"}]"#.into()).unwrap();
    let items = ["../../../outside", "other"].into_iter().enumerate().map(|(index, key)| crate::ffi::upload_wire::WireUploadItem { values_json: serde_json::json!({"clientKey":key,"size":2,"position":index}).to_string(), path: source.to_string_lossy().into(), name: None, mime: None, variables_json: "{}".into() }).collect();
    let epoch = db.session_epoch();
    named_bound("upload batch", db.upload_create_batch(epoch, "Asset".into(), items, crate::ffi::upload_wire::WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() })).await.unwrap();
    named_bound("finish transfers", db.sync.as_ref().unwrap().wait_uploads()).await;
    let identities = transport.identities.lock().unwrap();
    let parts: Vec<_> = identities.iter().filter(|identity| identity.item_id == "../../../outside").collect();
    assert_eq!(parts.len(), 3);
    assert!(parts.iter().all(|identity| matches!(&identity.step, crate::transport::TransferStep::Part { .. })));
    drop(identities);
    let ranges = std::fs::canonicalize(directory.path().join("db.sqlite.cache/ranges")).unwrap();
    assert!(transport.paths.lock().unwrap().iter().filter(|(item, _)| item == "../../../outside").all(|(_, path)| path.starts_with(&ranges)));
    named_bound("close database", db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn complete_failure_then_retry_keeps_the_confirmed_parts() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::write(&source, vec![0; 11 * 1024 * 1024]).unwrap();
    let transport = Arc::new(MultipartTransport::default());
    let db = open(&directory.path().join("db.sqlite"), transport.clone()).await;
    let epoch = db.session_epoch();
    let blob = named_bound("upload", db.clone().upload_blob(epoch, "video".into(), "CreateBlob".into(), WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())))).await.unwrap();
    assert!(named_bound("failed complete", db.clone().wait_blob(epoch, blob.id.clone())).await.is_err());
    named_bound("retry", db.clone().retry_blob(epoch, blob.id.clone())).await.unwrap();
    assert_eq!(named_bound("uploaded blob", db.clone().wait_blob(epoch, blob.id)).await.unwrap(), "signed");
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.iter().filter(|name| *name == "InitMultipart").count(), 1);
    assert_eq!(calls.iter().filter(|name| *name == "PresignParts").count(), 1);
    assert_eq!(calls.iter().filter(|name| *name == "CompleteMultipart").count(), 2);
    drop(calls);
    let mut puts = transport.puts.lock().unwrap().clone();
    puts.sort();
    assert_eq!(puts, vec!["https://storage/1", "https://storage/2", "https://storage/3"]);
    named_bound("close database", db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_offline_discard_routes_its_reopened_abort_as_a_transfer() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("video");
    std::fs::write(&source, vec![0; 11 * 1024 * 1024]).unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(TransferTransport::default());
    let db = open(&path, transport.clone()).await;
    let blob = named_bound("upload", db.clone().upload_blob(db.session_epoch(), "video".into(), "CreateBlob".into(), WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())))).await.unwrap();
    named_bound("presign", transport.presign.notified()).await;
    let files = directory.path().join("db.sqlite.files");
    assert_eq!(std::fs::read_dir(&files).unwrap().count(), 1);
    db.set_online(false).unwrap();
    named_bound("discard", db.clone().discard_blob(db.session_epoch(), blob.id)).await.unwrap();
    assert_eq!(std::fs::read_dir(&files).unwrap().count(), 0);
    assert!(source.exists());
    named_bound("close", db.close_database()).await.unwrap();
    drop(db);
    let db = open(&path, transport.clone()).await;
    db.set_online(false).unwrap();
    db.set_online(true).unwrap();
    named_bound("abort", transport.abort.notified()).await;
    named_bound("finish abort", db.resume_uploads(db.session_epoch())).await.unwrap();
    db.set_online(false).unwrap();
    db.set_online(true).unwrap();
    named_bound("repeat online", db.resume_uploads(db.session_epoch())).await.unwrap();
    let calls = transport.calls.lock().unwrap();
    let aborts: Vec<_> = calls.iter().filter(|call| call.operation_name == "AbortMultipart").collect();
    assert_eq!(aborts.len(), 1);
    assert_eq!(aborts[0].purpose, crate::transport::NetworkPurpose::Transfer, "cleanup must use the transfer network");
    drop(calls);
    named_bound("close reopened database", db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn discarding_a_queued_photo_offline_routes_the_abort_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("photo");
    std::fs::write(&source, vec![0; 11 * 1024 * 1024]).unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(TransferTransport::default());
    let models = serde_json::to_string(&vec![crate::ffi::sealed_errors_tests::moment_spec()]).unwrap();
    let make_db = || Database::open(Config { path: Some(path.to_string_lossy().into()), data_version: None, cache_rows: 8 }, models.clone(), Some(transport.clone()), None, None, None, Some(UploadOperations::empty().multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart")), None);
    let operations = r#"[{"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"#;
    let db = make_db().await.unwrap();
    db.register_operations(operations.into()).unwrap();
    let epoch = db.session_epoch();
    let receipt = named_bound("queue photo", db.send(crate::ffi::CommandRequest { epoch, model: "Moment".into(), values_json: r#"{"text":"group"}"#.into(), args_json: "{}".into(), upload: Some(crate::ffi::command::CommandUpload { action: "upload".into(), request: WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())) }) })).await.unwrap();
    named_bound("presign", transport.presign.notified()).await;
    db.set_online(false).unwrap();
    named_bound("discard", db.discard(epoch, "Moment".into(), receipt.row.id.clone(), None)).await.unwrap();
    assert!(db.read_row(epoch, "Moment".into(), receipt.row.id).await.unwrap().is_none());
    named_bound("close", db.close_database()).await.unwrap();
    drop(db);
    let db = make_db().await.unwrap();
    db.register_operations(operations.into()).unwrap();
    db.set_online(false).unwrap();
    db.set_online(true).unwrap();
    named_bound("abort", transport.abort.notified()).await;
    named_bound("finish abort", db.resume_uploads(db.session_epoch())).await.unwrap();
    db.set_online(false).unwrap();
    db.set_online(true).unwrap();
    named_bound("repeat online", db.resume_uploads(db.session_epoch())).await.unwrap();
    let calls = transport.calls.lock().unwrap();
    let aborts: Vec<_> = calls.iter().filter(|call| call.operation_name == "AbortMultipart").collect();
    assert_eq!(aborts.len(), 1);
    assert_eq!(aborts[0].purpose, crate::transport::NetworkPurpose::Transfer);
    drop(calls);
    named_bound("close reopened database", db.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn attached_files_survive_reopen_and_detaching_one_keeps_the_other() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("files.sqlite");
    let spec = crate::schema::ModelSpec::new("Files", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::file("photo").nullable()]);
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = || Config { path: Some(path.to_string_lossy().into()), data_version: None, cache_rows: 8 };
    let db = Database::open(config(), models.clone(), None, None, None, None, None, None).await.unwrap();
    let epoch = db.session_epoch();
    let first = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().id;
    let second = db.create(epoch, "Files".into(), "{}".into(), "{}".into()).await.unwrap().id;
    let small = vec![1, 2, 3];
    let large = vec![9; 300 * 1024];
    db.attach(epoch, "Files".into(), first.clone(), "photo".into(), small.clone(), "small".into(), "image/png".into()).await.unwrap();
    db.attach(epoch, "Files".into(), second.clone(), "photo".into(), large.clone(), "large".into(), "image/png".into()).await.unwrap();
    assert_eq!(db.read(epoch, "Files".into(), first.clone(), "photo".into()).await.unwrap(), Some(small));
    let url = db.url(epoch, "Files".into(), second.clone(), "photo".into()).await.unwrap().unwrap();
    assert_eq!(std::fs::read(url).unwrap(), large);
    db.attach(epoch, "Files".into(), first.clone(), "photo".into(), vec![4], "replacement".into(), "image/png".into()).await.unwrap();
    assert!(db.detach(epoch, "Files".into(), first.clone(), "photo".into()).await.unwrap());
    db.close_database().await.unwrap();
    drop(db);
    let db = Database::open(config(), models, None, None, None, None, None, None).await.unwrap();
    let epoch = db.session_epoch();
    assert_eq!(db.read(epoch, "Files".into(), first, "photo".into()).await.unwrap(), None);
    assert_eq!(db.read(epoch, "Files".into(), second.clone(), "photo".into()).await.unwrap(), Some(large));
    let info = db.file(epoch, "Files".into(), second, "photo".into()).await.unwrap().unwrap();
    let info: serde_json::Value = serde_json::from_str(&info).unwrap();
    assert_eq!(info["name"], "large");
    assert_eq!(info["size"], 300 * 1024);
    db.close_database().await.unwrap();
}
