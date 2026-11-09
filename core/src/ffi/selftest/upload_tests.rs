//! Uploads through the public API of `Database` on the app path: the real session, the fake server, a transport that records the PUTs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::json;

use super::rig_tests::bound;
use crate::ffi::tests::encrypted_tests::ThroughServer;
use crate::ffi::tests::sealed_errors_tests::moment_spec;
use crate::ffi::tests::session_guards_tests::{Rig as SessionRig, SOCIAL_BODY};
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::{ByteProgress, Config, Database, DbError, EncryptedConfig, PutReply, FakeTransport, TransportRequest, WireOperationOutcome};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
use crate::transport::encrypted::tests::fake_server_tests::{decrypt_request, seal_response, FakeServer};
use crate::transport::NetworkPurpose;
use crate::owners::write::upload::{UploadFile, MEBIBYTE};

/// A transport that only takes the PUTs of the file steps: it records each range and answers with an etag.
#[derive(Default)]
pub(super) struct Puts(Mutex<Vec<(u64, u64)>>);

pub(super) struct UploadHttp(pub(super) Arc<FakeServer>, pub(super) serde_json::Value);

#[async_trait]
impl crate::ffi::FakeHttp for UploadHttp {
    async fn post(&self, path: String, body: String, purpose: NetworkPurpose) -> Result<crate::ffi::EncryptedReply, DbError> {
        let request = decrypt_request(&body).unwrap();
        if request["body"]["operationName"] == "Feed" {
            return Ok(crate::ffi::EncryptedReply { status: 200, body: seal_response(&request, request["sequence"].as_i64(), json!({"data":{"moments":self.1.clone()}})) });
        }
        crate::ffi::FakeHttp::post(&ThroughServer(self.0.clone()), path, body, purpose).await
    }
}

#[async_trait]
impl FakeTransport for Puts {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, file: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        self.0.lock().unwrap().push((file.offset, file.length));
        Ok(PutReply { status: 200, etag: Some("etag".into()) })
    }
}

pub(super) fn executed(rig: &SessionRig, operation: &str) -> Vec<serde_json::Value> {
    rig.server.executed().into_iter().filter(|body| body["operationName"] == operation).collect()
}

async fn opened(rig: &SessionRig, puts: Arc<Puts>) -> Arc<Database> {
    opened_with_model(rig, puts, moment_spec(), json!([])).await
}

/// The row 42 comes the way the app gets rows: the server answers the feed and the load stores it.
pub(super) async fn opened_with_row(rig: &SessionRig, puts: Arc<Puts>) -> Arc<Database> {
    let database = opened_with_model(rig, puts, moment_spec(), json!([{"id": "42", "text": "clip"}])).await;
    let document = "query Feed { moments { id text } }";
    rig.server.register(document);
    database.register_operations(json!([{"name":"Feed", "document":document, "rootPath":"moments"}]).to_string()).await.unwrap();
    let load = database.begin_load(database.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).unwrap();
    bound(load.result()).await.unwrap();
    database
}

async fn opened_with_model(rig: &SessionRig, puts: Arc<Puts>, model: crate::schema::ModelSpec, feed: serde_json::Value) -> Arc<Database> {
    let database = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts { transport: Some(puts), encrypted: Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(UploadHttp(rig.server.clone(), feed))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    let registered: Vec<_> = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart")].into_iter().map(|(name, root)| {
        let document = format!("mutation {name} {{ {root} {{ id }} }}");
        rig.server.register(&document);
        json!({"name": name, "document": document, "rootPath": root})
    }).collect();
    database.register_operations(json!(registered).to_string()).await.unwrap();
    database.set_online(true).unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    rig.server.reply_with(json!({
        "createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"},
        "initMultipart": {"key": "blob-key", "uploadId": "multi"},
        "presignParts": {"parts": [{"partNumber": 1, "url": "https://p1"}, {"partNumber": 2, "url": "https://p2"}, {"partNumber": 3, "url": "https://p3"}]},
        "completeMultipart": {"signedId": "multi-sid", "blobId": "bid"},
        "abortMultipart": {"ok": true},
        "makeMoment": {"id": "42", "text": "clip"},
    }));
    database
}

#[tokio::test(flavor = "multi_thread")]
async fn a_send_with_a_file_joins_a_loaded_create_window() {
    let rig = SessionRig::new();
    let mut model = moment_spec();
    model.relations.iter_mut().find(|relation| relation.name == "feed").unwrap().joins_created = true;
    let database = opened_with_model(&rig, Arc::new(Puts::default()), model, json!([])).await;
    let document = "query Feed { moments { id text } }";
    rig.server.register(document);
    database.register_operations(json!([{"name":"Feed", "document":document, "rootPath":"moments"}]).to_string()).await.unwrap();
    let epoch = database.session_epoch();
    let load = database.begin_load(epoch, "Moment".into(), Some("feed".into()), "{}".into()).unwrap();
    bound(load.result()).await.unwrap();
    database.set_online(false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let receipt = bound(database.send(crate::ffi::CommandRequest { epoch, model: "Moment".into(), values_json: r#"{"text":"clip"}"#.into(), args_json: "{}".into(), upload: Some(crate::ffi::command::CommandUpload { action: "upload".into(), request: request(&source) }) })).await.unwrap();
    let id = receipt.optimistic_value.unwrap().id;
    let query = json!({"model":"Moment", "relation":"feed", "params":{}}).to_string();
    let rows = bound(database.read_query(epoch, crate::ffi::QuerySpec::json(query))).await.unwrap();
    assert_eq!(rows.into_iter().map(|row| row.id).collect::<Vec<_>>(), [id]);
    database.close_now();
}

pub(super) fn request(path: &std::path::Path) -> WireUploadRequest { WireUploadRequest::of(&UploadFile::new(path.to_str().unwrap())) }

/// xfer_ready_start, xfer_completed, xfer_mutation_confirmed: a small file takes one blob step, then its mutation carries the signed id.
#[tokio::test(flavor = "multi_thread")]
async fn a_small_file_uploads_in_one_step_and_its_mutation_carries_the_signed_id() {
    let rig = SessionRig::new();
    let puts = Arc::new(Puts::default());
    let database = opened_with_row(&rig, puts.clone()).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let receipt = bound(database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), request(&source))).await.unwrap();
    let outcome = bound(database.outcome(receipt)).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Confirmed { .. }), "{outcome:?}");
    assert_eq!(*puts.0.lock().unwrap(), vec![(0, 3)]);
    let made = executed(&rig, "MakeMoment");
    assert_eq!(made.len(), 1);
    assert!(made[0]["variables"].to_string().contains("sid"), "the mutation has no signed id: {}", made[0]["variables"]);
    database.close_now();
}

/// xfer_step_ok, xfer_all_parts: a file over the threshold goes in parts of 5 MiB, `complete` lists every etag, the mutation carries the id of `complete`.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_file_uploads_in_parts_and_completes_before_its_mutation() {
    let rig = SessionRig::new();
    let puts = Arc::new(Puts::default());
    let database = opened_with_row(&rig, puts.clone()).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("long.mp4");
    std::fs::File::create(&source).unwrap().set_len(11 * MEBIBYTE).unwrap();
    let receipt = bound(database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), request(&source))).await.unwrap();
    let outcome = bound(database.outcome(receipt)).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Confirmed { .. }), "{outcome:?}");
    assert_eq!(*puts.0.lock().unwrap(), vec![(0, 5 * MEBIBYTE), (5 * MEBIBYTE, 5 * MEBIBYTE), (10 * MEBIBYTE, MEBIBYTE)]);
    let completed = executed(&rig, "CompleteMultipart");
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0]["variables"]["parts"].as_array().map(Vec::len), Some(3));
    assert!(executed(&rig, "MakeMoment")[0]["variables"].to_string().contains("multi-sid"));
    database.close_now();
}

/// xfer_ready_cancel: a transfer that waits for the network is cancelled, its file never goes out and its mutation is never sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_upload_sends_nothing() {
    let rig = SessionRig::new();
    let puts = Arc::new(Puts::default());
    let database = opened_with_row(&rig, puts.clone()).await;
    database.set_online(false).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let epoch = database.session_epoch();
    bound(database.upload(epoch, "Moment".into(), "42".into(), "upload".into(), request(&source))).await.unwrap();
    assert!(bound(database.cancel_upload(epoch, "Moment".into(), "42".into(), "upload".into())).await.unwrap());
    database.set_online(true).unwrap();
    bound(database.operations_quiet()).await;
    assert!(puts.0.lock().unwrap().is_empty());
    assert!(executed(&rig, "MakeMoment").is_empty());
    database.close_now();
}

/// A send with a file writes the row at once and the confirmed row replaces it when the file and its mutation went out.
#[tokio::test(flavor = "multi_thread")]
async fn a_send_with_a_file_confirms_the_row_the_server_answers() {
    let rig = SessionRig::new();
    let database = opened(&rig, Arc::new(Puts::default())).await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let epoch = database.session_epoch();
    let receipt = bound(database.send(crate::ffi::CommandRequest { epoch, model: "Moment".into(), values_json: r#"{"text":"clip"}"#.into(), args_json: "{}".into(), upload: Some(crate::ffi::command::CommandUpload { action: "upload".into(), request: request(&source) }) })).await.unwrap();
    let shown = receipt.optimistic_value.clone().expect("the receipt carries the row");
    assert_eq!(shown.uploads.iter().map(|entry| entry.action.as_str()).collect::<Vec<_>>(), ["upload"], "the row shows the transfer of its action at once");
    let outcome = bound(database.outcome(receipt)).await.unwrap();
    let WireOperationOutcome::Confirmed { value, .. } = outcome else { panic!("{outcome:?}") };
    assert_eq!(value.map(|row| row.id), Some("42".to_string()));
    database.close_now();
}
