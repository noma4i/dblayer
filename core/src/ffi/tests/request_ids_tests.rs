use std::sync::Arc;

use super::load_tests::relation;
use super::{Config, Database};
use crate::runtime::{Clock, ManualClock};
use crate::schema::{FieldSpec, ModelSpec};
use crate::transport::{Author, MockTransport, Reply};
use crate::value::ScalarKind;

const FEED: &str = r#"{"model":"Moment","relation":"feed","params":{}}"#;

async fn life(transport: &Arc<MockTransport>, clock: Arc<dyn Clock>) -> Arc<Database> {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.relations.push(relation("feed", "5.minutes"));
    let wired = transport.clone();
    let database = Database::start_with(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), clock, move |db, _| {
        super::signed_session_tests::signed_session(db);
        Ok(super::database::Wiring { transport: Some(wired), encrypted: None, cable: None, seal: None })
    }).unwrap();
    database.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"}]"#.into()).await.unwrap();
    database.set_online(true).await.unwrap();
    database
}

/// sess_request_ids_unique_per_process: two lives of a database in 1 process never share an `(author, request_id)` pair, so the second load takes the second reply of the queue.
#[tokio::test]
async fn sess_request_ids_unique_per_process() {
    let transport = Arc::new(MockTransport::new());
    transport.enqueue("Feed", Reply::data(serde_json::json!({"moments": [{"id": "m1", "text": "a"}]})));
    transport.enqueue("Feed", Reply::data(serde_json::json!({"moments": []})));
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(std::time::Duration::from_secs(1)));
    let first = life(&transport, clock.clone()).await;
    first.load(first.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    first.close_database().await.unwrap();
    let second = life(&transport, clock).await;
    second.load(second.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let pairs: Vec<(Author, u64)> = transport.calls().iter().filter(|call| call.operation_name == "Feed").map(|call| { let wire = call.wire.expect("a window request names its author"); (wire.author, wire.request_id) }).collect();
    assert_eq!(pairs.len(), 2, "one load per life");
    assert_ne!(pairs[0], pairs[1], "the second life does not reuse the pair of the first");
    assert_eq!(second.read_query(second.session_epoch(), super::QuerySpec::json(FEED)).await.unwrap().len(), 0, "the second load took the second reply");
    second.close_database().await.unwrap();
}

const OPERATIONS_WITH_UPLOADS: &str = r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"},{"name":"MakeMoment","document":"mutation MakeMoment { makeMoment { id text } }","rootPath":"makeMoment"},{"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }"},{"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }"},{"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url } } }"},{"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }"},{"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }"}]"#;

/// Refuses every named operation with a transient server error (503), then a permanent one (400) once
/// `fatal` is set, so each owner backs off once and its retry resolves. Session traffic without an
/// operation name (the social sign-in) reaches the server.
struct RefusingHttp {
    session: super::encrypted_tests::ThroughServer,
    fatal: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl super::FakeHttp for RefusingHttp {
    async fn post(&self, path: String, body: String, purpose: crate::transport::NetworkPurpose) -> Result<super::EncryptedReply, super::DbError> {
        let request = crate::transport::encrypted::tests::fake_server_tests::decrypt_request(&body).unwrap();
        if path != "/api/ticket" && request["body"]["operationName"].as_str().is_none() {
            return super::FakeHttp::post(&self.session, path, body, purpose).await;
        }
        let status = if self.fatal.load(std::sync::atomic::Ordering::SeqCst) { 400 } else { 503 };
        Ok(super::EncryptedReply { status, body: String::new() })
    }
}

/// wire_prop_retry_progress_one_source: the public `RetryProgress` lists one waiting entry per owner that backs off (Session, Window, Outbox, Transfer), each keyed by its owner and first attempt, and every entry leaves when its tick fires (wire.md P-WIRE-03, D-274).
#[tokio::test(flavor = "multi_thread")]
async fn wire_prop_retry_progress_one_source() {
    use std::collections::BTreeSet;
    use crate::transport::{RetryEntry, RetryOwner};
    use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
    use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};

    let clock = super::TestClock::new(1_800_000_000_000);
    let server = FakeServer::new(clock.manual());
    let credentials = Arc::new(MemoryCredentials::default());
    let http = Arc::new(RefusingHttp { session: super::encrypted_tests::ThroughServer(server.clone()), fatal: std::sync::atomic::AtomicBool::new(false) });
    let mut encrypted = super::EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() };
    encrypted.endpoints.session_operations.insert("ticket".into(), super::EncryptedSessionOperation { path: "/api/ticket".into(), class: crate::transport::EncryptedOperationClass::Sessionless });
    let ports = super::OpenPorts {
        transport: Some(Arc::new(super::encrypted_tests::SubscriptionsOnly)),
        encrypted: Some(encrypted),
        credentials: Some(Arc::new(super::encrypted_tests::Held(credentials))),
        http: Some(http.clone()),
        endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()),
        ..Default::default()
    };
    let spec = super::sealed_errors_tests::moment_spec();
    let database = Database::open_with_clock(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![spec]).unwrap()), ports, clock.clone()).await.unwrap();
    database.register_operations(OPERATIONS_WITH_UPLOADS.into()).await.unwrap();
    database.set_online(true).await.unwrap();
    database.session_call("social".into(), super::session_guards_tests::SOCIAL_BODY.into()).await.unwrap();
    let epoch = database.session_epoch();

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"clip").unwrap();

    let created = database.create(epoch, "Moment".into(), "{\"text\":\"a\"}".into(), "{}".into()).await.unwrap();
    let row_id = created.optimistic_value.as_ref().unwrap().id.clone();
    let window = tokio::spawn({ let db = database.clone(); async move { db.load(epoch, "Moment".into(), "feed".into(), "{}".into()).await } });

    let progress = database.retry_progress();
    let reader = progress.subscribe();
    let waiting = loop {
        let Ok(snapshot) = tokio::time::timeout(std::time::Duration::from_secs(8), reader.next()).await else { panic!("the registry stalled before Window and Outbox backed off: {:?}", progress.current()) };
        let snapshot = snapshot.unwrap();
        if snapshot.waiting.len() == 2 { break snapshot; }
    };
    let owners: BTreeSet<RetryOwner> = waiting.waiting.iter().map(|entry: &RetryEntry| entry.owner).collect();
    assert_eq!(owners, [RetryOwner::Window, RetryOwner::Outbox].into_iter().collect(), "one waiting entry per owner of the epoch that backs off at once");
    assert!(waiting.waiting.iter().all(|entry| entry.attempt == 1), "the first retry of each owner: {:?}", waiting.waiting);

    http.fatal.store(true, std::sync::atomic::Ordering::SeqCst);
    for entry in &waiting.waiting {
        clock.advance(entry.delay_ms);
    }
    let drained = loop {
        let snapshot = crate::test_wait::within(reader.next()).await.unwrap();
        if snapshot.waiting.is_empty() { break snapshot; }
    };
    assert!(drained.waiting.is_empty(), "every tick removed its entry: {drained:?}");
    let failed = crate::test_wait::within(database.confirmed(created.clone(), false)).await;
    assert!(matches!(failed, Err(crate::ffi::DbError::Rejected { kept: true, .. })), "the fatal attempt of the queue ends the operation before the network recovers: {failed:?}");

    http.fatal.store(false, std::sync::atomic::Ordering::SeqCst);
    let uploaded = database.upload(epoch, "Moment".into(), row_id, "upload".into(), crate::ffi::upload_wire::WireUploadRequest { source: crate::ffi::upload_wire::WireSource::File { path: source.to_str().unwrap().to_string(), name: None, mime: None, prepare: None }, variables_json: "{}".into() }).await.unwrap();
    let transfer = loop {
        let snapshot = crate::test_wait::within(reader.next()).await.unwrap();
        if snapshot.waiting.len() == 1 { break snapshot; }
    };
    assert_eq!((transfer.waiting[0].owner, transfer.waiting[0].attempt), (RetryOwner::Transfer, 1), "the upload step backs off under its own owner once the pair of the queue is free: {transfer:?}");
    http.fatal.store(true, std::sync::atomic::Ordering::SeqCst);
    clock.advance(transfer.waiting[0].delay_ms);
    let drained = loop {
        let snapshot = crate::test_wait::within(reader.next()).await.unwrap();
        if snapshot.waiting.is_empty() { break snapshot; }
    };
    assert!(drained.waiting.is_empty(), "the tick removed the transfer entry: {drained:?}");
    let failed = crate::test_wait::within(database.confirmed(uploaded.clone(), false)).await;
    assert!(matches!(failed, Err(crate::ffi::DbError::Rejected { kept: true, .. })), "the fatal attempt of the upload ends it before the network recovers: {failed:?}");

    http.fatal.store(false, std::sync::atomic::Ordering::SeqCst);
    let ticket = tokio::spawn({ let db = database.clone(); async move { db.session_call("ticket".into(), "{}".into()).await } });
    let session = loop {
        let snapshot = crate::test_wait::within(reader.next()).await.unwrap();
        if snapshot.waiting.len() == 1 { break snapshot; }
    };
    assert_eq!((session.waiting[0].owner, session.waiting[0].attempt), (RetryOwner::Session, 1), "the direct sessionless call backs off under its own owner: {session:?}");
    http.fatal.store(true, std::sync::atomic::Ordering::SeqCst);
    clock.advance(session.waiting[0].delay_ms);
    let drained = loop {
        let snapshot = crate::test_wait::within(reader.next()).await.unwrap();
        if snapshot.waiting.is_empty() { break snapshot; }
    };
    assert!(drained.waiting.is_empty(), "the tick removed the session entry: {drained:?}");
    assert!(crate::test_wait::within(ticket).await.unwrap().is_err(), "the permanent refusal ends the call");

    http.fatal.store(false, std::sync::atomic::Ordering::SeqCst);
    let call = tokio::spawn({ let db = database.clone(); async move { db.run(epoch, "Feed".into(), "{}".into()).await } });
    let calls = loop {
        let snapshot = crate::test_wait::within(reader.next()).await.unwrap();
        if snapshot.waiting.iter().any(|entry| entry.owner == RetryOwner::Calls) { break snapshot; }
    };
    let calls_retry = calls.waiting.iter().find(|entry| entry.owner == RetryOwner::Calls).unwrap();
    assert_eq!(calls_retry.attempt, 1);
    http.fatal.store(true, std::sync::atomic::Ordering::SeqCst);
    clock.advance(calls_retry.delay_ms);
    assert!(crate::test_wait::within(call).await.unwrap().is_err());

    window.abort();
    database.close_database().await.unwrap();
}
