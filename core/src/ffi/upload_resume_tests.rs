use super::*;
use std::sync::Arc;
use serde_json::json;
use super::encrypted_tests::ThroughServer;
use super::sealed_errors_tests::moment_spec;
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::upload::{UploadFile, UploadOperations};
use crate::test_wait::within;

pub(super) struct Bytes;

#[async_trait::async_trait]
impl Transport for Bytes {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        Ok(PutReply { status: 200, etag: Some("part".into()) })
    }
}

pub(super) fn uploads() -> UploadOperations {
    UploadOperations::empty().create_blob("CreateBlob").multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart")
}

async fn open(rig: &Rig, path: &std::path::Path) -> Arc<Database> {
    let mut note = crate::schema::ModelSpec::new("Note", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::scalar("text", crate::value::ScalarKind::Str), crate::schema::FieldSpec::file("attachment").optional()]);
    note.storage = crate::schema::Storage::Local;
    let database = Database::open(Config { path: Some(path.to_string_lossy().into_owned()), data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec(), note]).unwrap(), Some(Arc::new(Bytes)), Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() }), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))), Some(uploads()), None).await.unwrap();
    let operations: Vec<_> = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart")].into_iter().map(|(name, root)| {
        let document = format!("mutation {name} {{ {root} {{ id }} }}");
        rig.server.register(&document);
        json!({"name": name, "document": document, "rootPath": root})
    }).collect();
    database.register_operations(json!(operations).to_string()).unwrap();
    database
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_resume_and_retry_execute_the_saved_step_once_after_reopen() {
    for (step, fault_at, multipart) in [("CreateBlob", 0, false), ("MakeMoment", 1, false), ("InitMultipart", 0, true), ("PresignParts", 1, true), ("CompleteMultipart", 2, true)] {
      for restart in [false, true] {
        let rig = Rig::new();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("db.sqlite");
        let source = directory.path().join("clip.mp4");
        std::fs::write(&source, if multipart { vec![0; 11 * 1024 * 1024] } else { b"abc".to_vec() }).unwrap();
        let database = open(&rig, &path).await;
        database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
        rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42", "text": "clip"},
            "initMultipart": {"key": "key", "uploadId": "upload"}, "presignParts": {"parts": ([1, 2, 3].map(|number| json!({"partNumber": number, "url": "https://upload"})))},
            "completeMultipart": {"signedId": "sid", "blobId": "bid"}, "abortMultipart": {"ok": true}}));
        let input = UploadFile::new(source.to_str().unwrap());
        let server = Arc::downgrade(&rig.server);
        let seen = std::sync::atomic::AtomicUsize::new(0);
        rig.server.on_request(move || {
            if seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == fault_at {
                server.upgrade().unwrap().refuse_next("ACCOUNT_UNAVAILABLE", false);
            }
        });
        database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), crate::ffi::upload_wire::WireUploadRequest::of(&input)).await.unwrap();
        database.sync.as_ref().unwrap().wait_uploads().await;
        let database = if restart { drop(database); open(&rig, &path).await } else { database };
        database.session_resume().await.unwrap();
        database.retry(database.session_epoch(), "Moment".into(), "42".into(), None, None).await.unwrap();
        database.sync.as_ref().unwrap().wait_uploads().await;
        let effects = rig.server.executed().into_iter().filter(|body| body["operationName"] == step).count();
        assert_eq!(effects, 1, "step={step} restart={restart}");
        assert!(database.db.outbox().unwrap().is_empty());
      }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_preserves_the_session_code_in_its_failure() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let database = open(&rig, &directory.path().join("db.sqlite")).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    let input = UploadFile::new(source.to_str().unwrap());
    database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), crate::ffi::upload_wire::WireUploadRequest::of(&input)).await.unwrap();
    database.sync.as_ref().unwrap().wait_uploads().await;
    let entry = database.db.outbox().unwrap().remove(0);
    assert_eq!(entry.operation.error.unwrap().kind, crate::syncstate::FailureKind::Session(SessionCode::AccountUnavailable));
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_returns_a_direct_mutation_without_creating_an_upload_owner() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let database = open(&rig, &directory.path().join("db.sqlite")).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"makeMoment": {"id": "42"}}));
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    database.run(database.session_epoch(), "MakeMoment".into(), "{}".into()).await.unwrap_err();
    let answer = database.session_resume().await.unwrap().unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&answer).unwrap()["data"]["makeMoment"]["id"], "42");
    assert!(database.db.outbox().unwrap().is_empty());
    assert_eq!(database.session_resume().await.unwrap(), None);
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "MakeMoment").count(), 1);
}

fn legacy_format(path: &std::path::Path) {
    let connection = rusqlite::Connection::open(path).unwrap();
    let old: bool = connection.query_row("SELECT EXISTS (SELECT 1 FROM pragma_table_info('encrypted_pending') WHERE name = 'owner_seq')", [], |row| row.get(0)).unwrap();
    if old { return; }
    connection.execute_batch(include_str!("../../../fixtures/transfer-legacy.sql")).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn opening_legacy_pending_keeps_the_encrypted_sequence_and_session() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let database = open(&rig, &path).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    let note = database.create(database.session_epoch(), "Note".into(), r#"{"text":"kept"}"#.into(), "{}".into()).await.unwrap().optimistic_value.unwrap();
    let bytes = vec![7; 300 * 1024];
    database.attach(database.session_epoch(), "Note".into(), note.id.clone(), "attachment".into(), bytes.clone(), "file".into(), "application/octet-stream".into()).await.unwrap();
    let mut seeded = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Event, database.db.generation());
    seeded.entity_ops.push(crate::commit::EntityOp::Upsert { model: crate::identity::ModelKey::new("Moment"), id: crate::identity::RowId::new("cached").unwrap(), row: json!({"id": "cached", "text": "server"}).as_object().unwrap().clone() });
    database.db.land(&database.db.admit().unwrap(), seeded).unwrap();
    assert!(database.read_row(database.session_epoch(), "Moment".into(), "cached".into()).await.unwrap().is_some());
    rig.server.reply_with(json!({"makeMoment": {"id": "42"}}));
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    database.run(database.session_epoch(), "MakeMoment".into(), "{}".into()).await.unwrap_err();
    within(database.close_database()).await.unwrap();
    drop(database);
    legacy_format(&path);
    let database = open(&rig, &path).await;
    assert!(database.read_row(database.session_epoch(), "Moment".into(), "cached".into()).await.unwrap().is_none());
    assert!(database.read_row(database.session_epoch(), "Note".into(), note.id.clone()).await.unwrap().is_some());
    assert_eq!(database.read(database.session_epoch(), "Note".into(), note.id, "attachment".into()).await.unwrap(), Some(bytes));
    let answer = within(database.session_resume()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&answer).unwrap()["data"]["makeMoment"]["id"], "42");
    let next = within(database.run(database.session_epoch(), "MakeMoment".into(), "{}".into())).await.unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&next).unwrap()["id"], "42");
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "MakeMoment").count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn opening_a_legacy_pending_upload_keeps_its_owner_and_file() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let database = open(&rig, &path).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42"}}));
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    let input = UploadFile::new(source.to_str().unwrap());
    database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), crate::ffi::upload_wire::WireUploadRequest::of(&input)).await.unwrap();
    within(database.sync.as_ref().unwrap().wait_uploads()).await;
    within(database.close_database()).await.unwrap();
    drop(database);
    legacy_format(&path);
    let database = open(&rig, &path).await;
    within(database.session_resume()).await.unwrap();
    within(database.retry(database.session_epoch(), "Moment".into(), "42".into(), None, None)).await.unwrap();
    within(database.sync.as_ref().unwrap().wait_uploads()).await;
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "CreateBlob").count(), 1);
    assert!(database.db.outbox().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_legacy_transfer_item_needs_a_new_file_before_its_mutation() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let database = open(&rig, &path).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42"}}));
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), crate::ffi::upload_wire::WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap()))).await.unwrap();
    within(database.sync.as_ref().unwrap().wait_uploads()).await;
    within(database.close_database()).await.unwrap();
    drop(database);
    legacy_format(&path);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute("DELETE FROM op_item", []).unwrap();
    drop(connection);
    let database = open(&rig, &path).await;
    let before = within(database.retry_all(database.session_epoch())).await.unwrap_err();
    assert!(before.to_string().contains("select the file again"), "{before:?}");
    assert!(within(database.session_resume()).await.is_err());
    let after = within(database.retry_all(database.session_epoch())).await.unwrap_err();
    assert!(after.to_string().contains("select the file again"), "{after:?}");
    let without = within(database.retry(database.session_epoch(), "Moment".into(), "42".into(), None, None)).await.unwrap_err();
    assert!(without.to_string().contains("select the file again"), "{without:?}");
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "MakeMoment").count(), 0);
    let replacement = directory.path().join("again.mp4");
    std::fs::write(&replacement, b"abcd").unwrap();
    rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid-2", "blobId": "bid-2"}, "makeMoment": {"id": "42"}}));
    let receipt = within(database.retry(database.session_epoch(), "Moment".into(), "42".into(), None, Some(crate::ffi::upload_wire::WireUploadRequest::of(&UploadFile::new(replacement.to_str().unwrap()))))).await.unwrap();
    assert!(matches!(within(database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "MakeMoment").count(), 1);
    assert!(database.db.outbox().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn opening_a_legacy_pending_abort_does_not_repeat_the_remote_cleanup() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let database = open(&rig, &path).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"abortMultipart": {"ok": true}}));
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    database.run(database.session_epoch(), "AbortMultipart".into(), r#"{"key":"key","uploadId":"upload"}"#.into()).await.unwrap_err();
    within(database.close_database()).await.unwrap();
    drop(database);
    legacy_format(&path);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.execute("INSERT INTO upload_orphan (storage_key, upload_id, abort_operation) VALUES ('key', 'upload', 'AbortMultipart')", []).unwrap();
    drop(connection);
    let database = open(&rig, &path).await;
    within(database.session_resume()).await.unwrap();
    within(database.resume_uploads(database.session_epoch())).await.unwrap();
    within(database.sync.as_ref().unwrap().wait_uploads()).await;
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "AbortMultipart").count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_finishes_a_cancelled_multipart_abort_after_reopen() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let database = open(&rig, &path).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"abortMultipart": {"ok": true}}));
    let input = UploadFile::new(source.to_str().unwrap());
    let state = crate::upload::UploadState { key: Some("key".into()), upload_id: Some("upload".into()), phase: crate::upload::UploadStatus::Cancelled, ..crate::upload::UploadState::queued() };
    let row = crate::identity::RowId::new("42").unwrap();
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    let sync = database.sync.as_ref().unwrap().clone();
    sync.inner.begin_upload(&database.db.admit().unwrap(), sync.inner.db.generation(), "Moment", &row, "upload", &input, state).unwrap();
    sync.inner.start_uploads().unwrap();
    sync.wait_uploads().await;
    database.cancel_upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into()).await.unwrap();
    sync.wait_uploads().await;
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "AbortMultipart").count(), 0);
    assert!(matches!(database.session.as_ref().unwrap().pending_owner().unwrap(), Some(crate::transport::RequestOwner::Transfer { step: crate::upload::UploadStage::Abort, .. })));
    drop(sync);
    database.db.close().unwrap();
    drop(database);
    let database = open(&rig, &path).await;
    database.session_resume().await.unwrap();
    database.retry(database.session_epoch(), "Moment".into(), "42".into(), None, None).await.unwrap();
    database.sync.as_ref().unwrap().wait_uploads().await;
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "AbortMultipart").count(), 1);
    assert!(database.db.outbox().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_consumes_a_lost_upload_answer_without_repeating_the_effect() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let database = open(&rig, &directory.path().join("db.sqlite")).await;
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42"}}));
    let server = Arc::downgrade(&rig.server);
    let seen = std::sync::atomic::AtomicUsize::new(0);
    rig.server.on_request(move || match seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
        0 => server.upgrade().unwrap().lose_next_answer(),
        1 => server.upgrade().unwrap().refuse_next("ACCOUNT_UNAVAILABLE", false),
        _ => {},
    });
    let input = UploadFile::new(source.to_str().unwrap());
    database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), crate::ffi::upload_wire::WireUploadRequest::of(&input)).await.unwrap();
    database.sync.as_ref().unwrap().wait_uploads().await;
    database.session_resume().await.unwrap();
    database.retry(database.session_epoch(), "Moment".into(), "42".into(), None, None).await.unwrap();
    database.sync.as_ref().unwrap().wait_uploads().await;
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "CreateBlob").count(), 1);
    assert!(database.db.outbox().unwrap().is_empty());
}
