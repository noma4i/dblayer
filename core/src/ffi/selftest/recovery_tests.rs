use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use super::rig_tests::{bound, named_bound, Gate};
use crate::ffi::encrypted_tests::ThroughServer;
use crate::ffi::sealed_errors_tests::{moment_spec, session, signed_in};
use crate::ffi::session_guards_tests::{Rig as SessionRig, SOCIAL_BODY};
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::{ByteProgress, CableLease, Config, Database, DbError, EncryptedConfig, EncryptedSocketEvent, PutReply, SessionCode, SubscriptionSink, Transport, TransportRequest};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::upload::{UploadFile, UploadOperations};

fn effects(rig: &SessionRig, operation: &str) -> Vec<serde_json::Value> {
    rig.server.executed().into_iter().filter(|body| body["operationName"] == operation).collect()
}

/// C14, R-10: a queued mutation is refused without a confirmed sequence, the sign-in is blocked by the pending request, a data reset keeps that pending, the session reset frees it, and the next sign-in leaves the server with no effect from the refused request.
#[tokio::test(flavor = "multi_thread")]
async fn a_data_reset_between_a_refused_pending_request_and_the_session_reset_keeps_the_pending() {
    let rig = SessionRig::new();
    let database = signed_in(&rig).await;
    rig.server.reply_with(json!({ "makeMoment": { "id": "42", "text": "hello" } }));
    let row = database.create_durable("Moment", r#"{"text":"hello"}"#).await;
    rig.server.refuse_next("UNAUTHENTICATED", false);
    let refused = bound(database.settled(database.session_epoch(), "Moment".into(), row.id.clone(), None)).await.unwrap_err();
    assert_eq!(session(refused).0, SessionCode::Unauthenticated);

    database.db.reset_all().unwrap();
    let blocked = bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap_err();

    assert!(matches!(blocked, DbError::Network { .. }), "the data reset dropped the pending request: {blocked:?}");
    database.session_reset().await.unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    assert!(effects(&rig, "MakeMoment").is_empty(), "the refused request ran at the server");
    database.close_now();
}

/// C17, I32: a pending encrypted request survives the close, the reopen of the same file resumes it once, and a reopen with a new data version keeps the credential and the next sequence.
#[tokio::test(start_paused = true)]
async fn a_pending_request_survives_close_reopen_and_a_new_schema_and_runs_once() {
    let rig = SessionRig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let first = rig.open(Some(&path), pin()).await.unwrap();
    rig.server.drop_next_request();
    crate::ffi::encrypted_tests::cancel_on_retry(&rig.retries, first.session_call("otp".into(), r#"{"phone":"+61400000000"}"#.into())).await;
    first.close_now();
    drop(first);

    let reopened = rig.open(Some(&path), pin()).await.unwrap();
    let resumed = reopened.session_resume().await.unwrap();
    assert_eq!(resumed.as_deref(), Some(r#"{"nextStep":"authenticated"}"#));
    assert_eq!(rig.server.executed().len(), 1);
    reopened.close_now();
    drop(reopened);

    let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
    let migrated = Database::open(Config { path: Some(path.to_string_lossy().into_owned()), data_version: Some("2".into()), cache_rows: 8 }, "[]".to_string(), Some(Arc::new(crate::ffi::encrypted_tests::SubscriptionsOnly)), Some(config), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))), None, None).await.unwrap();
    assert_eq!(migrated.session_resume().await.unwrap(), None, "the new schema resent a request that already ran");
    migrated.session_call("otp".into(), r#"{"phone":"+61400000000"}"#.into()).await.unwrap();
    assert_eq!(rig.server.executed().len(), 2, "the next request after the new schema did not run once");
    migrated.close_now();
}

struct HeldPut(Arc<Gate>);

#[async_trait]
impl Transport for HeldPut {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        self.0.pass().await;
        Ok(PutReply { status: 200, etag: Some("part".into()) })
    }
}

/// C15, G06: one upload holds its PUT, a second waits behind it. The session reset or the data logout arrives, every waiter ends, the PUT then answers late, and the next sign-in runs each upload at most once and none after a data logout.
#[tokio::test(flavor = "multi_thread")]
async fn an_active_and_a_queued_upload_end_at_the_exit_and_the_next_sign_in_runs_each_once() {
    for data_logout in [false, true] {
        let rig = SessionRig::new();
        let gate = Gate::new();
        let directory = tempfile::tempdir().unwrap();
        let operations = UploadOperations::empty().create_blob("CreateBlob").multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart");
        let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec()]).unwrap(), Some(Arc::new(HeldPut(gate.clone()))), Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() }), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))), Some(operations), None).await.unwrap();
        let registered: Vec<_> = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart")].into_iter().map(|(name, root)| {
            let document = format!("mutation {name} {{ {root} {{ id }} }}");
            rig.server.register(&document);
            json!({"name": name, "document": document, "rootPath": root})
        }).collect();
        database.register_operations(json!(registered).to_string()).unwrap();
        named_bound("first sign-in", database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
        rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42", "text": "clip"}}));
        for id in ["42", "43"] {
            let source = directory.path().join(format!("{id}.mp4"));
            std::fs::write(&source, b"abc").unwrap();
            named_bound(&format!("upload {id}"), database.upload(database.session_epoch(), "Moment".into(), id.into(), "upload".into(), WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())))).await.unwrap();
        }
        gate.entered_for("the first PUT").await;
        let waiters: Vec<_> = ["42", "43"].into_iter().map(|id| {
            let database = database.clone();
            tokio::spawn(async move { database.settled(database.session_epoch(), "Moment".into(), id.into(), None).await })
        }).collect();

        crate::test_wait::yield_until("both settle calls parked", || database.sync.as_ref().unwrap().counters().settle_parks >= 2).await;

        if data_logout { named_bound("logout", database.session_reset()).await.unwrap() } else { database.session_reset().await.unwrap() }

        for (index, waiter) in waiters.into_iter().enumerate() {
            let ended = named_bound(&format!("settled of upload {index}"), waiter).await.unwrap();
            assert!(ended.is_err() || !data_logout, "data_logout={data_logout}: waiter {index} reported a success for work that was cleared");
        }
        for _ in 0..8 { gate.release(); }
        named_bound("second sign-in", database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
        named_bound("resume_uploads", database.resume_uploads()).await.unwrap();
        named_bound("wait_uploads", database.sync.as_ref().unwrap().wait_uploads()).await;

        let made = effects(&rig, "MakeMoment");
        assert!(made.len() <= 2, "data_logout={data_logout}: {} mutations ran for 2 uploads", made.len());
        if data_logout { assert!(made.is_empty(), "data_logout={data_logout}: the late PUT of cleared work reached the server"); }
        database.close_now();
    }
}
