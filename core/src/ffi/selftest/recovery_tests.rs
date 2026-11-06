use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use super::rig_tests::{bound, named_bound, Gate};
use crate::ffi::encrypted_tests::ThroughServer;
use crate::ffi::sealed_errors_tests::moment_spec;
use crate::ffi::session_guards_tests::{Rig as SessionRig, SOCIAL_BODY};
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::{ByteProgress, CableLease, Config, Database, DbError, EncryptedConfig, EncryptedSocketEvent, PutReply, SubscriptionSink, Transport, TransportRequest};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};
use crate::upload::UploadFile;

fn effects(rig: &SessionRig, operation: &str) -> Vec<serde_json::Value> {
    rig.server.executed().into_iter().filter(|body| body["operationName"] == operation).collect()
}

struct HeldPut { gate: Arc<Gate>, ended: Arc<tokio::sync::Notify> }

struct PutEnd(Arc<tokio::sync::Notify>);

impl Drop for PutEnd {
    fn drop(&mut self) { self.0.notify_one(); }
}

#[async_trait]
impl Transport for HeldPut {
    async fn open_cable(&self, _: Arc<CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        let _ended = PutEnd(self.ended.clone());
        self.gate.pass().await;
        Ok(PutReply { status: 200, etag: Some("part".into()) })
    }
}

#[derive(Default)]
struct Jobs(std::sync::Mutex<Vec<(u64, u64)>>);

impl crate::ffi::RoundJobsSink for Jobs {
    fn schedule_job(&self, request_id: u64, epoch: u64) { self.0.lock().unwrap().push((request_id, epoch)); }
    fn end_job(&self, _: u32) {}
}

/// P-XFER-05, xfer_round_needed_new_epoch: a round that runs while the session ends asks for the job of the next epoch only when the platform ends the running one.
#[tokio::test(flavor = "multi_thread")]
async fn a_round_running_at_the_session_exit_asks_for_the_job_of_the_next_epoch_after_it_ends() {
    let rig = SessionRig::new();
    let gate = Gate::new();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("upload.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec()]).unwrap(), Some(Arc::new(HeldPut { gate, ended: Arc::new(tokio::sync::Notify::new()) })), Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() }), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))), None).await.unwrap();
    let registered: Vec<_> = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart")].into_iter().map(|(name, root)| {
        let document = format!("mutation {name} {{ {root} {{ id }} }}");
        rig.server.register(&document);
        json!({"name": name, "document": document, "rootPath": root})
    }).collect();
    database.register_operations(json!(registered).to_string()).unwrap();
    database.set_online(true).unwrap();
    let jobs = Arc::new(Jobs::default());
    database.set_round_jobs(jobs.clone());
    named_bound("first sign-in", database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let first = database.session_epoch();
    let file = || WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap()));
    named_bound("upload in A", database.upload(first, "Moment".into(), "42".into(), "upload".into(), file())).await.unwrap();
    let asked = jobs.0.lock().unwrap().clone();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].1, first);
    database.round_started(asked[0].0, 7, first);
    named_bound("reset A", database.session_reset()).await.unwrap();
    named_bound("second sign-in", database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let second = database.session_epoch();
    assert_ne!(first, second);
    named_bound("upload in B", database.upload(second, "Moment".into(), "43".into(), "upload".into(), file())).await.unwrap();
    assert_eq!(jobs.0.lock().unwrap().len(), 1, "the running round of A asked for a second job");
    database.round_ended(7, first);
    crate::test_wait::within_secs(10, async { while jobs.0.lock().unwrap().len() < 2 { tokio::task::yield_now().await; } }).await;
    let asked = jobs.0.lock().unwrap().clone();
    assert_eq!(asked[1].1, second);
    database.close_now();
}

/// C15, G06: one upload holds its PUT, a second waits behind it. The session reset or the data logout arrives, every waiter ends, the PUT then answers late, and the next sign-in runs each upload at most once and none after a data logout.
#[tokio::test(flavor = "multi_thread")]
async fn an_active_and_a_queued_upload_end_at_the_exit_and_the_next_sign_in_runs_each_once() {
    {
        let rig = SessionRig::new();
        let gate = Gate::new();
        let put_ended = Arc::new(tokio::sync::Notify::new());
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![moment_spec()]).unwrap(), Some(Arc::new(HeldPut { gate: gate.clone(), ended: put_ended.clone() })), Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::protocol_tests::config() }), Some(rig.credentials.clone()), Some(Arc::new(ThroughServer(rig.server.clone()))), None).await.unwrap();
        let registered: Vec<_> = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart")].into_iter().map(|(name, root)| {
            let document = format!("mutation {name} {{ {root} {{ id }} }}");
            rig.server.register(&document);
            json!({"name": name, "document": document, "rootPath": root})
        }).collect();
        database.register_operations(json!(registered).to_string()).unwrap();
        database.set_online(true).unwrap();
        named_bound("first sign-in", database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
        rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42", "text": "clip"}}));
        let mut receipts = Vec::new();
        for id in ["42", "43"] {
            let source = directory.path().join(format!("{id}.mp4"));
            std::fs::write(&source, b"abc").unwrap();
            receipts.push(named_bound(&format!("upload {id}"), database.upload(database.session_epoch(), "Moment".into(), id.into(), "upload".into(), WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())))).await.unwrap());
        }
        gate.entered_for("the first PUT").await;
        let waiters: Vec<_> = receipts.into_iter().map(|receipt| {
            let database = database.clone();
            let (started, ready) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let mut waiting = Box::pin(database.outcome(receipt));
                let mut started = Some(started);
                std::future::poll_fn(|context| {
                    let result = waiting.as_mut().poll(context);
                    if result.is_pending() { if let Some(started) = started.take() { let _ = started.send(()); } }
                    result
                }).await
            });
            (task, ready)
        }).collect();

        let mut tasks = Vec::new();
        for (task, ready) in waiters { bound(ready).await.unwrap(); tasks.push(task); }

        named_bound("session reset", database.session_reset()).await.unwrap();

        let refused = named_bound("upload without a session", database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), WireUploadRequest::of(&UploadFile::new(directory.path().join("42.mp4").to_str().unwrap())))).await;
        assert!(matches!(refused, Err(DbError::NoSession { .. })));

        for (index, waiter) in tasks.into_iter().enumerate() {
            let ended = named_bound(&format!("outcome of upload {index}"), waiter).await.unwrap();
            assert!(matches!(ended, Ok(crate::ffi::WireOperationOutcome::Interrupted)), "waiter {index} returned {ended:?}");
        }
        named_bound("second sign-in", database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
        for _ in 0..8 { gate.release(); }
        bound(put_ended.notified()).await;

        let made = effects(&rig, "MakeMoment");
        assert!(made.is_empty(), "the late PUT of cleared work reached the server");
        for id in ["42", "43"] {
            let row = bound(database.read_row(database.session_epoch(), "Moment".into(), id.into())).await.unwrap();
            assert!(row.is_none(), "the new account retained upload row {id}: {row:?}");
        }
        database.close_now();
    }
}
