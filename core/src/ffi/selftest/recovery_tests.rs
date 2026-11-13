use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use super::rig_tests::{bound, Gate};
use crate::ffi::tests::sealed_errors_tests::moment_spec;
use crate::ffi::tests::session_guards_tests::{Rig as SessionRig, SOCIAL_BODY};
use crate::ffi::upload_wire::WireUploadRequest;
use crate::ffi::{ByteProgress, Config, Database, DbError, EncryptedConfig, PutReply, FakeTransport, TransportRequest};
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};
use crate::owners::write::upload::UploadFile;

fn effects(rig: &SessionRig, operation: &str) -> Vec<serde_json::Value> {
    rig.server.executed().into_iter().filter(|body| body["operationName"] == operation).collect()
}

const FEED: &str = "query Feed { moments { id text } }";

/// The rows 42 and 43 come the way the app gets rows: the server answers the feed and the load of the epoch stores them.
async fn load_rows(database: &Database) {
    let load = database.begin_load(database.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into()).unwrap();
    bound(load.result()).await.unwrap();
}

async fn declare_feed(rig: &SessionRig, database: &Database) {
    rig.server.register(FEED);
    database.register_operations(json!([{"name": "Feed", "document": FEED, "rootPath": "moments"}]).to_string()).await.unwrap();
}

fn two_rows() -> serde_json::Value { json!([{"id": "42", "text": "clip"}, {"id": "43", "text": "clip"}]) }

struct HeldPut { gate: Arc<Gate>, ended: Arc<tokio::sync::Notify> }

struct PutEnd(Arc<tokio::sync::Notify>);

impl Drop for PutEnd {
    fn drop(&mut self) { self.0.notify_one(); }
}

#[async_trait]
impl FakeTransport for HeldPut {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        let _ended = PutEnd(self.ended.clone());
        self.gate.pass().await;
        Ok(PutReply { status: 200, etag: Some("part".into()) })
    }
}

#[derive(Default)]
struct Jobs(std::sync::Mutex<Vec<(u64, u64)>>);

impl crate::ffi::RoundJobsSink for Jobs {
    fn schedule_job(&self, job: crate::ffi::RoundJob) { self.0.lock().unwrap().push((job.request_id, job.epoch)); }
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
    let database = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![moment_spec()]).unwrap()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(HeldPut { gate, ended: Arc::new(tokio::sync::Notify::new()) })), encrypted: Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(super::upload_tests::UploadHttp(rig.server.clone(), two_rows()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
    let registered: Vec<_> = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart")].into_iter().map(|(name, root)| {
        let document = format!("mutation {name} {{ {root} {{ id }} }}");
        rig.server.register(&document);
        json!({"name": name, "document": document, "rootPath": root})
    }).collect();
    database.register_operations(json!(registered).to_string()).await.unwrap();
    declare_feed(&rig, &database).await;
    database.set_online(true).await.unwrap();
    let jobs = Arc::new(Jobs::default());
    database.set_round_jobs(jobs.clone());
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    load_rows(&database).await;
    let first = database.session_epoch();
    let file = || WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap()));
    bound(database.upload(first, "Moment".into(), "42".into(), "upload".into(), file())).await.unwrap();
    let asked = jobs.0.lock().unwrap().clone();
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].1, first);
    database.round_started(asked[0].0, 7, first);
    bound(database.session_reset()).await.unwrap();
    bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
    let second = database.session_epoch();
    assert_ne!(first, second);
    load_rows(&database).await;
    bound(database.upload(second, "Moment".into(), "43".into(), "upload".into(), file())).await.unwrap();
    assert_eq!(jobs.0.lock().unwrap().len(), 1, "the running round of A asked for a second job");
    database.round_ended(7, first);
    crate::test_wait::settled(&database, "the second job was asked for", || jobs.0.lock().unwrap().len() >= 2).await;
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
        let database = Database::open(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![moment_spec()]).unwrap()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(HeldPut { gate: gate.clone(), ended: put_ended.clone() })), encrypted: Some(EncryptedConfig { origin: origin().into(), server_key: public_key_der_b64().into(), pin: pin().into(), ..crate::transport::encrypted::tests::protocol_tests::config() }), credentials: Some(rig.credentials.clone()), http: Some(Arc::new(super::upload_tests::UploadHttp(rig.server.clone(), two_rows()))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.unwrap();
        let registered: Vec<_> = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart")].into_iter().map(|(name, root)| {
            let document = format!("mutation {name} {{ {root} {{ id }} }}");
            rig.server.register(&document);
            json!({"name": name, "document": document, "rootPath": root})
        }).collect();
        database.register_operations(json!(registered).to_string()).await.unwrap();
        declare_feed(&rig, &database).await;
        database.set_online(true).await.unwrap();
        bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
        rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42", "text": "clip"}}));
        load_rows(&database).await;
        let mut receipts = Vec::new();
        for id in ["42", "43"] {
            let source = directory.path().join(format!("{id}.mp4"));
            std::fs::write(&source, b"abc").unwrap();
            receipts.push(bound(database.upload(database.session_epoch(), "Moment".into(), id.into(), "upload".into(), WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap())))).await.unwrap());
        }
        gate.entered().await;
        let waiters: Vec<_> = receipts.into_iter().map(|receipt| {
            let database = database.clone();
            let (started, ready) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let mut waiting = Box::pin(database.confirmed(receipt, false));
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

        bound(database.session_reset()).await.unwrap();

        let refused = bound(database.upload(database.session_epoch(), "Moment".into(), "42".into(), "upload".into(), WireUploadRequest::of(&UploadFile::new(directory.path().join("42.mp4").to_str().unwrap())))).await;
        assert!(matches!(refused, Err(DbError::NoSession { .. })));

        for (index, waiter) in tasks.into_iter().enumerate() {
            let ended = bound(waiter).await.unwrap();
            assert!(matches!(ended, Err(crate::ffi::DbError::NoSession { .. })), "waiter {index} returned {ended:?}");
        }
        bound(database.session_call("social".into(), SOCIAL_BODY.into())).await.unwrap();
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
