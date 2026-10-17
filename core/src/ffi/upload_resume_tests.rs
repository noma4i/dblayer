use super::*;
use std::sync::Arc;
use serde_json::json;
use super::encrypted_tests::ThroughServer;
use super::sealed_errors_tests::moment_spec;
use super::session_guards_tests::{Rig, SOCIAL_BODY};
use crate::transport::encrypted::vectors::{ORIGIN, PIN, PUBLIC_KEY_DER_B64};
use crate::upload::{UploadInput, UploadSteps};

pub(super) struct Bytes;

#[async_trait::async_trait]
impl Transport for Bytes {
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<String, DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<ByteProgress>) -> Result<PutReply, DbError> {
        Ok(PutReply { status: 200, etag: Some("part".into()) })
    }
}

fn open(rig: &Rig, path: &std::path::Path) -> Arc<Database> {
    let database = Database::open_encrypted(
        Config { path: Some(path.to_string_lossy().into_owned()), data_version: None, cache_rows: 8 },
        serde_json::to_string(&vec![moment_spec()]).unwrap(),
        EncryptedConfig { origin: ORIGIN.into(), server_key: PUBLIC_KEY_DER_B64.into(), pin: PIN.into() },
        Arc::new(ThroughServer(rig.server.clone())), rig.credentials.clone(), Arc::new(Bytes),
    ).unwrap();
    let operations: Vec<_> = [("CreateBlob", "createBlob"), ("MakeMoment", "makeMoment"), ("InitMultipart", "initMultipart"), ("PresignParts", "presignParts"), ("CompleteMultipart", "completeMultipart"), ("AbortMultipart", "abortMultipart")].into_iter().map(|(name, root)| {
        let document = format!("mutation {name} {{ {root} {{ id }} }}");
        rig.server.register(&document);
        json!({"name": name, "document": document, "rootPath": root})
    }).collect();
    database.register_operations(json!(operations).to_string()).unwrap();
    database
}

#[tokio::test(start_paused = true)]
async fn resume_does_not_repeat_a_step_owned_by_an_active_upload() {
    use std::future::Future;
    use std::task::{Context, Waker};

    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let database = open(&rig, &directory.path().join("db.sqlite"));
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42", "text": "clip"}}));
    let sync = database.sync.as_ref().unwrap().clone();
    let lost = Arc::new(tokio::sync::Notify::new());
    let notified = lost.clone();
    let paused = sync.clone();
    let server = Arc::downgrade(&rig.server);
    let first = std::sync::atomic::AtomicBool::new(true);
    rig.server.on_request(move || {
        if first.swap(false, std::sync::atomic::Ordering::SeqCst) {
            paused.set_online(false);
            server.upgrade().unwrap().lose_next_answer();
            notified.notify_one();
        }
    });
    let input = UploadInput::new(source.to_str().unwrap(), UploadSteps::new("MakeMoment").single("CreateBlob"));
    sync.upload("Moment", &crate::identity::RowId::new("42").unwrap(), input).await.unwrap();
    lost.notified().await;
    let mut resume = Box::pin(super::encrypted::resume(database.session.clone().unwrap(), Some(sync.clone())));
    assert!(resume.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    sync.set_online(true);
    sync.wait_uploads().await;
    resume.await.unwrap();
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "CreateBlob").count(), 1);
    assert!(database.db.outbox().unwrap().is_empty());
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
        let database = open(&rig, &path);
        database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
        rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42", "text": "clip"},
            "initMultipart": {"key": "key", "uploadId": "upload"}, "presignParts": {"parts": ([1, 2, 3].map(|number| json!({"partNumber": number, "url": "https://upload"})))},
            "completeMultipart": {"signedId": "sid", "blobId": "bid"}, "abortMultipart": {"ok": true}}));
        let input = UploadInput::new(source.to_str().unwrap(), UploadSteps::new("MakeMoment").single("CreateBlob").multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart"));
        let server = Arc::downgrade(&rig.server);
        let seen = std::sync::atomic::AtomicUsize::new(0);
        rig.server.on_request(move || {
            if seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == fault_at {
                server.upgrade().unwrap().refuse_next("ACCOUNT_UNAVAILABLE", false);
            }
        });
        database.upload("Moment".into(), "42".into(), serde_json::to_string(&input).unwrap()).await.unwrap();
        database.sync.as_ref().unwrap().wait_uploads().await;
        let database = if restart { drop(database); open(&rig, &path) } else { database };
        database.session_resume().await.unwrap();
        database.retry("Moment".into(), "42".into()).await.unwrap();
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
    let database = open(&rig, &directory.path().join("db.sqlite"));
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    let input = UploadInput::new(source.to_str().unwrap(), UploadSteps::new("MakeMoment").single("CreateBlob"));
    database.upload("Moment".into(), "42".into(), serde_json::to_string(&input).unwrap()).await.unwrap();
    database.sync.as_ref().unwrap().wait_uploads().await;
    let entry = database.db.outbox().unwrap().remove(0);
    assert_eq!(entry.operation.error.unwrap().kind, crate::syncstate::FailureKind::Session(SessionCode::AccountUnavailable));
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_returns_a_direct_mutation_without_creating_an_upload_owner() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let database = open(&rig, &directory.path().join("db.sqlite"));
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"makeMoment": {"id": "42"}}));
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    database.run("MakeMoment".into(), "{}".into()).await.unwrap_err();
    let answer = database.session_resume().await.unwrap().unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&answer).unwrap()["data"]["makeMoment"]["id"], "42");
    assert!(database.db.outbox().unwrap().is_empty());
    assert_eq!(database.session_resume().await.unwrap(), None);
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "MakeMoment").count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_finishes_a_cancelled_multipart_abort_after_reopen() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"abc").unwrap();
    let database = open(&rig, &path);
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"abortMultipart": {"ok": true}}));
    let input = UploadInput::new(source.to_str().unwrap(), UploadSteps::new("MakeMoment").multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart"));
    let state = crate::upload::UploadState { key: Some("key".into()), upload_id: Some("upload".into()), ..crate::upload::UploadState::queued() };
    let row = crate::identity::RowId::new("42").unwrap();
    database.sync.as_ref().unwrap().inner.begin_upload("Moment", &row, &input, state).unwrap();
    rig.server.refuse_next("ACCOUNT_UNAVAILABLE", false);
    database.cancel_upload("Moment".into(), "42".into()).await.unwrap();
    drop(database);
    let database = open(&rig, &path);
    database.session_resume().await.unwrap();
    database.retry("Moment".into(), "42".into()).await.unwrap();
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
    let database = open(&rig, &directory.path().join("db.sqlite"));
    database.session_call("social".into(), SOCIAL_BODY.into()).await.unwrap();
    rig.server.reply_with(json!({"createBlob": {"url": "https://upload", "signedId": "sid", "blobId": "bid"}, "makeMoment": {"id": "42"}}));
    let server = Arc::downgrade(&rig.server);
    let seen = std::sync::atomic::AtomicUsize::new(0);
    rig.server.on_request(move || match seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
        0 => server.upgrade().unwrap().lose_next_answer(),
        1 => server.upgrade().unwrap().refuse_next("ACCOUNT_UNAVAILABLE", false),
        _ => {},
    });
    let input = UploadInput::new(source.to_str().unwrap(), UploadSteps::new("MakeMoment").single("CreateBlob"));
    database.upload("Moment".into(), "42".into(), serde_json::to_string(&input).unwrap()).await.unwrap();
    database.sync.as_ref().unwrap().wait_uploads().await;
    database.session_resume().await.unwrap();
    database.retry("Moment".into(), "42".into()).await.unwrap();
    database.sync.as_ref().unwrap().wait_uploads().await;
    assert_eq!(rig.server.executed().iter().filter(|body| body["operationName"] == "CreateBlob").count(), 1);
    assert!(database.db.outbox().unwrap().is_empty());
}
