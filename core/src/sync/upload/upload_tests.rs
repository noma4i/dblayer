use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use super::*;

use serde_json::{json, Value};

use crate::commit::{CommitEnvelope, OperationTransition, WriteOrigin};
use crate::operation::OperationStatus;
use crate::identity::RowId;
use crate::pacing::Backoff;
use crate::syncstate::{FailureKind, OperationFailure, SyncState};
use crate::transport::{PutReply, Reply, TransportError};
use crate::upload::{UploadConfig, UploadFile, UploadOperation, UploadOperations, UploadPart, UploadScenario, UploadStage, UploadState, UploadStatus, WaitReason};

use super::super::rig::*;
use super::super::{OperationDef, SyncConfig};
use crate::test_wait::join_within;

pub(super) fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

pub(super) fn small() -> UploadConfig {
    UploadConfig {
        multipart_threshold: 10,
        part_size: 5,
        parallel_parts: 2,
        backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)),
        ..UploadConfig::default()
    }
}

pub(super) async fn await_released<T>(weak: &std::sync::Weak<T>) {
    for _ in 0..5000 {
        if weak.upgrade().is_none() { return; }
        std::thread::sleep(Duration::from_millis(1));
        tokio::task::yield_now().await;
    }
}

pub(super) fn uploads() -> UploadOperations {
    UploadOperations::empty().create_blob("CreateBlob").multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart")
}

pub(super) fn upload_config(config: UploadConfig) -> SyncConfig {
    SyncConfig { upload: config, uploads: Some(uploads()), ..SyncConfig::default() }
}

pub(super) fn register_upload_operations(sync: &Sync) {
    for (name, root) in [
        ("CreateBlob", "createBlob"),
        ("InitMultipart", "initMultipart"),
        ("PresignParts", "presignParts"),
        ("CompleteMultipart", "completeMultipart"),
        ("AbortMultipart", "abortMultipart"),
    ] {
        sync.register(OperationDef::new(name, "operation x { x }", root));
    }
}

pub(super) fn upload_rig(config: UploadConfig) -> Rig {
    let rig = rig_with(upload_config(config));
    register_upload_operations(&rig.sync);
    put(&rig, "moments", moment("m1", "x"));
    put(&rig, "moments", moment("m2", "y"));
    rig
}

pub(super) fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".mp4").tempfile().unwrap();
    file.write_all(bytes).unwrap();
    file
}

pub(super) fn input(file: &tempfile::NamedTempFile) -> UploadFile {
    UploadFile::new(file.path().to_str().unwrap()).variable("text", json!("clip"))
}

pub(super) fn blob() -> Reply {
    Reply::data(json!({"createBlob": {"url": "https://s3/single", "headers": {"x-a": "1"}, "signedId": "sid", "blobId": "b1"}}))
}

fn init() -> Reply {
    Reply::data(json!({"initMultipart": {"key": "k1", "uploadId": "u1"}}))
}

fn presign(urls: &[(u32, &str)]) -> Reply {
    let parts: Vec<Value> = urls.iter().map(|(number, url)| json!({"partNumber": number, "url": url})).collect();
    Reply::data(json!({"presignParts": {"parts": parts}}))
}

fn complete() -> Reply {
    Reply::data(json!({"completeMultipart": {"signedId": "sid", "blobId": "b1"}}))
}

fn abort() -> Reply {
    Reply::data(json!({"abortMultipart": {"ok": true}}))
}

pub(super) fn created(id: &str) -> Reply {
    Reply::data(json!({"createMoment": {"id": id, "text": "clip", "userId": null}}))
}

pub(super) fn multipart_replies(rig: &Rig) {
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("AbortMultipart", abort());
    rig.transport.enqueue("CreateMoment", created("m9"));
}

pub(super) fn single_replies(rig: &Rig) {
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m9"));
}

pub(super) fn variables(rig: &Rig, operation: &str) -> Vec<Value> {
    rig.transport.calls().into_iter().filter(|call| call.operation_name == operation).map(|call| call.variables).collect()
}

pub(super) fn upload_of(rig: &Rig, id: &str) -> Option<crate::upload::UploadInfo> {
    rig.db.sync_info("moments", &rid(id)).unwrap().uploads.get("upload").cloned()
}

pub(super) fn stored_state(rig: &Rig) -> UploadState {
    rig.db.outbox().unwrap().into_iter().find_map(|entry| entry.operation.upload).unwrap()
}

pub(super) const TWELVE: &[u8] = b"0123456789ab";

#[tokio::test(start_paused = true)]
async fn inv_upload_14_a_temp_record_upload_reports_its_size_before_create_and_sends_the_confirmed_id() {
    let rig = upload_rig(small());
    let local = rig.db.as_app().create("moments", params(json!({"text": "hi"}))).unwrap();
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &local.id, "upload", input(&source).variable("id", json!(local.id.as_str())))).await.unwrap();
    let waiting = upload_of(&rig, local.id.as_str()).unwrap();
    assert_eq!((waiting.status, waiting.wait_reason, waiting.total_bytes, waiting.sent_bytes, waiting.fraction()), (UploadStatus::Waiting, Some(WaitReason::CreateUnconfirmed), 6, 0, 0.0));
    assert!(rig.transport.calls().is_empty());
    rig.transport.enqueue("CreateMoment", created("m77"));
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m77"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "CreateMoment")[1], json!({"id": "m77", "text": "clip", "signedId": "sid"}));
    assert!(rig.db.outbox().unwrap().is_empty());
    assert!(upload_of(&rig, "m77").is_none());
}

#[tokio::test(start_paused = true)]
async fn inv_upload_16_complete_failure_keeps_parts_through_queue_restart_and_retry() {
    let rig = upload_rig(small());
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("CompleteMultipart", Reply::fail(TransportError::Http { status: 422, body: "rejected".into() }));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("AbortMultipart", abort());
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = stored_state(&rig);
    assert_eq!(failed.upload_id.as_deref(), Some("u1"));
    assert_eq!((failed.stage, failed.parts.len(), failed.confirmed_bytes), (UploadStage::Complete, 3, 12));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
    rig.transport.enqueue("CreateMoment", created("m9"));
    crate::sync::rig::app(&rig.sync, rig.sync.retry("moments", &rid("m1"), None)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CompleteMultipart"), 2);
    assert_eq!(rig.transport.call_count("InitMultipart"), 1);
    assert_eq!(rig.transport.call_count("PresignParts"), 1);
    assert_eq!(rig.transport.put_calls().len(), 3);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1, "outbox after complete retry: {:?}", rig.db.outbox().unwrap());
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn inv_upload_17_cancel_after_mutation_start_returns_false_without_waiting() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m9").after(Duration::from_secs(1)));
    let source = file(b"abcdef");
    let id = rid("m1");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &id, "upload", input(&source))).await.unwrap();
    rig.transport.await_calls("CreateMoment", 1).await;
    assert!(rig.db.outbox().unwrap()[0].operation.transport_started);
    let mut cancel = Box::pin(crate::sync::rig::app(&rig.sync, rig.sync.cancel_upload("moments", &id, "upload")));
    assert!(matches!(cancel.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Ok(false))));
    drop(cancel);
    tokio::time::advance(Duration::from_secs(1)).await;
    rig.sync.wait_uploads().await;
    assert_eq!(text_of(&rig, "moments", "m9"), Some("clip".into()));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn http_put_failure_does_not_confirm_transfer() {
    use crate::transport::{Transport, GraphQLRequest, Response, ServerSubscription, PutRequest, PutResponse};
    struct HttpStatus(crate::transport::MockTransport, u16);
    #[async_trait::async_trait]
    impl Transport for HttpStatus {
        async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> { self.0.query(request).await }
        async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> { self.0.mutate(request).await }
        async fn subscribe(&self, request: GraphQLRequest) -> Result<ServerSubscription, TransportError> { self.0.subscribe(request).await }
        async fn put(&self, _: PutRequest) -> Result<PutResponse, TransportError> { Ok(PutResponse { status: self.1, etag: None }) }
    }
    for status in [503, 408, 429, 400, 403, 302] {
        let config = UploadConfig { backoff: small().backoff.limited(0), ..small() };
        let rig = upload_rig(config.clone());
        single_replies(&rig);
        let sync = Sync::new(rig.db.clone(), Arc::new(HttpStatus(rig.transport.clone(), status)), Arc::new(rig.clock.clone()), upload_config(config));
        register_operations(&sync);
        register_upload_operations(&sync);
        sync.register(OperationDef::new("CreateBlob", "mutation { createBlob }", "createBlob"));
        let source = file(b"abcdef");
        crate::sync::rig::app(&sync, sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
        sync.wait_uploads().await;
        assert_eq!(rig.transport.call_count("CreateMoment"), 0, "HTTP {status} PUT confirmed transfer");
        assert!(!stored_state(&rig).is_transferred());
    }
}

#[tokio::test(start_paused = true)]
async fn cancel_multipart_offline_returns_and_aborts_after_reconnecting() {
    let rig = upload_rig(small());
    let source = file(TWELVE);
    rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(Vec::new()))).unwrap();
    rig.transport.enqueue("AbortMultipart", abort());
    rig.sync.set_online(false);
    let id = rid("m1");
    let cancelled = tokio::time::timeout(Duration::from_secs(1), crate::sync::rig::app(&rig.sync, rig.sync.cancel_upload("moments", &id, "upload"))).await;
    assert!(cancelled.is_ok(), "offline cancellation waited for connectivity");
    assert!(cancelled.unwrap().unwrap());
    assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
    assert_eq!(stored_state(&rig).attempts(), 0);
    assert_eq!(stored_state(&rig).upload_id.as_deref(), Some("u1"));
    rig.sync.set_online(true);
    rig.transport.await_calls("AbortMultipart", 1).await;
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("AbortMultipart"), 1);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn repeated_expired_grants_wait_before_renewal() {
    let mut observations = Vec::new();
    for multipart in [false, true] {
        let rig = upload_rig(UploadConfig { backoff: small().backoff.limited(2), ..small() });
        let (operation, url) = if multipart { ("PresignParts", "https://s3/p1") } else { ("CreateBlob", "https://s3/single") };
        rig.transport.enqueue("InitMultipart", init());
        rig.transport.enqueue("AbortMultipart", abort());
        for _ in 0..3 {
            rig.transport.enqueue(operation, if multipart { presign(&[(1, url), (2, "https://s3/p2"), (3, "https://s3/p3")]) } else { blob() });
            rig.transport.enqueue_put(url, PutReply::status(403));
        }
        let source = file(if multipart { TWELVE } else { b"abcdef" });
        let started = tokio::time::Instant::now();
        crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
        rig.transport.await_calls(operation, 1).await;
        let before_delay = rig.transport.call_count(operation);
        rig.sync.wait_uploads().await;
        observations.push((multipart, before_delay, rig.transport.call_count(operation), started.elapsed() >= small().backoff.delay(0) + small().backoff.delay(1), stored_state(&rig).attempts()));
        assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    }
    assert_eq!(observations, [(false, 1, 3, true, 2), (true, 1, 3, true, 2)]);
}

#[tokio::test]
async fn wait_uploads_parks_while_resume_owns_the_upload() {
    use std::future::Future;
    use std::task::{Context, Poll, Wake, Waker};
    struct Wakes(std::sync::atomic::AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) { self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
    }
    let rig = upload_rig(small());
    let source = file(b"abcdef");
    let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued())).unwrap();
    let owner = crate::transport::RequestOwner { seq, item_position: 0, stage: "prepare".into() };
    let resume = rig.sync.inner.claim_upload_resume(&owner).await.unwrap().unwrap();
    let wakes = Arc::new(Wakes(std::sync::atomic::AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let (send, receive) = std::sync::mpsc::channel();
    let sync = rig.sync.clone();
    let thread = std::thread::spawn(move || {
        let mut waiting = Box::pin(sync.wait_uploads());
        let first = waiting.as_mut().poll(&mut Context::from_waker(&waker));
        send.send(matches!(first, Poll::Pending)).unwrap();
    });
    let parked = receive.recv_timeout(Duration::from_secs(1));
    assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    let active = locked(&rig.sync.inner.uploads).active.remove(&(seq, 0)).unwrap();
    active.done.send_replace(true);
    join_within(thread);
    rig.db.as_app().discard("moments", &rid("m1"), None).unwrap();
    drop(resume);
    assert_eq!(parked, Ok(true), "wait_uploads did not return Pending");
}

#[tokio::test(start_paused = true)]
async fn retryable_storage_refusal_retries_upload() {
    for code in ["UPLOAD_STORAGE_FAILURE", "IN_PROGRESS", "RATE_LIMITED", "INVALID_RECEIPT", "INVALID_OPERATION", "SERVICE_UNAVAILABLE"] {
        let rig = upload_rig(small());
        let response = serde_json::from_value(json!({"errors":[{"message":"retry", "extensions":{"code":code,"retryable":true}}]})).unwrap();
        rig.transport.enqueue("CreateBlob", Reply::response(response));
        single_replies(&rig);
        rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
        let source = file(b"abcdef");
        let started = tokio::time::Instant::now();
        crate::sync::rig::app(&rig.sync, rig.sync.upload_create("moments", "upload", params(json!({"text":"clip"})), input(&source))).await.unwrap();
        rig.sync.wait_uploads().await;
        assert_eq!(rig.transport.call_count("CreateBlob"), 2, "{code}");
        assert!(started.elapsed() >= Duration::from_secs(1));
        assert_eq!(rig.transport.put_count("https://s3/single"), 1);
        assert_eq!(rig.transport.call_count("CreateMoment"), 1);
        assert_eq!(text_of(&rig, "moments", "m9"), Some("clip".to_string()));
        assert!(rig.db.outbox().unwrap().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn uploads_use_the_declared_result_of_each_content_mutation() {
    let mut chat = crate::schema::ModelSpec::new("Chat", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::scalar("text", crate::value::ScalarKind::Str)]);
    for (name, mutation, root) in [("create_photo", "ChatGroupCreateFile", "groupChatCreate.chat"), ("update_photo", "ChatGroupUpdateFile", "groupChatUpdate.chat")] {
        let mut action = crate::schema::ActionDeclaration::new(name, crate::schema::RootShape::Upsert);
        action.mutation = Some(mutation.into());
        action.upload = true;
        action.root = Some(root.into());
        chat.actions.push(action);
    }
    let db = crate::db::Db::open_in_memory(vec![chat], None).unwrap();
    let rig = rig_on(db, crate::transport::MockTransport::new(), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    rig.sync.register(OperationDef::new("CreateBlob", "mutation B { createBlob }", "createBlob"));
    let source = file(b"photo");
    for (mutation, root, title) in [("ChatGroupCreateFile", "groupChatCreate", "created"), ("ChatGroupUpdateFile", "groupChatUpdate", "updated")] {
        rig.sync.register(OperationDef::new(mutation, "mutation C { chat }", root));
        rig.transport.enqueue("CreateBlob", blob());
        rig.transport.enqueue(mutation, Reply::data(json!({root: {"chat": {"id": "c1", "text": title}}})));
        let input = UploadFile::new(source.path().to_str().unwrap());
        if title == "created" {
            crate::sync::rig::app(&rig.sync, rig.sync.upload_create("Chat", "create_photo", params(json!({"text": "pending"})), input)).await.unwrap();
        } else {
            crate::sync::rig::app(&rig.sync, rig.sync.upload("Chat", &rid("c1"), "update_photo", input)).await.unwrap();
        }
        rig.sync.wait_uploads().await;
        assert_eq!(text_of(&rig, "Chat", "c1"), Some(title.into()), "{mutation}");
        assert!(rig.db.outbox().unwrap().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn idempotency_upload_final_mutation_keeps_its_key_on_retry() {
    let mut models = keeping_specs();
    let action = &mut models[1].actions[0];
    action.wrap = Some("input".into());
    let mut encoded = serde_json::to_value(&action).unwrap();
    encoded["client_mutation_id"] = json!(true);
    *action = serde_json::from_value(encoded).unwrap();
    let db = crate::db::Db::open_in_memory(models, None).unwrap();
    let rig = rig_on(db, crate::transport::MockTransport::new(), SyncConfig { uploads: Some(UploadOperations::empty().create_blob("CreateBlob")), ..SyncConfig::default() });
    rig.sync.register(OperationDef::new("CreateBlob", "mutation B { createBlob }", "createBlob"));
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Interrupted("lost response".into())));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"photo");
    let row = crate::sync::rig::app(&rig.sync, rig.sync.upload_create("moments", "upload", params(json!({"text": "clip"})), input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    crate::sync::rig::app(&rig.sync, rig.sync.retry("moments", &row, None)).await.unwrap();
    rig.sync.wait_uploads().await;
    let calls = variables(&rig, "CreateMoment");
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
    assert!(calls[0]["input"]["clientMutationId"].as_str().is_some());
    assert_eq!(calls[0]["input"]["signedId"], "sid");
    assert!(variables(&rig, "CreateBlob")[0].get("clientMutationId").is_none());
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn dropping_sync_stops_uploads_before_the_next_request() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob().after(Duration::from_secs(60)));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.transport.await_calls("CreateBlob", 1).await;
    let inner = Arc::downgrade(&rig.sync.inner);
    drop(rig.sync);
    await_tasks_ended().await;
    await_released(&inner).await;
    assert!(inner.upgrade().is_none());
    tokio::time::sleep(Duration::from_secs(61)).await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
}

#[tokio::test(start_paused = true)]
async fn single_upload_creates_puts_and_sends_the_mutation_once() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    let puts = rig.transport.put_calls();
    assert_eq!((puts.len(), puts[0].url.as_str(), puts[0].body.as_slice()), (1, "https://s3/single", b"abcdef".as_slice()));
    assert_eq!(puts[0].headers, [("x-a".to_string(), "1".to_string())]);
    let create = &variables(&rig, "CreateBlob")[0];
    assert_eq!((create["filename"].as_str().unwrap().ends_with(".mp4"), &create["contentType"], &create["byteSize"]), (true, &json!("video/mp4"), &json!(6)));
    assert_eq!(create["checksum"], json!("6AtQFwmJUPxYqtg8jBSXjg=="));
    assert_eq!(variables(&rig, "CreateMoment"), [json!({"text": "clip", "signedId": "sid"})]);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(upload_of(&rig, "m1"), None);
    assert_eq!(text_of(&rig, "moments", "m9"), Some("clip".to_string()));
}

async fn await_upload_finished(rig: &Rig) {
    for _ in 0..5000 {
        if rig.db.outbox().unwrap().is_empty() && locked(&rig.sync.inner.uploads).active.is_empty() { return; }
        std::thread::sleep(Duration::from_millis(1));
        tokio::task::yield_now().await;
    }
    panic!("the upload did not finish");
}

#[tokio::test(start_paused = true)]
async fn inv_upload_12_finished_upload_tasks_are_reaped_by_the_next_launch_without_wait_uploads() {
    const UPLOADS: usize = 40;
    let rig = upload_rig(small());
    for _ in 0..UPLOADS {
        single_replies(&rig);
        let source = file(b"abcdef");
        crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
        await_upload_finished(&rig).await;
    }
    let runs = locked(&rig.sync.inner.uploads);
    assert!(runs.tasks.len() <= 3, "{} finished tasks are held after {UPLOADS} uploads", runs.tasks.len());
    assert!(runs.active.is_empty() && runs.ready.is_empty());
}

#[tokio::test(start_paused = true)]
async fn observers_and_reads_see_the_upload_state_and_bytes_while_it_runs() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(4)));
    type Seen = Vec<Option<(UploadStatus, UploadStage, u64)>>;
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let sink = seen.clone();
    let _observer = rig
        .db
        .observe_row("moments", &rid("m1"), move |change| {
            let upload = change.rows.first().and_then(|row| row.sync.uploads.get("upload")).map(|upload| (upload.status, upload.stage, upload.sent_bytes));
            sink.lock().unwrap().push(upload);
        })
        .unwrap();
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let running = upload_of(&rig, "m1").unwrap();
    assert_eq!((running.status, running.stage, running.total_bytes), (UploadStatus::Uploading, UploadStage::Put, 6));
    assert!(running.sent_bytes >= 1 && running.fraction() > 0.0);
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().state, SyncState::Sending);
    let last = seen.lock().unwrap().last().cloned().unwrap().unwrap();
    assert_eq!((last.0, last.1), (UploadStatus::Uploading, UploadStage::Put));
    rig.sync.wait_uploads().await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(seen.lock().unwrap().last().cloned().unwrap(), None);
}

#[tokio::test(start_paused = true)]
async fn multipart_runs_init_presign_parts_and_complete_in_part_order() {
    let rig = upload_rig(small());
    multipart_replies(&rig);
    for url in ["https://s3/p1", "https://s3/p2", "https://s3/p3"] {
        rig.transport.enqueue_put(url, PutReply::etag(&format!("e-{url}")).after(Duration::from_secs(2)));
    }
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    let init_calls = variables(&rig, "InitMultipart");
    assert_eq!((init_calls.len(), &init_calls[0]["checksum"]), (1, &json!("tgxSv0hJBn8LV8i9MJhUZg==")));
    assert_eq!(variables(&rig, "PresignParts"), [json!({"key": "k1", "uploadId": "u1", "partNumbers": [1, 2, 3]})]);
    let mut bodies: Vec<_> = rig.transport.put_calls().into_iter().map(|call| (call.url, call.body)).collect();
    bodies.sort();
    assert_eq!(
        bodies,
        [
            ("https://s3/p1".to_string(), b"01234".to_vec()),
            ("https://s3/p2".to_string(), b"56789".to_vec()),
            ("https://s3/p3".to_string(), b"ab".to_vec()),
        ]
    );
    let sent = &variables(&rig, "CompleteMultipart")[0];
    assert_eq!(
        sent["parts"],
        json!([{"partNumber": 1, "etag": "e-https://s3/p1"}, {"partNumber": 2, "etag": "e-https://s3/p2"}, {"partNumber": 3, "etag": "e-https://s3/p3"}])
    );
    assert_eq!(rig.transport.parallel_puts_peak(), 2);
    assert_eq!(variables(&rig, "CreateMoment").len(), 1);
    assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn one_parallel_part_means_sequential_mode() {
    let rig = upload_rig(UploadConfig { parallel_parts: 1, ..small() });
    multipart_replies(&rig);
    for url in ["https://s3/p1", "https://s3/p2", "https://s3/p3"] {
        rig.transport.enqueue_put(url, PutReply::etag("e").after(Duration::from_secs(2)));
    }
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.parallel_puts_peak(), 1);
    assert_eq!(rig.transport.put_calls().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn a_fatal_part_aborts_the_multipart_and_retry_starts_over_without_the_mutation_in_between() {
    let rig = upload_rig(small());
    multipart_replies(&rig);
    rig.transport.enqueue_put("https://s3/p2", PutReply::status(400));
    rig.transport.enqueue_put("https://s3/p2", PutReply::etag("e2"));
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage), (UploadStatus::Failed, UploadStage::Part));
    assert!(!failed.is_retryable());
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    let state = stored_state(&rig);
    assert_eq!((state.upload_id, state.parts.len(), state.sent_bytes), (None, 0, 0));
    crate::sync::rig::app(&rig.sync, rig.sync.retry("moments", &rid("m1"), None)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("InitMultipart"), 2);
    assert_eq!(rig.transport.call_count("CompleteMultipart"), 1);
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn transient_errors_back_off_and_retry_the_put() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::status(503));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    let source = file(b"abcdef");
    let started = tokio::time::Instant::now();
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_count("https://s3/single"), 2);
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn running_out_of_attempts_fails_with_a_retryable_error_and_the_attempt_count() {
    let rig = upload_rig(UploadConfig { backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).limited(2), ..small() });
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::status(503));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_count("https://s3/single"), 3);
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage, failed.attempts), (UploadStatus::Failed, UploadStage::Put, 2));
    assert_eq!(failed.error.as_ref().map(|error| error.kind), Some(FailureKind::Server));
    assert!(failed.is_retryable());
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
}

#[tokio::test(start_paused = true)]
async fn inv_upload_09_offline_waits_without_spending_attempts() {
    let rig = upload_rig(UploadConfig { backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).limited(0), ..small() });
    single_replies(&rig);
    rig.sync.set_online(false);
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(100)).await;
    assert_eq!(rig.transport.call_count("CreateBlob"), 0);
    let waiting = upload_of(&rig, "m1").unwrap();
    assert_eq!((waiting.status, waiting.wait_reason, waiting.attempts), (UploadStatus::Waiting, Some(WaitReason::Offline), 0));
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().state, SyncState::Pending);
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateBlob"), 1);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_offline_wait_ends_with_the_network_and_the_upload_shows_sending() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(8)));
    rig.sync.set_online(false);
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.transport.call_count("CreateBlob"), 0);
    rig.sync.set_online(true);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let running = upload_of(&rig, "m1").unwrap();
    assert_eq!((running.status, running.wait_reason), (UploadStatus::Uploading, None));
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().state, SyncState::Sending);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateBlob"), 1);
}

#[tokio::test(start_paused = true)]
async fn inv_upload_09_a_network_error_while_offline_is_not_an_attempt() {
    let rig = upload_rig(UploadConfig { backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).limited(0), ..small() });
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::fail(TransportError::Network("lost".into())).after(Duration::from_secs(8)));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    rig.sync.set_online(false);
    tokio::time::sleep(Duration::from_secs(60)).await;
    let waiting = upload_of(&rig, "m1").unwrap();
    assert_eq!((waiting.status, waiting.attempts), (UploadStatus::Waiting, 0));
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_count("https://s3/single"), 2);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_expired_part_url_gets_a_fresh_presign_for_that_part_only() {
    let rig = upload_rig(UploadConfig { parallel_parts: 1, ..small() });
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("PresignParts", presign(&[(2, "https://s3/p2b")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("CreateMoment", created("m9"));
    rig.transport.enqueue_put("https://s3/p2", PutReply::status(403));
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "PresignParts")[1], json!({"key": "k1", "uploadId": "u1", "partNumbers": [2]}));
    assert_eq!((rig.transport.put_count("https://s3/p2"), rig.transport.put_count("https://s3/p2b")), (1, 1));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn urls_older_than_the_presign_ttl_are_requested_again_for_unfinished_parts() {
    let rig = upload_rig(UploadConfig { parallel_parts: 1, ..small() });
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("PresignParts", presign(&[(2, "https://s3/p2b"), (3, "https://s3/p3b")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("CreateMoment", created("m9"));
    rig.transport.enqueue_put("https://s3/p1", PutReply::etag("e1").after(Duration::from_secs(8)));
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    rig.clock.advance(Duration::from_secs(51 * 60));
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "PresignParts")[1]["partNumbers"], json!([2, 3]));
    assert_eq!((rig.transport.put_count("https://s3/p2"), rig.transport.put_count("https://s3/p2b"), rig.transport.put_count("https://s3/p3b")), (0, 1, 1));
}

#[tokio::test(start_paused = true)]
async fn inv_upload_02_an_unanswered_mutation_is_delivery_unknown_never_repeated_and_retry_skips_the_upload() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    let unknown = upload_of(&rig, "m1").unwrap();
    assert_eq!((unknown.status, unknown.stage), (UploadStatus::DeliveryUnknown, UploadStage::Mutation));
    let state = stored_state(&rig);
    assert_eq!((state.signed_id.as_deref(), state.blob_id.as_deref()), (Some("sid"), Some("b1")));
    crate::sync::rig::app(&rig.sync, rig.sync.retry("moments", &rid("m1"), None)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!((rig.transport.call_count("CreateBlob"), rig.transport.put_calls().len(), rig.transport.call_count("CreateMoment")), (1, 1, 2));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_unreached_mutation_waits_pending_and_is_sent_again_without_a_new_transfer() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Network("no connection".into())));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    let waiting = rig.db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((waiting.state, waiting.attempts), (crate::syncstate::SyncState::Pending, 1));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    rig.clock.advance(Duration::from_secs(2));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!((rig.transport.call_count("CreateBlob"), rig.transport.put_calls().len(), rig.transport.call_count("CreateMoment")), (1, 1, 2));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn inv_upload_13_the_mutation_waits_offline_without_a_call_and_goes_out_when_online() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(8)));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    rig.sync.set_online(false);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().attempts, 0);
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_rejected_mutation_fails_without_repeating_and_discard_removes_the_operation() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::graphql_error("nope", "FORBIDDEN"));
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage, failed.is_retryable()), (UploadStatus::Failed, UploadStage::Mutation, false));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert_eq!(rig.db.as_app().discard("moments", &rid("m1"), None).unwrap(), 1);
    assert_eq!(upload_of(&rig, "m1"), None);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn graphql_extensions_survive_upload_step_failures() {
    use crate::transport::{GraphqlExtensions, TransactionDisposition};
    let fields = GraphqlExtensions { code: Some("DUPLICATE_CLIENT_KEY".into()), transaction_disposition: Some(TransactionDisposition::Keep), fields: Default::default() };
    for (reply, expected) in [
        (Reply::graphql_error("nope", "DUPLICATE_CLIENT_KEY"), GraphqlExtensions { code: Some("DUPLICATE_CLIENT_KEY".into()), transaction_disposition: None, fields: Default::default() }),
        (Reply::fail(TransportError::Graphql { messages: vec!["nope".into()], extensions: Some(fields.clone()) }), fields),
    ] {
        let rig = upload_rig(small());
        rig.transport.enqueue("CreateBlob", reply);
        let source = file(b"abcdef");
        crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
        rig.sync.wait_uploads().await;
        let failed = upload_of(&rig, "m1").unwrap();
        assert_eq!(failed.error.unwrap().extensions, Some(expected));
        assert_eq!(rig.transport.call_count("CreateBlob"), 1);
        assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn cancel_stops_the_transfer_aborts_the_multipart_and_skips_the_mutation() {
    let rig = upload_rig(small());
    multipart_replies(&rig);
    for url in ["https://s3/p1", "https://s3/p2", "https://s3/p3"] {
        rig.transport.enqueue_put(url, PutReply::etag("e").after(Duration::from_secs(60)));
    }
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(crate::sync::rig::app(&rig.sync, rig.sync.cancel_upload("moments", &rid("m1"), "upload")).await.unwrap());
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(upload_of(&rig, "m1"), None);
    rig.sync.wait_uploads().await;
    assert!(!crate::sync::rig::app(&rig.sync, rig.sync.cancel_upload("moments", &rid("m1"), "upload")).await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn inv_upload_10_files_at_a_time_are_limited_and_the_waiting_one_is_queued() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(10)));
    let (first, second) = (file(b"abcdef"), file(b"ghijkl"));
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&first))).await.unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m2"), "upload", input(&second))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(upload_of(&rig, "m1").unwrap().status, UploadStatus::Uploading);
    assert_eq!(upload_of(&rig, "m2").unwrap().status, UploadStatus::Queued);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.parallel_puts_peak(), 1);
    assert_eq!(rig.transport.call_count("CreateMoment"), 2);

    let wide = upload_rig(UploadConfig { max_files: 2, ..small() });
    single_replies(&wide);
    wide.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(10)));
    crate::sync::rig::app(&wide.sync, wide.sync.upload("moments", &rid("m1"), "upload", input(&first))).await.unwrap();
    crate::sync::rig::app(&wide.sync, wide.sync.upload("moments", &rid("m2"), "upload", input(&second))).await.unwrap();
    wide.sync.wait_uploads().await;
    assert_eq!(wide.transport.parallel_puts_peak(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_second_upload_for_the_same_record_is_refused() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    assert!(crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.is_err());
    rig.sync.wait_uploads().await;
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_source_fails_at_prepare_without_touching_the_transport() {
    let rig = upload_rig(small());
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", UploadFile::new("/nonexistent/clip.mp4"))).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage, failed.is_retryable()), (UploadStatus::Failed, UploadStage::Prepare, false));
    assert!(rig.transport.calls().is_empty());
}

fn resumable(parts: Vec<UploadPart>) -> UploadState {
    UploadState {
        phase: UploadStatus::Uploading,
        stage: UploadStage::Part,
        scenario: Some(UploadScenario::Multipart),
        total_bytes: 12,
        sent_bytes: 5,
        key: Some("k1".to_string()),
        upload_id: Some("u1".to_string()),
        parts,
        ..UploadState::queued()
    }
}

#[tokio::test(start_paused = true)]
async fn inv_upload_07_a_stored_multipart_resumes_from_its_finished_parts_after_a_restart() {
    let rig = upload_rig(small());
    rig.transport.enqueue("PresignParts", presign(&[(2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(TWELVE);
    let state = resumable(vec![UploadPart { number: 1, etag: "e1".to_string(), confirmed_bytes: 5 }]);
    rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), state)).unwrap();
    let report = crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    assert!(report.operations.is_empty());
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("InitMultipart"), 0);
    assert_eq!(variables(&rig, "PresignParts"), [json!({"key": "k1", "uploadId": "u1", "partNumbers": [2, 3]})]);
    assert_eq!(rig.transport.put_calls().len(), 2);
    assert_eq!(variables(&rig, "CompleteMultipart")[0]["parts"][0], json!({"partNumber": 1, "etag": "e1"}));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_orphaned_multipart_of_a_failed_upload_is_aborted_on_start() {
    let rig = upload_rig(small());
    rig.transport.enqueue("AbortMultipart", abort());
    let source = file(TWELVE);
    let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(Vec::new()))).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, rig.db.generation());
    envelope.operation_transitions.push(OperationTransition::Fail {
        seq,
        status: OperationStatus::Failed,
        failure: OperationFailure::new(FailureKind::Network, "abort lost"),
    });
    rig.db.shared.store.commit(envelope).unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(stored_state(&rig).upload_id, None);
    assert_eq!(upload_of(&rig, "m1").unwrap().status, UploadStatus::Failed);
}

#[tokio::test]
async fn inv_upload_01_parallel_uploads_for_one_record_leave_exactly_one_open() {
    let rig = upload_rig(small());
    for round in 0..30 {
        let id = rid(&format!("r{round}"));
        put(&rig, "moments", moment(id.as_str(), "x"));
        let barrier = Arc::new(crate::test_wait::Rendezvous::new(8));
        let accepted = (0..8)
            .map(|_| {
                let (inner, id, barrier) = (rig.sync.inner.clone(), id.clone(), barrier.clone());
                std::thread::spawn(move || {
                    let input = UploadFile::new("/nonexistent/file.bin");
                    barrier.meet();
                    inner.db.in_current_epoch(|_| inner.begin_upload(inner.db.generation(), "moments", &id, "upload", &input, UploadState::queued())).is_ok()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| join_within(handle))
            .filter(|accepted| *accepted)
            .count();
        assert_eq!(accepted, 1, "round {round}");
    }
}

#[tokio::test(start_paused = true)]
async fn inv_upload_06_a_retry_that_lands_while_the_old_run_is_ending_still_starts_the_upload() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), UploadState::queued())).unwrap();
    let (run, _) = rig.sync.inner.register(seq, 0).unwrap();
    rig.sync.inner.start_uploads().unwrap();
    rig.sync.inner.finish_run(seq, 0, run);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn inv_upload_05_a_cancel_whose_abort_fails_keeps_the_upload_id_until_the_abort_goes_through() {
    let rig = upload_rig(small());
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("AbortMultipart", Reply::fail(TransportError::Network("down".into())));
    rig.transport.enqueue("AbortMultipart", abort());
    for url in ["https://s3/p1", "https://s3/p2", "https://s3/p3"] {
        rig.transport.enqueue_put(url, PutReply::etag("e").after(Duration::from_secs(60)));
    }
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    crate::sync::rig::app(&rig.sync, rig.sync.cancel_upload("moments", &rid("m1"), "upload")).await.unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("AbortMultipart"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_held_create_shows_its_row_at_once_and_one_mutation_swaps_the_temp_id() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    let temp = crate::sync::rig::app(&rig.sync, rig.sync.upload_create("moments", "upload", params(json!({"text": "clip"})), input(&source))).await.unwrap();
    assert!(temp.is_temp());
    assert_eq!(text_of(&rig, "moments", temp.as_str()), Some("clip".to_string()));
    let queued: Vec<String> = rig.db.outbox().unwrap().into_iter().map(|entry| entry.operation.action_key).collect();
    assert_eq!(queued, [Operation::upload_action_key("moments", "upload")]);
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "CreateMoment"), [json!({"text": "clip", "signedId": "sid"})]);
    assert_eq!(text_of(&rig, "moments", temp.as_str()), None);
    assert_eq!(text_of(&rig, "moments", "m9"), Some("clip".to_string()));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_failed_held_create_keeps_its_row_and_retry_sends_once_without_a_second_row() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue_put("https://s3/single", PutReply::status(400));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    single_replies(&rig);
    let source = file(b"abcdef");
    let temp = crate::sync::rig::app(&rig.sync, rig.sync.upload_create("moments", "upload", params(json!({"text": "clip"})), input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(upload_of(&rig, temp.as_str()).map(|failed| failed.status), Some(UploadStatus::Failed));
    assert_eq!(text_of(&rig, "moments", temp.as_str()), Some("clip".to_string()));
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    crate::sync::rig::app(&rig.sync, rig.sync.retry("moments", &temp, None)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert_eq!(text_of(&rig, "moments", temp.as_str()), None);
    assert_eq!(text_of(&rig, "moments", "m9"), Some("clip".to_string()));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn discarding_a_failed_held_create_removes_the_row_and_sends_nothing() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue_put("https://s3/single", PutReply::status(400));
    let source = file(b"abcdef");
    let temp = crate::sync::rig::app(&rig.sync, rig.sync.upload_create("moments", "upload", params(json!({"text": "clip"})), input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.db.as_app().discard("moments", &temp, None).unwrap(), 1);
    assert_eq!(text_of(&rig, "moments", temp.as_str()), None);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn declared_roots_override_the_registered_roots_of_both_transfer_scenarios() {
    let uploads = UploadOperations {
        create_blob: Some(UploadOperation::rooted("CreateBlob", "uploadCreate.directUpload")),
        multipart: Some(crate::upload::MultipartOperations {
            initiate: UploadOperation::rooted("InitMultipart", "multipart.init"),
            presign: UploadOperation::rooted("PresignParts", "multipart.presign"),
            complete: UploadOperation::rooted("CompleteMultipart", "multipart.complete"),
            abort: UploadOperation::new("AbortMultipart"),
        }),
    };
    let rig = rig_with(SyncConfig { upload: small(), uploads: Some(uploads), ..SyncConfig::default() });
    for name in ["CreateBlob", "InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart"] {
        rig.sync.register(OperationDef::new(name, "operation x { x }", "wrong"));
    }
    put(&rig, "moments", moment("m1", "x"));
    put(&rig, "moments", moment("m2", "y"));
    rig.transport.enqueue(
        "CreateBlob",
        Reply::data(json!({"uploadCreate": {"directUpload": {"url": "https://s3/single", "headers": "{\"x-a\":\"1\"}", "signedId": "sid", "blobId": "b1"}}})),
    );
    rig.transport.enqueue("InitMultipart", Reply::data(json!({"multipart": {"init": {"key": "k1", "uploadId": "u1"}}})));
    rig.transport.enqueue("PresignParts", Reply::data(json!({"multipart": {"presign": {"parts": [{"partNumber": 1, "url": "https://s3/p1"}, {"partNumber": 2, "url": "https://s3/p2"}, {"partNumber": 3, "url": "https://s3/p3"}]}}})));
    rig.transport.enqueue("CompleteMultipart", Reply::data(json!({"multipart": {"complete": {"signedId": "sid", "blobId": "b1"}}})));
    rig.transport.enqueue("CreateMoment", created("m9"));
    rig.transport.enqueue("CreateMoment", created("m9"));
    for (id, bytes) in [("m1", &b"abcdef"[..]), ("m2", TWELVE)] {
        let source = file(bytes);
        crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid(id), "upload", input(&source))).await.unwrap();
        rig.sync.wait_uploads().await;
    }
    let puts = rig.transport.put_calls();
    assert_eq!(puts.first().map(|put| put.headers.clone()), Some(vec![("x-a".to_string(), "1".to_string())]));
    assert_eq!((puts.len(), rig.transport.call_count("CreateMoment")), (4, 2));
}

fn fail_open_upload(rig: &Rig, seq: i64) {
    fail_open_upload_as(rig, seq, "lost");
}

fn fail_open_upload_as(rig: &Rig, seq: i64, message: &str) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, rig.db.generation());
    envelope.operation_transitions.push(OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: OperationFailure::new(FailureKind::Network, message) });
    rig.db.shared.store.commit(envelope).unwrap();
}

fn orphan_count(rig: &Rig) -> i64 {
    rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT COUNT(*) FROM upload_orphan", [], |row| row.get::<_, i64>(0))?)).unwrap()
}

fn confirmed_parts() -> Vec<UploadPart> {
    [(1, 5), (2, 5), (3, 2)].map(|(number, bytes)| UploadPart { number, etag: format!("e{number}"), confirmed_bytes: bytes }).to_vec()
}

#[tokio::test(start_paused = true)]
async fn inv_upload_19_discarding_a_failed_multipart_aborts_it_once_with_or_without_confirmed_parts() {
    for (name, parts) in [("unconfirmed", Vec::new()), ("all confirmed", confirmed_parts())] {
        let rig = upload_rig(small());
        rig.transport.enqueue("AbortMultipart", abort());
        let source = file(TWELVE);
        let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(parts))).unwrap();
        fail_open_upload(&rig, seq);
        assert_eq!(rig.db.as_app().discard("moments", &rid("m1"), None).unwrap(), 1, "{name}");
        assert!(rig.db.outbox().unwrap().is_empty(), "{name}");
        assert_eq!(orphan_count(&rig), 1, "{name}: the abort must outlive the operation");
        crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.unwrap();
        crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.unwrap();
        assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})], "{name}");
        assert_eq!(rig.transport.call_count("CompleteMultipart"), 0, "{name}");
        assert_eq!(orphan_count(&rig), 0, "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_orphaned_abort_waits_offline_and_is_sent_when_online() {
    let rig = upload_rig(small());
    rig.transport.enqueue("AbortMultipart", abort());
    let source = file(TWELVE);
    let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(Vec::new()))).unwrap();
    fail_open_upload(&rig, seq);
    rig.db.as_app().discard("moments", &rid("m1"), None).unwrap();
    rig.sync.set_online(false);
    crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.unwrap();
    assert_eq!((rig.transport.call_count("AbortMultipart"), orphan_count(&rig)), (0, 1));
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.unwrap();
    assert_eq!((rig.transport.call_count("AbortMultipart"), orphan_count(&rig)), (1, 0));
}

#[tokio::test(start_paused = true)]
async fn inv_upload_20_a_failed_upload_older_than_seven_days_is_aborted_and_removed_without_a_timer() {
    for (name, parts) in [("unconfirmed", Vec::new()), ("all confirmed", confirmed_parts())] {
        let rig = upload_rig(small());
        rig.transport.enqueue("AbortMultipart", abort());
        let source = file(TWELVE);
        let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(parts))).unwrap();
        fail_open_upload(&rig, seq);
        rig.clock.advance(Duration::from_secs(7 * 24 * 3600 - 3600));
        crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.unwrap();
        assert_eq!((rig.db.outbox().unwrap().len(), rig.transport.call_count("AbortMultipart")), (1, 0), "{name}: not yet due");
        rig.clock.advance(Duration::from_secs(2 * 3600));
        crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.unwrap();
        assert!(rig.db.outbox().unwrap().is_empty(), "{name}");
        assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})], "{name}");
        assert_eq!(rig.transport.call_count("CompleteMultipart"), 0, "{name}");
        assert_eq!(orphan_count(&rig), 0, "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn upload_housekeeping_runs_at_the_first_flush_and_when_coming_online_never_on_a_timer() {
    let rig = upload_rig(small());
    rig.transport.enqueue("AbortMultipart", abort());
    rig.transport.enqueue("AbortMultipart", abort());
    let source = file(TWELVE);
    let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(Vec::new()))).unwrap();
    fail_open_upload(&rig, seq);
    rig.clock.advance(Duration::from_secs(8 * 24 * 3600));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(rig.db.outbox().unwrap().len(), 1, "nothing runs without an event");
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.wait_uploads().await;
    assert!(rig.db.outbox().unwrap().is_empty(), "boot flush");
    assert_eq!(rig.transport.call_count("AbortMultipart"), 1);
    let source = file(TWELVE);
    let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m2"), "upload", &input(&source), resumable(Vec::new()))).unwrap();
    fail_open_upload(&rig, seq);
    rig.clock.advance(Duration::from_secs(8 * 24 * 3600));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.db.outbox().unwrap().len(), 1, "a second flush is not an event");
    rig.sync.set_online(false);
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert!(rig.db.outbox().unwrap().is_empty(), "online transition");
    assert_eq!(rig.transport.call_count("AbortMultipart"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_finished_multipart_upload_never_records_an_abort() {
    let rig = upload_rig(small());
    multipart_replies(&rig);
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(TWELVE);
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&source))).await.unwrap();
    rig.sync.wait_uploads().await;
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!((orphan_count(&rig), rig.transport.call_count("AbortMultipart")), (0, 0));
    crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.unwrap();
    assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
}

#[tokio::test(start_paused = true)]
async fn committing_an_upload_whose_multipart_id_is_still_stored_never_records_an_abort() {
    let rig = upload_rig(small());
    let source = file(TWELVE);
    let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(confirmed_parts()))).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, rig.db.generation());
    envelope.operation_transitions.push(OperationTransition::Close { seq, status: OperationStatus::Committed });
    rig.db.shared.store.commit(envelope).unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(orphan_count(&rig), 0);
}

#[tokio::test(start_paused = true)]
async fn a_panicking_upload_task_releases_its_file_slot_and_fails_only_its_own_operation() {
    let rig = upload_rig(small());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    rig.transport.respond_with(move |request| {
        if request.operation_name == "CreateBlob" && seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 { panic!("transport bug"); }
        None
    });
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m2"));
    let (first, second) = (file(b"first"), file(b"second"));
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", input(&first))).await.unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m2"), "upload", input(&second))).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), rig.sync.wait_uploads()).await.expect("a panicked upload kept the only file slot");
    assert_eq!(upload_of(&rig, "m1").map(|upload| upload.status), Some(UploadStatus::Failed));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert_eq!(locked(&rig.sync.inner.uploads).active_files, 0);
}

#[tokio::test(start_paused = true)]
async fn one_unreadable_upload_does_not_lose_the_others_waiting_to_start() {
    let rig = upload_rig(UploadConfig { max_files: 2, ..small() });
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m2"));
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m1"));
    let (first, second) = (file(b"first"), file(b"second"));
    rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&first), UploadState::queued())).unwrap();
    rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m2"), "upload", &input(&second), UploadState::queued())).unwrap();
    rig.db.shared.store.lock().connection.execute("INSERT INTO op_error (seq, item_position, node, parent, pos, name, kind, text) SELECT seq, position, 0, -1, 0, NULL, 4, 'bad' FROM op_item WHERE row_id = 'm1'", []).unwrap();
    assert!(rig.sync.inner.start_uploads().is_err());
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1, "the readable upload must start in the same pass");
    rig.db.shared.store.lock().connection.execute("DELETE FROM op_error", []).unwrap();
    rig.sync.inner.start_uploads().unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 2, "the unreadable upload must be retried once it reads");
}

#[tokio::test(start_paused = true)]
async fn a_recorded_abort_survives_closing_and_reopening_the_database_and_waits_for_its_operation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("1.sqlite");
    let source = file(TWELVE);
    {
        let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), upload_config(small()));
        register_upload_operations(&rig.sync);
        put(&rig, "moments", moment("m1", "x"));
        let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(confirmed_parts()))).unwrap();
        fail_open_upload(&rig, seq);
        rig.db.as_app().discard("moments", &rid("m1"), None).unwrap();
        assert_eq!(orphan_count(&rig), 1);
        rig.sync.wait_uploads().await;
    }
    let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), SyncConfig { upload: small(), ..SyncConfig::default() });
    assert_eq!(orphan_count(&rig), 1, "the abort must survive the restart");
    assert!(crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.is_err(), "the abort operation is not registered");
    assert_eq!(orphan_count(&rig), 1);
    rig.sync.register(OperationDef::new("AbortMultipart", "operation x { x }", "abortMultipart"));
    rig.transport.enqueue("AbortMultipart", abort());
    crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping()).await.unwrap();
    assert_eq!((variables(&rig, "AbortMultipart"), orphan_count(&rig)), (vec![json!({"key": "k1", "uploadId": "u1"})], 0));
}

#[tokio::test(start_paused = true)]
async fn expiry_skips_an_upload_that_was_retried_or_removed_after_it_was_selected() {
    let rig = upload_rig(small());
    let source = file(TWELVE);
    let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), "upload", &input(&source), resumable(Vec::new()))).unwrap();
    fail_open_upload(&rig, seq);
    rig.clock.advance(Duration::from_secs(8 * 24 * 3600));
    let due = rig.sync.inner.now_ms() - 7 * 24 * 3600 * 1000;
    let stale = rig.db.failed_upload_candidates(due).unwrap();
    assert_eq!(stale.len(), 1);
    rig.db.as_app().retry("moments", &rid("m1"), None).unwrap();
    assert_eq!(rig.db.as_app().expire_candidates(stale.clone(), due, &[]).unwrap(), 0);
    assert_eq!(rig.db.outbox().unwrap().len(), 1, "a retried upload is not expired by an old snapshot");
    rig.db.as_app().discard("moments", &rid("m1"), None).unwrap();
    assert_eq!(rig.db.as_app().expire_candidates(stale, due, &[]).unwrap(), 0, "a removed upload is skipped, not an error");
}

#[tokio::test(start_paused = true)]
async fn uploads_are_identified_by_their_declared_action_and_each_cancels_alone() {
    let rig = upload_rig(small());
    rig.sync.set_online(false);
    let source = file(b"abcdef");
    let generation = rig.db.generation();
    let begin = |action: &str| rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(generation, "moments", &rid("m1"), action, &input(&source), UploadState::queued()));
    begin("upload").unwrap();
    begin("cover").unwrap();
    assert!(matches!(begin("upload"), Err(SyncError::Params(_))), "the same action twice on one record is refused");
    assert!(begin("missing").is_err(), "an undeclared action admits nothing");
    let mut keys: Vec<String> = rig.db.outbox().unwrap().into_iter().map(|entry| entry.operation.action_key).collect();
    keys.sort();
    assert_eq!(keys, [Operation::upload_action_key("moments", "cover"), Operation::upload_action_key("moments", "upload")]);
    assert!(crate::sync::rig::app(&rig.sync, rig.sync.cancel_upload("moments", &rid("m1"), "cover")).await.unwrap());
    let left: Vec<String> = rig.db.outbox().unwrap().into_iter().map(|entry| entry.operation.action_key).collect();
    assert_eq!(left, [Operation::upload_action_key("moments", "upload")], "cancelling one action keeps the other");
    assert!(!crate::sync::rig::app(&rig.sync, rig.sync.cancel_upload("moments", &rid("m1"), "cover")).await.unwrap());
}

fn statuses(rig: &Rig) -> Vec<(String, OperationStatus)> {
    let mut found: Vec<_> = rig.db.outbox().unwrap().into_iter().map(|entry| (entry.operation.action_key, entry.operation.status)).collect();
    found.sort_by(|left, right| left.0.cmp(&right.0));
    found
}

async fn two_failed_uploads() -> (Rig, tempfile::NamedTempFile) {
    let rig = upload_rig(small());
    rig.sync.set_online(false);
    let source = file(TWELVE);
    for action in ["upload", "cover"] {
        let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), action, &input(&source), resumable(Vec::new()))).unwrap();
        fail_open_upload(&rig, seq);
    }
    (rig, source)
}

#[tokio::test(start_paused = true)]
async fn retry_and_discard_act_on_one_declared_action_or_the_whole_record() {
    let (rig, _source) = two_failed_uploads().await;
    let key = |action: &str| Operation::upload_action_key("moments", action);
    assert!(rig.db.as_app().retry("moments", &rid("m1"), Some("missing")).is_err(), "an unknown action never falls back to the whole record");
    assert!(rig.db.as_app().discard("moments", &rid("m1"), Some("missing")).is_err());
    assert_eq!(rig.db.as_app().retry("moments", &rid("m1"), Some("cover")).unwrap(), 1);
    assert_eq!(statuses(&rig), [(key("cover"), OperationStatus::Pending), (key("upload"), OperationStatus::Failed)]);
    assert_eq!(rig.db.as_app().discard("moments", &rid("m1"), Some("upload")).unwrap(), 1);
    assert_eq!(statuses(&rig), [(key("cover"), OperationStatus::Pending)]);
    assert_eq!(rig.db.as_app().discard("moments", &rid("m1"), None).unwrap(), 1);
    assert!(statuses(&rig).is_empty());
}

#[tokio::test(start_paused = true)]
async fn settled_waits_only_for_the_selected_upload_action() {
    let (rig, _source) = two_failed_uploads().await;
    rig.db.as_app().retry("moments", &rid("m1"), Some("cover")).unwrap();
    rig.db.as_app().discard("moments", &rid("m1"), Some("upload")).unwrap();
    assert!(matches!(rig.sync.settled("moments", &rid("m1"), Some("upload")).await, Ok(())), "nothing of that action is open");
    assert!(matches!(rig.sync.settled("moments", &rid("m1"), Some("cover")).await, Err(SyncError::Offline)), "the pending cover upload is awaited");
}

#[tokio::test(start_paused = true)]
async fn a_single_upload_takes_its_mutation_from_the_declaration_and_refuses_one_without_it() {
    let source = file(b"abcdef");
    let try_upload = |declared: Option<&str>| {
        let mut specs = crate::sync::rig::specs();
        specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().actions.iter_mut().find(|action| action.name == "upload").unwrap().mutation = declared.map(str::to_string);
        let rig = rig_on(crate::db::Db::open_in_memory(specs, None).unwrap(), crate::transport::MockTransport::new(), upload_config(small()));
        register_upload_operations(&rig.sync);
        put(&rig, "moments", moment("m1", "x"));
        (rig, input(&source))
    };
    let (rig, request) = try_upload(Some("CreateMoment"));
    crate::sync::rig::app(&rig.sync, rig.sync.upload("moments", &rid("m1"), "upload", request)).await.unwrap();
    assert_eq!(rig.db.outbox().unwrap().len(), 1, "the declared mutation admits the upload");
    let (bare, request) = try_upload(None);
    assert!(crate::sync::rig::app(&bare.sync, bare.sync.upload("moments", &rid("m1"), "upload", request)).await.is_err(), "no mutation anywhere admits nothing");
    assert!(bare.db.outbox().unwrap().is_empty());
}

async fn reason(rig: &Rig, action: Option<&str>) -> Result<(), String> {
    rig.sync.settled("moments", &rid("m1"), action).await.map_err(|error| error.to_string())
}

#[tokio::test(start_paused = true)]
async fn settled_for_one_action_reports_only_the_refusal_that_action_owns() {
    let rig = upload_rig(small());
    rig.db.as_app().update("moments", &rid("m1"), params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("not allowed", "FORBIDDEN"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.sync.set_online(false);
    let source = file(TWELVE);
    for action in ["upload", "cover"] {
        let seq = rig.sync.inner.db.in_current_epoch(|_| rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), action, &input(&source), resumable(Vec::new()))).unwrap();
        fail_open_upload_as(&rig, seq, &format!("{action}-lost"));
    }
    let cover = reason(&rig, Some("cover")).await.unwrap_err();
    assert!(cover.contains("cover-lost") && !cover.contains("upload-lost") && !cover.contains("not allowed"), "{cover}");
    let upload = reason(&rig, Some("upload")).await.unwrap_err();
    assert!(upload.contains("upload-lost") && !upload.contains("cover-lost") && !upload.contains("not allowed"), "{upload}");
    rig.db.as_app().discard("moments", &rid("m1"), Some("upload")).unwrap();
    rig.db.as_app().discard("moments", &rid("m1"), Some("cover")).unwrap();
    reason(&rig, Some("cover")).await.expect("an unread refusal of an ordinary action is not the cover upload's");
    assert!(reason(&rig, None).await.unwrap_err().contains("not allowed"), "the whole record still reports it");
}
