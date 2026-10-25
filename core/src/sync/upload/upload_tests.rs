use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use super::*;

use serde_json::{json, Value};

use crate::commit::{CommitEnvelope, OperationStatus, OperationTransition, WriteOrigin};
use crate::identity::RowId;
use crate::pacing::Backoff;
use crate::syncstate::{FailureKind, OperationFailure, SyncState};
use crate::transport::{PutReply, Reply, TransportError};
use crate::upload::{UploadConfig, UploadInput, UploadPart, UploadScenario, UploadStage, UploadState, UploadStatus, UploadSteps};

use super::super::rig::*;
use super::super::{OperationDef, SyncConfig};

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

pub(super) fn upload_rig(config: UploadConfig) -> Rig {
    let rig = rig_with(SyncConfig { upload: config, ..SyncConfig::default() });
    for (name, root) in [
        ("CreateBlob", "createBlob"),
        ("InitMultipart", "initMultipart"),
        ("PresignParts", "presignParts"),
        ("CompleteMultipart", "completeMultipart"),
        ("AbortMultipart", "abortMultipart"),
    ] {
        rig.sync.register(OperationDef::new(name, "operation x { x }", root));
    }
    put(&rig, "moments", moment("m1", "x"));
    put(&rig, "moments", moment("m2", "y"));
    rig
}

fn steps() -> UploadSteps {
    UploadSteps::new("CreateMoment").single("CreateBlob").multipart("InitMultipart", "PresignParts", "CompleteMultipart", "AbortMultipart")
}

pub(super) fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".mp4").tempfile().unwrap();
    file.write_all(bytes).unwrap();
    file
}

pub(super) fn input(file: &tempfile::NamedTempFile) -> UploadInput {
    UploadInput::new(file.path().to_str().unwrap(), steps()).variable("text", json!("clip"))
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

fn multipart_replies(rig: &Rig) {
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

fn variables(rig: &Rig, operation: &str) -> Vec<Value> {
    rig.transport.calls().into_iter().filter(|call| call.operation_name == operation).map(|call| call.variables).collect()
}

fn upload_of(rig: &Rig, id: &str) -> Option<crate::upload::UploadInfo> {
    rig.db.upload_info("moments", &rid(id)).unwrap()
}

fn stored_state(rig: &Rig) -> UploadState {
    rig.db.outbox().unwrap().into_iter().find_map(|entry| entry.operation.upload).unwrap()
}

const TWELVE: &[u8] = b"0123456789ab";

#[tokio::test(start_paused = true)]
async fn inv_upload_16_complete_failure_keeps_parts_through_queue_restart_and_retry() {
    let rig = upload_rig(small());
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("CompleteMultipart", Reply::fail(TransportError::Http { status: 422, body: "rejected".into() }));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("AbortMultipart", abort());
    let source = file(TWELVE);
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = stored_state(&rig);
    assert_eq!(failed.upload_id.as_deref(), Some("u1"));
    assert_eq!((failed.stage, failed.parts.len(), failed.confirmed_bytes), (UploadStage::Complete, 3, 12));
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
    rig.transport.enqueue("CreateMoment", created("m9"));
    rig.sync.retry("moments", &rid("m1")).await.unwrap();
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
    rig.sync.upload("moments", &id, input(&source)).await.unwrap();
    rig.transport.await_calls("CreateMoment", 1).await;
    assert!(rig.db.outbox().unwrap()[0].operation.transport_started);
    let mut cancel = Box::pin(rig.sync.cancel_upload("moments", &id));
    assert!(matches!(cancel.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(Ok(false))));
    drop(cancel);
    tokio::time::advance(Duration::from_secs(1)).await;
    rig.sync.wait_uploads().await;
    assert_eq!(text_of(&rig, "moments", "m9"), Some("clip".into()));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn http_put_failure_does_not_confirm_transfer() {
    use crate::transport::{Transport, GraphQLRequest, Response, Subscription, PutRequest, PutResponse};
    struct HttpStatus(crate::transport::MockTransport, u16);
    #[async_trait::async_trait]
    impl Transport for HttpStatus {
        async fn query(&self, request: GraphQLRequest) -> Result<Response, TransportError> { self.0.query(request).await }
        async fn mutate(&self, request: GraphQLRequest) -> Result<Response, TransportError> { self.0.mutate(request).await }
        async fn subscribe(&self, request: GraphQLRequest) -> Result<Subscription, TransportError> { self.0.subscribe(request).await }
        async fn put(&self, _: PutRequest) -> Result<PutResponse, TransportError> { Ok(PutResponse { status: self.1, etag: None }) }
    }
    for status in [503, 408, 429, 400, 403, 302] {
        let config = UploadConfig { backoff: small().backoff.limited(0), ..small() };
        let rig = upload_rig(config.clone());
        single_replies(&rig);
        let sync = Sync::new(rig.db.clone(), Arc::new(HttpStatus(rig.transport.clone(), status)), Arc::new(rig.clock.clone()), SyncConfig { upload: config, ..SyncConfig::default() });
        register_operations(&sync);
        sync.register(OperationDef::new("CreateBlob", "mutation { createBlob }", "createBlob"));
        let source = file(b"abcdef");
        sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
        sync.wait_uploads().await;
        assert_eq!(rig.transport.call_count("CreateMoment"), 0, "HTTP {status} PUT confirmed transfer");
        assert!(!stored_state(&rig).is_transferred());
    }
}

#[tokio::test(start_paused = true)]
async fn cancel_multipart_offline_returns_and_aborts_after_reconnecting() {
    let rig = upload_rig(small());
    let source = file(TWELVE);
    rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(Vec::new())).unwrap();
    rig.transport.enqueue("AbortMultipart", abort());
    rig.sync.set_online(false);
    let id = rid("m1");
    let cancelled = tokio::time::timeout(Duration::from_secs(1), rig.sync.cancel_upload("moments", &id)).await;
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
        rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), UploadState::queued()).unwrap();
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
    thread.join().unwrap();
    rig.db.discard("moments", &rid("m1")).unwrap();
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
        rig.sync.upload_create("moments", params(json!({"text":"clip"})), input(&source)).await.unwrap();
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
    let rig = rig_on(db, crate::transport::MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("CreateBlob", "mutation B { createBlob }", "createBlob"));
    let source = file(b"photo");
    for (mutation, root, title) in [("ChatGroupCreateFile", "groupChatCreate", "created"), ("ChatGroupUpdateFile", "groupChatUpdate", "updated")] {
        rig.sync.register(OperationDef::new(mutation, "mutation C { chat }", root));
        rig.transport.enqueue("CreateBlob", blob());
        rig.transport.enqueue(mutation, Reply::data(json!({root: {"chat": {"id": "c1", "text": title}}})));
        let input = UploadInput::new(source.path().to_str().unwrap(), UploadSteps::new(mutation).single("CreateBlob"));
        if title == "created" {
            rig.sync.upload_create("Chat", params(json!({"text": "pending"})), input).await.unwrap();
        } else {
            rig.sync.upload("Chat", &rid("c1"), input).await.unwrap();
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
    let rig = rig_on(db, crate::transport::MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("CreateBlob", "mutation B { createBlob }", "createBlob"));
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Interrupted("lost response".into())));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"photo");
    let row = rig.sync.upload_create("moments", params(json!({"text": "clip"})), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    rig.sync.retry("moments", &row).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let puts = rig.transport.put_calls();
    assert_eq!((puts.len(), puts[0].url.as_str(), puts[0].body.as_slice()), (1, "https://s3/single", b"abcdef".as_slice()));
    assert_eq!(puts[0].headers, [("x-a".to_string(), "1".to_string())]);
    let create = &variables(&rig, "CreateBlob")[0];
    assert_eq!((create["filename"].as_str().unwrap().ends_with(".mp4"), &create["contentType"], &create["byteSize"]), (true, &json!("video/mp4"), &json!(6)));
    assert_eq!(create["checksum"], json!("6AtQFwmJUPxYqtg8jBSXjg=="));
    assert_eq!(variables(&rig, "CreateMoment"), [json!({"text": "clip", "signedId": "sid", "blobId": "b1"})]);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(upload_of(&rig, "m1"), None);
    assert_eq!(text_of(&rig, "moments", "m9"), Some("clip".to_string()));
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
            let upload = change.rows.first().and_then(|row| row.upload()).map(|upload| (upload.status, upload.stage, upload.sent_bytes));
            sink.lock().unwrap().push(upload);
        })
        .unwrap();
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage), (UploadStatus::Failed, UploadStage::Part));
    assert!(!failed.is_retryable());
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    let state = stored_state(&rig);
    assert_eq!((state.upload_id, state.parts.len(), state.sent_bytes), (None, 0, 0));
    rig.sync.retry("moments", &rid("m1")).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.put_count("https://s3/single"), 3);
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage, failed.attempts), (UploadStatus::Failed, UploadStage::Put, 2));
    assert_eq!(failed.error.as_ref().map(|error| error.kind), Some(FailureKind::Server));
    assert!(failed.is_retryable());
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
}

#[tokio::test(start_paused = true)]
async fn offline_waits_without_spending_attempts() {
    let rig = upload_rig(UploadConfig { backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).limited(0), ..small() });
    single_replies(&rig);
    rig.sync.set_online(false);
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(100)).await;
    assert_eq!(rig.transport.call_count("CreateBlob"), 0);
    let waiting = upload_of(&rig, "m1").unwrap();
    assert_eq!((waiting.status, waiting.attempts), (UploadStatus::Uploading, 0));
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_network_error_while_offline_is_not_an_attempt() {
    let rig = upload_rig(UploadConfig { backoff: Backoff::new(Duration::from_secs(1), Duration::from_secs(15)).limited(0), ..small() });
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::fail(TransportError::Network("lost".into())).after(Duration::from_secs(8)));
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e"));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    rig.sync.set_online(false);
    tokio::time::sleep(Duration::from_secs(60)).await;
    let waiting = upload_of(&rig, "m1").unwrap();
    assert_eq!((waiting.status, waiting.attempts), (UploadStatus::Uploading, 0));
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    rig.clock.advance(Duration::from_secs(51 * 60));
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "PresignParts")[1]["partNumbers"], json!([2, 3]));
    assert_eq!((rig.transport.put_count("https://s3/p2"), rig.transport.put_count("https://s3/p2b"), rig.transport.put_count("https://s3/p3b")), (0, 1, 1));
}

#[tokio::test(start_paused = true)]
async fn an_unanswered_mutation_is_delivery_unknown_never_repeated_and_retry_skips_the_upload() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", Reply::fail(TransportError::Interrupted("reset".into())));
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    let unknown = upload_of(&rig, "m1").unwrap();
    assert_eq!((unknown.status, unknown.stage), (UploadStatus::DeliveryUnknown, UploadStage::Mutation));
    let state = stored_state(&rig);
    assert_eq!((state.signed_id.as_deref(), state.blob_id.as_deref()), (Some("sid"), Some("b1")));
    rig.sync.retry("moments", &rid("m1")).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let waiting = rig.db.sync_info("moments", &rid("m1")).unwrap();
    assert_eq!((waiting.state, waiting.attempts), (crate::syncstate::SyncState::Pending, 1));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    rig.clock.advance(Duration::from_secs(2));
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!((rig.transport.call_count("CreateBlob"), rig.transport.put_calls().len(), rig.transport.call_count("CreateMoment")), (1, 1, 2));
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_mutation_waits_offline_without_a_call_and_goes_out_when_online() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(8)));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    let failed = upload_of(&rig, "m1").unwrap();
    assert_eq!((failed.status, failed.stage, failed.is_retryable()), (UploadStatus::Failed, UploadStage::Mutation, false));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    assert_eq!(rig.db.discard("moments", &rid("m1")).unwrap(), 1);
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
        rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(rig.sync.cancel_upload("moments", &rid("m1")).await.unwrap());
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(upload_of(&rig, "m1"), None);
    rig.sync.wait_uploads().await;
    assert!(!rig.sync.cancel_upload("moments", &rid("m1")).await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn files_at_a_time_are_limited_and_the_waiting_one_is_queued() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(10)));
    let (first, second) = (file(b"abcdef"), file(b"ghijkl"));
    rig.sync.upload("moments", &rid("m1"), input(&first)).await.unwrap();
    rig.sync.upload("moments", &rid("m2"), input(&second)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(upload_of(&rig, "m1").unwrap().status, UploadStatus::Uploading);
    assert_eq!(upload_of(&rig, "m2").unwrap().status, UploadStatus::Queued);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.parallel_puts_peak(), 1);
    assert_eq!(rig.transport.call_count("CreateMoment"), 2);

    let wide = upload_rig(UploadConfig { max_files: 2, ..small() });
    single_replies(&wide);
    wide.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(10)));
    wide.sync.upload("moments", &rid("m1"), input(&first)).await.unwrap();
    wide.sync.upload("moments", &rid("m2"), input(&second)).await.unwrap();
    wide.sync.wait_uploads().await;
    assert_eq!(wide.transport.parallel_puts_peak(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_second_upload_for_the_same_record_is_refused() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    assert!(rig.sync.upload("moments", &rid("m1"), input(&source)).await.is_err());
    rig.sync.wait_uploads().await;
}

#[tokio::test(start_paused = true)]
async fn an_unreadable_source_fails_at_prepare_without_touching_the_transport() {
    let rig = upload_rig(small());
    rig.sync.upload("moments", &rid("m1"), UploadInput::new("/nonexistent/clip.mp4", steps())).await.unwrap();
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
async fn a_stored_multipart_resumes_from_its_finished_parts_after_a_restart() {
    let rig = upload_rig(small());
    rig.transport.enqueue("PresignParts", presign(&[(2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("CompleteMultipart", complete());
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(TWELVE);
    let state = resumable(vec![UploadPart { number: 1, etag: "e1".to_string(), confirmed_bytes: 5 }]);
    rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), state).unwrap();
    let report = rig.sync.flush().await.unwrap();
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
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(Vec::new())).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, rig.db.generation());
    envelope.operation_transitions.push(OperationTransition::Fail {
        seq,
        status: OperationStatus::Failed,
        failure: OperationFailure::new(FailureKind::Network, "abort lost"),
    });
    rig.db.shared.store.commit(envelope).unwrap();
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "AbortMultipart"), [json!({"key": "k1", "uploadId": "u1"})]);
    assert_eq!(stored_state(&rig).upload_id, None);
    assert_eq!(upload_of(&rig, "m1").unwrap().status, UploadStatus::Failed);
}

#[tokio::test]
async fn inv_op_10_parallel_uploads_for_one_record_leave_exactly_one_open() {
    let rig = upload_rig(small());
    for round in 0..30 {
        let id = rid(&format!("r{round}"));
        put(&rig, "moments", moment(id.as_str(), "x"));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let accepted = (0..8)
            .map(|_| {
                let (inner, id, barrier) = (rig.sync.inner.clone(), id.clone(), barrier.clone());
                std::thread::spawn(move || {
                    let input = UploadInput::new("/nonexistent/file.bin", steps());
                    barrier.wait();
                    inner.begin_upload(inner.db.generation(), "moments", &id, &input, UploadState::queued()).is_ok()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|accepted| *accepted)
            .count();
        assert_eq!(accepted, 1, "round {round}");
    }
}

#[tokio::test(start_paused = true)]
async fn inv_op_09_a_retry_that_lands_while_the_old_run_is_ending_still_starts_the_upload() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), UploadState::queued()).unwrap();
    let (run, _) = rig.sync.inner.register(seq, 0).unwrap();
    rig.sync.inner.start_uploads().unwrap();
    rig.sync.inner.finish_run(seq, 0, run);
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}

#[tokio::test(start_paused = true)]
async fn inv_op_09_a_cancel_whose_abort_fails_keeps_the_upload_id_until_the_abort_goes_through() {
    let rig = upload_rig(small());
    rig.transport.enqueue("InitMultipart", init());
    rig.transport.enqueue("PresignParts", presign(&[(1, "https://s3/p1"), (2, "https://s3/p2"), (3, "https://s3/p3")]));
    rig.transport.enqueue("AbortMultipart", Reply::fail(TransportError::Network("down".into())));
    rig.transport.enqueue("AbortMultipart", abort());
    for url in ["https://s3/p1", "https://s3/p2", "https://s3/p3"] {
        rig.transport.enqueue_put(url, PutReply::etag("e").after(Duration::from_secs(60)));
    }
    let source = file(TWELVE);
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    rig.sync.cancel_upload("moments", &rid("m1")).await.unwrap();
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("AbortMultipart"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_held_create_shows_its_row_at_once_and_one_mutation_swaps_the_temp_id() {
    let rig = upload_rig(small());
    single_replies(&rig);
    let source = file(b"abcdef");
    let temp = rig.sync.upload_create("moments", params(json!({"text": "clip"})), input(&source)).await.unwrap();
    assert!(temp.is_temp());
    assert_eq!(text_of(&rig, "moments", temp.as_str()), Some("clip".to_string()));
    let queued: Vec<String> = rig.db.outbox().unwrap().into_iter().map(|entry| entry.operation.action_key).collect();
    assert_eq!(queued, [Operation::action_key("moments", "upload")]);
    rig.sync.wait_uploads().await;
    assert_eq!(variables(&rig, "CreateMoment"), [json!({"text": "clip", "signedId": "sid", "blobId": "b1"})]);
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
    let temp = rig.sync.upload_create("moments", params(json!({"text": "clip"})), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(upload_of(&rig, temp.as_str()).map(|failed| failed.status), Some(UploadStatus::Failed));
    assert_eq!(text_of(&rig, "moments", temp.as_str()), Some("clip".to_string()));
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    rig.sync.retry("moments", &temp).await.unwrap();
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
    let temp = rig.sync.upload_create("moments", params(json!({"text": "clip"})), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.db.discard("moments", &temp).unwrap(), 1);
    assert_eq!(text_of(&rig, "moments", temp.as_str()), None);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn the_blob_step_reads_a_dotted_root_and_headers_given_as_a_json_string() {
    let rig = upload_rig(small());
    rig.sync.register(OperationDef::new("CreateDirectBlob", "operation x { x }", "uploadCreate.directUpload"));
    rig.transport.enqueue(
        "CreateDirectBlob",
        Reply::data(json!({"uploadCreate": {"directUpload": {"url": "https://s3/single", "headers": "{\"x-a\":\"1\"}", "signedId": "sid", "blobId": "b1"}}})),
    );
    rig.transport.enqueue("CreateMoment", created("m9"));
    let source = file(b"abcdef");
    let steps = UploadSteps::new("CreateMoment").single("CreateDirectBlob");
    let direct = UploadInput::new(source.path().to_str().unwrap(), steps).variable("text", json!("clip"));
    rig.sync.upload("moments", &rid("m1"), direct).await.unwrap();
    rig.sync.wait_uploads().await;
    let puts = rig.transport.put_calls();
    assert_eq!(puts.first().map(|put| put.headers.clone()), Some(vec![("x-a".to_string(), "1".to_string())]));
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
}

fn fail_open_upload(rig: &Rig, seq: i64) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, rig.db.generation());
    envelope.operation_transitions.push(OperationTransition::Fail { seq, status: OperationStatus::Failed, failure: OperationFailure::new(FailureKind::Network, "lost") });
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
        let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(parts)).unwrap();
        fail_open_upload(&rig, seq);
        assert_eq!(rig.db.discard("moments", &rid("m1")).unwrap(), 1, "{name}");
        assert!(rig.db.outbox().unwrap().is_empty(), "{name}");
        assert_eq!(orphan_count(&rig), 1, "{name}: the abort must outlive the operation");
        rig.sync.inner.upload_housekeeping().await.unwrap();
        rig.sync.inner.upload_housekeeping().await.unwrap();
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
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(Vec::new())).unwrap();
    fail_open_upload(&rig, seq);
    rig.db.discard("moments", &rid("m1")).unwrap();
    rig.sync.set_online(false);
    rig.sync.inner.upload_housekeeping().await.unwrap();
    assert_eq!((rig.transport.call_count("AbortMultipart"), orphan_count(&rig)), (0, 1));
    rig.sync.set_online(true);
    rig.sync.wait_uploads().await;
    rig.sync.inner.upload_housekeeping().await.unwrap();
    assert_eq!((rig.transport.call_count("AbortMultipart"), orphan_count(&rig)), (1, 0));
}

#[tokio::test(start_paused = true)]
async fn inv_upload_20_a_failed_upload_older_than_seven_days_is_aborted_and_removed_without_a_timer() {
    for (name, parts) in [("unconfirmed", Vec::new()), ("all confirmed", confirmed_parts())] {
        let rig = upload_rig(small());
        rig.transport.enqueue("AbortMultipart", abort());
        let source = file(TWELVE);
        let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(parts)).unwrap();
        fail_open_upload(&rig, seq);
        rig.clock.advance(Duration::from_secs(7 * 24 * 3600 - 3600));
        rig.sync.inner.upload_housekeeping().await.unwrap();
        assert_eq!((rig.db.outbox().unwrap().len(), rig.transport.call_count("AbortMultipart")), (1, 0), "{name}: not yet due");
        rig.clock.advance(Duration::from_secs(2 * 3600));
        rig.sync.inner.upload_housekeeping().await.unwrap();
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
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(Vec::new())).unwrap();
    fail_open_upload(&rig, seq);
    rig.clock.advance(Duration::from_secs(8 * 24 * 3600));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(rig.db.outbox().unwrap().len(), 1, "nothing runs without an event");
    rig.sync.flush().await.unwrap();
    rig.sync.wait_uploads().await;
    assert!(rig.db.outbox().unwrap().is_empty(), "boot flush");
    assert_eq!(rig.transport.call_count("AbortMultipart"), 1);
    let source = file(TWELVE);
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m2"), &input(&source), resumable(Vec::new())).unwrap();
    fail_open_upload(&rig, seq);
    rig.clock.advance(Duration::from_secs(8 * 24 * 3600));
    rig.sync.flush().await.unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&source)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!((orphan_count(&rig), rig.transport.call_count("AbortMultipart")), (0, 0));
    rig.sync.inner.upload_housekeeping().await.unwrap();
    assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
}

#[tokio::test(start_paused = true)]
async fn committing_an_upload_whose_multipart_id_is_still_stored_never_records_an_abort() {
    let rig = upload_rig(small());
    let source = file(TWELVE);
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(confirmed_parts())).unwrap();
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
    rig.sync.upload("moments", &rid("m1"), input(&first)).await.unwrap();
    rig.sync.upload("moments", &rid("m2"), input(&second)).await.unwrap();
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
    rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&first), UploadState::queued()).unwrap();
    rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m2"), &input(&second), UploadState::queued()).unwrap();
    rig.db.shared.store.lock().connection.execute("UPDATE op_item SET error_kind = 'network', error_message = 'x', error_extensions = '{bad' WHERE row_id = 'm1'", []).unwrap();
    assert!(rig.sync.inner.start_uploads().is_err());
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1, "the readable upload must start in the same pass");
    rig.db.shared.store.lock().connection.execute("UPDATE op_item SET error_kind = NULL, error_message = NULL, error_extensions = NULL WHERE row_id = 'm1'", []).unwrap();
    rig.sync.inner.start_uploads().unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 2, "the unreadable upload must be retried once it reads");
}

#[tokio::test(start_paused = true)]
async fn a_recorded_abort_that_cannot_be_sent_is_reported_and_retried_at_the_next_event() {
    let rig = rig_with(SyncConfig { upload: small(), ..SyncConfig::default() });
    put(&rig, "moments", moment("m1", "x"));
    let source = file(TWELVE);
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(Vec::new())).unwrap();
    fail_open_upload(&rig, seq);
    rig.db.discard("moments", &rid("m1")).unwrap();
    assert!(rig.sync.inner.upload_housekeeping().await.is_err(), "the abort operation is not registered");
    assert_eq!(orphan_count(&rig), 1);
    rig.sync.register(OperationDef::new("AbortMultipart", "operation x { x }", "abortMultipart"));
    rig.transport.enqueue("AbortMultipart", abort());
    rig.sync.inner.upload_housekeeping().await.unwrap();
    assert_eq!((rig.transport.call_count("AbortMultipart"), orphan_count(&rig)), (1, 0));
}

#[tokio::test(start_paused = true)]
async fn a_recorded_abort_survives_closing_and_reopening_the_database() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("1.sqlite");
    let source = file(TWELVE);
    {
        let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), SyncConfig { upload: small(), ..SyncConfig::default() });
        put(&rig, "moments", moment("m1", "x"));
        let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(confirmed_parts())).unwrap();
        fail_open_upload(&rig, seq);
        rig.db.discard("moments", &rid("m1")).unwrap();
        assert_eq!(orphan_count(&rig), 1);
        rig.sync.wait_uploads().await;
    }
    let rig = rig_on(crate::db::Db::open(&path, specs(), None).unwrap(), crate::transport::MockTransport::new(), SyncConfig { upload: small(), ..SyncConfig::default() });
    rig.sync.register(OperationDef::new("AbortMultipart", "operation x { x }", "abortMultipart"));
    rig.transport.enqueue("AbortMultipart", abort());
    assert_eq!(orphan_count(&rig), 1, "the abort must survive the restart");
    rig.sync.inner.upload_housekeeping().await.unwrap();
    assert_eq!((variables(&rig, "AbortMultipart"), orphan_count(&rig)), (vec![json!({"key": "k1", "uploadId": "u1"})], 0));
}

#[tokio::test(start_paused = true)]
async fn expiry_skips_an_upload_that_was_retried_or_removed_after_it_was_selected() {
    let rig = upload_rig(small());
    let source = file(TWELVE);
    let seq = rig.sync.inner.begin_upload(rig.db.generation(), "moments", &rid("m1"), &input(&source), resumable(Vec::new())).unwrap();
    fail_open_upload(&rig, seq);
    rig.clock.advance(Duration::from_secs(8 * 24 * 3600));
    let due = rig.sync.inner.now_ms() - 7 * 24 * 3600 * 1000;
    let stale = rig.db.failed_upload_candidates(due).unwrap();
    assert_eq!(stale.len(), 1);
    rig.db.retry("moments", &rid("m1")).unwrap();
    assert_eq!(rig.db.expire_candidates(stale.clone(), due, &[]).unwrap(), 0);
    assert_eq!(rig.db.outbox().unwrap().len(), 1, "a retried upload is not expired by an old snapshot");
    rig.db.discard("moments", &rid("m1")).unwrap();
    assert_eq!(rig.db.expire_candidates(stale, due, &[]).unwrap(), 0, "a removed upload is skipped, not an error");
}
