use std::collections::BTreeMap;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering}};
use std::time::Duration;
use serde_json::{json, Map};
use crate::db::Db;
use crate::schema::{ActionDeclaration, RootShape};
use crate::transport::{MockTransport, Reply, TransportError};
use crate::upload::{BlobState, UploadConfig, UploadFile, BLOB_LIFETIME_MS};
use super::super::rig::{Rig, specs, rig_without_operations, put, moment, text_of, START_MS};
use super::super::{OperationDef, Sync, SyncConfig};
use super::upload_tests::{file, input, small, uploads, register_upload_operations, single_replies, multipart_replies, rid};

fn models() -> Vec<crate::schema::ModelSpec> {
    let mut specs = specs();
    let model = specs.iter_mut().find(|model| model.key.as_str() == "moments").unwrap();
    let mut action = ActionDeclaration::new("set_video", RootShape::Upsert);
    action.mutation = Some("SetVideo".into());
    action.root = Some("setVideo.profile".into());
    action.args.insert("video".into(), "blob".into());
    action.variables = BTreeMap::from([("signedId".into(), "video".into()), ("profileId".into(), "id".into())]);
    model.actions.push(action);
    specs
}

fn setup(db: Db, config: UploadConfig) -> Rig {
    let rig = rig_without_operations(db, MockTransport::new(), SyncConfig { uploads: Some(uploads()), upload: config, ..SyncConfig::default() });
    register_upload_operations(&rig.sync);
    rig.sync.register(OperationDef::new("SetVideo", "mutation SetVideo { setVideo { profile { id text } } }", "setVideo"));
    rig
}
fn rig() -> Rig {
    let rig = setup(Db::open_in_memory(models(), None).unwrap(), small());
    put(&rig, "moments", moment("m1", "before"));
    put(&rig, "moments", moment("m2", "other"));
    rig
}
fn use_blob(rig: &Rig, id: &str, row: &str) -> Result<crate::db::Acted, crate::db::DbError> {
    rig.db.act(&rig.db.admit().unwrap(), "moments", "set_video", &rid(row), Map::new(), json!({"video":id}).as_object().unwrap().clone())
}
fn count(rig: &Rig, sql: &str) -> i64 { rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row(sql, [], |row| row.get(0))?)).unwrap() }
fn accepted() -> Reply { Reply::data(json!({"setVideo":{"profile":{"id":"m1","text":"after","userId":null}}})) }

#[tokio::test]
async fn blob_before_action() {
    let rig = rig();
    single_replies(&rig);
    let source = file(b"hello");
    let id = crate::sync::rig::app(&rig.sync, rig.sync.upload_blob(&rig.sync.inner.db.admit().unwrap(), "video_intro", "CreateBlob", input(&source))).await.unwrap();
    assert!(!id.is_empty());
    rig.sync.wait_uploads().await;
    let snapshot = rig.sync.blob_snapshot(&id).unwrap();
    assert_eq!(snapshot.state, BlobState::Uploaded);
    assert_eq!(snapshot.signed_id.as_deref(), Some("sid"));
    assert_eq!(snapshot.upload.sent_bytes, 5);
    assert_eq!(rig.sync.wait_blob(&id).await.unwrap(), "sid");
    assert_eq!(rig.transport.put_calls().len(), 1);
    assert_eq!(rig.transport.call_count("SetVideo"), 0);
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert_eq!(count(&rig, "SELECT count(*) FROM row_moments"), 2);
    assert_eq!(count(&rig, "SELECT count(*) FROM op_row"), 0, "the upload has no model row");
    assert_eq!(text_of(&rig, "moments", "m1").as_deref(), Some("before"));
}

#[tokio::test]
async fn blob_action_ownership() {
    for mode in ["ready", "uploading", "refused", "unknown"] {
        let rig = rig();
        single_replies(&rig);
        if mode == "uploading" { rig.transport.respond_with(|request| (request.operation_name == "CreateBlob").then(|| super::upload_tests::blob().after(Duration::from_secs(1)))); }
        let source = file(b"hello");
        let id = crate::sync::rig::app(&rig.sync, rig.sync.upload_blob(&rig.sync.inner.db.admit().unwrap(), "video_intro", "CreateBlob", input(&source))).await.unwrap();
        if mode != "uploading" { rig.sync.wait_uploads().await; }
        let owned = use_blob(&rig, &id, "m1").unwrap();
        assert!(use_blob(&rig, &id, "m2").is_err(), "{mode}: second pin must fail");
        if mode == "uploading" {
            crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
            assert_eq!(rig.transport.call_count("SetVideo"), 0);
        }
        rig.transport.enqueue("SetVideo", match mode { "refused" => Reply::graphql_messages(&["refused"]), "unknown" => Reply::fail(TransportError::Interrupted("lost response".into())), _ => accepted() });
        rig.sync.wait_uploads().await;
        crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
        let snapshot = rig.sync.blob_snapshot(&id).unwrap();
        match mode {
            "refused" => {
                assert_eq!(snapshot.state, BlobState::Uploaded);
                assert_eq!(snapshot.consumer, None);
                assert!(use_blob(&rig, &id, "m2").is_ok());
            }
            "unknown" => {
                assert_eq!(snapshot.consumer, Some(owned.operation));
                assert!(rig.sync.discard_blob(&rig.sync.inner.db.admit().unwrap(), &id).await.is_err());
                assert!(use_blob(&rig, &id, "m2").is_err());
            }
            _ => {
                assert_eq!(snapshot.state, BlobState::Consumed);
                assert_eq!(count(&rig, "SELECT count(*) FROM file"), 0);
                assert!(use_blob(&rig, &id, "m2").is_err());
            }
        }
        let calls: Vec<_> = rig.transport.calls().into_iter().filter(|call| call.operation_name == "SetVideo").collect();
        assert_eq!(calls.len(), 1, "{mode}");
        assert_eq!(calls[0].purpose, crate::transport::NetworkPurpose::Transfer, "{mode}: the consumer uses the upload network");
        assert_eq!(calls[0].variables["signedId"], "sid");
        assert_eq!(rig.transport.put_calls().len(), 1, "{mode}: no second PUT");
    }
}

#[tokio::test]
async fn blob_seven_days() {
    for trigger in ["use", "online", "start", "failed", "failed_use", "background_refused"] {
        let rig = rig();
        single_replies(&rig);
        let created_at = Arc::new(AtomicU64::new(0));
        let recorded = created_at.clone();
        let clock = rig.clock.clone();
        rig.transport.respond_with(move |request| {
            if request.operation_name != "CreateBlob" { return None; }
            recorded.store(crate::pacing::Clock::now(&clock).as_millis() as u64, Ordering::SeqCst);
            Some(super::upload_tests::blob())
        });
        let advanced = AtomicBool::new(false);
        let clock = rig.clock.clone();
        rig.transport.on_put_progress(move || {
            if !advanced.swap(true, Ordering::SeqCst) { clock.advance(Duration::from_secs(2 * 86400)); }
        });
        let source = file(b"hello");
        let id = crate::sync::rig::app(&rig.sync, rig.sync.upload_blob(&rig.sync.inner.db.admit().unwrap(), "video_intro", "CreateBlob", input(&source))).await.unwrap();
        rig.sync.wait_uploads().await;
        let created = created_at.load(Ordering::SeqCst) as i64;
        assert!(created > 0);
        rig.clock.set(Duration::from_millis((created + BLOB_LIFETIME_MS - 1000) as u64));
        rig.sync.inner.expire_blobs(&rig.db.admit().unwrap()).await.unwrap();
        assert_eq!(rig.sync.blob_snapshot(&id).unwrap().state, BlobState::Uploaded);
        if trigger.starts_with("failed") || trigger == "background_refused" {
            let generation = rig.db.generation();
            let seq = rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT seq FROM blob_upload WHERE id = ?1", [&id], |row| row.get::<_, i64>(0))?)).unwrap();
            let mut state = rig.sync.blob_snapshot(&id).unwrap().upload;
            state.transferred = false;
            let failed_at = created + BLOB_LIFETIME_MS - 1000;
            state.failed_at = Some(failed_at as u64);
            let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Response, generation);
            if trigger == "background_refused" {
                envelope.operation_transitions.push(crate::commit::OperationTransition::BackgroundRefused { seq, item_position: 0, detail: "refused".into() });
            } else {
                envelope.operation_transitions.push(crate::commit::OperationTransition::Progress { seq, item_position: 0, state: Box::new(state) });
                envelope.operation_transitions.push(crate::commit::OperationTransition::Fail { seq, status: crate::operation::OperationStatus::Failed, failure: crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, "failed") });
            }
            rig.db.land(&rig.db.admit().unwrap(), envelope).unwrap();
            let persisted_failure = rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT failed_at FROM blob_upload WHERE id = ?1", [&id], |row| row.get::<_, i64>(0))?)).unwrap();
            assert!(persisted_failure >= failed_at && persisted_failure <= crate::pacing::Clock::now(&rig.clock).as_millis() as i64);
            rig.clock.set(Duration::from_millis((persisted_failure + BLOB_LIFETIME_MS - 1000) as u64));
            rig.sync.inner.expire_blobs(&rig.db.admit().unwrap()).await.unwrap();
            assert_eq!(rig.sync.blob_snapshot(&id).unwrap().state, BlobState::Failed);
            rig.clock.set(Duration::from_millis((persisted_failure + BLOB_LIFETIME_MS) as u64));
        } else {
            rig.clock.set(Duration::from_millis((created + BLOB_LIFETIME_MS) as u64));
        }
        match trigger {
            "use" | "failed_use" => assert!(use_blob(&rig, &id, "m1").unwrap_err().to_string().contains("expired")),
            "online" => { rig.sync.set_online(false); rig.sync.set_online(true); rig.sync.wait_uploads().await; },
            _ => { crate::sync::rig::app(&rig.sync, rig.sync.inner.upload_housekeeping(&rig.db.admit().unwrap())).await.unwrap(); },
        }
        assert_eq!(rig.sync.blob_snapshot(&id).unwrap().state, BlobState::Expired, "{trigger}");
        assert_eq!(count(&rig, "SELECT count(*) FROM file"), 0, "{trigger}");
        assert_eq!(rig.transport.call_count("SetVideo"), 0);
        assert_eq!(rig.transport.put_calls().len(), 1);
    }
}

#[tokio::test]
async fn blob_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blob.sqlite");
    let source = file(b"123456789012");
    let id;
    {
        let rig = setup(Db::open(&path, models(), None).unwrap(), small());
        multipart_replies(&rig);
        rig.transport.respond_with(|request| (request.operation_name == "CompleteMultipart").then(|| Reply::graphql_messages(&["refused complete"])));
        id = crate::sync::rig::app(&rig.sync, rig.sync.upload_blob(&rig.sync.inner.db.admit().unwrap(), "video_intro", "CreateBlob", input(&source))).await.unwrap();
        rig.sync.wait_uploads().await;
        let snapshot = rig.sync.blob_snapshot(&id).unwrap();
        assert_eq!(snapshot.state, BlobState::Failed);
        assert!(snapshot.upload.all_parts_confirmed());
        assert_eq!(snapshot.upload.sent_bytes, 12);
        assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
    }
    let rig = setup(Db::open(&path, models(), None).unwrap(), small());
    multipart_replies(&rig);
    let restored = rig.sync.blob_snapshot(&id).unwrap();
    assert_eq!(restored.id, id);
    assert_eq!(restored.upload.parts.len(), 3);
    assert_eq!(restored.upload.sent_bytes, 12);
    crate::sync::rig::app(&rig.sync, rig.sync.retry_blob(&rig.sync.inner.db.admit().unwrap(), &id)).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.sync.wait_blob(&id).await.unwrap(), "sid");
    assert_eq!(rig.transport.call_count("InitMultipart"), 0);
    assert_eq!(rig.transport.call_count("PresignParts"), 0);
    assert!(rig.transport.put_calls().is_empty());
    assert_eq!(rig.transport.call_count("CompleteMultipart"), 1);
    assert_eq!(rig.transport.call_count("AbortMultipart"), 0);
}

struct IdleClock { clock: crate::pacing::TestClock, sleeps: AtomicUsize, wakes: AtomicUsize }
impl crate::pacing::Clock for IdleClock {
    fn now(&self) -> Duration { crate::pacing::Clock::now(&self.clock) }
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { crate::pacing::Clock::sleep(&self.clock, duration).await; self.wakes.fetch_add(1, Ordering::SeqCst); })
    }
}

#[tokio::test]
async fn blob_idle_work() {
    let db = Db::open_in_memory(models(), None).unwrap();
    let transport = MockTransport::new();
    let clock = Arc::new(IdleClock { clock: crate::pacing::TestClock::new(Duration::from_millis(START_MS)), sleeps: AtomicUsize::new(0), wakes: AtomicUsize::new(0) });
    let sync = Sync::new(db.clone(), Arc::new(transport.clone()), clock.clone(), SyncConfig { uploads: Some(uploads()), ..SyncConfig::default() });
    register_upload_operations(&sync);
    let rig = Rig { db, sync, transport, clock: clock.clock.clone() };
    single_replies(&rig);
    let source = file(b"hello");
    let id = crate::sync::rig::app(&rig.sync, rig.sync.upload_blob(&rig.sync.inner.db.admit().unwrap(), "video_intro", "CreateBlob", UploadFile::new(source.path().to_str().unwrap()))).await.unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.sync.blob_snapshot(&id).unwrap().state, BlobState::Uploaded);
    let before = (rig.transport.calls().len(), rig.transport.put_calls().len(), clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst));
    tokio::time::pause();
    clock.clock.advance(Duration::from_secs(8 * 86400));
    tokio::time::advance(Duration::from_secs(8 * 86400)).await;
    tokio::task::yield_now().await;
    assert_eq!(before, (rig.transport.calls().len(), rig.transport.put_calls().len(), clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst)));
    assert_eq!(rig.sync.blob_snapshot(&id).unwrap().state, BlobState::Uploaded, "expiry is event driven");
}
