use std::sync::{Arc, Mutex};

use crate::ffi::{CloudAccount, CloudApplication, CloudBackend, CloudConfig, CloudFetched, CloudInbound, CloudOutbound, CloudPort, CloudSendResult, CloudSendStatus, CloudSignals, CloudWireScope, Config, Database, DbError, OpenPorts};

#[derive(Debug, PartialEq, Eq)]
enum PortCall {
    Observe,
    Start(CloudWireScope, Option<Vec<u8>>),
    CancelStart(u64),
    Stop(u64),
    Unobserve,
    Send(CloudWireScope, Vec<CloudOutbound>),
}

struct RecordingCloudPort {
    calls: tokio::sync::mpsc::UnboundedSender<PortCall>,
    hold_first: std::sync::atomic::AtomicBool,
    release: tokio::sync::Notify,
    release_unobserve: tokio::sync::Notify,
    signals: Mutex<Option<Arc<CloudSignals>>>,
    conflict: std::sync::Mutex<Option<CloudInbound>>,
}

impl RecordingCloudPort {
    fn signals(&self) -> Arc<CloudSignals> { self.signals.lock().unwrap().as_ref().unwrap().clone() }
}

struct Starting<'a>(&'a RecordingCloudPort, u64);

impl Drop for Starting<'_> {
    fn drop(&mut self) { drop(self.0.calls.send(PortCall::CancelStart(self.1))); }
}

#[async_trait::async_trait]
impl CloudPort for RecordingCloudPort {
    async fn observe(&self, signals: Arc<CloudSignals>) {
        *self.signals.lock().unwrap() = Some(signals.clone());
        self.calls.send(PortCall::Observe).unwrap();
        signals.account(CloudAccount::Available { key: "test-account".into() });
    }

    async fn unobserve(&self) {
        self.calls.send(PortCall::Unobserve).unwrap();
        crate::test_wait::within(self.release_unobserve.notified()).await;
    }

    async fn start(&self, scope: CloudWireScope, progress: Option<Vec<u8>>, _: Arc<CloudSignals>) -> Result<(), DbError> {
        let generation = scope.generation;
        self.calls.send(PortCall::Start(scope, progress)).unwrap();
        if self.hold_first.swap(false, std::sync::atomic::Ordering::SeqCst) {
            let _starting = Starting(self, generation);
            crate::test_wait::within(self.release.notified()).await;
        }
        Ok(())
    }

    async fn stop(&self, generation: u64) { self.calls.send(PortCall::Stop(generation)).unwrap(); }

    async fn send(&self, scope: CloudWireScope, records: Vec<CloudOutbound>) -> Result<Vec<CloudSendResult>, DbError> {
        let conflict = self.conflict.lock().unwrap().take();
        let results = records.iter().map(|record| {
            let status = match conflict.as_ref().filter(|remote| remote.key == record.key) {
                Some(remote) => CloudSendStatus::Conflict { record: remote.clone() },
                None => CloudSendStatus::Saved { system_fields: None },
            };
            CloudSendResult { key: record.key.clone(), version: record.version, status }
        }).collect();
        self.calls.send(PortCall::Send(scope, records)).unwrap();
        Ok(results)
    }
}

#[tokio::test]
async fn cloud_open_stops_its_generation_and_detaches_on_close() {
    use crate::schema::{FieldSpec, ModelSpec, Storage};
    use crate::value::ScalarKind;

    let model = ModelSpec { storage: Storage::Local, ..ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]) };
    let models = crate::ffi::specs(serde_json::to_string(&[model]).unwrap());
    let (calls, mut received) = tokio::sync::mpsc::unbounded_channel();
    let port = Arc::new(RecordingCloudPort { calls, hold_first: true.into(), release: Default::default(), release_unobserve: Default::default(), signals: Default::default(), conflict: Default::default() });
    let db = Database::open(Config { path: None }, models, OpenPorts {
        cloud: Some(CloudConfig {
            backend: CloudBackend::Drive,
            application: CloudApplication::Standalone { namespace: "test-app".into() },
            port: port.clone(),
            initially_enabled: true,
        }),
        ..Default::default()
    }).await.unwrap();
    assert_eq!(received.try_recv().unwrap(), PortCall::Observe);
    let signals = port.signals();
    let previous = db.create(db.session_epoch(), "notes".into(), r#"{"text":"saved before attaching"}"#.into(), "{}".into()).await.unwrap();
    let previous = previous.optimistic_value.unwrap();

    db.set_online(true).await.unwrap();
    let Some(PortCall::Start(first, None)) = crate::test_wait::within(received.recv()).await else { panic!("cloud did not start with an absent cursor") };
    assert_eq!(first.epoch, db.session_epoch());
    assert!(!first.domain.is_empty());

    let record = crate::cloud::CloudRecord { model: "notes".into(), row_id: "restored".into(), server_revision: None, deleted: false, fields: Vec::new() };
    let inbound = CloudInbound { key: crate::cloud::record_key("notes", "restored"), payload: crate::cloud::encode(&record).unwrap(), system_fields: None };
    let page = CloudFetched { scope: first.clone(), records: vec![inbound], progress: Some(b"durable".to_vec()), initial_complete: true };
    crate::test_wait::within(signals.fetched(page)).await.unwrap();
    let row = db.read_row(first.epoch, "notes".into(), "restored".into()).await.unwrap();
    assert_eq!(row.unwrap().id, "restored");
    let Some(PortCall::Send(seed_scope, seeded)) = crate::test_wait::within(received.recv()).await else { panic!("first read did not seed the existing local row") };
    assert_eq!(seed_scope, first);
    assert_eq!(seeded.len(), 1);
    let seed = crate::cloud::decode(&seeded[0].key, &seeded[0].payload).unwrap();
    assert_eq!(seed.row_id, previous.id);
    assert_eq!(seed.fields[0].data.value, "saved before attaching");
    assert_eq!((seed.fields[0].data.stamp.millis, seed.fields[0].data.stamp.counter), (0, 0));

    db.set_online(false).await.unwrap();
    assert_eq!(crate::test_wait::within(received.recv()).await, Some(PortCall::CancelStart(first.generation)));
    assert_eq!(crate::test_wait::within(received.recv()).await, Some(PortCall::Stop(first.generation)));

    db.set_online(true).await.unwrap();
    let Some(PortCall::Start(second, progress)) = crate::test_wait::within(received.recv()).await else { panic!("cloud did not resume") };
    assert_eq!((second.epoch, &second.domain), (first.epoch, &first.domain));
    assert!(second.generation > first.generation);
    assert_eq!(progress.as_deref(), Some(b"durable".as_slice()));

    let stale = CloudFetched { scope: first, records: Vec::new(), progress: Some(b"stale".to_vec()), initial_complete: true };
    assert!(crate::test_wait::within(signals.fetched(stale)).await.is_err());

    let edit = db.create(second.epoch, "notes".into(), r#"{"text":"local edit"}"#.into(), "{}".into()).await.unwrap();
    let edited = edit.optimistic_value.unwrap();
    crate::test_wait::settled(&db, "a committed local edit waits for the initial cloud read", || received.try_recv().is_err()).await;

    let ready = CloudFetched { scope: second.clone(), records: Vec::new(), progress: None, initial_complete: true };
    crate::test_wait::within(signals.fetched(ready)).await.unwrap();
    let Some(PortCall::Send(sent_scope, sent)) = crate::test_wait::within(received.recv()).await else { panic!("cloud did not send the committed edit") };
    assert_eq!(sent_scope, second);
    assert_eq!(sent.len(), 1);
    let sent = &sent[0];
    assert_eq!(sent.version, edit.revision);
    let payload = crate::cloud::decode(&sent.key, &sent.payload).unwrap();
    assert_eq!(payload.row_id, edited.id);
    assert_eq!(payload.fields[0].data.value, "local edit");

    signals.account(CloudAccount::Available { key: "another-account".into() });
    assert_eq!(crate::test_wait::within(received.recv()).await, Some(PortCall::Stop(second.generation)));
    let Some(PortCall::Start(third, cursor)) = crate::test_wait::within(received.recv()).await else { panic!("new cloud account did not start after erasing") };
    assert_ne!(third.domain, second.domain);
    assert_eq!(third.epoch, second.epoch);
    assert_eq!(cursor, None);
    assert!(db.read_row(third.epoch, "notes".into(), "restored".into()).await.unwrap().is_none());
    assert!(db.read_row(third.epoch, "notes".into(), previous.id).await.unwrap().is_none());
    assert!(db.read_row(third.epoch, "notes".into(), edited.id).await.unwrap().is_none());

    let mut closing = Box::pin(db.close_database());
    crate::test_wait::within(async {
        tokio::select! {
            _ = &mut closing => panic!("close returned before the observer detached"),
            _ = async {
                assert_eq!(crate::test_wait::within(received.recv()).await, Some(PortCall::Stop(third.generation)));
                assert_eq!(crate::test_wait::within(received.recv()).await, Some(PortCall::Unobserve));
            } => {}
        }
    }).await;
    port.release_unobserve.notify_one();
    crate::test_wait::within(closing).await.unwrap();
    assert!(received.try_recv().is_err());
}

#[tokio::test]
async fn cloud_session_uses_a_confirmed_application_account_after_reopen() {
    use crate::ffi::tests::encrypted_tests::{Held, SubscriptionsOnly, ThroughServer};
    use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cloud-session.sqlite").to_string_lossy().into_owned();
    let server = FakeServer::new(Arc::new(crate::runtime::SystemClock));
    let credentials = Arc::new(MemoryCredentials::default());
    let open = |port| Database::open(Config { path: Some(path.clone()) }, crate::ffi::EMPTY_SPECS.into(), OpenPorts {
        cloud: Some(CloudConfig { backend: CloudBackend::Drive, application: CloudApplication::Session { account_path: "nextStep".into() }, port, initially_enabled: true }),
        transport: Some(Arc::new(SubscriptionsOnly)),
        encrypted: Some(crate::transport::encrypted::tests::protocol_tests::config()),
        credentials: Some(Arc::new(Held(credentials.clone()))),
        http: Some(Arc::new(ThroughServer(server.clone()))),
        endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()),
        partial_operations: true,
        ..Default::default()
    });

    let mut previous = None;
    for restored in [false, true] {
        let (calls, mut received) = tokio::sync::mpsc::unbounded_channel();
        let port = Arc::new(RecordingCloudPort { calls, hold_first: false.into(), release: Default::default(), release_unobserve: Default::default(), signals: Default::default(), conflict: Default::default() });
        let db = crate::test_wait::within(open(port.clone())).await.unwrap();
        assert_eq!(received.try_recv().unwrap(), PortCall::Observe);
        db.set_online(true).await.unwrap();
        if !restored {
            crate::test_wait::settled(&db, "cloud waits for confirmed app sign-in", || received.try_recv().is_err()).await;
            let answer = crate::test_wait::within(db.session_call("social".into(), "{}".into())).await.unwrap();
            assert_eq!(answer, r#"{"nextStep":"authenticated"}"#);
        }
        let Some(PortCall::Start(scope, _)) = crate::test_wait::within(received.recv()).await else { panic!("confirmed application account did not start cloud") };
        assert_eq!(scope.epoch, db.session_epoch());
        assert_eq!(scope.provider_fingerprint, "3001f754dd606e290b8f3c77e4246d6c2293435f9bf1cf970059e0f7dfb90b2e");
        if let Some(previous) = &previous { assert_eq!(&scope.domain, previous); }
        previous = Some(scope.domain.clone());
        if restored {
            let signals = port.signals();
            let page = CloudFetched { scope: scope.clone(), records: Vec::new(), progress: Some(b"old-cursor".to_vec()), initial_complete: true };
            crate::test_wait::within(signals.fetched(page)).await.unwrap();
            crate::test_wait::within(db.session_reset()).await.unwrap();
            assert_eq!(crate::test_wait::within(received.recv()).await, Some(PortCall::Stop(scope.generation)));
            let stale = CloudFetched { scope: scope.clone(), records: Vec::new(), progress: Some(b"late".to_vec()), initial_complete: true };
            assert!(crate::test_wait::within(signals.fetched(stale)).await.is_err());
            crate::test_wait::within(db.session_call("social".into(), "{}".into())).await.unwrap();
            let Some(PortCall::Start(resumed, cursor)) = crate::test_wait::within(received.recv()).await else { panic!("sign-in did not restart cloud after reset") };
            assert_eq!(resumed.domain, scope.domain);
            assert!(resumed.epoch > scope.epoch);
            assert_eq!(cursor, None);
        }
        port.release_unobserve.notify_one();
        crate::test_wait::within(db.close_database()).await.unwrap();
    }
}

#[tokio::test]
async fn cloud_status_reopens_a_blocked_record_and_clears_it_on_a_new_version() {
    use crate::ffi::{CloudActivity, CloudIssue, CloudIssueCause, CloudPhase, CloudRecordStatus};
    use crate::schema::{FieldSpec, ModelSpec, Storage};
    use crate::test_wait::within;
    use crate::value::ScalarKind;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cloud.sqlite").to_string_lossy().into_owned();
    let model = ModelSpec { storage: Storage::Local, ..ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]) };
    let models = serde_json::to_string(&[model]).unwrap();
    let mut row_id = None;
    for reopen in [false, true] {
        let (calls, mut received) = tokio::sync::mpsc::unbounded_channel();
        let port = Arc::new(RecordingCloudPort { calls, hold_first: false.into(), release: Default::default(), release_unobserve: Default::default(), signals: Default::default(), conflict: Default::default() });
        let db = Database::open(Config { path: Some(path.clone()) }, crate::ffi::specs(models.clone()), OpenPorts {
            cloud: Some(CloudConfig { backend: CloudBackend::Drive, application: CloudApplication::Standalone { namespace: "notes".into() }, port: port.clone(), initially_enabled: true }),
            ..Default::default()
        }).await.unwrap();
        assert_eq!(received.try_recv().unwrap(), PortCall::Observe);
        let changes = db.cloud_changes().unwrap();
        assert!(matches!(within(changes.next()).await.unwrap().phase, CloudPhase::WaitingForAccount | CloudPhase::Offline | CloudPhase::Reading));
        db.set_online(true).await.unwrap();
        let Some(PortCall::Start(scope, _)) = within(received.recv()).await else { panic!("cloud did not start") };
        within(async { while changes.next().await.unwrap().activity != CloudActivity::Reading {} }).await;
        let signals = port.signals();
        within(signals.fetched(CloudFetched { scope: scope.clone(), records: Vec::new(), progress: Some(b"cursor".to_vec()), initial_complete: true })).await.unwrap();
        if !reopen {
            within(async {
                loop {
                    let status = within(changes.next()).await.unwrap();
                    if status.phase == CloudPhase::Ready { break; }
                }
            }).await;
            let text = "x".repeat(901_000);
            let created = db.create(scope.epoch, "notes".into(), serde_json::json!({"text":text}).to_string(), "{}".into()).await.unwrap();
            row_id = Some(created.optimistic_value.unwrap().id);
        }
        let id = row_id.clone().unwrap();
        let expected = CloudIssue { model: "notes".into(), row_id: id.clone(), cause: CloudIssueCause::TooLarge };
        let status = within(async {
            loop {
                let status = within(changes.next()).await.unwrap();
                if !status.issues.is_empty() { break status; }
            }
        }).await;
        assert_eq!(status.phase, CloudPhase::Ready);
        assert_eq!(status.issues, [expected]);
        assert_eq!(within(db.cloud_record_status("notes".into(), id.clone())).await.unwrap(), CloudRecordStatus::TooLarge);
        assert!(received.try_recv().is_err());

        if reopen {
            db.update(scope.epoch, "notes".into(), id.clone(), r#"{"text":"smaller"}"#.into(), "{}".into()).await.unwrap();
            let Some(PortCall::Send(_, records)) = within(received.recv()).await else { panic!("edited record did not send") };
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].key, crate::cloud::record_key("notes", &id));
            within(async { while !changes.next().await.unwrap().issues.is_empty() {} }).await;
            assert!(db.cloud_status().issues.is_empty());
        }

        port.release_unobserve.notify_one();
        within(db.close_database()).await.unwrap();
        let closed = within(changes.next()).await.unwrap();
        assert_eq!(closed.phase, CloudPhase::Closed);
        assert!(closed.issues.is_empty());
        assert!(within(changes.next()).await.is_none());
    }
}

#[tokio::test]
async fn cloud_conflict_keeps_both_edits_and_retries_the_same_version() {
    use crate::cloud::{CloudField, CloudRecord, CloudStamp, StampedValue};
    use crate::schema::{FieldSpec, ModelSpec, Storage};
    use crate::test_wait::within;
    use crate::value::ScalarKind;

    let clock = crate::ffi::TestClock::new(0);
    let model = ModelSpec { storage: Storage::Local, ..ModelSpec::new("notes", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("title", ScalarKind::Str),
        FieldSpec::scalar("text", ScalarKind::Str),
    ]) };
    let (calls, mut received) = tokio::sync::mpsc::unbounded_channel();
    let port = Arc::new(RecordingCloudPort { calls, hold_first: false.into(), release: Default::default(), release_unobserve: Default::default(), signals: Default::default(), conflict: Default::default() });
    let db = Database::open_with_clock(Config { path: None }, crate::ffi::specs(serde_json::to_string(&[model]).unwrap()), OpenPorts {
        cloud: Some(CloudConfig { backend: CloudBackend::CloudKit, application: CloudApplication::Standalone { namespace: "notes".into() }, port: port.clone(), initially_enabled: true }),
        ..Default::default()
    }, clock.clone()).await.unwrap();
    assert_eq!(received.try_recv().unwrap(), PortCall::Observe);
    let row = db.create(db.session_epoch(), "notes".into(), r#"{"title":"left edit","text":"before"}"#.into(), "{}".into()).await.unwrap().optimistic_value.unwrap();
    let stamp = CloudStamp { millis: 100, counter: 0, replica: [7; 16] };
    let remote = CloudRecord { model: "notes".into(), row_id: row.id.clone(), server_revision: None, deleted: false,
        fields: vec![CloudField { path: vec!["text".into()], data: StampedValue { value: "right edit".into(), stamp } }],
    };
    let key = crate::cloud::record_key("notes", &row.id);
    let tag = b"remote-system".to_vec();
    *port.conflict.lock().unwrap() = Some(CloudInbound { key: key.clone(), payload: crate::cloud::encode(&remote).unwrap(), system_fields: Some(tag.clone()) });

    db.set_online(true).await.unwrap();
    let Some(PortCall::Start(scope, None)) = within(received.recv()).await else { panic!("cloud did not start") };
    within(port.signals().fetched(CloudFetched { scope, records: Vec::new(), progress: None, initial_complete: true })).await.unwrap();
    let Some(PortCall::Send(_, first)) = within(received.recv()).await else { panic!("cloud did not send before conflict") };
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].key, key);
    within(clock.await_armed(0)).await;
    clock.advance(2_000);
    let Some(PortCall::Send(_, retried)) = within(received.recv()).await else { panic!("cloud did not retry the merged version") };
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].version, first[0].version);
    assert_eq!(retried[0].system_fields.as_ref(), Some(&tag));
    let merged = crate::cloud::decode(&key, &retried[0].payload).unwrap();
    assert_eq!((&merged.fields[0].data.value, merged.fields[0].data.stamp), (&serde_json::json!("right edit"), stamp));
    assert_eq!(merged.fields[1].data.value, "left edit");

    port.release_unobserve.notify_one();
    within(db.close_database()).await.unwrap();
}

#[tokio::test]
async fn cloud_account_erasure_fails_a_queued_receipt_without_ending_the_app_session() {
    use crate::cloud::{CloudField, CloudRecord, CloudStamp, StampedValue};
    use crate::ffi::tests::encrypted_tests::{Held, SubscriptionsOnly, ThroughServer};
    use crate::schema::{ActionDeclaration, FieldSpec, ModelSpec, OnFailure, RootShape};
    use crate::test_wait::within;
    use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};
    use crate::value::ScalarKind;

    let server = FakeServer::new(Arc::new(crate::runtime::SystemClock));
    let credentials = Arc::new(MemoryCredentials::default());
    let mut update = ActionDeclaration::new("update", RootShape::Upsert);
    update.mutation = Some("NoteUpdate".into());
    update.root = Some("noteUpdate".into());
    update.variables = [("id".into(), "id".into()), ("text".into(), "text".into())].into();
    update.args.insert("text".into(), "string".into());
    update.on_failure = OnFailure::Keep;
    let model = ModelSpec {
        actions: vec![update],
        ..ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)])
    };
    let (calls, mut received) = tokio::sync::mpsc::unbounded_channel();
    let port = Arc::new(RecordingCloudPort { calls, hold_first: false.into(), release: Default::default(), release_unobserve: Default::default(), signals: Default::default(), conflict: Default::default() });
    let db = within(Database::open(Config { path: None }, crate::ffi::specs(serde_json::to_string(&[model]).unwrap()), OpenPorts {
        cloud: Some(CloudConfig { backend: CloudBackend::Drive, application: CloudApplication::Session { account_path: "nextStep".into() }, port: port.clone(), initially_enabled: true }),
        transport: Some(Arc::new(SubscriptionsOnly)),
        encrypted: Some(crate::transport::encrypted::tests::protocol_tests::config()),
        credentials: Some(Arc::new(Held(credentials))),
        http: Some(Arc::new(ThroughServer(server))),
        endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()),
        partial_operations: true,
        ..Default::default()
    })).await.unwrap();
    assert_eq!(received.try_recv().unwrap(), PortCall::Observe);
    within(db.register_operations(r#"[{"name":"NoteUpdate","document":"mutation NoteUpdate($id: ID!, $text: String!) { noteUpdate(id: $id, text: $text) { id text } }"}]"#.into())).await.unwrap();
    within(db.set_online(true)).await.unwrap();
    within(db.session_call("social".into(), "{}".into())).await.unwrap();
    let app = db.session_status().unwrap();
    let Some(PortCall::Start(scope, None)) = within(received.recv()).await else { panic!("cloud did not start") };
    let body = CloudRecord { model: "Note".into(), row_id: "tracked".into(), server_revision: None, deleted: false, fields: vec![
        CloudField { path: vec!["text".into()], data: StampedValue { value: "from cloud".into(), stamp: CloudStamp { millis: 100, counter: 0, replica: [2; 16] } } },
    ] };
    let record = CloudInbound { key: crate::cloud::record_key("Note", "tracked"), payload: crate::cloud::encode(&body).unwrap(), system_fields: None };
    let signals = port.signals();
    within(signals.fetched(CloudFetched { scope: scope.clone(), records: vec![record], progress: Some(b"cursor".to_vec()), initial_complete: true })).await.unwrap();

    within(db.set_online(false)).await.unwrap();
    assert_eq!(within(received.recv()).await, Some(PortCall::Stop(scope.generation)));
    let receipt = within(db.update(app.epoch, "Note".into(), "tracked".into(), r#"{"text":"queued"}"#.into(), "{}".into())).await.unwrap();
    assert!(receipt.operation.is_some());
    signals.account(CloudAccount::Available { key: "different-cloud-account".into() });
    let Err(DbError::Rejected { failure, kept: true }) = within(db.confirmed(receipt, false)).await else { panic!("cloud erasure did not fail the pending receipt") };
    assert_eq!(failure.message, "cloud account changed");
    assert!(within(db.read_row(app.epoch, "Note".into(), "tracked".into())).await.unwrap().is_none());
    let retained = db.session_status().unwrap();
    assert_eq!((retained.epoch, retained.identity, retained.phase), (app.epoch, app.identity, app.phase));
    port.release_unobserve.notify_one();
    within(db.close_database()).await.unwrap();
}

#[tokio::test]
async fn cloud_disable_joins_the_generation_and_reopens_the_same_cursor() {
    use crate::ffi::{CloudActivity, CloudPhase};
    use crate::test_wait::{settled, within};

    let (calls, mut received) = tokio::sync::mpsc::unbounded_channel();
    let port = Arc::new(RecordingCloudPort { calls, hold_first: false.into(), release: Default::default(), release_unobserve: Default::default(), signals: Default::default(), conflict: Default::default() });
    let db = within(Database::open(Config { path: None }, crate::ffi::EMPTY_SPECS.into(), OpenPorts {
        cloud: Some(CloudConfig { backend: CloudBackend::Drive, application: CloudApplication::Standalone { namespace: "notes".into() }, port: port.clone(), initially_enabled: true }),
        ..Default::default()
    })).await.unwrap();
    assert_eq!(received.try_recv().unwrap(), PortCall::Observe);
    within(db.set_online(true)).await.unwrap();
    let Some(PortCall::Start(first, None)) = within(received.recv()).await else { panic!("expected an initial read") };
    let old = port.signals();
    within(old.fetched(CloudFetched { scope: first.clone(), records: Vec::new(), progress: Some(b"saved".to_vec()), initial_complete: true })).await.unwrap();

    let mut off = Box::pin(db.set_cloud_enabled(false));
    within(async {
        tokio::select! {
            _ = &mut off => panic!("disable answered before the observer detached"),
            _ = async {
                assert_eq!(within(received.recv()).await, Some(PortCall::Stop(first.generation)));
                assert_eq!(within(received.recv()).await, Some(PortCall::Unobserve));
            } => {}
        }
    }).await;
    let stale = CloudFetched { scope: first.clone(), records: Vec::new(), progress: Some(b"late".to_vec()), initial_complete: false };
    assert!(within(old.fetched(stale)).await.is_err());
    let changes = db.cloud_changes().unwrap();
    within(async { while changes.next().await.unwrap().activity != CloudActivity::Stopping {} }).await;
    changes.cancel();
    port.release_unobserve.notify_one();
    assert!(!within(off).await.unwrap());
    assert_eq!(db.cloud_status().phase, CloudPhase::Disabled);
    assert_eq!(db.cloud_status().activity, CloudActivity::Idle);
    assert_eq!(db.db.stored_cloud_enabled().unwrap(), Some(false));

    assert!(within(db.set_cloud_enabled(true)).await.unwrap());
    assert_eq!(within(received.recv()).await, Some(PortCall::Observe));
    let Some(PortCall::Start(second, cursor)) = within(received.recv()).await else { panic!("expected a fresh scope") };
    assert_eq!(cursor.as_deref(), Some(b"saved".as_slice()));
    assert!(second.generation > first.generation);
    assert_eq!(second.domain, first.domain);
    old.account(CloudAccount::SignedOut);
    settled(&db, "a previous account observer cannot stop the new generation", || received.try_recv().is_err()).await;

    port.release_unobserve.notify_one();
    within(db.close_database()).await.unwrap();
}

#[tokio::test]
async fn cloud_disabled_reopen_captures_local_changes_for_the_same_account() {
    use crate::ffi::{CloudPhase, CloudRecordStatus};
    use crate::schema::{FieldSpec, ModelSpec, Storage};
    use crate::test_wait::within;
    use crate::value::ScalarKind;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("paused.sqlite").to_string_lossy().into_owned();
    let notes = ModelSpec { storage: Storage::Local, ..ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]) };
    let models = crate::ffi::specs(serde_json::to_string(&[notes]).unwrap());
    let open = |port| Database::open(Config { path: Some(path.clone()) }, models.clone(), OpenPorts {
        cloud: Some(CloudConfig { backend: CloudBackend::Drive, application: CloudApplication::Standalone { namespace: "notes".into() }, port, initially_enabled: true }),
        ..Default::default()
    });
    let (calls, mut received) = tokio::sync::mpsc::unbounded_channel();
    let port = Arc::new(RecordingCloudPort { calls, hold_first: false.into(), release: Default::default(), release_unobserve: Default::default(), signals: Default::default(), conflict: Default::default() });
    let db = within(open(port.clone())).await.unwrap();
    assert_eq!(within(received.recv()).await, Some(PortCall::Observe));
    let changes = db.cloud_changes().unwrap();
    within(db.set_online(true)).await.unwrap();
    let Some(PortCall::Start(first, None)) = within(received.recv()).await else { panic!("expected the original generation") };
    within(port.signals().fetched(CloudFetched { scope: first.clone(), records: Vec::new(), progress: Some(b"saved".to_vec()), initial_complete: true })).await.unwrap();
    within(async { while changes.next().await.unwrap().phase != CloudPhase::Ready {} }).await;
    changes.cancel();
    port.release_unobserve.notify_one();
    assert!(!within(db.set_cloud_enabled(false)).await.unwrap());
    assert_eq!(received.try_recv().unwrap(), PortCall::Stop(first.generation));
    assert_eq!(received.try_recv().unwrap(), PortCall::Unobserve);
    within(db.close_database()).await.unwrap();
    drop(db);

    let db = within(open(port.clone())).await.unwrap();
    assert_eq!(db.cloud_status().phase, CloudPhase::Disabled);
    within(db.set_online(true)).await.unwrap();
    let edit = within(db.create(db.session_epoch(), "notes".into(), r#"{"text":"saved while disabled"}"#.into(), "{}".into())).await.unwrap();
    let id = edit.optimistic_value.unwrap().id;
    assert!(received.try_recv().is_err());
    assert_eq!(within(db.cloud_record_status("notes".into(), id.clone())).await.unwrap(), CloudRecordStatus::Queued);
    assert!(within(db.set_cloud_enabled(true)).await.unwrap());
    assert_eq!(within(received.recv()).await, Some(PortCall::Observe));
    let Some(PortCall::Start(resumed, cursor)) = within(received.recv()).await else { panic!("expected the same account to resume") };
    assert_eq!(resumed.domain, first.domain);
    assert_eq!(cursor.as_deref(), Some(b"saved".as_slice()));
    within(port.signals().fetched(CloudFetched { scope: resumed, records: Vec::new(), progress: None, initial_complete: true })).await.unwrap();
    let Some(PortCall::Send(_, records)) = within(received.recv()).await else { panic!("the edit from the disabled open did not send") };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].version, edit.revision);
    let record = crate::cloud::decode(&records[0].key, &records[0].payload).unwrap();
    assert_eq!(record.row_id, id);
    assert_eq!(record.fields[0].data.value, "saved while disabled");
    port.release_unobserve.notify_one();
    within(db.close_database()).await.unwrap();
}
