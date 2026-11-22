use std::sync::{Arc, OnceLock};

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
    signals: OnceLock<Arc<CloudSignals>>,
}

struct Starting<'a>(&'a RecordingCloudPort, u64);

impl Drop for Starting<'_> {
    fn drop(&mut self) { drop(self.0.calls.send(PortCall::CancelStart(self.1))); }
}

#[async_trait::async_trait]
impl CloudPort for RecordingCloudPort {
    async fn observe(&self, signals: Arc<CloudSignals>) {
        assert!(self.signals.set(signals.clone()).is_ok());
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
        let results = records.iter().map(|record| CloudSendResult {
            key: record.key.clone(), version: record.version, status: CloudSendStatus::Saved { system_fields: None },
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
    let port = Arc::new(RecordingCloudPort { calls, hold_first: true.into(), release: Default::default(), release_unobserve: Default::default(), signals: OnceLock::new() });
    let db = Database::open(Config { path: None }, models, OpenPorts {
        cloud: Some(CloudConfig {
            backend: CloudBackend::Drive,
            application: CloudApplication::Standalone { namespace: "test-app".into() },
            port: port.clone(),
        }),
        ..Default::default()
    }).await.unwrap();
    assert_eq!(received.try_recv().unwrap(), PortCall::Observe);
    let signals = port.signals.get().unwrap();
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
        cloud: Some(CloudConfig { backend: CloudBackend::Drive, application: CloudApplication::Session { account_path: "nextStep".into() }, port }),
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
        let port = Arc::new(RecordingCloudPort { calls, hold_first: false.into(), release: Default::default(), release_unobserve: Default::default(), signals: OnceLock::new() });
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
            let signals = port.signals.get().unwrap();
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
    use crate::ffi::{CloudIssue, CloudIssueCause, CloudPhase};
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
        let port = Arc::new(RecordingCloudPort { calls, hold_first: false.into(), release: Default::default(), release_unobserve: Default::default(), signals: OnceLock::new() });
        let db = Database::open(Config { path: Some(path.clone()) }, crate::ffi::specs(models.clone()), OpenPorts {
            cloud: Some(CloudConfig { backend: CloudBackend::Drive, application: CloudApplication::Standalone { namespace: "notes".into() }, port: port.clone() }),
            ..Default::default()
        }).await.unwrap();
        assert_eq!(received.try_recv().unwrap(), PortCall::Observe);
        let changes = db.cloud_changes().unwrap();
        assert!(matches!(within(changes.next()).await.unwrap().phase, CloudPhase::WaitingForAccount | CloudPhase::Offline | CloudPhase::Reading));
        db.set_online(true).await.unwrap();
        let Some(PortCall::Start(scope, _)) = within(received.recv()).await else { panic!("cloud did not start") };
        let signals = port.signals.get().unwrap();
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
        assert!(received.try_recv().is_err());

        if reopen {
            db.update(scope.epoch, "notes".into(), id.clone(), r#"{"text":"smaller"}"#.into(), "{}".into()).await.unwrap();
            let Some(PortCall::Send(_, records)) = within(received.recv()).await else { panic!("edited record did not send") };
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].key, crate::cloud::record_key("notes", &id));
            let repaired = within(changes.next()).await.unwrap();
            assert!(repaired.issues.is_empty());
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
