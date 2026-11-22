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
