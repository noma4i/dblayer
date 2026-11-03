#![recursion_limit = "256"]
use std::sync::Arc;
use dblayer::ffi::{CommandRequest, Config, Database, LivePhase, StreamDelivery, StreamEnd, StreamRequest, StreamValue, Transport, ValueSink};
use dblayer::schema::{ActionDeclaration, FieldSpec, ModelSpec, RootShape};
use dblayer::value::ScalarKind;

#[derive(Default)]
struct Listener(std::sync::Mutex<Vec<StreamValue>>);
impl ValueSink for Listener {
    fn delivered(&self, delivery: StreamDelivery) { self.0.lock().unwrap().push(delivery.value); }
}

#[tokio::test]
async fn bridge_file_calls_reach_the_file_store() {
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.durable = true;
    let mut model = ModelSpec::new("Moment", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("text", ScalarKind::Str),
        FieldSpec::file("media").nullable(),
    ]);
    model.actions.push(create);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 32 },
        serde_json::to_string(&vec![model]).unwrap(), None::<Arc<dyn Transport>>, None, None, None, None, None).await.unwrap();
    let epoch = db.session_epoch();
    let id = db.send(CommandRequest { epoch, model: "Moment".into(), values_json: r#"{"text":"hello"}"#.into(), args_json: "{}".into(), upload: None }).await.unwrap().row.id;
    db.attach("Moment".into(), id.clone(), "media".into(), b"abc".to_vec(), "a.txt".into(), "text/plain".into()).await.unwrap();
    assert_eq!(db.read("Moment".into(), id, "media".into()).await.unwrap(), Some(b"abc".to_vec()));
}

#[tokio::test]
async fn bridge_subscription_without_a_sync_tells_its_subscriber_in_the_state_and_ends() {
    use dblayer::schema::{Coverage, RelationSpec, RemoteShape, RemoteSpec};
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(),
        empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 8 },
        serde_json::to_string(&vec![model]).unwrap(), None::<Arc<dyn Transport>>, None, None, None, None, None).await.unwrap();
    let listener = Arc::new(Listener::default());
    db.subscribe(db.session_epoch(), StreamRequest::Query { query_json: r#"{"model":"Moment","relation":"feed","params":{}}"#.into() }, listener.clone());
    for _ in 0..200 {
        if listener.0.lock().unwrap().len() >= 2 { break; }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let values = listener.0.lock().unwrap().clone();
    assert!(matches!(&values[0], StreamValue::State { state } if state.phase == LivePhase::Failed && state.error_kind.as_deref() == Some("invalid")), "first value {:?}", values[0]);
    assert!(matches!(values[1], StreamValue::Ended { reason: StreamEnd::Invalid }));
}

struct Recorder(std::sync::Mutex<Vec<String>>);
#[async_trait::async_trait]
impl Transport for Recorder {
    async fn open_cable(&self, _: Arc<dblayer::ffi::CableLease>) -> Result<(), dblayer::ffi::DbError> { Err(dblayer::ffi::DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), dblayer::ffi::DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<dblayer::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: dblayer::ffi::TransportRequest) -> Result<String, dblayer::ffi::DbError> { unreachable!() }
    async fn mutate(&self, request: dblayer::ffi::TransportRequest) -> Result<String, dblayer::ffi::DbError> {
        self.0.lock().unwrap().push(request.operation_name);
        Ok(r#"{"data":{"createMoment":{"id":"m1","text":"hello"}}}"#.into())
    }
    async fn subscribe(&self, _: dblayer::ffi::TransportRequest, _: Arc<dblayer::ffi::SubscriptionSink>) -> Result<(), dblayer::ffi::DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: dblayer::ffi::PutFile, _: String, _: Arc<dblayer::ffi::ByteProgress>) -> Result<dblayer::ffi::PutReply, dblayer::ffi::DbError> { unreachable!() }
}

#[tokio::test]
async fn a_create_left_pending_by_the_last_session_waits_for_registration_after_open() {
    use dblayer::schema::{ActionDeclaration, RootShape};
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateMoment".into());
    create.durable = true;
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.actions.push(create);
    let models = serde_json::to_string(&vec![model]).unwrap();
    let folder = tempfile::tempdir().unwrap();
    let path = folder.path().join("db.sqlite").to_string_lossy().into_owned();
    let config = || Config { path: Some(path.clone()), data_version: None, cache_rows: 32 };
    let first = Database::open(config(), models.clone(), None::<Arc<dyn Transport>>, None, None, None, None, None).await.unwrap();
    first.send(CommandRequest { epoch: first.session_epoch(), model: "Moment".into(), values_json: r#"{"text":"hello"}"#.into(), args_json: "{}".into(), upload: None }).await.unwrap();
    drop(first);

    let transport = Arc::new(Recorder(Default::default()));
    let second = Database::open(config(), models, Some(transport.clone() as Arc<dyn Transport>), None, None, None, None, None).await.unwrap();
    second.maintain().await.unwrap();
    assert!(transport.0.lock().unwrap().is_empty());
    second.register_operations(r#"[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id text } }"}]"#.into()).unwrap();
    second.flush().await.unwrap();
    assert_eq!(*transport.0.lock().unwrap(), ["CreateMoment"]);
}
