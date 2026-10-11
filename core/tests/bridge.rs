use std::sync::Arc;
use dblayer::ffi::{Config, Database, LiveChange, QueryListener, Transport};
use dblayer::schema::{FieldSpec, ModelSpec};
use dblayer::value::ScalarKind;

struct Listener;
impl QueryListener for Listener {
    fn changed(&self, _: LiveChange) {}
}

#[tokio::test]
async fn bridge_writes_reach_live_rows_and_files() {
    let model = ModelSpec::new("Moment", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("text", ScalarKind::Str),
        FieldSpec::file("media").nullable(),
    ]);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 32 },
        serde_json::to_string(&vec![model]).unwrap(), None::<Arc<dyn Transport>>).unwrap();
    let live = db.live_query(r#"{"model":"Moment"}"#.into(), Arc::new(Listener)).unwrap();
    assert!(live.current().await.rows.is_empty());
    let row: serde_json::Value = serde_json::from_str(&db.create("Moment".into(), r#"{"text":"hello"}"#.into()).unwrap()).unwrap();
    let id = row["id"].as_str().unwrap().to_string();
    let found = db.live_find("Moment".into(), id.clone(), Arc::new(Listener)).unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&found.current().await.rows[0]).unwrap()["text"], "hello");
    db.attach("Moment".into(), id.clone(), "media".into(), b"abc".to_vec(), "a.txt".into(), "text/plain".into()).unwrap();
    assert_eq!(db.read("Moment".into(), id.clone(), "media".into()).unwrap(), Some(b"abc".to_vec()));
    db.update("Moment".into(), id.clone(), r#"{"text":"edited"}"#.into()).unwrap();
    let edited = db.live_find("Moment".into(), id.clone(), Arc::new(Listener)).unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&edited.current().await.rows[0]).unwrap()["text"], "edited");
    db.destroy("Moment".into(), id.clone()).unwrap();
    let gone = db.live_find("Moment".into(), id, Arc::new(Listener)).unwrap();
    assert!(gone.current().await.rows.is_empty());
}

struct States(std::sync::Mutex<Vec<String>>);
impl dblayer::ffi::StateListener for States {
    fn changed(&self, state: dblayer::ffi::RelationStateChange) { self.0.lock().unwrap().push(state.phase); }
}

#[tokio::test]
async fn bridge_state_needs_a_sync_to_report_on() {
    use dblayer::schema::{Coverage, RelationSpec, RemoteShape, RemoteSpec};
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(),
        empty_stale_time: None, selects_page_info: false, updates: Vec::new(), page_size: None, variables: Default::default(), root: None,
    });
    model.relations.push(feed);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 8 },
        serde_json::to_string(&vec![model]).unwrap(), None::<Arc<dyn Transport>>).unwrap();
    let listener = Arc::new(States(Default::default()));
    let result = db.live_state("Moment".into(), "feed".into(), "{}".into(), listener.clone());
    assert!(result.is_err(), "without a transport there is no sync to report on");
}

struct Recorder(std::sync::Mutex<Vec<String>>);
#[async_trait::async_trait]
impl Transport for Recorder {
    async fn query(&self, _: dblayer::ffi::TransportRequest) -> Result<String, dblayer::ffi::DbError> { unreachable!() }
    async fn mutate(&self, request: dblayer::ffi::TransportRequest) -> Result<String, dblayer::ffi::DbError> {
        self.0.lock().unwrap().push(request.operation_name);
        Ok(r#"{"data":{"createMoment":{"id":"m1","text":"hello"}}}"#.into())
    }
    async fn subscribe(&self, _: dblayer::ffi::TransportRequest, _: Arc<dblayer::ffi::SubscriptionSink>) -> Result<String, dblayer::ffi::DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<dblayer::ffi::ByteProgress>) -> Result<dblayer::ffi::PutReply, dblayer::ffi::DbError> { unreachable!() }
}

#[tokio::test]
async fn a_create_left_pending_by_the_last_session_waits_for_registration_after_open() {
    use dblayer::schema::{ActionDeclaration, RootShape};
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateMoment".into());
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.actions.push(create);
    let models = serde_json::to_string(&vec![model]).unwrap();
    let folder = tempfile::tempdir().unwrap();
    let path = folder.path().join("db.sqlite").to_string_lossy().into_owned();
    let config = || Config { path: Some(path.clone()), data_version: None, cache_rows: 32 };
    let first = Database::open(config(), models.clone(), None::<Arc<dyn Transport>>).unwrap();
    first.create("Moment".into(), r#"{"text":"hello"}"#.into()).unwrap();
    drop(first);

    let transport = Arc::new(Recorder(Default::default()));
    let second = Database::open(config(), models, Some(transport.clone() as Arc<dyn Transport>)).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(transport.0.lock().unwrap().is_empty());
    second.register_operations(r#"[{"name":"CreateMoment","document":"mutation CreateMoment { createMoment { id text } }"}]"#.into()).unwrap();
    second.flush().await.unwrap();
    assert_eq!(*transport.0.lock().unwrap(), ["CreateMoment"]);
}
