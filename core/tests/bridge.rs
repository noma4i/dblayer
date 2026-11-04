#![recursion_limit = "256"]
use std::sync::Arc;
use dblayer::ffi::{Config, Database, LivePhase, StreamEnd, StreamRequest, StreamValue, Transport, ValueSink};
use dblayer::schema::{FieldSpec, ModelSpec};
use dblayer::value::ScalarKind;

#[derive(Default)]
struct Listener(tokio::sync::Notify);
impl ValueSink for Listener {
    fn available(&self) { self.0.notify_one(); }
}

#[tokio::test]
async fn bridge_file_calls_reach_the_file_store() {
    let model = ModelSpec::new("Moment", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("text", ScalarKind::Str),
        FieldSpec::file("media").nullable(),
    ]);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 32 },
        serde_json::to_string(&vec![model]).unwrap(), None::<Arc<dyn Transport>>, None, None, None, None, None).await.unwrap();
    let id = db.create(db.session_epoch(), "Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    db.attach(db.session_epoch(), "Moment".into(), id.clone(), "media".into(), b"abc".to_vec(), "a.txt".into(), "text/plain".into()).await.unwrap();
    assert_eq!(db.read(db.session_epoch(), "Moment".into(), id, "media".into()).await.unwrap(), Some(b"abc".to_vec()));
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
    let stream = db.subscribe(db.session_epoch(), StreamRequest::Query { query_json: r#"{"model":"Moment","relation":"feed","params":{}}"#.into() }, listener.clone());
    let mut values = Vec::new();
    loop {
        let available = listener.0.notified();
        while let Some(delivery) = db.read_stream(stream) { values.push(delivery.value); }
        if values.len() >= 2 { break; }
        tokio::time::timeout(std::time::Duration::from_secs(10), available).await.expect("the stream got no further value");
    }
    assert!(matches!(&values[0], StreamValue::State { state } if state.phase == LivePhase::Failed && state.error_kind.as_deref() == Some("invalid")), "first value {:?}", values[0]);
    assert!(matches!(values[1], StreamValue::Ended { reason: StreamEnd::Invalid }));
}
