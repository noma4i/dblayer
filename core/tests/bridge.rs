#![recursion_limit = "256"]
use dblayer::ffi::{Config, Database, ReadEnd, ReadRequest, ReadValue};
use dblayer::schema::{FieldSpec, ModelSpec};
use dblayer::value::ScalarKind;

#[tokio::test]
async fn bridge_file_calls_reach_the_file_store() {
    let model = ModelSpec::new("Moment", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("text", ScalarKind::Str),
        FieldSpec::file("media").nullable(),
    ]);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 32 },
        serde_json::to_string(&vec![model]).unwrap(), dblayer::ffi::OpenPorts::default()).await.unwrap();
    let id = db.create(db.session_epoch(), "Moment".into(), r#"{"text":"hello"}"#.into(), "{}".into()).await.unwrap().optimistic_value.unwrap().id;
    db.attach(db.session_epoch(), "Moment".into(), id.clone(), "media".into(), dblayer::ffi::FileBytes { bytes: b"abc".to_vec(), name: "a.txt".into(), mime: "text/plain".into() }).await.unwrap();
    assert_eq!(db.read(db.session_epoch(), "Moment".into(), id, "media".into()).await.unwrap(), Some(b"abc".to_vec()));
}

#[tokio::test]
async fn bridge_subscription_without_a_session_ends_for_a_server_relation() {
    use dblayer::schema::{Coverage, RelationSpec, RemoteShape, RemoteSpec};
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(),
        empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 8 },
        serde_json::to_string(&vec![model]).unwrap(), dblayer::ffi::OpenPorts::default()).await.unwrap();
    let stream = db.subscribe_values(db.session_epoch(), ReadRequest::List { query_json: r#"{"model":"Moment","relation":"feed","params":{}}"#.into() });
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), stream.pull()).await.expect("the stream got a value").unwrap();
    assert!(matches!(first, Some(ReadValue::Ended { cause: ReadEnd::SessionEnded })), "first value {first:?}");
}
