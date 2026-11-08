use std::sync::Arc;

use super::load_tests::relation;
use super::{Config, Database};
use crate::runtime::{Clock, ManualClock};
use crate::schema::{FieldSpec, ModelSpec};
use crate::transport::{Author, MockTransport, Reply};
use crate::value::ScalarKind;

const FEED: &str = r#"{"model":"Moment","relation":"feed","params":{}}"#;

async fn life(transport: &Arc<MockTransport>, clock: Arc<dyn Clock>) -> Arc<Database> {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.relations.push(relation("feed", "5.minutes"));
    let wired = transport.clone();
    let database = Database::start_with(Config { path: None, data_version: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), clock, move |db, _| {
        super::signed_session_tests::signed_session(db);
        Ok(super::database::Wiring { transport: Some(wired), encrypted: None, cable: None, seal: None })
    }).unwrap();
    database.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"}]"#.into()).await.unwrap();
    database.set_online(true).unwrap();
    database
}

/// sess_request_ids_unique_per_process: two lives of a database in 1 process never share an `(author, request_id)` pair, so the second load takes the second reply of the queue.
#[tokio::test]
async fn sess_request_ids_unique_per_process() {
    let transport = Arc::new(MockTransport::new());
    transport.enqueue("Feed", Reply::data(serde_json::json!({"moments": [{"id": "m1", "text": "a"}]})));
    transport.enqueue("Feed", Reply::data(serde_json::json!({"moments": []})));
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(std::time::Duration::from_secs(1)));
    let first = life(&transport, clock.clone()).await;
    first.load(first.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    first.close_database().await.unwrap();
    let second = life(&transport, clock).await;
    second.load(second.session_epoch(), "Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let pairs: Vec<(Author, u64)> = transport.calls().iter().filter(|call| call.operation_name == "Feed").map(|call| { let wire = call.wire.expect("a window request names its author"); (wire.author, wire.request_id) }).collect();
    assert_eq!(pairs.len(), 2, "one load per life");
    assert_ne!(pairs[0], pairs[1], "the second life does not reuse the pair of the first");
    assert_eq!(second.read_query(second.session_epoch(), super::QuerySpec::json(FEED)).await.unwrap().len(), 0, "the second load took the second reply");
    second.close_database().await.unwrap();
}
