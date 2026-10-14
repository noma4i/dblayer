use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::{Config, Database, DbError, Transport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;

struct Refusing {
    calls: AtomicUsize,
    answer: Result<&'static str, fn() -> DbError>,
}

#[async_trait::async_trait]
impl Transport for Refusing {
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.answer.map(str::to_string).map_err(|make| make())
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<String, DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: Vec<u8>, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

fn rig(answer: Result<&'static str, fn() -> DbError>) -> (Arc<Database>, Arc<Refusing>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut relation = RelationSpec::global("feed");
    relation.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(),
        empty_stale_time: None, selects_page_info: false, updates: Vec::new(), page_size: None, variables: Default::default(), root: None,
    });
    model.relations.push(relation);
    let transport = Arc::new(Refusing { calls: AtomicUsize::new(0), answer });
    let models = serde_json::to_string(&vec![model]).unwrap();
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 8 }, models, Some(transport.clone() as Arc<dyn Transport>)).unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id } }","rootPath":"moments"}]"#.into()).unwrap();
    (db, transport)
}

fn unauthorized() -> DbError {
    DbError::Http { status: 401, detail: "unauthorized".into() }
}

const REFUSED: &str = r#"{"errors":[{"message":"nope"},{"message":"bad"}]}"#;

#[tokio::test]
async fn a_run_refused_with_http_401_keeps_its_status_and_is_sent_once() {
    let (db, transport) = rig(Err(unauthorized));
    let error = db.run("Feed".into(), "{}".into()).await.unwrap_err();
    assert!(matches!(&error, DbError::Http { status: 401, detail } if detail.contains("unauthorized")), "{error:?}");
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_load_refused_with_http_401_keeps_its_status_and_is_sent_once() {
    let (db, transport) = rig(Err(unauthorized));
    let error = db.refresh("Moment".into(), "feed".into(), "{}".into()).await.unwrap_err();
    assert!(matches!(&error, DbError::Http { status: 401, detail } if detail.contains("unauthorized")), "{error:?}");
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_run_answered_with_graphql_errors_keeps_every_message_and_is_sent_once() {
    let (db, transport) = rig(Ok(REFUSED));
    let error = db.run("Feed".into(), "{}".into()).await.unwrap_err();
    assert!(matches!(&error, DbError::Graphql { messages } if messages == &["nope", "bad"]), "{error:?}");
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_load_answered_with_graphql_errors_keeps_every_message_and_is_sent_once() {
    let (db, transport) = rig(Ok(REFUSED));
    let error = db.refresh("Moment".into(), "feed".into(), "{}".into()).await.unwrap_err();
    assert!(matches!(&error, DbError::Graphql { messages } if messages == &["nope", "bad"]), "{error:?}");
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
}
