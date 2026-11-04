use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{Config, Database, DbError, TestClock, Transport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec};
use crate::test_wait::within;
use crate::value::ScalarKind;

struct Endless {
    queries: AtomicUsize,
}

#[async_trait::async_trait]
impl Transport for Endless {
    async fn open_cable(&self, _: Arc<super::CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        let number = self.queries.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(2)).await;
        Ok(format!(r#"{{"data":{{"feed":{{"nodes":[{{"id":"m{number}","text":"t"}}],"pageInfo":{{"hasNextPage":true,"endCursor":"c{number}"}}}}}}}}"#))
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

async fn open() -> (Arc<Database>, Arc<Endless>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let transport = Arc::new(Endless { queries: AtomicUsize::new(0) });
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport.clone()), None, TestClock::new(1_000_000), None, None, None, None).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).unwrap();
    (db, transport)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_load_all_whose_caller_went_away_asks_the_server_for_no_more_pages() {
    let (db, transport) = open().await;
    let caller = tokio::spawn({ let db = db.clone(); async move { db.load_all(db.session_epoch(), "Moment".into(), "feed".into(), "{}".into(), false).await } });
    within(async { while transport.queries.load(Ordering::SeqCst) < 6 { tokio::time::sleep(Duration::from_millis(1)).await; } }).await;

    caller.abort();
    let _ = caller.await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stopped = transport.queries.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(transport.queries.load(Ordering::SeqCst), stopped, "the chain went on asking for pages after its caller went away");
    db.close_database().await.unwrap();
}
