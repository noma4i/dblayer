use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

use super::{Config, Database, DbError, LoadMoreResult, TestClock, Transport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;
use crate::test_wait::{serving, within};

struct Paged {
    queries: AtomicUsize,
    held: Notify,
    release: Notify,
    last_has_next: bool,
}

#[async_trait::async_trait]
impl Transport for Paged {
    async fn open_cable(&self, _: Arc<super::CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        let number = self.queries.fetch_add(1, Ordering::SeqCst);
        if number == 1 {
            self.held.notify_one();
            serving(self.release.notified()).await;
        }
        let has_next = number == 0 || (number == 1 && self.last_has_next);
        Ok(format!(r#"{{"data":{{"feed":{{"nodes":[{{"id":"m{number}","text":"t"}}],"pageInfo":{{"hasNextPage":{has_next},"endCursor":"c{number}"}}}}}}}}"#))
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

async fn open(last_has_next: bool) -> (Arc<Database>, Arc<Paged>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let transport = Arc::new(Paged { queries: AtomicUsize::new(0), held: Notify::new(), release: Notify::new(), last_has_next });
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport.clone()), None, TestClock::new(1_000_000)).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).unwrap();
    (db, transport)
}

async fn more(db: &Database) -> LoadMoreResult {
    db.load_more("Moment".into(), "feed".into(), "{}".into()).await.unwrap()
}

#[tokio::test]
async fn inv_loadm_18_a_load_more_during_another_flight_says_already_loading_not_no_more() {
    let (db, transport) = open(true).await;
    db.load("Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let first = tokio::spawn({ let db = db.clone(); async move { more(&db).await } });
    within(transport.held.notified()).await;
    assert_eq!(more(&db).await, LoadMoreResult::AlreadyLoading);
    transport.release.notify_one();
    assert_eq!(first.await.unwrap(), LoadMoreResult::Loaded { has_more: true });
    assert_eq!(transport.queries.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn inv_loadm_18_the_last_page_says_loaded_without_more_and_a_closed_chain_says_no_more() {
    let (db, transport) = open(false).await;
    assert_eq!(more(&db).await, LoadMoreResult::NoMore);
    db.load("Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let last = tokio::spawn({ let db = db.clone(); async move { more(&db).await } });
    within(transport.held.notified()).await;
    transport.release.notify_one();
    assert_eq!(last.await.unwrap(), LoadMoreResult::Loaded { has_more: false });
    assert_eq!(more(&db).await, LoadMoreResult::NoMore);
}
