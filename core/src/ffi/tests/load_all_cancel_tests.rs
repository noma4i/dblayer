use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::{Config, Database, DbError, TestClock, FakeTransport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec};
use crate::test_wait::within_secs;
use crate::value::ScalarKind;

struct Endless {
    queries: AtomicUsize,
    sixth_started: tokio::sync::Notify,
    sixth_cancelled: tokio::sync::Notify,
}

struct OnCancel<'a>(&'a tokio::sync::Notify);

impl Drop for OnCancel<'_> {
    fn drop(&mut self) { self.0.notify_one(); }
}

#[async_trait::async_trait]
impl FakeTransport for Endless {
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        let number = self.queries.fetch_add(1, Ordering::SeqCst);
        if number == 5 {
            let _cancelled = OnCancel(&self.sixth_cancelled);
            self.sixth_started.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(format!(r#"{{"data":{{"feed":{{"nodes":[{{"id":"m{number}","text":"t"}}],"pageInfo":{{"hasNextPage":true,"endCursor":"c{number}"}}}}}}}}"#))
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

async fn open() -> (Arc<Database>, Arc<Endless>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let transport = Arc::new(Endless { queries: AtomicUsize::new(0), sixth_started: tokio::sync::Notify::new(), sixth_cancelled: tokio::sync::Notify::new() });
    let db = Database::open_with_clock(Config { path: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts { partial_operations: true, transport: Some(transport.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, TestClock::new(1_000_000)).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).await.unwrap();
    db.set_online(true).await.unwrap();
    (db, transport)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_load_all_whose_caller_went_away_asks_the_server_for_no_more_pages() {
    let (db, transport) = open().await;
    let caller = tokio::spawn({ let db = db.clone(); async move { db.load_all(db.session_epoch(), "Moment".into(), Some("feed".into()), "{}".into(), false).await } });
    within_secs(20, transport.sixth_started.notified()).await;

    caller.abort();
    let _ = caller.await;
    within_secs(20, transport.sixth_cancelled.notified()).await;
    assert_eq!(transport.queries.load(Ordering::SeqCst), 6, "the chain went on asking for pages after its caller went away");
    within_secs(20, db.close_database()).await.unwrap();
}
