use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::related_tests::{ids, Tape};
use super::{Config, Database, DbError, TestClock, Transport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec};
use crate::test_wait::spin_until;
use crate::value::ScalarKind;

struct Pages {
    queries: AtomicUsize,
    closes_after: usize,
}

#[async_trait::async_trait]
impl Transport for Pages {
    async fn open_cable(&self, _: Arc<super::CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: super::TransportRequest) -> Result<String, DbError> {
        let page = self.queries.fetch_add(1, Ordering::SeqCst);
        let nodes: Vec<String> = (0..3).map(|row| format!(r#"{{"id":"m{page}{row}","text":"t{:02}"}}"#, 30 - page * 3 + row)).collect();
        let has_next = page + 1 < self.closes_after;
        Ok(format!(r#"{{"data":{{"feed":{{"nodes":[{}],"pageInfo":{{"hasNextPage":{has_next},"endCursor":"c{page}"}}}}}}}}"#, nodes.join(",")))
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

async fn open(closes_after: usize) -> (Arc<Database>, Arc<Pages>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let transport = Arc::new(Pages { queries: AtomicUsize::new(0), closes_after });
    let db = Database::open_with_clock(Config { path: None, data_version: None, cache_rows: 64 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport.clone()), None, TestClock::new(1_000_000)).await.unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { feed { nodes { id text } pageInfo { hasNextPage endCursor } } }","rootPath":"feed"}]"#.into()).unwrap();
    (db, transport)
}

const WINDOW: &str = r#"{"model":"Moment","relation":"feed","params":{},"order":[{"field":"text","direction":"asc"}]}"#;

fn open_window(db: &Database, max: Option<u32>) -> (Arc<Tape>, Arc<super::LiveSubscription>) {
    let tape = Arc::new(Tape(Mutex::default()));
    let live = db.live_window(WINDOW.into(), 3, max, tape.clone());
    (tape, live)
}

fn shown(tape: &Tape) -> Vec<String> { ids(tape) }

#[tokio::test]
async fn a_window_connects_its_scope_and_expand_before_loads_one_page_when_the_database_has_no_older_rows() {
    let (db, transport) = open(3).await;
    let (tape, live) = open_window(&db, None);
    spin_until("the first page", || shown(&tape).len() == 3);
    assert_eq!(shown(&tape), ["m00", "m01", "m02"]);
    assert_eq!(transport.queries.load(Ordering::SeqCst), 1);

    live.expand_before(2);
    spin_until("the older page", || shown(&tape).len() == 5);
    assert_eq!(shown(&tape), ["m11", "m12", "m00", "m01", "m02"]);
    assert_eq!(transport.queries.load(Ordering::SeqCst), 2, "one page for the request");
}

#[tokio::test]
async fn a_window_whose_scope_has_no_more_pages_stops_asking_and_keeps_the_rows_it_has() {
    let (db, transport) = open(2).await;
    let (tape, live) = open_window(&db, None);
    spin_until("the first page", || shown(&tape).len() == 3);
    live.expand_before(2);
    spin_until("the older page", || shown(&tape).len() == 5);
    live.expand_before(5);
    spin_until("every loaded row", || shown(&tape).len() == 6);
    assert_eq!(transport.queries.load(Ordering::SeqCst), 2, "the closed chain asks for no third page");
    drop(live);
}

#[tokio::test]
async fn a_window_that_grows_past_max_shrinks_to_its_first_size() {
    let (db, _transport) = open(3).await;
    let (tape, live) = open_window(&db, Some(5));
    spin_until("the first page", || shown(&tape).len() == 3);
    live.expand_before(2);
    spin_until("the widest window", || shown(&tape).len() == 5);
    live.expand_before(1);
    spin_until("the shrunk window", || shown(&tape).len() == 3);
    assert_eq!(shown(&tape), ["m00", "m01", "m02"]);
}
