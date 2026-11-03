use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::related_tests::{ids, Tape};
use super::{Config, Database, DbError, TestClock, Transport};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RemoteSpec};
use crate::test_wait::spin_until;
use crate::value::ScalarKind;

struct Pages {
    queries: AtomicUsize,
    active: AtomicUsize,
    closes_after: usize,
    members: bool,
    permits: tokio::sync::Semaphore,
    fail_at: Mutex<Option<usize>>,
    variables: Mutex<Vec<serde_json::Value>>,
}

struct ActiveQuery<'a>(&'a AtomicUsize);
impl Drop for ActiveQuery<'_> {
    fn drop(&mut self) { self.0.fetch_sub(1, Ordering::SeqCst); }
}

#[async_trait::async_trait]
impl Transport for Pages {
    async fn open_cable(&self, _: Arc<super::CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: super::TransportRequest) -> Result<String, DbError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveQuery(&self.active);
        let variables: serde_json::Value = serde_json::from_str(&request.variables_json).unwrap();
        self.variables.lock().unwrap().push(variables.clone());
        let requested = self.queries.fetch_add(1, Ordering::SeqCst);
        let page = if self.members { variables["after"].as_str().map_or(0, |cursor| cursor[1..].parse::<usize>().unwrap() + 1) } else { requested };
        if self.members { self.permits.acquire().await.unwrap().forget(); }
        if *self.fail_at.lock().unwrap() == Some(page) {
            self.fail_at.lock().unwrap().take();
            return Ok(r#"{"errors":[{"message":"members refused"}]}"#.into());
        }
        let size = if self.members { match page { 0 => 2, 1 => 0, _ => 100 } } else { 3 };
        let nodes: Vec<String> = (0..size).map(|row| format!(r#"{{"id":"m{page}-{row}","text":"t{page:02}-{row:03}"}}"#)).collect();
        let nodes = if self.members { nodes } else { (0..3).map(|row| format!(r#"{{"id":"m{page}{row}","text":"t{:02}"}}"#, 30 - page * 3 + row)).collect() };
        let has_next = page + 1 < self.closes_after;
        Ok(format!(r#"{{"data":{{"feed":{{"nodes":[{}],"pageInfo":{{"hasNextPage":{has_next},"endCursor":"c{page}"}}}}}}}}"#, nodes.join(",")))
    }
    async fn mutate(&self, _: super::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: super::TransportRequest, _: Arc<super::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: std::collections::HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

async fn open(closes_after: usize) -> (Arc<Database>, Arc<Pages>) {
    open_pages(closes_after, false, None).await
}

async fn open_pages(closes_after: usize, members: bool, fail_at: Option<usize>) -> (Arc<Database>, Arc<Pages>) {
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut feed = RelationSpec::global("feed");
    feed.members_complete = members;
    feed.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None },
        stale_time: "5.minutes".into(), empty_stale_time: None, selects_page_info: true, page_size: Some(100), variables: Default::default(), root: None, total_from: None,
    });
    model.relations.push(feed);
    let transport = Arc::new(Pages { queries: AtomicUsize::new(0), active: AtomicUsize::new(0), closes_after, members, permits: tokio::sync::Semaphore::new(1), fail_at: Mutex::new(fail_at), variables: Mutex::default() });
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

fn last(tape: &Tape) -> super::LiveChange { tape.0.lock().unwrap().last().unwrap().clone() }

fn latest(tape: &Tape, ready: impl Fn(&super::LiveChange) -> bool) -> bool { tape.0.lock().unwrap().last().is_some_and(ready) }

#[tokio::test]
async fn members_complete_chain() {
    for ending in ["success", "error", "empty_error", "offline", "reset"] {
        let fail_at = match ending { "error" => Some(1), "empty_error" => Some(0), _ => None };
        let (db, transport) = open_pages(4, true, fail_at).await;
        let tape = Arc::new(Tape(Mutex::default()));
        let live = db.live_window(WINDOW.into(), 50, Some(200), tape.clone());
        if ending == "empty_error" {
            crate::test_wait::within(live.wait_for_initial_load()).await.expect_err("the member chain failed");
            assert!(last(&tape).error.is_some());
            assert_eq!(last(&tape).phase, Some(super::LivePhase::Failed));
            assert!(!last(&tape).complete);
            assert!(shown(&tape).is_empty());
            live.cancel();
            continue;
        }
        spin_until("partial members with their load state", || shown(&tape).len() == 2 && transport.queries.load(Ordering::SeqCst) == 2 && latest(&tape, |change| change.phase != Some(super::LivePhase::NotLoaded)));
        assert_eq!(last(&tape).phase, Some(super::LivePhase::Loading));
        assert!(!last(&tape).complete);
        assert_eq!(transport.variables.lock().unwrap()[1]["after"], "c0");
        assert_eq!(transport.variables.lock().unwrap()[0]["first"], 100);
        if ending == "offline" {
            db.set_online(false).unwrap();
            crate::test_wait::within(live.wait_for_initial_load()).await.expect_err("the member chain went offline");
            assert!(last(&tape).error.is_some());
            assert_eq!(last(&tape).phase, Some(super::LivePhase::Loaded));
            assert!(!last(&tape).complete);
            db.set_online(true).unwrap();
            spin_until("online continues the cursor", || transport.queries.load(Ordering::SeqCst) == 3);
            assert_eq!(transport.variables.lock().unwrap()[2]["after"], "c0");
            transport.permits.add_permits(3);
            spin_until("online finishes the members", || latest(&tape, |change| change.complete));
            live.cancel();
            continue;
        }
        if ending == "reset" {
            db.db.reset_all().unwrap();
            let reset_revision = db.db.revision_seq().unwrap();
            spin_until("reset rows", || latest(&tape, |change| change.size == 0 && change.revision >= reset_revision));
            transport.permits.add_permits(4);
            spin_until("reload after reset", || latest(&tape, |change| change.complete && change.revision > reset_revision));
            assert!(!shown(&tape).is_empty());
            live.cancel();
            continue;
        }
        if ending == "error" {
            transport.permits.add_permits(1);
            crate::test_wait::within(live.wait_for_initial_load()).await.expect_err("the member chain failed");
            assert!(last(&tape).error.is_some());
            assert_eq!(last(&tape).phase, Some(super::LivePhase::Loaded));
            assert!(!last(&tape).complete);
            assert_eq!(shown(&tape).len(), 2);
            live.cancel();
            let resumed = db.live_window(WINDOW.into(), 50, Some(200), tape.clone());
            spin_until("resume the saved cursor", || transport.queries.load(Ordering::SeqCst) == 3);
            assert_eq!(transport.variables.lock().unwrap()[2]["after"], "c0");
            transport.permits.add_permits(3);
            spin_until("complete after reopening", || latest(&tape, |change| change.complete));
            resumed.cancel();
            continue;
        }
        transport.permits.add_permits(3);
        spin_until("the confirmed end", || latest(&tape, |change| change.complete));
        assert_eq!(last(&tape).phase, Some(super::LivePhase::Loaded));
        assert_eq!(transport.queries.load(Ordering::SeqCst), 4, "short and empty pages with hasNextPage are followed");
        assert_eq!(last(&tape).window.unwrap().total, Some(202));
        assert_eq!(shown(&tape).len(), 50);
        live.expand_before(150);
        spin_until("200 member rows", || shown(&tape).len() == 200);
        assert!(tape.0.lock().unwrap().iter().all(|change| change.rows.len() <= 200 && change.size <= 200));
        live.expand_before(1);
        spin_until("reset the member window size", || shown(&tape).len() == 50);
        let refused = Arc::new(Tape(Mutex::default()));
        let invalid = db.live_window(WINDOW.into(), 201, Some(201), refused.clone());
        spin_until("refuse an oversized member window", || !refused.0.lock().unwrap().is_empty());
        assert_eq!(last(&refused).error_kind.as_deref(), Some("invalid"));
        invalid.cancel();
        live.cancel();
        let count = transport.queries.load(Ordering::SeqCst);
        db.set_online(false).unwrap();
        db.set_online(true).unwrap();
        db.sync().unwrap().resume_settled().await;
        assert_eq!(transport.queries.load(Ordering::SeqCst), count, "closed readers do no network work");
    }
}

#[tokio::test]
async fn members_idle_work() {
    let (db, transport) = open_pages(4, true, None).await;
    let tape = Arc::new(Tape(Mutex::default()));
    let first = db.live_window(WINDOW.into(), 50, Some(200), tape.clone());
    spin_until("second page pending", || transport.queries.load(Ordering::SeqCst) == 2 && shown(&tape).len() == 2);
    let other = Arc::new(Tape(Mutex::default()));
    let second = db.live_window(WINDOW.into(), 50, Some(200), other.clone());
    let key = db.sync().unwrap().window_key("Moment", "feed", &Default::default()).unwrap();
    spin_until("second reader joins", || db.sync().unwrap().inner.owner.members_readers(&key) == 2);
    first.cancel();
    transport.permits.add_permits(3);
    spin_until("shared chain completes", || latest(&other, |change| change.complete));
    assert_eq!(transport.queries.load(Ordering::SeqCst), 4);
    second.cancel();
    let calls = transport.queries.load(Ordering::SeqCst);
    db.set_online(false).unwrap();
    db.set_online(true).unwrap();
    db.sync().unwrap().resume_settled().await;
    assert_eq!(transport.queries.load(Ordering::SeqCst), calls);
    db.db.reset_all().unwrap();
    let pending = db.live_window(WINDOW.into(), 50, Some(200), Arc::new(Tape(Mutex::default())));
    transport.permits.add_permits(1);
    spin_until("a new partial chain", || transport.queries.load(Ordering::SeqCst) == calls + 2);
    pending.cancel();
    spin_until("last reader cancels the request", || transport.active.load(Ordering::SeqCst) == 0);
    db.set_online(false).unwrap();
    db.set_online(true).unwrap();
    db.sync().unwrap().resume_settled().await;
    assert_eq!(transport.queries.load(Ordering::SeqCst), calls + 2);
    let tail = Arc::new(Tape(Mutex::default()));
    let reopened = db.live_window(WINDOW.into(), 50, Some(200), tail.clone());
    spin_until("reopen at the persisted cursor", || transport.queries.load(Ordering::SeqCst) == calls + 3);
    assert_eq!(transport.variables.lock().unwrap().last().unwrap()["after"], "c0");
    transport.permits.add_permits(3);
    spin_until("resumed chain completes", || latest(&tail, |change| change.complete));
    reopened.cancel();
}
