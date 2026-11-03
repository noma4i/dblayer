use std::sync::{Arc, Mutex};

use super::{Cell, Config, Database, LiveChange, QueryListener, Transport};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use crate::test_wait::join_within;

struct Tape(Mutex<Vec<LiveChange>>);

#[tokio::test]
async fn inv_conc_06_live_connect_returns_while_storage_is_locked() {
    let (database, _) = with_rows(1).await;
    for warmed in [false, true] {
        let writer = database.db.shared.store.lock();
        let db = database.clone();
        let tape = Arc::new(Tape(Mutex::default()));
        let sink = tape.clone();
        let (sent, received) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let live = db.live_query(r#"{"model":"Moment"}"#.into(), sink);
            sent.send(()).unwrap();
            live
        });
        let returned = received.recv_timeout(std::time::Duration::from_secs(2)).is_ok();
        drop(writer);
        let live = join_within(thread);
        database.db.await_revision(database.db.revision_seq().unwrap());
        assert!(returned, "live connect waited for storage, warmed={warmed}");
        assert_eq!(tape.0.lock().unwrap().last().unwrap().rows.len(), 1);
        live.cancel();
        assert!(database.db.peek_query(&crate::query::Query::new("Moment")).is_some());
    }
}

#[tokio::test]
async fn inv_api_01_one_live_call_gives_the_value_the_events_and_the_reader_count() {
    let (db, _) = with_rows(2).await;
    let tape = Arc::new(Tape(Mutex::default()));
    let live = db.live_query(r#"{"model":"Moment"}"#.into(), tape.clone());
    assert_eq!(live.current().await.rows.len(), 2);
    let counted = db.subscription_census();
    assert_eq!((counted.subscriptions, counted.observed_queries), (1, 1));
    db.create("Moment".into(), r#"{"text":"z"}"#.into(), "{}".into()).await.unwrap();
    db.db.await_revision(db.db.revision_seq().unwrap());
    assert_eq!(tape.0.lock().unwrap().last().unwrap().inserted.len(), 1);
    live.cancel();
    drop(live);
    let left = db.subscription_census();
    assert_eq!((left.subscriptions, left.observed_queries), (0, 0));
}

#[tokio::test]
async fn changes_classify_insert_update_move_and_remove_without_list_state() {
    let (db, ids) = with_rows(3).await;
    let tape = Arc::new(Tape(Mutex::default()));
    let live = db.live_query(r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#.into(), tape.clone());
    db.db.await_revision(db.db.revision_seq().unwrap());
    assert_eq!(tape.0.lock().unwrap()[0].inserted, vec![0, 1, 2]);
    let row = db.create("Moment".into(), r#"{"text":"z"}"#.into(), "{}".into()).await.unwrap();
    db.db.await_revision(db.db.revision_seq().unwrap());
    let inserted = tape.0.lock().unwrap().last().unwrap().clone();
    assert_eq!(inserted.inserted, vec![0]);
    assert_eq!(inserted.row_indexes, vec![3]);
    assert!(inserted.updated.is_empty());
    db.update("Moment".into(), ids[0].clone(), r#"{"text":"zz"}"#.into(), "{}".into()).await.unwrap();
    db.db.await_revision(db.db.revision_seq().unwrap());
    let updated = tape.0.lock().unwrap().last().unwrap().clone();
    assert!(updated.inserted.is_empty());
    assert_eq!(updated.updated, vec![0]);
    assert_eq!(updated.moved.iter().map(|place| (&place.id, place.index)).collect::<Vec<_>>(), vec![(&ids[0], 3)]);
    db.destroy("Moment".into(), row.id.clone()).await.unwrap();
    db.db.await_revision(db.db.revision_seq().unwrap());
    assert_eq!(tape.0.lock().unwrap().last().unwrap().removed, vec![row.id]);
    live.cancel();
}

impl QueryListener for Tape {
    fn changed(&self, change: LiveChange) {
        self.0.lock().unwrap().push(change);
    }
}

async fn with_rows(count: usize) -> (Arc<Database>, Vec<String>) {
    let model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 64 }, serde_json::to_string(&vec![model]).unwrap(), None::<Arc<dyn Transport>>, None, None, None, None, None).await.unwrap();
    let mut ids = Vec::new();
    for n in 0..count {
        ids.push(db.create("Moment".into(), format!(r#"{{"text":"t{n:05}"}}"#), "{}".into()).await.unwrap().id);
    }
    (db, ids)
}

#[tokio::test]
async fn inv_observe_17_a_live_change_of_one_changed_row_carries_one_row_at_every_list_size() {
    let mut sizes = Vec::new();
    for count in [20, 200, 2000] {
        let (db, ids) = with_rows(count).await;
        let tape = Arc::new(Tape(Mutex::default()));
        let _live = db.live_query(r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#.into(), tape.clone());
        db.db.await_revision(db.db.revision_seq().unwrap());
        assert_eq!(tape.0.lock().unwrap()[0].rows.len(), count, "the first delivery is the whole list");
        db.update("Moment".into(), ids[count / 2].clone(), format!(r#"{{"text":"t{:05}x"}}"#, count / 2), "{}".into()).await.unwrap();
        db.db.await_revision(db.db.revision_seq().unwrap());
        let last = tape.0.lock().unwrap().last().unwrap().clone();
        sizes.push((last.rows.len(), last.full));
    }
    assert_eq!(sizes, vec![(1, false); 3], "(rows on the wire, full) after 1 update at 20, 200, 2000 rows");
}

#[test]
fn inv_panic_01_live_subscriptions_cancel_after_a_poisoned_lock() {
    struct Quiet;
    impl super::QueryListener for Quiet {
        fn changed(&self, _: LiveChange) {}
    }
    let live = super::LiveSubscription { feed: super::live_feed::LiveFeed::new(Arc::new(Quiet)), handles: Mutex::new(None), registration: Default::default() };
    crate::db::fixtures::poison(&live.handles);
    live.cancel();
}

#[tokio::test]
async fn a_live_query_leaves_its_rows_to_peek_and_no_subscription() {
    let (db, ids) = with_rows(3).await;
    let query = r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#;
    assert!(db.peek_query(query.into()).await.unwrap().is_none());
    let tape = Arc::new(Tape(Mutex::default()));
    let live = db.live_query(query.into(), tape.clone());
    db.db.await_revision(db.db.revision_seq().unwrap());
    live.cancel();
    drop(live);
    let peeked = db.peek_query(query.into()).await.unwrap().unwrap();
    assert_eq!((peeked.rows.len(), peeked.full), (3, true));
    assert_eq!(peeked.revision, tape.0.lock().unwrap()[0].revision);
    assert_eq!((db.subscription_census().subscriptions, db.subscription_census().observed_queries), (0, 0));
    db.update("Moment".into(), ids[0].clone(), r#"{"text":"zz"}"#.into(), "{}".into()).await.unwrap();
    db.db.await_revision(db.db.revision_seq().unwrap());
    let after = db.peek_query(query.into()).await.unwrap().unwrap();
    assert!(after.revision > peeked.revision);
    assert_eq!(after.rows[2].cells, vec![Cell::Text { value: "zz".into() }]);
}

#[tokio::test]
async fn cached_rows_carry_the_revision_of_the_value_after_an_edit() {
    let (db, ids) = with_rows(1).await;
    let before = db.peek_row("Moment".into(), ids[0].clone()).unwrap().unwrap();
    db.update("Moment".into(), ids[0].clone(), r#"{"text":"edited"}"#.into(), "{}".into()).await.unwrap();
    let stored = db.db.find("Moment", &crate::identity::RowId::new(&ids[0]).unwrap()).unwrap().unwrap();
    let after = db.peek_row("Moment".into(), ids[0].clone()).unwrap().unwrap();
    assert!(after.revision > before.revision);
    assert_eq!(after.revision, stored.revision);
    assert_eq!(after.cells, vec![Cell::Text { value: "edited".into() }]);
}

