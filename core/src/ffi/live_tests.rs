use std::sync::{Arc, Mutex};

use super::{Cell, Config, Database, LiveChange, QueryListener, Transport};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

struct Tape(Mutex<Vec<LiveChange>>);

impl QueryListener for Tape {
    fn changed(&self, change: LiveChange) {
        self.0.lock().unwrap().push(change);
    }
}

fn with_rows(count: usize) -> (Arc<Database>, Vec<String>) {
    let model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 64 }, serde_json::to_string(&vec![model]).unwrap(), None::<Arc<dyn Transport>>).unwrap();
    let ids = (0..count)
        .map(|n| super::database::executor().unwrap().block_on(db.create("Moment".into(), format!(r#"{{"text":"t{n:05}"}}"#), "{}".into())).unwrap().id)
        .collect();
    (db, ids)
}

#[test]
fn inv_observe_17_a_live_change_of_one_changed_row_carries_one_row_at_every_list_size() {
    let sizes: Vec<(usize, bool)> = [20, 200, 2000]
        .into_iter()
        .map(|count| {
            let (db, ids) = with_rows(count);
            let tape = Arc::new(Tape(Mutex::default()));
            let _live = db.live_query(r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#.into(), tape.clone()).unwrap();
            db.db.settle();
            assert_eq!(tape.0.lock().unwrap()[0].rows.len(), count, "the first delivery is the whole list");
            super::database::executor().unwrap().block_on(db.update("Moment".into(), ids[count / 2].clone(), format!(r#"{{"text":"t{:05}x"}}"#, count / 2), "{}".into())).unwrap();
            db.db.settle();
            let last = tape.0.lock().unwrap().last().unwrap().clone();
            (last.rows.len(), last.full)
        })
        .collect();
    assert_eq!(sizes, vec![(1, false); 3], "(rows on the wire, full) after 1 update at 20, 200, 2000 rows");
}

#[test]
fn inv_panic_01_live_and_state_subscriptions_cancel_after_a_poisoned_lock() {
    let (latest, _) = tokio::sync::watch::channel(None);
    let (_, initial_load) = tokio::sync::watch::channel(Some(Ok(())));
    let live = super::LiveSubscription { latest, initial_load, handles: Mutex::new(None) };
    crate::db::fixtures::poison(&live.handles);
    live.cancel();
    let state = super::StateSubscription { task: Mutex::new(None) };
    crate::db::fixtures::poison(&state.task);
    state.cancel();
}

#[test]
fn a_live_query_leaves_its_rows_to_peek_and_no_subscription() {
    let (db, ids) = with_rows(3);
    let query = r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#;
    assert!(db.peek_query(query.into()).unwrap().is_none());
    let tape = Arc::new(Tape(Mutex::default()));
    let live = db.live_query(query.into(), tape.clone()).unwrap();
    db.db.settle();
    live.cancel();
    drop(live);
    let peeked = db.peek_query(query.into()).unwrap().unwrap();
    assert_eq!((peeked.rows.len(), peeked.full), (3, true));
    assert_eq!(peeked.revision, tape.0.lock().unwrap()[0].revision);
    assert_eq!((db.subscription_census().subscriptions, db.subscription_census().observed_queries), (0, 0));
    super::database::executor().unwrap().block_on(db.update("Moment".into(), ids[0].clone(), r#"{"text":"zz"}"#.into(), "{}".into())).unwrap();
    db.db.settle();
    let after = db.peek_query(query.into()).unwrap().unwrap();
    assert!(after.revision > peeked.revision);
    assert_eq!(after.rows[2].cells, vec![Cell::Text { value: "zz".into() }]);
}

#[test]
fn cached_rows_carry_the_revision_of_the_value_after_an_edit() {
    let (db, ids) = with_rows(1);
    let before = db.peek_row("Moment".into(), ids[0].clone()).unwrap().unwrap();
    super::database::executor().unwrap().block_on(db.update("Moment".into(), ids[0].clone(), r#"{"text":"edited"}"#.into(), "{}".into())).unwrap();
    let stored = db.db.find("Moment", &crate::identity::RowId::new(&ids[0]).unwrap()).unwrap().unwrap();
    let after = db.peek_row("Moment".into(), ids[0].clone()).unwrap().unwrap();
    assert!(after.revision > before.revision);
    assert_eq!(after.revision, stored.revision);
    assert_eq!(after.cells, vec![Cell::Text { value: "edited".into() }]);
}

#[test]
fn a_live_window_puts_only_its_rows_on_the_wire_at_every_list_size() {
    let costs: Vec<_> = [20, 200, 2000]
        .into_iter()
        .map(|count| {
            let (db, ids) = with_rows(count);
            let tape = Arc::new(Tape(Mutex::default()));
            let live = db.live_window(r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#.into(), 10, tape.clone()).unwrap();
            db.db.settle();
            let first = tape.0.lock().unwrap()[0].clone();
            let edge = first.window.clone().unwrap();
            assert_eq!((first.rows.len(), first.full, edge.has_before, edge.has_after, edge.total), (10, true, true, false, Some(count as u32)));
            let update = |at: usize| {
                super::database::executor().unwrap().block_on(db.update("Moment".into(), ids[at].clone(), format!(r#"{{"text":"t{at:05}x"}}"#), "{}".into())).unwrap();
                db.db.settle();
            };
            let calls = tape.0.lock().unwrap().len();
            update(0);
            assert_eq!(tape.0.lock().unwrap().len(), calls, "a row before the window is not on the wire");
            update(count - 1);
            let inside = tape.0.lock().unwrap().last().unwrap().clone();
            live.expand_before(5);
            db.db.settle();
            let wider = tape.0.lock().unwrap().last().unwrap().clone();
            (inside.rows.len(), inside.full, wider.rows.len(), wider.window.unwrap().has_before)
        })
        .collect();
    assert_eq!(costs, vec![(1, false, 5, true); 3], "(rows of an update inside, full, rows of an expand, has_before) at 20, 200, 2000 rows");
}
