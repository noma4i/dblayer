use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{Config, Database, LiveChange, QueryListener};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

struct Tape(Mutex<Vec<LiveChange>>);

impl QueryListener for Tape {
    fn changed(&self, change: LiveChange) {
        self.0.lock().unwrap().push(change);
    }
}

async fn heard(tape: &Tape, count: usize) -> Vec<LiveChange> {
    for _ in 0..200 {
        let seen = tape.0.lock().unwrap().clone();
        if seen.len() >= count { return seen; }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("expected {count} changes, heard {}", tape.0.lock().unwrap().len());
}

#[tokio::test]
async fn a_reported_desync_makes_the_next_delivery_the_whole_list_of_the_database() {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    let database = Database::open(Config { path: None, data_version: None, cache_rows: 16 }, serde_json::to_string(&vec![model]).unwrap(), None, None, None, None, None, None).await.unwrap();
    let settle = || database.db.await_revision(database.db.revision_seq().unwrap());
    for text in ["a", "b", "c"] {
        database.create("Note".into(), serde_json::json!({"text": text}).to_string(), "{}".into()).await.unwrap();
        settle();
    }
    let tape = Arc::new(Tape(Mutex::new(Vec::new())));
    let subscription = database.live_query(r#"{"model":"Note","order":[{"field":"text","direction":"asc"}]}"#.into(), tape.clone());
    let first = heard(&tape, 1).await;
    assert!(first[0].full && first[0].rows.len() == 3);
    database.create("Note".into(), r#"{"text":"d"}"#.into(), "{}".into()).await.unwrap();
    settle();
    let seen = heard(&tape, 2).await;
    assert!(!seen[1].full, "a later change arrives as a delta");
    subscription.report_desync();
    let seen = heard(&tape, 3).await;
    let whole = &seen[2];
    assert!(whole.full && whole.size == 4);
    assert_eq!(whole.rows.len(), 4);
    subscription.cancel();
}
