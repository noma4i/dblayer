use std::sync::Arc;
use std::time::Duration;

use super::stream::tape::Rows;
use super::stream::StreamValue;
use super::{Config, Database};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

async fn heard(tape: &Rows, count: usize) -> Vec<StreamValue> {
    for _ in 0..200 {
        let seen = tape.values();
        if seen.len() >= count { return seen; }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("expected {count} values, heard {}", tape.values().len());
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
    let tape = Arc::new(Rows::default());
    let stream = database.watch_query(r#"{"model":"Note","order":[{"field":"text","direction":"asc"}]}"#, tape.clone());
    let first = heard(&tape, 1).await;
    assert!(matches!(&first[0], StreamValue::Snapshot { rows, .. } if rows.len() == 3));
    database.create("Note".into(), r#"{"text":"d"}"#.into(), "{}".into()).await.unwrap();
    settle();
    let seen = heard(&tape, 2).await;
    assert!(matches!(seen[1], StreamValue::Delta { .. }), "a later change arrives as a delta");
    stream.report_desync();
    let seen = heard(&tape, 3).await;
    assert!(matches!(&seen[2], StreamValue::Snapshot { rows, .. } if rows.len() == 4), "the answer to a reported desync is one whole list");
    stream.cancel();
}
