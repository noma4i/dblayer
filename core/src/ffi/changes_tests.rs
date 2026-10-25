use super::*;
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;
use crate::test_wait::within;

#[tokio::test]
async fn a_slow_reader_receives_a_bounded_replacement_with_every_current_row() {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    let database = Database::open(super::super::Config { path: None, data_version: None, cache_rows: 16 }, serde_json::to_string(&vec![model]).unwrap(), None, None, None, None, None, None).await.unwrap();
    let query = r#"{"model":"Note","order":[{"field":"text","direction":"asc"}]}"#;
    let queue = database.changes(query.into()).unwrap();
    let mut ids = Vec::new();
    for number in 0..(CHANGE_QUEUE_LIMIT * 3) {
        ids.push(database.create("Note".into(), serde_json::json!({"text": number.to_string()}).to_string(), "{}".into()).await.unwrap().id);
        database.db.await_revision(database.db.revision_seq().unwrap());
    }
    database.update("Note".into(), ids[0].clone(), r#"{"text":"edited"}"#.into(), "{}".into()).await.unwrap();
    database.destroy("Note".into(), ids[1].clone()).await.unwrap();
    database.db.await_revision(database.db.revision_seq().unwrap());
    for _ in 0..CHANGE_QUEUE_LIMIT {
        database.create("Note".into(), r#"{"text":"new"}"#.into(), "{}".into()).await.unwrap();
        database.db.await_revision(database.db.revision_seq().unwrap());
        let state = crate::db::locked(&queue.state);
        if state.pending.len() == 1 && state.pending[0].full { break; }
    }
    assert!(crate::db::locked(&queue.state).pending.len() <= CHANGE_QUEUE_LIMIT, "slow consumer queue exceeded the core limit");
    let replacement = within(queue.next()).await.unwrap();
    assert!(replacement.full);
    let current = database.peek_query(query.into()).await.unwrap().unwrap();
    assert_eq!(replacement.rows, current.rows);
    let mut expected = current.rows;
    let from = expected[0].id.clone();
    expected[0].id = "canonical".into();
    let mut renamed = LiveChange::whole(current.revision + 1, vec![expected[0].clone()]);
    renamed.full = false;
    renamed.size = expected.len() as u32;
    renamed.row_indexes = vec![0];
    renamed.inserted.clear();
    renamed.updated = vec![0];
    renamed.replaced = vec![super::super::live::Replacement { from, to: "canonical".into() }];
    renamed.renamed = renamed.replaced.clone();
    queue.push(renamed);
    for index in 0..CHANGE_QUEUE_LIMIT { queue.push(LiveChange { error: Some(DbError::Network { detail: "lost".into() }), ..LiveChange::empty(current.revision + 2 + index as i64) }); }
    let replacement = within(queue.next()).await.unwrap();
    assert!(replacement.full);
    assert_eq!(replacement.rows, expected);
    assert!(matches!(replacement.error, Some(DbError::Network { .. })));
    queue.cancel();
    assert!(within(queue.next()).await.is_none());
}
