use std::sync::Arc;

use serde_json::json;

use super::stand::{chat, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::{ReadFetch, ReadValue};

#[tokio::test(flavor = "multi_thread")]
async fn operation_after_reopen_does_not_take_removed_rows_answer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let credentials = Arc::new(WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("ChatNode", json!({"node": chat("c1", "before")}));
    let rows = stand.entity("Chat", "c1", ReadFetch::Stale);
    crate::test_wait::settled(&stand, "chat loaded", || stand.text_of(&rows, "Chat", "c1", "name").as_deref() == Some("before")).await;
    let (events, token) = stand.events("Chat").await;
    stand.settle().await;
    let received = stand.asked().len();
    stand.answer("ChatPin", json!({"chatPin": {"chat": chat("c1", "old answer")}}));
    stand.hold(&["ChatPin"]);
    let _first = bound(stand.database.act(epoch, "Chat".into(), "c1".into(), "pin".into(), "{}".into(), json!({"pin":true}).to_string())).await.unwrap();
    stand.received(received + 1).await;
    assert_eq!(stand.requests("ChatPin"), 1);
    let old_id: i64 = rusqlite::Connection::open(&path).unwrap().query_row("SELECT seq FROM op", [], |row| row.get(0)).unwrap();
    let old_pair: (String, i64) = rusqlite::Connection::open(&path).unwrap().query_row("SELECT request_id, sequence FROM encrypted_pending", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    stand.push(&token, json!({"area":"CHAT", "kind":"DELETED", "entityId":"c1", "chat":null}));
    assert!(matches!(bound(events.pull_next()).await, Some(ReadValue::Event { .. })));
    stand.settle().await;
    let count: i64 = rusqlite::Connection::open(&path).unwrap().query_row("SELECT COUNT(*) FROM op WHERE seq = ?1", [old_id], |row| row.get(0)).unwrap();
    assert_eq!(count, 0, "the removed row no longer owns the operation");
    stand.release(&["ChatPin"]);
    stand.settle().await;
    let answers: i64 = rusqlite::Connection::open(&path).unwrap().query_row("SELECT COUNT(*) FROM encrypted_answer WHERE operation_id = ?1", [old_id], |row| row.get(0)).unwrap();
    assert_eq!(answers, 1, "the old answer survives its operation");
    stand.database.close_now();

    let reopened = Stand::prepare_on(Some(path.to_string_lossy().into_owned()), credentials).await;
    let epoch = reopened.database.session_epoch();
    reopened.answer("ChatNode", json!({"node": chat("c2", "second")}));
    let rows = reopened.entity("Chat", "c2", ReadFetch::Stale);
    crate::test_wait::settled(&reopened, "second chat loaded", || reopened.text_of(&rows, "Chat", "c2", "name").as_deref() == Some("second")).await;
    reopened.answer("ChatPin", json!({"chatPin": {"chat": chat("c2", "new answer")}}));
    reopened.settle().await;
    let received = reopened.asked().len();
    reopened.hold(&["ChatPin"]);
    let receipt = bound(reopened.database.act(epoch, "Chat".into(), "c2".into(), "pin".into(), "{}".into(), json!({"pin":true}).to_string())).await.unwrap();
    reopened.received(received + 1).await;
    assert_eq!(reopened.requests("ChatPin"), 1);
    let connection = rusqlite::Connection::open(&path).unwrap();
    let new_id: i64 = connection.query_row("SELECT seq FROM op", [], |row| row.get(0)).unwrap();
    let new_pair: (String, i64) = connection.query_row("SELECT request_id, sequence FROM encrypted_pending", [], |row| Ok((row.get(0)?, row.get(1)?))).unwrap();
    assert_eq!(new_id, old_id, "the new operation reuses the old operation id");
    assert_ne!(new_pair, old_pair, "different bodies do not reuse the network idempotency pair");
    reopened.release(&["ChatPin"]);
    let result = bound(reopened.database.confirmed(receipt, false)).await.unwrap();
    assert!(matches!(result.value, Some(row) if row.id == "c2"));
    assert_eq!(reopened.requests("ChatPin"), 1, "the new operation reaches the server");
    reopened.database.close_now();
}
