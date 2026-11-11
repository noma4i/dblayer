use std::sync::{Arc, Mutex};
use serde_json::{json, Value};

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::tape::{Reader, Rows, StreamDelivery};
use crate::ffi::{QuerySpec, ReadRequest, ReadValue};
use crate::test_wait::settled;

#[derive(Default)]
struct History { rows: Rows, has_more: Mutex<Option<bool>> }

impl Reader for History {
    fn delivered(&self, delivery: StreamDelivery) {
        match &delivery.value {
            ReadValue::Snapshot { state, .. } | ReadValue::State { state, .. } | ReadValue::Delta { state, .. } => *self.has_more.lock().unwrap() = Some(state.has_more),
            _ => {}
        }
        self.rows.delivered(delivery);
    }
}

pub(super) fn page(first: i32, last: i32, more: bool, total: i32) -> Value {
    let nodes: Vec<Value> = (first..=last).map(|n| json!({"id":format!("m{n}"),"chatId":"c1","body":format!("message {n}"),"kind":"TEXT","status":"SENT","oneTime":false,"expired":false,"isSystem":false,"isForwarded":false,"isReply":false,"unread":false,"sequenceNumber":n,"createdAt":"2026-10-27T00:00:00Z","updatedAt":"2026-10-27T00:00:00Z"})).collect();
    let edges: Vec<_> = nodes.into_iter().map(|node| json!({"node":node})).collect();
    json!({"node":{"messages":{"totalCount":total,"edges":edges,"pageInfo":{"hasNextPage":more,"endCursor":first.to_string()}}}})
}

async fn three_pages() -> (Stand, Arc<History>, Arc<crate::ffi::tape::Watched>) {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", page(41, 65, true, 65));
    let history = Arc::new(History::default());
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let stream = stand.database.watch(ReadRequest::List { query }, history.clone());
    settled(&stand, "the first history page", || history.rows.len() == 25).await;
    stand.answer("chatThread", page(16, 40, true, 65));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    settled(&stand, "the second history page", || history.rows.len() == 50).await;
    stand.answer("chatThread", page(1, 15, false, 65));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.settle().await;
    assert_eq!(ids(&history), (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(stand.requests("chatThread"), 3);
    (stand, history, stream)
}

fn ids(history: &History) -> Vec<String> {
    history.rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_refresh_keeps_older_pages() {
    let (stand, history, _stream) = three_pages().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", page(41, 65, true, 65));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.settle().await;
    let rows = ids(&history);
    assert_eq!(rows, (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>(), "refresh retained {} rows: {rows:?}", rows.len());
    assert_eq!(stand.requests("chatThread"), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_event_adds_after_three_pages() {
    let (stand, history, _stream) = three_pages().await;
    let (events, token) = stand.events("Message").await;
    let mut message = super::stand::message("m66", "c1", "message 66");
    message["sequenceNumber"] = json!(66);
    message["status"] = json!("SENT");
    stand.push(&token, json!({"area":"MESSAGE", "kind":"CREATED", "entityId":"m66", "parentId":"c1", "message":message}));
    let event = bound(events.pull_next()).await;
    assert!(matches!(event, Some(ReadValue::Event { ref id, .. }) if id == "m66"), "{event:?}");
    stand.settle().await;
    let rows = ids(&history);
    assert_eq!(rows, (1..=66).map(|n| format!("m{n}")).collect::<Vec<_>>(), "event left {} rows: {rows:?}", rows.len());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(stand.requests("chatThread"), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_refresh_removes_deleted_head_row() {
    let (stand, history, _stream) = three_pages().await;
    let mut answer = page(41, 65, true, 65);
    answer["node"]["messages"]["edges"].as_array_mut().unwrap().retain(|edge| edge["node"]["id"] != "m50");
    stand.answer("chatThread", answer);
    bound(stand.database.refresh(stand.database.session_epoch(), "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.settle().await;
    let rows = ids(&history);
    assert_eq!(rows, (1..=65).filter(|n| *n != 50).map(|n| format!("m{n}")).collect::<Vec<_>>(), "refresh after deletion retained {} rows: {rows:?}", rows.len());
    assert_eq!(stand.requests("chatThread"), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_load_more_after_end_is_quiet() {
    let (stand, history, _stream) = three_pages().await;
    let before = stand.asked().len();
    let result = bound(stand.database.load_more(stand.database.session_epoch(), "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    assert!(matches!(result, crate::ffi::LoadMoreResult::NoMore), "{result:?}");
    stand.settle().await;
    assert_eq!(stand.requests("chatThread"), 3);
    assert_eq!(stand.asked().len(), before);
    assert_eq!(ids(&history), (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_three_pages_keep_all_rows() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", page(41, 65, true, 65));
    let history = Arc::new(History::default());
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let _stream = stand.database.watch(ReadRequest::List { query }, history.clone());
    settled(&stand, "the first history page", || history.rows.len() == 25).await;
    assert_eq!(history.rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>(), (41..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(true));
    stand.answer("chatThread", page(16, 40, true, 65));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    settled(&stand, "the second history page", || history.rows.len() == 50).await;
    assert_eq!(history.rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>(), (16..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    stand.answer("chatThread", page(1, 15, false, 65));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.settle().await;
    let ids: Vec<_> = history.rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect();
    assert_eq!(ids, (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(stand.requests("chatThread"), 3);
}

/// The end of the history reached by the loaded pages stays known after the first page is read again.
#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_refresh_keeps_history_end() {
    let (stand, history, _stream) = three_pages().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", page(41, 65, true, 65));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.settle().await;
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_tail_thread_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("app.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(crate::ffi::tests::session_guards_tests::WatchedHeld::default());
    let stand = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    stand.sign_in().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", page(41, 65, true, 65));
    let history = Arc::new(History::default());
    let query = || QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let stream = stand.database.watch(ReadRequest::List { query: query() }, history.clone());
    settled(&stand, "the first page", || history.rows.len() == 25).await;
    stand.answer("chatThread", page(16, 40, true, 65));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    settled(&stand, "the second page", || history.rows.len() == 50).await;
    stand.answer("chatThread", page(1, 15, false, 65));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    settled(&stand, "all pages", || history.rows.len() == 65).await;
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    bound(stand.database.close_database()).await.unwrap();
    drop(stream);
    let reopened = Stand::prepare_on(Some(path), credentials).await;
    let history = Arc::new(History::default());
    let _stream = reopened.database.watch(ReadRequest::List { query: query() }, history.clone());
    settled(&reopened, "restored history", || history.rows.len() == 65).await;
    assert_eq!(ids(&history), (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(reopened.requests("chatThread"), 0);
    reopened.answer("chatThread", page(41, 65, true, 65));
    bound(reopened.database.refresh(reopened.database.session_epoch(), "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    reopened.settle().await;
    let rows = ids(&history);
    assert_eq!(rows, (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>(), "after reopen refresh: {} rows, has_more={:?}, requests={}", rows.len(), *history.has_more.lock().unwrap(), reopened.requests("chatThread"));
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(reopened.requests("chatThread"), 1);
    reopened.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_tail_invalid_cursor_restarts_history() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    let history = Arc::new(History::default());
    stand.answer("chatThread", page(41, 65, true, 65));
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let _stream = stand.database.watch(ReadRequest::List { query }, history.clone());
    settled(&stand, "first page", || history.rows.len() == 25).await;
    stand.answer("chatThread", page(16, 40, true, 65));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    settled(&stand, "second page", || history.rows.len() == 50).await;
    stand.answer_next("chatThread", json!({"errors":[{"message":"cursor", "extensions":{"code":"INVALID_CURSOR"}}]}));
    stand.answer("chatThread", page(50, 55, false, 6));
    let result = bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    assert!(matches!(result, crate::ffi::LoadMoreResult::Loaded { has_more: false }), "{result:?}");
    stand.settle().await;
    let rows = ids(&history);
    assert_eq!(rows, (50..=55).map(|n| format!("m{n}")).collect::<Vec<_>>(), "after invalid cursor: {} rows {rows:?}", rows.len());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    let requests: Vec<_> = stand.asked().into_iter().filter(|(name, _)| name == "chatThread").map(|(_, vars)| vars).collect();
    assert_eq!(requests.len(), 4, "{requests:?}");
    assert!(requests[0].get("after").is_none_or(Value::is_null), "{requests:?}");
    assert_eq!(requests[1]["after"], "41");
    assert_eq!(requests[2]["after"], "16");
    assert!(requests[3].get("after").is_none_or(Value::is_null), "{requests:?}");
    let result = bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    assert!(matches!(result, crate::ffi::LoadMoreResult::NoMore));
    stand.settle().await;
    assert_eq!(stand.requests("chatThread"), 4);
    stand.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_ordered_page_old_answer_after_delete() {
    let (stand, history, _stream) = three_pages().await;
    let (events, token) = stand.events("Message").await;
    let received = stand.request_mark();
    stand.hold(&["chatThread"]);
    stand.answer("chatThread", page(41, 65, true, 65));
    let database = stand.database.clone();
    let refresh = tokio::spawn(async move {
        bound(database.refresh(database.session_epoch(), "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await
    });
    stand.receive_after(received, "chatThread", Some(("chatId", "c1")), 1).await;
    stand.push(&token, json!({"area":"MESSAGE", "kind":"DELETED", "entityId":"m50", "parentId":"c1"}));
    let event = bound(events.pull_next()).await;
    assert!(matches!(event, Some(ReadValue::Event { ref id, .. }) if id == "m50"), "{event:?}");
    settled(&stand, "the deleted message leaves the history", || !ids(&history).contains(&"m50".to_string())).await;
    stand.release(&["chatThread"]);
    bound(refresh).await.unwrap().unwrap();
    stand.settle().await;
    assert_eq!(ids(&history), (1..=65).filter(|n| *n != 50).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(stand.requests("chatThread"), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_queued_operation_server_delete() {
    let (stand, history, _stream) = three_pages().await;
    let (events, token) = stand.events("Message").await;
    stand.hold(&["MessageUpdate"]);
    let received = stand.request_mark();
    let epoch = stand.database.session_epoch();
    let receipt = bound(stand.database.update(epoch, "Message".into(), "m50".into(), r#"{"body":"edited"}"#.into(), "{}".into())).await.unwrap();
    stand.receive_after(received, "MessageUpdate", None, 1).await;
    stand.push(&token, json!({"area":"MESSAGE", "kind":"DELETED", "entityId":"m50", "parentId":"c1"}));
    let event = bound(events.pull_next()).await;
    assert!(matches!(event, Some(ReadValue::Event { ref id, .. }) if id == "m50"), "{event:?}");
    let outcome = bound(stand.database.confirmed(receipt, false)).await;
    assert!(matches!(outcome, Err(crate::ffi::DbError::Interrupted { ref detail }) if detail.to_lowercase().contains("discard")), "{outcome:?}");
    stand.release(&["MessageUpdate"]);
    stand.settle().await;
    assert!(!ids(&history).contains(&"m50".to_string()));
    let operations = stand.database.subscribe_values(epoch, ReadRequest::Operations);
    let value = bound(operations.pull_next()).await;
    assert!(matches!(value, Some(ReadValue::Operations { ref items, .. }) if items.is_empty()), "{value:?}");
    assert_eq!(stand.requests("MessageUpdate"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_attempt_number_survives_restart() {
    use crate::ffi::tests::session_guards_tests::WatchedHeld;
    use crate::ffi::wire::Cell;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("attempt.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    first.sign_in().await;
    first.answer("chatThread", page(1, 1, false, 1));
    let epoch = first.database.session_epoch();
    bound(first.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    first.database.set_online(false).unwrap();
    let receipt = bound(first.database.update(epoch, "Message".into(), "m1".into(), r#"{"body":"edited"}"#.into(), "{}".into())).await.unwrap();
    let operation = receipt.operation.unwrap();
    let saved = || {
        let db = rusqlite::Connection::open(&path).unwrap();
        let attempts: i64 = db.query_row("SELECT attempts FROM op WHERE seq = ?1", [operation], |row| row.get(0)).unwrap();
        let pending: i64 = db.query_row("SELECT count(*) FROM encrypted_pending", [], |row| row.get(0)).unwrap();
        (attempts, pending)
    };
    let (attempts, pending) = saved();
    assert!(attempts >= 1, "the first attempt must already be recorded: {attempts}");
    assert_eq!(pending, 0, "the operation has no retained pair before the restart");
    assert_eq!(first.requests("MessageUpdate"), 0);
    drop(receipt);
    bound(first.database.close_database()).await.unwrap();
    drop(first);
    assert_eq!(saved(), (attempts, 0));

    let reopened = Stand::prepare_online(Some(path.clone()), credentials, None, false).await;
    let epoch = reopened.database.session_epoch();
    assert_eq!(saved(), (attempts, 0), "opening does not consume another attempt");
    let history = Arc::new(History::default());
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let _stream = reopened.database.watch(ReadRequest::List { query }, history.clone());
    settled(&reopened, "the restored message reaches the reader", || ids(&history) == ["m1"]).await;
    reopened.answer("MessageUpdate", json!({"messageUpdate":{"message":super::stand::message("m1", "c1", "server confirmed")}}));
    reopened.hold(&["MessageUpdate"]);
    let mark = reopened.request_mark();
    reopened.database.set_online(true).unwrap();
    reopened.receive_after(mark, "MessageUpdate", None, 1).await;
    assert_eq!(saved().0, attempts, "the restored send uses the recorded attempt");
    reopened.release(&["MessageUpdate"]);
    settled(&reopened, "the confirmed server value reaches the reader", || reopened.text_of(&history.rows, "Message", "m1", "body").as_deref() == Some("server confirmed")).await;
    let row = bound(reopened.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().unwrap();
    assert_eq!(reopened.cell("Message", &row, "body"), Cell::Text { value: "server confirmed".into() });
    let operations = reopened.database.subscribe_values(epoch, ReadRequest::Operations);
    let value = bound(operations.pull_next()).await;
    assert!(matches!(value, Some(ReadValue::Operations { ref items, .. }) if items.is_empty()), "{value:?}");
    assert_eq!(reopened.requests("MessageUpdate"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_destroy_not_found_removes_row() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", page(1, 1, false, 1));
    let history = Arc::new(History::default());
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let _stream = stand.database.watch(ReadRequest::List { query }, history.clone());
    settled(&stand, "the server message reaches the reader", || ids(&history) == ["m1"]).await;
    assert!(stand.database.peek_row(epoch, "Message".into(), "m1".into()).unwrap().is_some());

    stand.database.set_online(false).unwrap();
    let receipt = bound(stand.database.destroy(epoch, "Message".into(), "m1".into())).await.unwrap();
    settled(&stand, "the optimistic deletion reaches the reader", || history.rows.emptied()).await;
    assert!(stand.database.peek_row(epoch, "Message".into(), "m1".into()).unwrap().is_none());
    let after_delete = history.rows.1.lock().unwrap().len();
    stand.answer("chatThread", page(1, 0, false, 0));
    stand.answer_next("MessageDelete", json!({"errors":[{"message":"missing","extensions":{"code":"NOT_FOUND"}}]}));
    stand.database.set_online(true).unwrap();
    let confirmed = bound(stand.database.confirmed(receipt, false)).await;
    assert!(confirmed.is_ok(), "NOT_FOUND confirms the deletion: {confirmed:?}");
    stand.settle().await;

    assert!(stand.database.peek_row(epoch, "Message".into(), "m1".into()).unwrap().is_none());
    assert!(ids(&history).is_empty());
    assert!(history.rows.1.lock().unwrap()[after_delete..].iter().all(|frame| frame.rows.iter().all(|row| row.id != "m1")), "the reader must not receive the row again");
    let operations = stand.database.subscribe_values(epoch, ReadRequest::Operations);
    let value = bound(operations.pull_next()).await;
    assert!(matches!(value, Some(ReadValue::Operations { ref items, .. }) if items.is_empty()), "{value:?}");
    assert_eq!(stand.requests("MessageDelete"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn out_restore_removed_row() {
    use crate::ffi::tests::session_guards_tests::WatchedHeld;
    use crate::owners::store::StoreOp;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("removed.sqlite").to_string_lossy().into_owned();
    let credentials = Arc::new(WatchedHeld::default());
    let first = Stand::prepare_on(Some(path.clone()), credentials.clone()).await;
    first.sign_in().await;
    first.answer("chatThread", page(1, 1, false, 1));
    let epoch = first.database.session_epoch();
    bound(first.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    first.database.set_online(false).unwrap();
    let receipt = bound(first.database.update(epoch, "Message".into(), "m1".into(), r#"{"body":"edited"}"#.into(), "{}".into())).await.unwrap();
    assert!(receipt.optimistic_value.is_some());
    first.database.db.write_queue_ops(&[StoreOp::RemoveRows { model: "Message".into(), ids: vec!["m1".into()] }]).unwrap();
    first.database.db.mark_tombstone("Message", "m1", 100, i64::MAX).unwrap();
    assert!(first.database.db.is_tombstoned("Message", "m1").unwrap());
    let operations = first.database.subscribe_values(epoch, ReadRequest::Operations);
    assert!(matches!(bound(operations.pull_next()).await, Some(ReadValue::Operations { ref items, .. }) if items.iter().any(|item| item.row_id == "m1")));
    bound(first.database.close_database()).await.unwrap();
    drop(first);
    let reopened = Stand::prepare_online(Some(path), credentials, None, false).await;
    reopened.settle().await;
    let stream = reopened.database.subscribe_values(reopened.database.session_epoch(), ReadRequest::Operations);
    let snapshot = bound(stream.pull_next()).await;
    assert!(matches!(snapshot, Some(ReadValue::Operations { ref items, .. }) if items.iter().all(|item| item.row_id != "m1")), "restoration must discard the deleted row's operation: {snapshot:?}");
    assert!(reopened.database.peek_row(reopened.database.session_epoch(), "Message".into(), "m1".into()).unwrap().is_none());
    reopened.database.set_online(true).unwrap();
    reopened.settle().await;
    assert_eq!(reopened.requests("MessageUpdate"), 0);
    reopened.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_temp_parent_swap() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.database.set_online(false).unwrap();
    let values = json!({"uuid":"local-chat","kind":"GROUP","status":"PRIMARY","lists":["PRIMARY"],"premium":false,"pinned":false,"muted":false,"read":true,"serverRevision":1,"messagesCount":0,"membersCount":2,"lastActivityAt":"2026-11-08T00:00:00Z","createdAt":"2026-11-08T00:00:00Z","updatedAt":"2026-11-08T00:00:00Z","name":"group"});
    let receipt = bound(stand.database.create(epoch, "Chat".into(), values.to_string(), r#"{"userIds":[]}"#.into())).await.unwrap();
    let temporary = receipt.optimistic_value.expect("the optimistic chat").id;
    assert!(temporary.starts_with("temp-"), "{temporary}");
    let query = QuerySpec::json(json!({"model":"Message","relation":"thread","params":{"chatId":temporary}}).to_string());
    let history = Arc::new(History::default());
    let _stream = stand.database.watch(ReadRequest::List { query }, history.clone());
    stand.settle().await;
    assert_eq!(stand.requests("chatThread"), 0);
    stand.answer("ChatGroupCreate", json!({"groupChatCreate":{"skippedUserIds":[],"chat":super::stand::chat("c1", "group")}}));
    stand.answer("chatThread", page(1, 2, false, 2));
    stand.database.set_online(true).unwrap();
    settled(&stand, "the thread of the confirmed chat", || history.rows.len() == 2).await;
    assert_eq!(ids(&history), ["m1", "m2"]);
    assert_eq!(stand.requests_with("chatThread", "chatId", "c1"), 1);
    assert_eq!(stand.requests_with("chatThread", "chatId", &temporary), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_failed_page_retry() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    let history = Arc::new(History::default());
    stand.answer("chatThread", page(41, 65, true, 65));
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let _stream = stand.database.watch(ReadRequest::List { query }, history.clone());
    settled(&stand, "the first history page", || history.rows.len() == 25).await;
    stand.answer_next("chatThread", json!({"errors":[{"message":"temporarily unavailable","extensions":{"code":"SERVICE_UNAVAILABLE"}}]}));
    stand.answer("chatThread", page(16, 40, false, 50));
    let mut armed = stand.clock.armed();
    let load = stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into());
    let result = crate::test_wait::within_named(15, "the failed page is retried", async {
        tokio::pin!(load);
        loop {
            tokio::select! {
                result = &mut load => break result,
                () = stand.clock.await_armed(armed) => { armed = stand.clock.armed(); stand.clock.advance(16_000); }
            }
        }
    }).await;
    result.unwrap();
    stand.settle().await;
    assert_eq!(ids(&history), (16..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(stand.requests("chatThread"), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_stream_reset_and_gap() {
    let (stand, history, _stream) = three_pages().await;
    let (events, token) = stand.events("Message").await;
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let projection = stand.database.subscribe_values(stand.database.session_epoch(), ReadRequest::List { query });
    let first = bound(projection.pull(0)).await;
    assert!(matches!(first, Some(ReadValue::Snapshot { .. })), "{first:?}");
    projection.resync();
    let snapshot = bound(projection.pull(0)).await;
    assert!(matches!(snapshot, Some(ReadValue::Snapshot { ref rows, .. }) if rows.len() == 65), "{snapshot:?}");
    stand.answer("chatThread", page(41, 65, true, 65));
    stand.drop_cable(&token);
    let _token = stand.next_subscription().await;
    let gap = bound(events.pull_next()).await;
    assert!(matches!(gap, Some(ReadValue::Gap)), "{gap:?}");
    stand.settle().await;
    assert_eq!(ids(&history), (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(stand.requests("chatThread"), 4);
}
