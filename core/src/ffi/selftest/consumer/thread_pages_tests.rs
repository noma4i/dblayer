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

fn page(first: i32, last: i32, more: bool) -> Value {
    let nodes: Vec<Value> = (first..=last).map(|n| json!({"id":format!("m{n}"),"chatId":"c1","body":format!("message {n}"),"kind":"TEXT","status":"SENT","sequenceNumber":n,"createdAt":"2026-10-27T00:00:00Z","updatedAt":"2026-10-27T00:00:00Z"})).collect();
    json!({"node":{"messages":{"nodes":nodes,"pageInfo":{"hasNextPage":more,"endCursor":first.to_string()}}}})
}

async fn three_pages() -> (Stand, Arc<History>, Arc<crate::ffi::tape::Watched>) {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", page(41, 65, true));
    let history = Arc::new(History::default());
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let stream = stand.database.watch(ReadRequest::List { query }, history.clone());
    settled(&stand, "the first history page", || history.rows.len() == 25).await;
    stand.answer("chatThread", page(16, 40, true));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    settled(&stand, "the second history page", || history.rows.len() == 50).await;
    stand.answer("chatThread", page(1, 15, false));
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
    stand.answer("chatThread", page(41, 65, true));
    bound(stand.database.refresh(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.settle().await;
    let rows = ids(&history);
    assert_eq!(rows, (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>(), "refresh retained {} rows: {rows:?}", rows.len());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(stand.requests("chatThread"), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn consumer_thread_three_pages_keep_all_rows() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("chatThread", page(41, 65, true));
    let history = Arc::new(History::default());
    let query = QuerySpec::json(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#);
    let _stream = stand.database.watch(ReadRequest::List { query }, history.clone());
    settled(&stand, "the first history page", || history.rows.len() == 25).await;
    assert_eq!(history.rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>(), (41..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(true));
    stand.answer("chatThread", page(16, 40, true));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    settled(&stand, "the second history page", || history.rows.len() == 50).await;
    assert_eq!(history.rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect::<Vec<_>>(), (16..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    stand.answer("chatThread", page(1, 15, false));
    bound(stand.database.load_more(epoch, "Message".into(), Some("thread".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    stand.settle().await;
    let ids: Vec<_> = history.rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect();
    assert_eq!(ids, (1..=65).map(|n| format!("m{n}")).collect::<Vec<_>>());
    assert_eq!(*history.has_more.lock().unwrap(), Some(false));
    assert_eq!(stand.requests("chatThread"), 3);
}
