//! D-224 (window.md): a page that has no next cursor covers the list to its end, so a member of the window that is not in that last answer leaves the window. Consumer corpus `fixtures/consumer/app`: scope `User.blocked`, action `User.unblock` with `refetch :blocked`, the call order of the app.
use serde_json::{json, Value};

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::{QuerySpec, ReadRequest};

fn user(id: &str) -> Value {
    json!({"__typename": "User", "id": id, "fullName": format!("Name {id}"), "username": format!("name_{id}"), "online": false, "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"})
}

fn blocked(ids: &[&str]) -> Value {
    let edges: Vec<Value> = ids.iter().map(|id| json!({"cursor": format!("c-{id}"), "node": user(id)})).collect();
    json!({"viewer": {"blockedUsers": {"edges": edges, "pageInfo": {"hasNextPage": false, "endCursor": null}, "totalCount": ids.len()}}})
}

fn blocked_query() -> QuerySpec { QuerySpec::json(r#"{"model":"User","relation":"blocked","params":{}}"#) }

async fn ids(stand: &Stand) -> Vec<String> {
    let epoch = stand.database.session_epoch();
    let mut found: Vec<String> = bound(stand.database.read_query(epoch, blocked_query())).await.unwrap().into_iter().map(|row| row.id).collect();
    found.sort();
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn win_page_tail_leaves() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    stand.answer("blockedUsers", blocked(&["u1", "u2", "u3", "u4"]));
    let rows = std::sync::Arc::new(crate::ffi::tape::Rows::default());
    std::mem::forget(stand.database.watch(ReadRequest::List { query: blocked_query() }, rows.clone()));
    bound(stand.database.refresh(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
    stand.settle().await;
    assert_eq!(ids(&stand).await, ["u1", "u2", "u3", "u4"], "the page of 4 users landed");

    stand.answer("UserUnblock", json!({"userUnblock": {"user": user("u4")}}));
    stand.answer("blockedUsers", blocked(&["u1", "u2", "u3"]));
    bound(stand.database.act(epoch, "User".into(), "u4".into(), "unblock".into(), "{}".into(), "{}".into())).await.unwrap();
    stand.settle().await;
    assert_eq!(stand.requests("blockedUsers"), 2, "the unblock refetched the scope");
    let found = ids(&stand).await;
    assert_eq!(found, ["u1", "u2", "u3"], "the refetch answered 3 users without a next cursor: the scope reads 3 rows and count {}", found.len());
}
