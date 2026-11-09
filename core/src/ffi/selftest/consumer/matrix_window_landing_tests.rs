//! Matrix W for the membership of a landed list page (D-224, window.md): landing path x position of the removed member x page kind. The oracle is the rule of D-224 written as a function: a page covers the list from its start (page 1) or from its cursor (a next page) to the last arrived row, and to the end of the list when it has no next cursor; a member inside the covered segment that the answer does not carry leaves, members outside stay. Consumer corpus `User.blocked`, call order of the app. Paths driven: window read page 1 (`refresh`), operation refetch (`unblock` with `refetch :blocked`), next page (`load_more`). Not driven (listed in the report): INVALID_CURSOR restart, members_complete chain, the base_revision guard.
use serde_json::{json, Value};

use super::stand::Stand;
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::{QuerySpec, ReadRequest};
use crate::test_matrix::Matrix;

fn user(id: &str) -> Value {
    json!({"__typename": "User", "id": id, "fullName": format!("Name {id}"), "username": format!("name_{id}"), "online": false, "createdAt": "2026-11-08T00:00:00Z", "updatedAt": "2026-11-08T00:00:00Z"})
}

fn page(ids: &[&str], has_next: bool) -> Value {
    let edges: Vec<Value> = ids.iter().map(|id| json!({"cursor": format!("c-{id}"), "node": user(id)})).collect();
    let end = ids.last().map(|id| format!("c-{id}")).or_else(|| Some("c-none".to_string()));
    json!({"viewer": {"blockedUsers": {"edges": edges, "pageInfo": {"hasNextPage": has_next, "endCursor": if has_next { json!(end) } else { Value::Null }}, "totalCount": 9}}})
}

fn blocked_query() -> QuerySpec { QuerySpec::json(r#"{"model":"User","relation":"blocked","params":{}}"#) }

async fn members(stand: &Stand) -> Vec<String> {
    let epoch = stand.database.session_epoch();
    let mut found: Vec<String> = bound(stand.database.read_query(epoch, blocked_query())).await.unwrap().into_iter().map(|row| row.id).collect();
    found.sort();
    found
}

/// The rule of D-224: `segment` is the list the page covers from its start, `answer` the ids that arrived in order.
fn expected(segment: &[&str], answer: &[&str], has_next: bool, first_page: bool) -> Vec<String> {
    if first_page && answer.is_empty() { return Vec::new(); }
    let Some(last) = answer.last() else { return if has_next { segment.iter().map(|id| id.to_string()).collect() } else { Vec::new() } };
    if !has_next { return answer.iter().map(|id| id.to_string()).collect(); }
    let at = segment.iter().position(|id| id == last).unwrap_or(segment.len().saturating_sub(1));
    let mut kept: Vec<String> = answer.iter().map(|id| id.to_string()).collect();
    kept.extend(segment[at + 1..].iter().map(|id| id.to_string()));
    kept
}

const ALL: [&str; 4] = ["u1", "u2", "u3", "u4"];

fn removal(position: &str, segment: &[&'static str]) -> Vec<&'static str> {
    match position {
        "head" => segment[1..].to_vec(),
        "middle" => segment.iter().copied().enumerate().filter(|(index, _)| *index != 1).map(|(_, id)| id).collect(),
        "tail" => segment[..segment.len() - 1].to_vec(),
        _ => Vec::new(),
    }
}

async fn run(path: &str) {
    let mut matrix = Matrix::new(Box::leak(format!("W-window-landing-{path}").into_boxed_str()));
    for position in ["head", "middle", "tail", "all"] {
        for has_next in [false, true] {
            let stand = Stand::open().await;
            let epoch = stand.database.session_epoch();
            let rows = std::sync::Arc::new(crate::ffi::tape::Rows::default());
            std::mem::forget(stand.database.watch(ReadRequest::List { query: blocked_query() }, rows.clone()));
            let (cell_path, remaining, want): (String, Vec<&str>, Vec<String>);
            match path {
                "load_more" => {
                    if position == "middle" { continue; }
                    stand.answer("blockedUsers", page(&["u1", "u2"], true));
                    bound(stand.database.refresh(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
                    stand.answer("blockedUsers", page(&["u3", "u4"], false));
                    bound(stand.database.load_more(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
                    stand.settle().await;
                    let before = members(&stand).await;
                    if before != ["u1", "u2", "u3", "u4"] { matrix.check(&format!("{position}"), "setup", "members before", "u1..u4", &format!("{before:?}")); continue; }
                    stand.answer("blockedUsers", page(&["u1", "u2"], true));
                    bound(stand.database.refresh(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
                    stand.settle().await;
                    remaining = removal(position, &["u3", "u4"]);
                    stand.answer("blockedUsers", page(&remaining, has_next));
                    bound(stand.database.load_more(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
                    want = { let mut keep: Vec<String> = vec!["u1".into(), "u2".into()]; keep.extend(expected(&["u3", "u4"], &remaining, has_next, false)); keep.sort(); keep };
                    cell_path = format!("load_more / next page {}", if has_next { "with cursor" } else { "last" });
                }
                _ => {
                    stand.answer("blockedUsers", page(&ALL, false));
                    bound(stand.database.refresh(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
                    stand.settle().await;
                    remaining = removal(position, &ALL);
                    stand.answer("blockedUsers", page(&remaining, has_next));
                    if path == "refetch" {
                        stand.answer("UserUnblock", json!({"userUnblock": {"user": user("u4")}}));
                        bound(stand.database.act(epoch, "User".into(), "u4".into(), "unblock".into(), "{}".into(), "{}".into())).await.unwrap();
                    } else {
                        bound(stand.database.refresh(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
                    }
                    want = { let mut keep = expected(&ALL, &remaining, has_next, true); keep.sort(); keep };
                    cell_path = format!("{path} / page {}", if has_next { "with cursor" } else { "last" });
                }
            }
            stand.settle().await;
            let got = members(&stand).await;
            matrix.check(&cell_path, "members of the window", position, &format!("{want:?}"), &format!("{got:?}"));
        }
    }
    matrix.finish();
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_window_landing_refresh() { run("refresh").await }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_window_landing_refetch() { run("refetch").await }

#[tokio::test(flavor = "multi_thread")]
async fn matrix_window_landing_load_more() { run("load_more").await }

/// Restart after INVALID_CURSOR: a `load_more` of a window with 2 pages is refused, the window asks page 1 again and lands it.
#[tokio::test(flavor = "multi_thread")]
async fn matrix_window_landing_invalid_cursor() {
    let mut matrix = Matrix::new("W-window-landing-invalid_cursor");
    for position in ["head", "middle", "tail", "all"] {
        for has_next in [false, true] {
            let stand = Stand::open().await;
            let epoch = stand.database.session_epoch();
            let rows = std::sync::Arc::new(crate::ffi::tape::Rows::default());
            std::mem::forget(stand.database.watch(ReadRequest::List { query: blocked_query() }, rows.clone()));
            stand.answer("blockedUsers", page(&["u1", "u2"], true));
            bound(stand.database.refresh(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
            stand.answer("blockedUsers", page(&["u3", "u4"], true));
            bound(stand.database.load_more(epoch, "User".into(), Some("blocked".into()), "{}".into())).await.unwrap();
            stand.settle().await;
            let remaining = removal(position, &ALL);
            stand.answer_next("blockedUsers", json!({"errors": [{"message": "cursor", "extensions": {"code": "INVALID_CURSOR"}}]}));
            stand.answer("blockedUsers", page(&remaining, has_next));
            let outcome = bound(stand.database.load_more(epoch, "User".into(), Some("blocked".into()), "{}".into())).await;
            stand.settle().await;
            let mut want = expected(&ALL, &remaining, has_next, true);
            want.sort();
            let got = members(&stand).await;
            matrix.check(&format!("invalid cursor restart / page {}", if has_next { "with cursor" } else { "last" }), &format!("members of the window (load_more {})", if outcome.is_ok() { "ok" } else { "failed" }), position, &format!("{want:?}"), &format!("{got:?}"));
        }
    }
    matrix.finish();
}
