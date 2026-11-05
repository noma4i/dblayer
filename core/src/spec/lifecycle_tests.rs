use serde_json::json;

use super::lifecycle::{explore, LifecycleOwner};
use crate::check::model::run;
use crate::check::value::Input;

/// "name" or "name:argument" -> an input of the lifecycle vocabulary.
fn seq(items: &[&str]) -> Vec<Input> {
    items.iter().map(|item| match item.split_once(':') {
        Some(("drain", owner)) => Input::new("drain").with("owner", owner),
        Some(("call_data", epoch)) => Input::new("call_data").with("epoch", epoch),
        Some(("create", name)) => Input::new("create").with("as", name),
        _ if *item == "reset_models" => Input::new("reset_models").with("models", json!(["Moment"])),
        _ => Input::new(item),
    }).collect()
}

async fn check(test: &str, items: &[&str], full: bool) {
    let owner = if full { LifecycleOwner::full(None) } else { LifecycleOwner::public(None) };
    if let Err(divergence) = run(&owner, test, &seq(items)).await {
        panic!("{test}: {divergence}");
    }
}

async fn property(only: &'static str, full: bool, required: &[&[&str]]) {
    let owner = if full { LifecycleOwner::full(Some(only)) } else { LifecycleOwner::public(Some(only)) };
    let required: Vec<Vec<Input>> = required.iter().map(|items| seq(items)).collect();
    if let Err(divergence) = explore(&owner, 4, &[1, 2], 30, &required).await {
        panic!("{only}: {divergence}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn life_nosession_credential() {
    check("life_nosession_credential", &["login"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_nosession_reset() {
    check("life_nosession_reset", &["session_reset"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_nosession_close() {
    check("life_nosession_close", &["close"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_session_same_credential() {
    check("life_session_same_credential", &["login", "login"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_session_reset() {
    check("life_session_reset", &["login", "session_reset"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_session_server_ended() {
    check("life_session_server_ended", &["login", "logout"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_session_close() {
    check("life_session_close", &["login", "close"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_closed_refuses() {
    check("life_closed_refuses", &["login", "close", "session_reset", "close", "login"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_call_current_epoch() {
    check("life_call_current_epoch", &["login", "call_data:current"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_call_stale_epoch() {
    check("life_call_stale_epoch", &["login", "call_data:other"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_call_closed() {
    check("life_call_closed", &["login", "close", "call_data:current"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_call_nosession_data() {
    check("life_call_nosession_data", &["call_data:current"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_prop_epoch_monotonic() {
    property("P-LIFE-01", false, &[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_prop_no_work_after_end() {
    property("P-LIFE-02", false, &[&["login", "create:a", "session_reset", "login", "rows"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_prop_clear_before_start() {
    property("P-LIFE-03", false, &[&["login", "create:a", "session_reset", "login", "rows"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_prop_reply_once() {
    property("P-LIFE-04", false, &[&["login", "session_reset", "session_reset"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_prop_close_drains() {
    property("P-LIFE-05", false, &[&["login", "close", "call_data:current", "close"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_prop_session_kept() {
    property("P-LIFE-07", false, &[&["login", "close"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_prop_idle() {
    property("P-LIFE-08", false, &[&["login", "idle", "session_reset", "idle"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_findings_public() {
    check("life_findings_public", &["login", "create:a", "session_reset", "login", "rows"], false).await;
    check("life_findings_public", &["login", "session_reset", "session_reset"], false).await;
}
