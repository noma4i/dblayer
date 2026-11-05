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

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_nosession_server_ended() {
    check("life_nosession_server_ended", &["server_ended", "server_ended_stale"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_nosession_reset_models() {
    check("life_nosession_reset_models", &["store_hold", "reset_models", "store_ok"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_nosession_stray_store() {
    check("life_nosession_stray_store", &["stray_cleared", "stray_models_cleared", "stray_clear_failed"], true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_nosession_close() {
    check("life_nosession_close", &["close"], false).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_nosession_drained() {
    check("life_nosession_drained", &["stray_drained"], true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_session_same_credential() {
    check("life_session_same_credential", &["login", "login"], false).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_session_identity_changed() {
    check("life_session_identity_changed", &["login", "credential_other"], true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_session_reset() {
    check("life_session_reset", &["login", "session_reset"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_session_server_ended() {
    check("life_session_server_ended", &["login", "logout"], false).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_session_stale_server_ended() {
    check("life_session_stale_server_ended", &["login", "server_ended_stale"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_session_reset_models() {
    check("life_session_reset_models", &["login", "store_hold", "reset_models", "store_ok"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_session_stray_store() {
    check("life_session_stray_store", &["login", "stray_cleared", "stray_models_cleared", "stray_clear_failed"], true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn life_session_close() {
    check("life_session_close", &["login", "close"], false).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_session_drained() {
    check("life_session_drained", &["login", "stray_drained"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_credential() {
    check("life_ending_credential", &["login", "store_hold", "session_reset", "login"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_credential_after_failure() {
    check("life_ending_credential_after_failure", &["login", "store_hold", "session_reset", "store_fail", "login"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_reset_joins() {
    check("life_ending_reset_joins", &["login", "store_hold", "session_reset", "session_reset"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_reset_after_failure() {
    check("life_ending_reset_after_failure", &["login", "store_hold", "session_reset", "store_fail", "session_reset"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_server_ended() {
    check("life_ending_server_ended", &["login", "store_hold", "session_reset", "server_ended"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_reset_models() {
    check("life_ending_reset_models", &["login", "store_hold", "session_reset", "reset_models"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_cleared_signed_out() {
    check("life_ending_cleared_signed_out", &["login", "store_hold", "session_reset", "store_ok"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_cleared_next_identity() {
    check("life_ending_cleared_next_identity", &["login", "store_hold", "credential_other", "store_ok"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_clear_failed() {
    check("life_ending_clear_failed", &["login", "store_hold", "session_reset", "store_fail"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_stray_models() {
    check("life_ending_stray_models", &["login", "store_hold", "session_reset", "stray_models_cleared"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_close() {
    check("life_ending_close", &["login", "store_hold", "session_reset", "close"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_ending_drained() {
    check("life_ending_drained", &["login", "store_hold", "session_reset", "stray_drained"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_credential() {
    check("life_resetting_credential", &["login", "store_hold", "reset_models", "credential_other"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_same_credential() {
    check("life_resetting_same_credential", &["login", "store_hold", "reset_models", "login"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_session_reset() {
    check("life_resetting_session_reset", &["login", "store_hold", "reset_models", "session_reset"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_server_ended() {
    check("life_resetting_server_ended", &["login", "store_hold", "reset_models", "server_ended"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_stale_server_ended() {
    check("life_resetting_stale_server_ended", &["login", "store_hold", "reset_models", "server_ended_stale"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_reset_models() {
    check("life_resetting_reset_models", &["login", "store_hold", "reset_models", "reset_models", "store_ok", "store_ok"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_credential_signed_out() {
    check("life_resetting_credential_signed_out", &["store_hold", "reset_models", "login", "store_ok"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_cleared_then_start() {
    check("life_resetting_cleared_then_start", &["store_hold", "reset_models", "login", "store_ok", "rows"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_cleared_queued() {
    check("life_resetting_cleared_queued", &["login", "store_hold", "reset_models", "reset_models", "store_ok", "store_ok"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_cleared() {
    check("life_resetting_cleared", &["login", "store_hold", "reset_models", "store_ok"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_clear_failed() {
    check("life_resetting_clear_failed", &["login", "store_hold", "reset_models", "store_fail"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_stray_cleared() {
    check("life_resetting_stray_cleared", &["login", "store_hold", "reset_models", "stray_cleared"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_close() {
    check("life_resetting_close", &["login", "store_hold", "reset_models", "close"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_resetting_drained() {
    check("life_resetting_drained", &["login", "store_hold", "reset_models", "stray_drained"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_closing_last_drained() {
    check("life_closing_last_drained", &["login", "store_hold", "close", "drain:Window", "drain:Feed", "drain:Delivery", "drain:Outbox", "drain:Transfer", "drain:Network"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_closing_drained() {
    check("life_closing_drained", &["login", "store_hold", "close", "drain:Feed"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_closing_clear_done() {
    check("life_closing_clear_done", &["login", "store_hold", "session_reset", "close", "store_ok", "drain:Window", "drain:Feed", "drain:Delivery", "drain:Outbox", "drain:Transfer", "drain:Network"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_closing_close_joins() {
    check("life_closing_close_joins", &["login", "store_hold", "close", "close"], true).await;
}

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_closing_refuses() {
    check("life_closing_refuses", &["login", "store_hold", "close", "session_reset", "reset_models", "login"], true).await;
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

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_prop_generation() {
    property("P-LIFE-06", true, &[&["login", "create:a", "reset_models", "rows"]]).await;
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

#[cfg(feature = "spec_api")]
#[tokio::test(flavor = "multi_thread")]
async fn life_findings_full() {
    check("identity change over queued work", &["login", "create:a", "credential_other", "rows"], true).await;
    check("reset of models before the landing", &["login", "create:a", "reset_models", "rows"], true).await;
    check("close during the clearing", &["login", "store_hold", "session_reset", "close", "store_ok"], true).await;
    check("double reset while clearing", &["login", "store_hold", "session_reset", "session_reset", "store_release"], true).await;
}
