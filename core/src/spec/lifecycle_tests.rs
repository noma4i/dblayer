use serde_json::json;

use crate::test_wait::limited;

const LIMIT: std::time::Duration = std::time::Duration::from_secs(20);
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

#[test]
fn life_nosession_credential() {
    limited(LIMIT, async {
    check("life_nosession_credential", &["login"], false).await;
    });
}

#[test]
fn life_nosession_reset() {
    limited(LIMIT, async {
    check("life_nosession_reset", &["session_reset"], false).await;
    });
}

#[test]
fn life_nosession_close() {
    limited(LIMIT, async {
    check("life_nosession_close", &["close"], false).await;
    });
}

#[test]
fn life_session_same_credential() {
    limited(LIMIT, async {
    check("life_session_same_credential", &["login", "login"], false).await;
    });
}

#[test]
fn life_session_reset() {
    limited(LIMIT, async {
    check("life_session_reset", &["login", "session_reset"], false).await;
    });
}

#[test]
fn life_session_server_ended() {
    limited(LIMIT, async {
    check("life_session_server_ended", &["login", "logout"], false).await;
    });
}

#[test]
fn life_session_close() {
    limited(LIMIT, async {
    check("life_session_close", &["login", "close"], false).await;
    });
}

#[test]
fn life_closed_refuses() {
    limited(LIMIT, async {
    check("life_closed_refuses", &["login", "close", "session_reset", "close", "login"], false).await;
    });
}

#[test]
fn life_call_current_epoch() {
    limited(LIMIT, async {
    check("life_call_current_epoch", &["login", "call_data:current"], false).await;
    });
}

#[test]
fn life_call_stale_epoch() {
    limited(LIMIT, async {
    check("life_call_stale_epoch", &["login", "call_data:other"], false).await;
    });
}

#[test]
fn life_call_closed() {
    limited(LIMIT, async {
    check("life_call_closed", &["login", "close", "call_data:current"], false).await;
    });
}

#[test]
fn life_call_nosession_data() {
    limited(LIMIT, async {
    check("life_call_nosession_data", &["call_data:current"], false).await;
    });
}

#[test]
fn life_prop_epoch_monotonic() {
    limited(LIMIT, async {
    property("P-LIFE-01", false, &[]).await;
    });
}

#[test]
fn life_prop_no_work_after_end() {
    limited(LIMIT, async {
    property("P-LIFE-02", false, &[&["login", "create:a", "session_reset", "login", "rows"]]).await;
    });
}

#[test]
fn life_prop_clear_before_start() {
    limited(LIMIT, async {
    property("P-LIFE-03", false, &[&["login", "create:a", "session_reset", "login", "rows"]]).await;
    });
}

#[test]
fn life_prop_reply_once() {
    limited(LIMIT, async {
    property("P-LIFE-04", false, &[&["login", "session_reset", "session_reset"]]).await;
    });
}

#[test]
fn life_prop_close_drains() {
    limited(LIMIT, async {
    property("P-LIFE-05", false, &[&["login", "close", "call_data:current", "close"]]).await;
    });
}

#[test]
fn life_prop_session_kept() {
    limited(LIMIT, async {
    property("P-LIFE-07", false, &[&["login", "close"]]).await;
    });
}

#[test]
fn life_prop_idle() {
    limited(LIMIT, async {
    property("P-LIFE-08", false, &[&["login", "idle", "session_reset", "idle"]]).await;
    });
}

#[test]
fn life_findings_public() {
    limited(LIMIT, async {
    check("life_findings_public", &["login", "create:a", "session_reset", "login", "rows"], false).await;
    check("life_findings_public", &["login", "session_reset", "session_reset"], false).await;
    });
}
