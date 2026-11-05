use super::lifecycle::explore_inputs;
use super::store::StoreOwner;
use crate::check::model::run;
use crate::check::value::Input;
use serde_json::json;

fn seq(items: &[&str]) -> Vec<Input> {
    items.iter().map(|item| match item.split_once(':') {
        Some(("create", rest)) => match rest.split_once(':') {
            Some((name, "other")) => Input::new("create").with("as", name).with("epoch", "other"),
            _ => Input::new("create").with("as", rest),
        },
        Some(("commit_batch", names)) => Input::new("commit_batch").with("names", json!(names.split(',').collect::<Vec<_>>())),
        _ if *item == "reset_models" => Input::new("reset_models").with("models", json!(["Moment"])),
        _ => Input::new(item),
    }).collect()
}

async fn check(test: &str, items: &[&str], full: bool) {
    let owner = if full { StoreOwner::full(None) } else { StoreOwner::public(None) };
    if let Err(divergence) = run(&owner, test, &seq(items)).await {
        panic!("{test}: {divergence}");
    }
}

async fn property(only: &'static str, full: bool, required: &[&[&str]]) {
    let owner = if full { StoreOwner::full(Some(only)) } else { StoreOwner::public(Some(only)) };
    let required: Vec<Vec<Input>> = required.iter().map(|items| seq(items)).collect();
    if let Err(divergence) = explore_inputs(&owner, &required).await {
        panic!("{only}: {divergence}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn store_commit_stale_epoch() {
    check("store_commit_stale_epoch", &["login", "create:b:other", "rows"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn store_commit_ok() {
    check("store_commit_ok", &["login", "create:a", "rows"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn store_clear_account() {
    check("store_clear_account", &["login", "create:a", "session_reset", "login", "rows"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn store_epoch() {
    check("store_epoch", &["login", "create:a", "session_reset", "login", "create:b", "create:c:other", "rows"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn store_close() {
    check("store_close", &["login", "create:a", "close", "rows", "create:b"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn store_closed() {
    check("store_closed", &["login", "close", "create:a", "create:b:other"], false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn store_prop_no_stale_epoch() {
    property("P-STORE-03", false, &[&["login", "create:a", "session_reset", "login", "create:b:other", "rows"]]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn store_prop_reply_once() {
    property("P-STORE-05", false, &[&["login", "create:a", "close", "create:b"]]).await;
}
