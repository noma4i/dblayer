use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};

use super::super::rig::*;
use super::super::LoadOptions;
use super::fixtures::*;
use super::spec_fixtures::*;
use crate::query::Query;
use crate::transport::Reply;

fn serve(rig: &Rig, operation: &'static str, body: Value) {
    rig.transport.respond_with(move |request| (request.operation_name == operation).then(|| Reply::data(body.clone())));
}

async fn load_feed(rig: &Rig, nodes: Vec<Value>) {
    serve(rig, "MomentsFeed", feed_page(nodes, false, "c"));
    rig.sync.load("moments", "feed", &serde_json::Map::new(), LoadOptions { force: true }).await.unwrap();
}

async fn load_user(rig: &Rig, nodes: Vec<Value>) {
    serve(rig, "MomentsByUser", json!({"moments": nodes}));
    rig.sync.load("moments", "by_user", &params(json!({"userId": "u1"})), LoadOptions { force: true }).await.unwrap();
}

fn field(rig: &Rig, id: &str, path: &str) -> Option<Value> {
    let row = rig.db.find("moments", &rid(id)).unwrap()?;
    let mut value = row.get(path.split('.').next().unwrap())?.clone();
    for key in path.split('.').skip(1) {
        value = value.get(key)?.clone();
    }
    Some(value)
}

fn write_both(rig: &Rig, id: &str) {
    rig.db.as_app().set_local("moments", &rid(id), "tone", json!("warm")).unwrap();
    rig.db.as_app().set_local("moments", &rid(id), "cover.tone", json!({"hue": 30})).unwrap();
}

fn both_kept(rig: &Rig, id: &str) -> bool {
    field(rig, id, "tone") == Some(json!("warm")) && field(rig, id, "cover.tone") == Some(json!({"hue": 30}))
}

#[tokio::test(start_paused = true)]
async fn inv_merge_12_a_full_refresh_keeps_the_locally_written_fields() {
    let rig = rig();
    load_feed(&rig, vec![json!({"id": "m1", "text": "a", "cover": {"url": "one"}})]).await;
    write_both(&rig, "m1");
    load_feed(&rig, vec![json!({"id": "m1", "text": "b", "cover": {"url": "two", "tone": {"hue": 99}}, "tone": "cold"})]).await;
    assert_eq!((field(&rig, "m1", "text"), field(&rig, "m1", "cover.url")), (Some(json!("b")), Some(json!("two"))));
    assert!(both_kept(&rig, "m1"));
}

#[tokio::test(start_paused = true)]
async fn inv_merge_12_a_partial_response_and_a_mutation_answer_keep_the_locally_written_fields() {
    let rig = rig();
    load_feed(&rig, vec![json!({"id": "m1", "text": "a", "cover": {"url": "one"}})]).await;
    write_both(&rig, "m1");
    load_feed(&rig, vec![json!({"id": "m1", "mood": "calm"})]).await;
    assert!(both_kept(&rig, "m1"));
    rig.db.as_app().update("moments", &rid("m1"), params(json!({"text": "n", "cover": {"url": "three"}}))).unwrap();
    rig.transport.enqueue("UpdateMoment", updated("m1", "n"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    assert_eq!(field(&rig, "m1", "cover.url"), Some(json!("three")));
    assert!(both_kept(&rig, "m1"));
}

#[tokio::test(start_paused = true)]
async fn inv_merge_12_a_row_that_leaves_a_list_and_comes_back_keeps_the_locally_written_fields() {
    let rig = rig();
    load_user(&rig, vec![json!({"id": "m1", "userId": "u1", "cover": {"url": "one"}}), json!({"id": "m2", "userId": "u1"})]).await;
    write_both(&rig, "m1");
    load_user(&rig, vec![json!({"id": "m2", "userId": "u1"})]).await;
    load_user(&rig, vec![json!({"id": "m1", "userId": "u1", "cover": {"url": "one"}}), json!({"id": "m2", "userId": "u1"})]).await;
    assert!(both_kept(&rig, "m1"));
}

#[tokio::test(start_paused = true)]
async fn inv_merge_12_a_write_before_the_row_exists_is_applied_when_the_row_arrives_and_survives_the_next_refresh() {
    let rig = rig();
    write_both(&rig, "m9");
    assert!(rig.db.find("moments", &rid("m9")).unwrap().is_none());
    load_feed(&rig, vec![json!({"id": "m9", "text": "a", "cover": {"url": "one"}})]).await;
    assert!(both_kept(&rig, "m9"));
    assert_eq!(field(&rig, "m9", "cover.url"), Some(json!("one")));
    load_feed(&rig, vec![json!({"id": "m9", "text": "b", "cover": {"url": "two"}})]).await;
    assert!(both_kept(&rig, "m9"));
    assert_eq!(pending(&rig), 0);
}

fn pending(rig: &Rig) -> i64 {
    rig.db.shared.store.lock().connection.query_row("SELECT COUNT(*) FROM local_pending", [], |row| row.get(0)).unwrap()
}

#[tokio::test(start_paused = true)]
async fn inv_merge_12_the_locally_written_fields_survive_a_restart_and_go_with_the_row() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("12.sqlite");
    let rig = open_rig(&path);
    load_feed(&rig, vec![json!({"id": "m1", "text": "a", "cover": {"url": "one"}})]).await;
    write_both(&rig, "m1");
    rig.db.as_app().set_local("moments", &rid("m5"), "tone", json!("early")).unwrap();
    drop(rig);

    let rig = open_rig(&path);
    assert!(both_kept(&rig, "m1"));
    load_feed(&rig, vec![json!({"id": "m1", "text": "b"}), json!({"id": "m5", "text": "late"})]).await;
    assert!(both_kept(&rig, "m1"));
    assert_eq!(field(&rig, "m5", "tone"), Some(json!("early")));
    rig.db.as_app().destroy("moments", &rid("m1")).unwrap();
    assert!(field(&rig, "m1", "tone").is_none());
}

#[tokio::test(start_paused = true)]
async fn inv_merge_12_one_write_is_one_delivery_to_every_subscription_of_the_row_and_an_equal_write_is_none() {
    let rig = rig();
    load_feed(&rig, vec![json!({"id": "m1", "text": "a", "cover": {"url": "one"}})]).await;
    let (rows, row_count) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (rows_seen, row_seen) = (rows.clone(), row_count.clone());
    let _list = rig.db.observe(Query::new("moments"), move |_| {
        rows_seen.fetch_add(1, Ordering::SeqCst);
    }).unwrap();
    let _row = rig.db.observe_row("moments", &rid("m1"), move |_| {
        row_seen.fetch_add(1, Ordering::SeqCst);
    }).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let base = (rows.load(Ordering::SeqCst), row_count.load(Ordering::SeqCst));
    rig.db.as_app().set_local("moments", &rid("m1"), "cover.tone", json!({"hue": 30})).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!((rows.load(Ordering::SeqCst), row_count.load(Ordering::SeqCst)), (base.0 + 1, base.1 + 1));
    rig.db.as_app().set_local("moments", &rid("m1"), "cover.tone", json!({"hue": 30})).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!((rows.load(Ordering::SeqCst), row_count.load(Ordering::SeqCst)), (base.0 + 1, base.1 + 1));
}

#[tokio::test(start_paused = true)]
async fn inv_merge_12_a_local_field_never_enters_the_server_request_or_the_outbox() {
    let rig = rig();
    load_feed(&rig, vec![json!({"id": "m1", "text": "a", "cover": {"url": "one"}})]).await;
    write_both(&rig, "m1");
    rig.db.as_app().update("moments", &rid("m1"), params(json!({"text": "n", "tone": "forged"}))).unwrap();
    assert!(!rig.db.outbox().unwrap()[0].input.as_object().unwrap().contains_key("tone"));
    rig.transport.enqueue("UpdateMoment", updated("m1", "n"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    let sent: Vec<Value> = rig.transport.calls().into_iter().filter(|call| call.operation_name == "UpdateMoment").map(|call| call.variables).collect();
    assert_eq!(sent.len(), 1);
    assert!(!sent[0].to_string().contains("tone") && !sent[0].to_string().contains("forged"));
    assert!(both_kept(&rig, "m1"));
}

#[tokio::test(start_paused = true)]
async fn inv_merge_12_a_path_that_is_not_declared_local_is_refused() {
    let rig = rig();
    load_feed(&rig, vec![json!({"id": "m1", "text": "a"})]).await;
    for path in ["text", "cover", "cover.url", "ghost", "tone.deep"] {
        assert!(matches!(rig.db.as_app().set_local("moments", &rid("m1"), path, json!("x")), Err(crate::db::DbError::NotLocalField { .. })), "{path}");
    }
    assert!(matches!(rig.db.as_app().set_local("moments", &rid("m1"), "tone", json!(7)), Err(crate::db::DbError::LocalValue { .. })));
}
