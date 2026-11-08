use std::future::Future;
use std::sync::Arc;

use serde_json::{json, Value};

use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::{Database, DbError};

const FEED: &str = "query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }";
const NODE: &str = "query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title } } }";
const NODES: &str = "query ContainersByIds($ids: [ID!]!) { nodes(ids: $ids) { ... on Container { id title } } }";

fn feed_page(id: &str) -> Value {
    json!({"data":{"feed":{"nodes":[{"__typename":"Moment","id":id,"uuid":id,"likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":format!("after-{id}")}}}})
}

async fn rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    rig.database.register_operations(json!([{"name":"MomentsFeed","document":FEED},{"name":"ContainerNode","document":NODE},{"name":"ContainersByIds","document":NODES}]).to_string()).await.unwrap();
    rig.world.answer_with("MomentsFeed", feed_page("1"));
    rig.world.answer_with("ContainerNode", json!({"data":{"node":{"__typename":"Container","id":"k1","title":"x"}}}));
    rig.world.answer_with("ContainersByIds", json!({"data":{"nodes":[{"__typename":"Container","id":"k1","title":"x"}]}}));
    rig
}

fn requests(rig: &Rig, operation: &str) -> usize { rig.world.log().iter().filter(|entry| entry.operation == operation).count() }

/// L4 the second call of a scope registers while the first one is held at the server, then the first call is cancelled: the held request goes on for the second call.
async fn second_call_survives_cancel_of_first<F, Fut>(rig: &Rig, operation: &str, call: F, after_first_entered: impl FnOnce())
where
    F: Fn(Arc<Database>) -> Fut,
    Fut: Future<Output = Result<(), DbError>> + Send + 'static,
{
    let held = rig.world.hold_next(operation);
    let first = tokio::spawn(call(rig.database.clone()));
    held.entered().await;
    after_first_entered();
    let mut second = Box::pin(call(rig.database.clone()));
    std::future::poll_fn(|cx| { drop(second.as_mut().poll(cx)); std::task::Poll::Ready(()) }).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second = tokio::spawn(second);
    crate::ffi::owners_drained(&rig.database).await.unwrap();
    held.release();
    bound(second).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn refresh_survives_the_cancel_of_an_earlier_refresh() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    second_call_survives_cancel_of_first(&rig, "MomentsFeed", move |db| async move { db.refresh(epoch, "Moment".into(), Some("feed".into()), "{}".into()).await }, || ()).await;
    assert_eq!(requests(&rig, "MomentsFeed"), 1, "the second refresh sent a request of its own");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn load_more_waits_for_the_page_that_an_earlier_cancelled_load_more_started() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.begin_load(epoch, "Moment".into(), Some("feed".into()), "{}".into()).unwrap().result()).await.unwrap();
    rig.world.answer_with("MomentsFeed", feed_page("2"));
    let world = rig.world.clone();
    second_call_survives_cancel_of_first(&rig, "MomentsFeed", move |db| async move { db.load_more(epoch, "Moment".into(), Some("feed".into()), "{}".into()).await.map(drop) }, move || world.answer_with("MomentsFeed", feed_page("3"))).await;
    assert!(rig.database.peek_row(epoch, "Moment".into(), "2".into()).unwrap().is_some(), "the page of the held request did not land: the cancel of the first call stopped it");
    assert!(rig.database.peek_row(epoch, "Moment".into(), "3".into()).unwrap().is_some(), "the second load more did not read its own page");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn node_survives_the_cancel_of_an_earlier_node() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    second_call_survives_cancel_of_first(&rig, "ContainerNode", move |db| async move { db.node(epoch, "Container".into(), "node".into(), "k1".into(), true).await.map(drop) }, || ()).await;
    assert_eq!(requests(&rig, "ContainerNode"), 1, "the second node sent a request of its own");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn nodes_survives_the_cancel_of_an_earlier_nodes() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    second_call_survives_cancel_of_first(&rig, "ContainersByIds", move |db| async move { db.nodes(epoch, "Container".into(), "nodes".into(), vec!["k1".into()]).await.map(drop) }, || ()).await;
    assert_eq!(requests(&rig, "ContainersByIds"), 1, "the second nodes sent a request of its own");
    rig.database.close_now();
}

/// boundary_node_read_epoch: a node whose request is held while the session ends gets the end of its epoch.
#[tokio::test(flavor = "multi_thread")]
async fn node_held_at_the_server_ends_with_its_epoch_and_stores_no_row_for_the_next_session() {
    let rig = rig().await;
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_next("ContainerNode");
    let reading = { let database = rig.database.clone(); rig.spawn(async move { database.node(epoch, "Container".into(), "node".into(), "k1".into(), true).await }) };
    held.entered().await;
    bound(rig.database.session_reset()).await.unwrap();
    held.release();
    let result = bound(reading).await.unwrap();
    assert!(matches!(result, Err(DbError::Interrupted { .. } | DbError::NoSession { .. })), "{result:?}");
    rig.database.close_now();
}
