use std::sync::Arc;

use serde_json::json;

use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Rig, World};
use crate::ffi::tape::Rows;
use crate::ffi::ReadRequest;
use crate::test_wait::yield_until;

const FEED: &str = "query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }";

async fn feed_rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    rig.database.register_operations(json!([{"name":"MomentsFeed","document":FEED}]).to_string()).await.unwrap();
    rig.world.answer_with("MomentsFeed", json!({"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}));
    rig
}

fn feed_requests(rig: &Rig) -> usize { rig.world.log().iter().filter(|entry| entry.operation == "MomentsFeed").count() }

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_one_load_interest_keeps_the_other_call_alive() {
    let rig = feed_rig().await;
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_next("MomentsFeed");
    rig.world.mark("begin A");
    let first = rig.database.begin_load(epoch, "Moment".into(), Some("feed".into()), "{}".into()).unwrap();
    held.entered_for("the shared load").await;
    rig.world.mark("request of A entered");
    let second = rig.database.begin_load(epoch, "Moment".into(), Some("feed".into()), "{}".into()).unwrap();
    rig.world.mark("begin B");
    let issued = rig.database.loads.issued();
    yield_until("the second interest is registered in the window", || rig.database.loads.registered_through(issued)).await;
    rig.world.mark("B registered");
    first.cancel();
    rig.world.mark("A cancelled");
    held.release();
    rig.world.mark("answer released");
    bound(second.result()).await.unwrap();
    rig.world.mark("B result");
    assert_eq!(feed_requests(&rig), 1, "the second call sent a request of its own, steps and requests in order:\n{}", rig.world.journal());
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_that_refreshes_on_connect_loads_again_once_when_the_network_returns() {
    let rig = feed_rig().await;
    let rows = Arc::new(Rows::default());
    let query = json!({"model":"Moment","relation":"feed","params":{},"refreshOnConnect":true}).to_string();
    drop(rig.database.watch(ReadRequest::List { query: crate::ffi::QuerySpec::json(query) }, rows.clone()));
    yield_until("the list shows its first load", || rows.len() == 1).await;
    assert_eq!(feed_requests(&rig), 1);
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    yield_until("the list loaded again", || feed_requests(&rig) >= 2).await;
    for _ in 0..200 { tokio::task::yield_now().await; }
    assert_eq!(feed_requests(&rig), 2, "the list loaded more or less than once on reconnect: {:?}", rig.world.log());
    rig.database.close_now();
}
