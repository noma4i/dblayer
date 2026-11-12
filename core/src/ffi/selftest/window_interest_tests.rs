use serde_json::json;

use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Rig, World};

const FEED: &str = "query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }";

async fn feed_rig() -> Rig {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    rig.database.register_operations(super::rig_models::with_own_operations(&json!([{"name":"MomentsFeed","document":FEED}]).to_string())).await.unwrap();
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
    held.entered().await;
    rig.world.mark("request of A entered");
    let second = rig.database.begin_load(epoch, "Moment".into(), Some("feed".into()), "{}".into()).unwrap();
    rig.world.mark("begin B");
    first.cancel();
    rig.world.mark("A cancelled");
    held.release();
    rig.world.mark("answer released");
    bound(second.result()).await.unwrap();
    rig.world.mark("B result");
    assert_eq!(feed_requests(&rig), 1, "the second call sent a request of its own, steps and requests in order:\n{}", rig.world.journal());
    rig.database.close_now();
}
