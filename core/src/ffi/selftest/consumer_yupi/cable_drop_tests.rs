//! moment.dbl `by_user` windows and `changed_by "REACTION", via: :node` against feed.md l.22 (`gap`) and l.194 (ChannelConfirmed -> Stale of the observed windows).
use serde_json::json;

use super::stand::Stand;
use crate::ffi::ReadFetch;

fn page() -> serde_json::Value { json!({"user": {"moments": {"edges": [], "pageInfo": {"hasNextPage": false, "endCursor": null}}}}) }

#[tokio::test(flavor = "multi_thread")]
async fn consumer_yupi_cable_drop_resubscribes_and_reloads_only_observed() {
    let stand = Stand::open().await;
    stand.answer("userMoments", page());
    stand.answer("MomentNode", json!({"node": super::stand::moment("mo1", 0)}));
    let (events, token) = stand.events("Moment").await;
    let epoch = stand.database.session_epoch();
    let unobserved = stand.database.begin_load(epoch, "Moment".into(), Some("by_user".into()), json!({"userId": "u2"}).to_string()).unwrap();
    crate::ffi::selftest::rig_tests::bound(unobserved.result()).await.unwrap();
    drop(unobserved);
    let rows = std::sync::Arc::new(crate::ffi::tape::Rows::default());
    let observed = stand.database.watch_query(r#"{"model":"Moment","relation":"by_user","params":{"userId":"u1"}}"#, rows.clone());
    crate::ffi::selftest::rig_tests::bound(observed.wait_for_initial_load()).await.unwrap();
    stand.settle().await;
    assert_eq!((stand.requests_with("userMoments", "userId", "u1"), stand.requests_with("userMoments", "userId", "u2")), (1, 1));

    stand.drop_cable(&token);
    stand.database.set_online(false).unwrap();
    stand.database.set_online(true).unwrap();
    let token = stand.next_subscription().await;
    while !matches!(crate::ffi::selftest::rig_tests::bound(events.pull_next()).await, Some(crate::ffi::ReadValue::Gap)) {}
    stand.settle().await;
    assert_eq!((stand.requests_with("userMoments", "userId", "u1"), stand.requests_with("userMoments", "userId", "u2")), (2, 1));

    let _node = stand.entity("Moment", "mo1", ReadFetch::Stale);
    stand.settle().await;
    assert_eq!(stand.requests_with("MomentNode", "momentId", "mo1"), 1);
    stand.push(&token, json!({"area": "REACTION", "kind": "UPDATED", "entityId": "mo1"}));
    stand.settle().await;
    assert_eq!((stand.requests_with("MomentNode", "momentId", "mo1"), stand.requests("MomentNode")), (2, 2));
}
