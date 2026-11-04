use serde_json::json;

use super::bridge_tests::{group, member_list, seed_members, strings};
use super::rig_tests::{bound, Rig};

/// A member whose row is not cached is skipped by the optimistic join: the call reaches the server and ends with the answer, never with a missing-row error.
#[tokio::test(flavor = "multi_thread")]
async fn adding_a_member_whose_row_is_not_cached_ends_with_the_answer() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    seed_members(&rig, &created.id);
    let result = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "add_member".into(), "{}".into(), json!({"userIds": ["u4", "u9"]}).to_string())).await;
    assert!(result.is_ok(), "the call failed: {:?}", result.as_ref().err());
    rig.settle().await;
    assert_eq!(rig.world.count("AddMember"), 1, "the add did not reach the server");
    assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (strings(&["u1", "u2", "u3", "u4"]), strings(&["u2", "u1", "u3", "u4"])), "the cached row joined, the missing one did not");
    rig.database.close_now();
}
