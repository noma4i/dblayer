//! D-490 M86: a standalone operation with `refetch :scope, model: Model` marks that window stale after a successful answer only.

use std::sync::Arc;

use serde_json::json;

use super::bridge_support_tests::group;
use super::rig_tests::{bound, Rig, World};
use crate::schema::FieldSpec;
use crate::test_wait::settled;
use crate::value::ScalarKind;

struct Deliveries(tokio::sync::mpsc::UnboundedSender<crate::ffi::tape::StreamDelivery>);

impl crate::ffi::tape::Reader for Deliveries {
    fn delivered(&self, delivery: crate::ffi::tape::StreamDelivery) { let _ = self.0.send(delivery); }
}

fn group_ping() -> serde_json::Value {
    let mut returns = FieldSpec::object("group_ping");
    returns.app_type = Some("GroupPingResult".into());
    returns.fields = vec![FieldSpec::scalar("ok", ScalarKind::Bool).nullable()];
    json!([{"name": "GroupPing", "returns": returns, "refetch": ["Member.group_members"]}])
}

/// out_landing_committed_stale for a standalone operation: a refused call leaves the open window alone, the answered call makes its reader read the window again.
#[tokio::test(flavor = "multi_thread")]
async fn a_standalone_operation_refetches_after_success() {
    let rig = Rig::signed_with_operations(World::new("server", "A", true), false, |_| {}, group_ping()).await;
    let mut operations: Vec<serde_json::Value> = serde_json::from_str(&super::rig_models::plain_operations()).unwrap();
    operations.push(json!({"name": "GroupPing", "document": "mutation GroupPing { groupPing { ok } }"}));
    bound(rig.database.register_operations(serde_json::to_string(&operations).unwrap())).await.unwrap();
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    let id = bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(r#"{"model":"Group"}"#))).await.unwrap().into_iter().next().unwrap().id;
    rig.world.set_members("JOINED", &["u1", "u2"]);
    let params = json!({"groupId": id, "order": "JOINED"});
    bound(rig.database.refresh(epoch, "Member".into(), Some("group_members".into()), params.to_string())).await.unwrap();
    let (sender, _deliveries) = tokio::sync::mpsc::unbounded_channel();
    let query = json!({"model": "Member", "relation": "group_members", "params": params});
    let stream = rig.database.subscribe_reading(epoch, crate::ffi::ReadRequest::List { query: crate::ffi::QuerySpec::json(query.to_string()) }, Arc::new(Deliveries(sender)));
    bound(rig.database.loaded(stream)).await.unwrap();
    let before = rig.world.count("GroupMembers");

    rig.world.answer_with("GroupPing", json!({"errors": [{"message": "no", "extensions": {"code": "FORBIDDEN"}}]}));
    assert!(bound(rig.database.call(epoch, "GroupPing".into(), "{}".into())).await.is_err(), "the refused call fails");
    rig.clock.advance(1_000);
    settled(&rig, "a refused call reads nothing again", || rig.world.count("GroupMembers") == before).await;

    rig.world.answer_with("GroupPing", json!({"data": {"groupPing": {"ok": true}}}));
    bound(rig.database.call(epoch, "GroupPing".into(), "{}".into())).await.unwrap();
    rig.clock.advance(1_000);
    settled(&rig, "the answered call makes the open window read again", || rig.world.count("GroupMembers") > before).await;
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}
