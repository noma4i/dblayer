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

use super::rig_tests::Fault;

fn with_group_node(models: &mut serde_json::Value) {
    use crate::schema::{NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.into()];
    node.node = Some(NodeRead::One);
    let mut remote = crate::schema_fixtures::remote("GroupNode", RemoteShape::Single, "5.minutes");
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    let group = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Group").unwrap();
    group["relations"].as_array_mut().unwrap().push(serde_json::to_value(node).unwrap());
}

struct Open {
    rig: Rig,
    group: String,
    epoch: u64,
    stream: u64,
    node: Arc<crate::ffi::tape::Rows>,
    _watch: Arc<crate::ffi::tape::Watched>,
}

async fn group_with_open_readers() -> Open {
    let rig = Rig::signed_with_operations(World::new("server", "A", true), false, with_group_node, json!([])).await;
    let mut operations: Vec<serde_json::Value> = serde_json::from_str(&super::rig_models::plain_operations()).unwrap();
    operations.push(json!({"name": "GroupNode", "document": "query GroupNode($id: ID!) { node(id: $id) { ... on Group { id name } } }"}));
    bound(rig.database.register_operations(serde_json::to_string(&operations).unwrap())).await.unwrap();
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    bound(rig.database.confirmed(receipt, false)).await.unwrap();
    let group = bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(r#"{"model":"Group"}"#))).await.unwrap().into_iter().next().unwrap().id;
    rig.world.set_members("JOINED", &["u1", "u2"]);
    let params = json!({"groupId": group, "order": "JOINED"});
    bound(rig.database.refresh(epoch, "Member".into(), Some("group_members".into()), params.to_string())).await.unwrap();
    let (sender, _deliveries) = tokio::sync::mpsc::unbounded_channel();
    let query = json!({"model": "Member", "relation": "group_members", "params": params});
    let stream = rig.database.subscribe_reading(epoch, crate::ffi::ReadRequest::List { query: crate::ffi::QuerySpec::json(query.to_string()) }, Arc::new(Deliveries(sender)));
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.world.answer_with("GroupNode", json!({"data": {"node": {"__typename": "Group", "id": group, "name": "friends"}}}));
    let node = Arc::new(crate::ffi::tape::Rows::default());
    let watch = rig.database.watch_query(&json!({"model": "Group", "relation": "node", "params": {crate::schema::NODE_PARAM: group}}).to_string(), node.clone());
    settled(&rig, "the node is read", || node.len() == 1).await;
    Open { rig, group, epoch, stream, node, _watch: watch }
}

async fn add_member(open: &Open) -> Result<(), crate::ffi::DbError> {
    let receipt = bound(open.rig.database.act(open.epoch, "Group".into(), open.group.clone(), "add_member".into(), "{}".into(), json!({"userIds": ["u9"]}).to_string())).await.unwrap();
    bound(open.rig.database.confirmed(receipt, false)).await.map(|_| ())
}

/// M87: a refused operation that rolled back owes its receiver a read by the `node` scope and its `refetch` scopes a reload, once; a network failure owes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_operation_rereads_its_receiver_and_refetch_scopes() {
    let down = group_with_open_readers().await;
    let (nodes, members) = (down.rig.world.count("GroupNode"), down.rig.world.count("GroupMembers"));
    down.rig.world.fail_next("AddMember", Fault::Down);
    let receipt = bound(down.rig.database.act(down.epoch, "Group".into(), down.group.clone(), "add_member".into(), "{}".into(), json!({"userIds": ["u9"]}).to_string())).await.unwrap();
    down.rig.clock.advance(1_000);
    settled(&down.rig, "a network failure reads nothing again", || (down.rig.world.count("GroupNode"), down.rig.world.count("GroupMembers")) == (nodes, members)).await;
    drop(receipt);
    down.rig.database.cancel_stream(down.stream);
    down.rig.database.close_now();

    let refused = group_with_open_readers().await;
    let (nodes, members) = (refused.rig.world.count("GroupNode"), refused.rig.world.count("GroupMembers"));
    refused.rig.world.fail_next("AddMember", Fault::Business);
    assert!(add_member(&refused).await.is_err(), "the server refuses the operation");
    refused.rig.clock.advance(1_000);
    settled(&refused.rig, "one node read and one reload after the rollback", || (refused.rig.world.count("GroupNode"), refused.rig.world.count("GroupMembers")) == (nodes + 1, members + 1)).await;
    assert_eq!(refused.node.len(), 1);
    refused.rig.database.cancel_stream(refused.stream);
    refused.rig.database.close_now();
}
