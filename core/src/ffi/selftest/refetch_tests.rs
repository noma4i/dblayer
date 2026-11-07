use std::sync::Arc;

use serde_json::json;

use super::bridge_support_tests::group;
use super::rig_tests::{bound, Rig};
use crate::test_wait::yield_until;

struct Deliveries(tokio::sync::mpsc::UnboundedSender<crate::ffi::tape::StreamDelivery>);

impl crate::ffi::tape::Reader for Deliveries {
    fn delivered(&self, delivery: crate::ffi::tape::StreamDelivery) { let _ = self.0.send(delivery); }
}

async fn members_after_a_group_joined(rig: &Rig) -> (String, serde_json::Value, u64) {
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    let id = bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(r#"{"model":"Group"}"#))).await.unwrap().into_iter().next().unwrap().id;
    rig.world.set_members("JOINED", &["u1", "u2"]);
    let params = json!({"groupId": id, "order": "JOINED"});
    bound(rig.database.refresh(epoch, "Member".into(), Some("group_members".into()), params.to_string())).await.unwrap();
    (id, params, epoch)
}

async fn join(rig: &Rig, group: &str, epoch: u64) {
    rig.world.set_members("JOINED", &["u1", "u2", "u9"]);
    let receipt = bound(rig.database.act(epoch, "Group".into(), group.into(), "add_member".into(), "{}".into(), json!({"userIds":["u9"]}).to_string())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
}

/// out_landing_committed_stale: the confirmed action marks the window of its refetch relation stale and a held reader of the window reads it again.
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_action_reads_the_open_window_of_its_refetch_relation_again() {
    let rig = Rig::encrypted().await;
    let (group, params, epoch) = members_after_a_group_joined(&rig).await;
    let (sender, mut deliveries) = tokio::sync::mpsc::unbounded_channel();
    let query = json!({"model": "Member", "relation": "group_members", "params": params});
    let stream = rig.database.subscribe_reading(epoch, crate::ffi::ReadRequest::List { query: crate::ffi::QuerySpec::json(query.to_string()) }, Arc::new(Deliveries(sender)));
    bound(rig.database.loaded(stream)).await.unwrap();
    let before = rig.world.count("GroupMembers");
    join(&rig, &group, epoch).await;
    let shown = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(delivery) = crate::test_wait::within_secs(5, deliveries.recv()).await {
            let rows = match delivery.value {
                crate::ffi::ReadValue::Snapshot { rows, .. } => rows,
                crate::ffi::ReadValue::Delta { edits, .. } => crate::ffi::RowEdit::carried(&edits),
                _ => continue,
            };
            if rows.iter().any(|row| row.id == "u9") { return true; }
        }
        false
    }).await.unwrap_or(false);
    let ids: Vec<_> = bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(query.to_string()))).await.unwrap().into_iter().map(|row| row.id).collect();
    let all: Vec<_> = bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(r#"{"model":"Member"}"#))).await.unwrap().into_iter().map(|row| row.id).collect();
    assert!(shown, "the joined member was not shown; reads {} -> {}: stored {ids:?} members {all:?}", before, rig.world.count("GroupMembers"));
    yield_until("the open window is read again", || rig.world.count("GroupMembers") > before).await;
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}

/// out_landing_committed_stale: a window nobody reads is owed a read: the next reader opens it stale and the first delivery already carries the answer.
#[tokio::test(flavor = "multi_thread")]
async fn a_window_without_a_reader_is_read_again_when_the_next_reader_opens_it() {
    let rig = Rig::encrypted().await;
    let (group, params, epoch) = members_after_a_group_joined(&rig).await;
    let before = rig.world.count("GroupMembers");
    join(&rig, &group, epoch).await;
    assert_eq!(rig.world.count("GroupMembers"), before, "nobody reads the window: nothing is read");
    let (sender, _deliveries) = tokio::sync::mpsc::unbounded_channel();
    let query = json!({"model": "Member", "relation": "group_members", "params": params});
    let stream = rig.database.subscribe_reading(epoch, crate::ffi::ReadRequest::List { query: crate::ffi::QuerySpec::json(query.to_string()) }, Arc::new(Deliveries(sender)));
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("the owed read", || rig.world.count("GroupMembers") > before).await;
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}
