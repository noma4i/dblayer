use serde_json::json;

use super::super::rig::*;
use super::fixtures::*;
use super::spec_fixtures::*;
use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::ModelKey;
use crate::syncstate::SyncState;
use crate::transport::Reply;

const LARGE: usize = 300 * 1024;

fn feed_scope(rig: &Rig) -> ScopeRef {
    let spec = rig.db.spec("moments").unwrap();
    let relation = spec.relations.iter().find(|relation| relation.name == "feed").unwrap();
    ScopeRef::new(ModelKey::new("moments"), relation.scope_key("moments", &Default::default()).unwrap())
}

fn feed_members(rig: &Rig) -> usize {
    rig.db.shared.store.scope_members("moments", feed_scope(rig).key.as_str()).unwrap().len()
}

#[tokio::test(start_paused = true)]
async fn inv_rollback_05_inv_file_03_a_refused_destroy_after_a_sweep_and_a_restart_brings_everything_back() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("42.sqlite");
    let rig = open_rig(&path);
    let scope = feed_scope(&rig);
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m1"), row: params(moment("m1", "one")) });
    envelope.scope_ops.push(ScopeOp::Append { scope, row: rid("m1"), order_key: "a".into() });
    rig.db.land(envelope).unwrap();
    put(&rig, "vibes", json!({"id": "v1", "momentId": "m1"}));
    let large: Vec<u8> = (0..LARGE).map(|index| (index % 251) as u8).collect();
    rig.db.attach("moments", &rid("m1"), "media", b"small", "a.bin", "a/b").unwrap();
    rig.db.attach("vibes", &rid("v1"), "media", &large, "v.bin", "a/b").unwrap();

    rig.db.destroy("moments", &rid("m1")).unwrap();
    start(&rig.db, rig.db.outbox().unwrap()[0].operation.id.0);
    rig.db.sweep_files().unwrap();
    drop(rig);

    let rig = open_rig(&path);
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().state, SyncState::DeliveryUnknown);
    rig.db.sweep_files().unwrap();
    rig.db.retry("moments", &rid("m1")).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("denied", "FORBIDDEN"));
    rig.sync.flush().await.unwrap();

    assert!(rig.db.find("moments", &rid("m1")).unwrap().is_some());
    assert!(rig.db.find("vibes", &rid("v1")).unwrap().is_some());
    assert_eq!(rig.db.read("moments", &rid("m1"), "media").unwrap(), Some(b"small".to_vec()));
    assert_eq!(rig.db.read("vibes", &rid("v1"), "media").unwrap(), Some(large));
    assert!(rig.db.file("moments", &rid("m1"), "media").unwrap().is_some());
    assert_eq!(feed_members(&rig), 1);
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().state, SyncState::Failed);
}
