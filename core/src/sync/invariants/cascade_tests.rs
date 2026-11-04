use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};

use super::super::rig::*;
use super::super::subscribe::{EventEffect, SubscriptionDef};
use super::event_tests::event;
use crate::commit::ScopeRef;
use crate::db::fixtures::{all_specs, changes, obj, recorder, rid};
use crate::db::Db;
use crate::identity::{ModelKey, ListKey};
use crate::query::Query;
use crate::sync::SyncConfig;
use crate::transport::{EventSpec, MockTransport};

fn seeded(db: &Db) {
    let put_row = |model: &str, row: Value| {
        let row = obj(row);
        let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Event, db.generation());
        envelope.entity_ops.push(crate::commit::EntityOp::Upsert { model: ModelKey::new(model), id: rid(row["id"].as_str().unwrap()), row });
        db.land(&db.admit().unwrap(), envelope).unwrap();
    };
    put_row("users", json!({"id": "u1", "name": "Ann"}));
    put_row("users", json!({"id": "u2", "name": "Bob"}));
    for (id, user) in [("m1", "u1"), ("m2", "u1"), ("m3", "u2")] {
        put_row("moments", json!({"id": id, "userId": user, "text": id}));
    }
    for (id, moment) in [("v1", "m1"), ("v2", "m2"), ("v3", "m3")] {
        put_row("vibes", json!({"id": id, "momentId": moment, "kind": "like"}));
    }
    let feed = ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("feed"));
    let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Event, db.generation());
    for id in ["m1", "m2", "m3"] {
        envelope.scope_ops.push(crate::commit::ScopeOp::Append { scope: feed.clone(), row: rid(id), order_key: id.to_string() });
    }
    db.land(&db.admit().unwrap(), envelope).unwrap();
    db.attach(&db.admit().unwrap(), "moments", &rid("m1"), "media", b"m1 bytes", "a.bin", "a/b").unwrap();
    db.attach(&db.admit().unwrap(), "vibes", &rid("v1"), "media", b"v1 bytes", "b.bin", "a/b").unwrap();
}

fn left(db: &Db) -> BTreeMap<String, Vec<String>> {
    let mut found: BTreeMap<String, Vec<String>> = ["users", "moments", "vibes"].iter().map(|model| {
        let mut ids: Vec<String> = db.query(&Query::new(model)).unwrap().into_iter().map(|row| row.id.to_string()).collect();
        ids.sort();
        (model.to_string(), ids)
    }).collect();
    found.insert("feed".to_string(), db.shared.store.scope_members("moments", "feed").unwrap().into_iter().map(|(id, _)| id).collect());
    for (model, id) in [("moments", "m1"), ("vibes", "v1")] {
        found.insert(format!("{model}/{id}/media"), db.read(model, &rid(id), "media").unwrap().map(|bytes| String::from_utf8(bytes).unwrap()).into_iter().collect());
    }
    found
}

#[tokio::test(start_paused = true)]
async fn inv_event_04_inv_cascade_04_a_delete_event_removes_the_same_set_as_a_local_destroy_and_leaves_no_orphan() {
    let local = Db::open_in_memory(all_specs(), None).unwrap();
    seeded(&local);
    local.destroy(&local.admit().unwrap(), "users", &rid("u1")).unwrap();

    let rig = rig_on(Db::open_in_memory(all_specs(), None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(crate::sync::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    seeded(&rig.db);
    let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("user_gone"), "MomentChanged", "users", EventEffect::Destroy)).await.unwrap();
    rig.transport.emit("MomentChanged", event(json!({"id": "u1"})));
    rig.transport.await_consumed("MomentChanged").await;

    let expected: BTreeMap<String, Vec<String>> = [
        ("users", vec!["u2"]), ("moments", vec!["m3"]), ("vibes", vec!["v3"]), ("feed", vec!["m3"]),
        ("moments/m1/media", vec![]), ("vibes/v1/media", vec![]),
    ].into_iter().map(|(key, ids)| (key.to_string(), ids.into_iter().map(str::to_string).collect())).collect();
    assert_eq!(left(&local), expected);
    assert_eq!(left(&rig.db), expected);
}

#[tokio::test(start_paused = true)]
async fn inv_event_09_inv_cascade_03_a_batch_of_events_touches_each_resident_parent_once_and_skips_absent_ones() {
    let mut moments = moments_spec();
    moments.associations.iter_mut().for_each(|association| association.touches_parent = true);
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(crate::sync::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    let (log, callback) = recorder();
    let _observer = rig.db.observe(Query::new("users"), callback).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let event_spec = EventSpec::new("moment_changed").debounced(Duration::from_millis(500)).merged_by_row();
    let _subscription = rig.sync.subscribe(SubscriptionDef::new(event_spec, "MomentChanged", "moments", EventEffect::Upsert)).await.unwrap();
    for (id, user) in [("m1", "u1"), ("m2", "u1"), ("m3", "u9")] {
        rig.transport.emit("MomentChanged", event(json!({"id": id, "text": id, "userId": user})));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let waits = rig.transport.waits("MomentChanged");
    rig.clock.advance(Duration::from_millis(500));
    rig.transport.await_waits("MomentChanged", waits + 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(["m1", "m2", "m3"].map(|id| text_of(&rig, "moments", id)), [Some("m1".to_string()), Some("m2".to_string()), Some("m3".to_string())]);
    let touched: Vec<_> = changes(&log).iter().skip(1).flat_map(|change| change.diff.updated.clone()).collect();
    assert_eq!(touched, vec![rid("u1")]);
    assert_eq!(rig.db.find("users", &rid("u9")).unwrap(), None);
}
