use super::error::SyncError;
use super::subscribe::{EventEffect, SubscriptionDef};
use super::rig::*;
use crate::db::Db;
use crate::pacing::TestClock;
use crate::query::Query;
use crate::schema::{AssociationKind, AssociationSpec, Dependent, FieldSpec};
use crate::value::ScalarKind;
use crate::sync::{Sync, SyncConfig};
use crate::transport::MockTransport;
use crate::transport::{EventSpec, Response, SubscriptionEvent, SubscriptionFailure, SubscriptionFailureKind};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(super) fn change_feed() -> crate::schema::ChangeFeed {
    crate::schema::ChangeFeed { subscription: "Changes".into(), root: "recordEvents.changes".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() }
}

fn rig_with_subscription() -> Rig {
    let rig = rig();
    rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    rig
}

fn next(node: serde_json::Value) -> SubscriptionEvent {
    SubscriptionEvent::Next(Response::data(json!({"momentChanged": node})))
}

fn upsert_def() -> SubscriptionDef {
    SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "moments", EventEffect::Upsert)
}

#[tokio::test(start_paused = true)]
async fn initial_offline_subscription_makes_no_transport_call() {
    let rig = rig_with_subscription();
    rig.sync.set_online(false);
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    assert_eq!(rig.transport.call_count("MomentChanged"), 0);
    drop(subscription);
    rig.sync.set_online(true);
    assert_eq!(rig.transport.call_count("MomentChanged"), 0);
    rig.sync.set_online(false);
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.sync.set_online(true);
    rig.transport.await_calls("MomentChanged", 1).await;
    assert_eq!(rig.transport.call_count("MomentChanged"), 1);
    assert!(!subscription.status().degraded);
}

#[tokio::test(start_paused = true)]
async fn ready_then_failure_keeps_growing_backoff() {
    let rig = rig_with_subscription();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    for (attempt, ceiling) in [1, 2, 4, 8, 16, 30, 30].into_iter().enumerate() {
        rig.transport.emit("MomentChanged", SubscriptionEvent::Ready);
        rig.transport.emit("MomentChanged", failed(SubscriptionFailureKind::Transport));
        rig.transport.await_cancellations("MomentChanged", attempt + 1).await;
        tokio::time::advance(Duration::from_millis(ceiling * 800 - 10)).await;
        assert_eq!(rig.transport.call_count("MomentChanged"), attempt + 1, "attempt {attempt} reset its delay");
        tokio::time::advance(Duration::from_millis(ceiling * 200 + 20)).await;
        rig.transport.await_calls("MomentChanged", attempt + 2).await;
        assert_eq!(rig.transport.call_count("MomentChanged"), attempt + 2, "attempt {attempt} exceeded its cap");
    }
    rig.transport.emit("MomentChanged", SubscriptionEvent::Ready);
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "healthy"})));
    rig.transport.emit("MomentChanged", failed(SubscriptionFailureKind::Transport));
    rig.transport.await_cancellations("MomentChanged", 8).await;
    assert!(reconnects_within_first_backoff(&rig, 9).await, "a healthy Next did not reset the backoff");
}

async fn reconnects_within_first_backoff(rig: &Rig, call: usize) -> bool {
    tokio::time::timeout(Duration::from_millis(1020), rig.transport.await_calls("MomentChanged", call)).await.is_ok()
}

#[tokio::test(start_paused = true)]
async fn subscription_normalization_does_not_invert_reset_locks() {
    for effect in [EventEffect::Upsert, EventEffect::Patch] {
        let rig = rig_with_subscription();
        put(&rig, "moments", json!({"id": "m1", "text": "before", "userId": null}));
        rig.db.shared.store.require_unlocked_responses.store(true, std::sync::atomic::Ordering::SeqCst);
        let subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "moments", effect)).await.unwrap();
        rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "after", "author": {"id": "u1", "name": "Ann"}})));
        rig.transport.await_consumed("MomentChanged").await;
        assert_eq!(subscription.status().applied, 1);
        let row = rig.db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap().unwrap();
        assert_eq!(row.text("text"), Some("after"));
    }
}

fn failed(kind: SubscriptionFailureKind) -> SubscriptionEvent {
    SubscriptionEvent::Failed(SubscriptionFailure::new(kind, "boom"))
}

#[tokio::test(start_paused = true)]
async fn declared_removal_kinds_close_pending_writes_and_destroy_children_without_http() {
    removal_closes_pending_writes("DELETED").await;
}

#[tokio::test(start_paused = true)]
async fn access_revoked_closes_pending_writes_and_destroy_children_without_http() {
    removal_closes_pending_writes("ACCESS_REVOKED").await;
}

async fn removal_closes_pending_writes(kind: &str) {
    for area in ["MESSAGE", "CHAT", "MOMENT"] {
        let mut users = users_spec();
        users.actions.push(moments_spec().actions[1].clone());
        let mut node = users.relations[0].clone();
        node.name = "node".into();
        node.node = Some(crate::schema::NodeRead::One);
        node.text_params = vec![crate::schema::NODE_PARAM.into()];
        users.relations.push(node);
        let mut children = AssociationSpec::new("moments", AssociationKind::HasMany, "moments", "userId");
        children.dependent = Some(Dependent::Destroy);
        users.associations.push(children);
        users.change_feed = Some(change_feed());
        users.changes.push(serde_json::from_value(json!({"area":area, "relation":"node", "parameter":null, "removes_on":["DELETED", "ACCESS_REVOKED"]})).unwrap());
        let rig = rig_on(Db::open_in_memory(vec![users, moments_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
        put(&rig, "users", json!({"id":"u1", "name":"Ann"}));
        put(&rig, "moments", json!({"id":"m1", "text":"before", "userId":"u1"}));
        rig.db.update("moments", &crate::identity::RowId::new("m1").unwrap(), json!({"text":"pending"}).as_object().unwrap().clone()).unwrap();
        rig.db.update("users", &crate::identity::RowId::new("u1").unwrap(), json!({"name":"pending"}).as_object().unwrap().clone()).unwrap();
        assert_eq!(rig.db.outbox().unwrap().len(), 2);
        let log = Arc::new(Mutex::new(Vec::new()));
        let output = log.clone();
        let _observer = rig.db.observe(Query::new("moments"), move |change| output.lock().unwrap().push(change.clone())).unwrap();
        rig.db.await_revision(rig.db.revision_seq().unwrap());
        log.lock().unwrap().clear();
        let subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changes"), "Changes", "users", EventEffect::Upsert)).await.unwrap();
        rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[{"topic":area, "action":kind, "recordId":"u1"}]}}))));
        rig.transport.await_consumed("Changes").await;
        rig.db.await_revision(rig.db.revision_seq().unwrap());
        assert!(rig.db.query(&Query::new("users")).unwrap().is_empty(), "{kind}");
        assert!(rig.db.query(&Query::new("moments")).unwrap().is_empty(), "{kind}");
        assert!(rig.db.outbox().unwrap().is_empty(), "{kind}");
        assert_eq!(log.lock().unwrap().len(), 1, "{kind}");
        assert_eq!(log.lock().unwrap()[0].diff.removed, vec![crate::identity::RowId::new("m1").unwrap()]);
        assert_eq!(subscription.status().applied, 1, "{kind}");
        assert_eq!(rig.transport.calls().iter().filter(|call| call.kind != crate::transport::CallKind::Subscription).count(), 0, "{kind}");
    }
}

#[tokio::test(start_paused = true)]
async fn missing_change_kind_is_rejected_before_reading() {
    let mut users = users_spec();
    users.change_feed = Some(change_feed());
        users.changes.push(serde_json::from_value(json!({"area":"USER", "relation":"current", "parameter":null})).unwrap());
    let rig = rig_on(Db::open_in_memory(vec![users], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    let subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changes"), "Changes", "users", EventEffect::Upsert)).await.unwrap();
    rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[{"topic":"USER", "action":null, "recordId":"u1"}]}}))));
    rig.transport.await_consumed("Changes").await;
    assert_eq!(subscription.status().rejected, 1);
    assert_eq!(rig.transport.calls().iter().filter(|call| call.kind != crate::transport::CallKind::Subscription).count(), 0);
}

#[tokio::test(start_paused = true)]
async fn declared_removals_keep_created_and_updated_node_reads() {
    for kind in ["CREATED", "UPDATED"] {
        let original = super::node_tests::node_rig();
        let mut specs: Vec<_> = original.db.shared.specs.values().cloned().collect();
        specs.iter_mut().find(|spec| spec.key.as_str() == "chats").unwrap().changes.push(serde_json::from_value(json!({"area":"CHAT", "relation":"node", "parameter":null, "removes_on":["DELETED", "ACCESS_REVOKED"]})).unwrap());
        specs[0].change_feed = Some(change_feed());
    let rig = rig_on(Db::open_in_memory(specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
        for operation in original.sync.inner.operations.lock().unwrap().values() { rig.sync.register((**operation).clone()); }
        rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
        put(&rig, "chats", json!({"id":"c1", "title":"before"}));
        rig.transport.enqueue("ChatNode", crate::transport::Reply::data(json!({"node":{"id":"c1", "title":"after"}})));
        let subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changes"), "Changes", "chats", EventEffect::Upsert)).await.unwrap();
        rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[{"topic":"CHAT", "action":kind, "recordId":"c1"}]}}))));
        rig.transport.await_consumed("Changes").await;
        assert_eq!(rig.transport.call_count("ChatNode"), 1, "{kind}");
        assert_eq!(rig.db.find("chats", &crate::identity::RowId::new("c1").unwrap()).unwrap().unwrap().text("title"), Some("after"), "{kind}");
        assert_eq!(subscription.status().applied, 1);
    }
}

#[tokio::test(start_paused = true)]
async fn change_ids_reread_registered_nodes_and_count_undeclared_areas() {
    let original = super::node_tests::node_rig();
    let mut specs: Vec<_> = original.db.shared.specs.values().cloned().collect();
    specs.iter_mut().find(|spec| spec.key.as_str() == "chats").unwrap().changes.push(crate::schema::ChangeRead { area: "CHAT".into(), relation: "node".into(), parameter: None, removes_on: vec![], observed: false });
    specs.iter_mut().find(|spec| spec.key.as_str() == "messages").unwrap().changes.push(serde_json::from_value(json!({"area":"CHAT", "relation":"transcode_status", "parameter":null, "removes_on":["DELETED"]})).unwrap());
    specs[0].change_feed = Some(change_feed());
    let rig = rig_on(Db::open_in_memory(specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
    for operation in original.sync.inner.operations.lock().unwrap().values() { rig.sync.register((**operation).clone()); }
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    rig.transport.enqueue("ChatNode", crate::transport::Reply::data(json!({"node":{"id":"c1", "title":"reread"}})));
    let subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changes"), "Changes", "chats", EventEffect::Upsert)).await.unwrap();
    put(&rig, "messages", json!({"id":"c1", "text":"remove"}));
    rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[{"topic":"CHAT", "action":"DELETED", "recordId":"c1", "title":"untrusted"}, {"topic":"UNKNOWN", "action":"UPDATED", "recordId":"x"}]}}))));
    rig.transport.await_consumed("Changes").await;
    let row = rig.db.find("chats", &crate::identity::RowId::new("c1").unwrap()).unwrap();
    assert_eq!(row.and_then(|row| row.text("title").map(str::to_string)), Some("reread".into()));
    assert_eq!(subscription.status().rejected, 1);
    assert_eq!(rig.transport.calls().iter().filter(|call| call.operation_name == "ChatNode").count(), 1);
    assert!(rig.db.query(&Query::new("messages")).unwrap().is_empty());
    assert_eq!(subscription.status().applied, 2);
}

#[tokio::test(start_paused = true)]
async fn declared_subscription_rereads_or_ignores_without_copying_event_fields() {
    for effect in ["reread", "ignore"] {
        let mut model = users_spec();
        model.listens.push(serde_json::from_value(json!({"subscription":"RecordSignal", "root":"recordSignal", "apply":{"effect":effect, "via":"current"}})).unwrap());
        let rig = rig_on(Db::open_in_memory(vec![model], None).unwrap(), MockTransport::new(), SyncConfig::default());
        put(&rig, "users", json!({"id":"u1", "name":"before"}));
        rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u1", "name":"fetched"}})));
        rig.sync.register(super::OperationDef::new("RecordSignal", "subscription RecordSignal { recordSignal { id name } }", "recordSignal"));
        rig.sync.listen();
        rig.transport.await_calls("RecordSignal", 1).await;
        rig.transport.emit("RecordSignal", SubscriptionEvent::Next(Response::data(json!({"recordSignal":{"id":"u1", "name":"untrusted"}}))));
        rig.transport.await_consumed("RecordSignal").await;
        assert_eq!(rig.transport.call_count("CurrentUser"), usize::from(effect == "reread"));
        assert_eq!(rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap().text("name"), Some(if effect == "reread" { "fetched" } else { "before" }));
    }
}

#[tokio::test(start_paused = true)]
async fn declared_activity_changes_only_ephemeral_typing_and_expires() {
    let mut model = users_spec();
    model.listens.push(serde_json::from_value(json!({"subscription":"PresenceSignal", "root":"presenceSignal", "id":"memberId", "apply":{"effect":"ephemeral", "fields":{"typing":"active"}, "matches":{"kind":"ACTIVE"}}})).unwrap());
    let rig = rig_on(Db::open_in_memory(vec![model], None).unwrap(), MockTransport::new(), SyncConfig::default());
    put(&rig, "users", json!({"id":"u1", "name":"Ann"}));
    rig.sync.register(super::OperationDef::new("PresenceSignal", "subscription PresenceSignal { presenceSignal { memberId active kind } }", "presenceSignal"));
    rig.sync.listen();
    rig.transport.await_calls("PresenceSignal", 1).await;
    let row = || rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
    for (kind, active, expected) in [("ACTIVE", true, true), ("OTHER", false, true), ("ACTIVE", false, false), ("ACTIVE", true, true)] {
        rig.transport.emit("PresenceSignal", SubscriptionEvent::Next(Response::data(json!({"presenceSignal":{"memberId":"u1", "kind":kind, "active":active}}))));
        rig.transport.await_consumed("PresenceSignal").await;
        assert_eq!(row().get("typing"), Some(&json!(expected)));
    }
    rig.clock.advance(Duration::from_secs(6));
    rig.db.expire_ephemeral().unwrap();
    assert_eq!(row().get("typing"), Some(&json!(false)));
    assert_eq!(row().text("name"), Some("Ann"));
}

#[tokio::test(start_paused = true)]
async fn a_shared_change_area_reads_each_declared_node_once() {
    let original = super::node_tests::node_rig();
    let mut specs: Vec<_> = original.db.shared.specs.values().cloned().collect();
    for spec in &mut specs {
        let relation = spec.relations.iter().find(|relation| relation.node == Some(crate::schema::NodeRead::One)).unwrap().name.clone();
        spec.changes.push(crate::schema::ChangeRead { area:"REACTION".into(), relation: relation.clone(), parameter:None, removes_on: vec![], observed: false });
        spec.changes.push(crate::schema::ChangeRead { area:"MESSAGE".into(), relation, parameter:None, removes_on: vec!["DELETED".into(), "ACCESS_REVOKED".into()], observed: false });
    }
    specs[0].change_feed = Some(change_feed());
    let rig = rig_on(Db::open_in_memory(specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
    for operation in original.sync.inner.operations.lock().unwrap().values() { rig.sync.register((**operation).clone()); }
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    rig.transport.enqueue("ChatNode", crate::transport::Reply::data(json!({"node":null})));
    rig.transport.enqueue("MessageTranscodeStatus", crate::transport::Reply::data(json!({"node":{"id":"m1", "text":"updated"}})));
    let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changes"), "Changes", "messages", EventEffect::Upsert)).await.unwrap();
    rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[{"topic":"REACTION", "action":"DELETED", "recordId":"m1"}]}}))));
    rig.transport.await_consumed("Changes").await;
    assert_eq!(rig.transport.calls().iter().filter(|call| call.kind == crate::transport::CallKind::Query).count(), 2);
    assert_eq!(rig.db.find("messages", &crate::identity::RowId::new("m1").unwrap()).unwrap().unwrap().text("text"), Some("updated"));
    assert!(rig.db.query(&Query::new("chats")).unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn memory_events_without_server_ids_create_distinct_expiring_rows() {
    let fixtures: Vec<serde_json::Value> = serde_json::from_str(include_str!("../../../fixtures/change-events/subscriptions.json")).unwrap();
    for index in [1, 3] {
        let fixture = &fixtures[index];
        let root = fixture["result"]["data"].as_object().unwrap().keys().next().unwrap();
        let mut model = crate::schema::ModelSpec::new("notices", vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("body", ScalarKind::Str).optional(),
            FieldSpec::scalar("messageId", ScalarKind::Id).optional(),
            FieldSpec::scalar("actorUserId", ScalarKind::Id).optional(),
        ]);
        model.storage = crate::schema::Storage::Memory;
        model.expire_after = Some("5.seconds".into());
        let rig = rig_on(Db::open_in_memory(vec![model], None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(super::OperationDef::new("Notices", fixture["query"].as_str().unwrap(), root));
        let subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("notice"), "Notices", "notices", EventEffect::Upsert)).await.unwrap();
        for _ in 0..2 { rig.transport.emit("Notices", SubscriptionEvent::Next(serde_json::from_value(fixture["result"].clone()).unwrap())); }
        rig.transport.await_consumed("Notices").await;
        let rows = rig.db.query(&Query::new("notices")).unwrap();
        assert_eq!(rows.len(), 2, "{root}");
        assert_ne!(rows[0].id, rows[1].id);
        assert_eq!(subscription.status().applied, 2);
        assert!(rig.transport.calls().iter().all(|call| call.kind == crate::transport::CallKind::Subscription));
        rig.clock.advance(Duration::from_secs(6));
        rig.db.expire_ephemeral().unwrap();
        assert!(rig.db.query(&Query::new("notices")).unwrap().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn inv_live_03_repeated_ready_rereads_expired_watched_queries_after_cable_reconnect() {
    let rig = rig_with_subscription();
    for name in ["before", "after"] {
        rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u1", "name":name}})));
    }
    let _watch = rig.sync.watch("users", "current", &serde_json::Map::new()).unwrap();
    rig.sync.load("users", "current", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
    let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "users", EventEffect::Upsert)).await.unwrap();
    rig.transport.emit("MomentChanged", SubscriptionEvent::Ready);
    rig.transport.await_consumed("MomentChanged").await;
    rig.clock.advance(Duration::from_secs(300));
    rig.transport.emit("MomentChanged", SubscriptionEvent::Ready);
    rig.transport.await_consumed("MomentChanged").await;
    assert_eq!(rig.transport.call_count("CurrentUser"), 2);
    assert_eq!(rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap().text("name"), Some("after"));
}

#[tokio::test(start_paused = true)]
async fn reconnect_reads_only_expired_watched_nodes() {
    let mut measurements = Vec::new();
    let mut expected = Vec::new();
    for reopen in [false, true] {
        for count in [0, 1, 20] {
            for (fresh_for, age, expired_first) in [("5.minutes", 0, false), ("5.minutes", 300, false), ("5.minutes", 301, false), ("5.minutes", 0, true), ("0.seconds", 0, false)] {
                let mut moments = moments_spec();
                let mut node = crate::schema::RelationSpec::new("node", &[]);
                node.node = Some(crate::schema::NodeRead::One);
                node.text_params = vec![crate::schema::NODE_PARAM.into()];
                let mut read = remote("MomentNode", crate::schema::RemoteShape::Single, fresh_for);
                read.variables.insert("id".into(), crate::schema::VariableSource::Param { name: crate::schema::NODE_PARAM.into() });
                node.remote = Some(read);
                moments.relations.push(node);
                moments.changes.push(serde_json::from_value(json!({"area":"MOMENT", "relation":"node", "parameter":null})).unwrap());
                let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
                rig.sync.register(super::OperationDef::from_document("MomentNode", "query moment($id: ID!) { node(id: $id) { id text } }").unwrap());
                rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
                let mut watches = Vec::new();
                for index in 0..count {
                    let id = format!("m{index}");
                    rig.transport.enqueue("MomentNode", crate::transport::Reply::data(json!({"node":{"id":id, "text":"before"}})));
                    rig.sync.node("moments", "node", &id, false).await.unwrap();
                    watches.push(rig.sync.watch("moments", "node", &params(json!({"nodeId":id}))).unwrap());
                    if expired_first && index == 0 { rig.clock.advance(Duration::from_secs(300)); }
                }
                for index in 0..count {
                    rig.transport.enqueue("MomentNode", crate::transport::Reply::data(json!({"node":{"id":format!("m{index}"), "text":"after"}})));
                }
                let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
                rig.transport.emit("MomentChanged", SubscriptionEvent::Ready);
                rig.transport.await_consumed("MomentChanged").await;
                assert_eq!(rig.transport.call_count("MomentNode"), count, "first Ready");
                if reopen {
                    rig.transport.disconnect("MomentChanged");
                    rig.clock.advance(Duration::from_secs(age));
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    assert_eq!(subscription.status().connects, 2);
                } else {
                    rig.clock.advance(Duration::from_secs(age));
                }
                rig.transport.emit("MomentChanged", SubscriptionEvent::Ready);
                rig.transport.await_consumed("MomentChanged").await;
                let reads = rig.transport.call_count("MomentNode") - count;
                measurements.push((reopen, count, fresh_for, age, expired_first, reads));
                let expired = if fresh_for == "0.seconds" || age >= 300 { count } else if expired_first { count.min(1) } else { 0 };
                expected.push((reopen, count, fresh_for, age, expired_first, expired));
            }
        }
    }
    assert_eq!(measurements, expected);
}

#[tokio::test(start_paused = true)]
async fn partial_child_events_preserve_absent_parent_fields_and_apply_explicit_null() {
    for stored_child in [true, false] {
        let mut users = users_spec();
        users.fields.push(FieldSpec::scalar("lastMomentId", ScalarKind::Id).nullable().optional());
        users.fields.push(FieldSpec::scalar("lastText", ScalarKind::Str).nullable().optional());
        let mut moments = moments_spec();
        moments.fields.iter_mut().find(|field| field.name == "text").unwrap().nullable = true;
        moments.associations[0].sets = [("lastMomentId".to_string(), "id".to_string()), ("lastText".to_string(), "text".to_string())].into();
        let rig = rig_on(Db::open_in_memory(vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
        put(&rig, "users", json!({"id": "u1", "name": "Ann", "lastMomentId": "m1", "lastText": "kept"}));
        if stored_child {
            put(&rig, "moments", json!({"id": "m1", "userId": "u1", "text": "kept"}));
        }
        let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
        for (node, expected) in [(json!({"id": "m1", "userId": "u1"}), Some("kept")), (json!({"id": "m1", "userId": "u1", "text": null}), None)] {
            rig.transport.emit("MomentChanged", next(node));
            rig.transport.await_consumed("MomentChanged").await;
            let parent = rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
            assert_eq!(parent.text("lastText"), expected, "stored child: {stored_child}");
            assert_eq!(parent.text("lastMomentId"), Some("m1"));
        }
        assert_eq!(subscription.status().applied, 2);
    }
}

#[tokio::test(start_paused = true)]
async fn upsert_event_changes_the_row_and_fires_the_observer() {
    let rig = rig_with_subscription();
    put(&rig, "moments", json!({"id": "m1", "text": "before", "userId": null}));
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = seen.clone();
    let _observer = rig
        .db
        .observe(Query::new("moments"), move |change| {
            sink.lock().unwrap().push(change.rows.iter().filter_map(|row| row.text("text")).collect::<Vec<_>>().join(","));
        })
        .unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "after"})));
    rig.transport.await_consumed("MomentChanged").await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(text_of(&rig, "moments", "m1"), Some("after".to_string()));
    assert_eq!(*seen.lock().unwrap(), ["before", "after"]);
    assert_eq!(subscription.status().applied, 1);
}

#[tokio::test(start_paused = true)]
async fn upsert_event_lands_nested_author() {
    let rig = rig_with_subscription();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m9", "text": "t", "author": {"id": "u7", "name": "Zed"}})));
    rig.transport.await_consumed("MomentChanged").await;
    let user = rig.db.find("users", &crate::identity::RowId::new("u7").unwrap()).unwrap().unwrap();
    assert_eq!(user.text("name"), Some("Zed"));
}

#[tokio::test(start_paused = true)]
async fn patch_event_merges_fields_and_skips_unknown_rows() {
    let rig = rig_with_subscription();
    put(&rig, "moments", json!({"id": "m1", "text": "keep", "userId": "u1"}));
    let def = SubscriptionDef::new(EventSpec::new("moment_patched"), "MomentChanged", "moments", EventEffect::Patch);
    let _subscription = rig.sync.subscribe(def).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "patched"})));
    rig.transport.emit("MomentChanged", next(json!({"id": "ghost", "text": "x"})));
    rig.transport.await_consumed("MomentChanged").await;
    let row = rig.db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap().unwrap();
    assert_eq!((row.text("text"), row.text("userId")), (Some("patched"), Some("u1")));
    assert_eq!(text_of(&rig, "moments", "ghost"), None);
}

#[tokio::test(start_paused = true)]
async fn destroy_event_removes_the_row() {
    let rig = rig_with_subscription();
    put(&rig, "moments", json!({"id": "m1", "text": "bye", "userId": null}));
    let def = SubscriptionDef::new(EventSpec::new("moment_gone"), "MomentChanged", "moments", EventEffect::Destroy);
    let _subscription = rig.sync.subscribe(def).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m1"})));
    rig.transport.await_consumed("MomentChanged").await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn dropping_the_subscription_cancels_the_transport() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    drop(subscription);
    rig.transport.await_cancellations("MomentChanged", 1).await;
    assert_eq!(rig.transport.cancellations("MomentChanged"), 1);
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "late"})));
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn inv_live_03_dropped_connection_is_reopened_after_a_delay() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.disconnect("MomentChanged");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(rig.transport.call_count("MomentChanged"), 2);
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "again"})));
    rig.transport.await_consumed("MomentChanged").await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("again".to_string()));
    assert_eq!(subscription.status().connects, 2);
}

#[tokio::test(start_paused = true)]
async fn inv_live_03_offline_subscription_waits_without_spending_attempts_and_reconnects_online() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.sync.set_online(false);
    for _ in 0..4 {
        rig.transport.emit("MomentChanged", failed(SubscriptionFailureKind::Transport));
        tokio::time::sleep(Duration::from_secs(40)).await;
    }
    assert_eq!(rig.transport.call_count("MomentChanged"), 1);
    assert!(!subscription.status().degraded);
    rig.sync.set_online(true);
    rig.transport.await_calls("MomentChanged", 2).await;
    assert_eq!(rig.transport.call_count("MomentChanged"), 2);
    assert_eq!(subscription.status().connects, 2);
    rig.transport.emit("MomentChanged", failed(SubscriptionFailureKind::Transport));
    rig.transport.await_cancellations("MomentChanged", 2).await;
    assert!(reconnects_within_first_backoff(&rig, 3).await, "offline failures spent attempts");
}

#[tokio::test(start_paused = true)]
async fn inv_live_04_repeated_transport_failures_stay_degraded_until_next_after_ready() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    for failures in 1..=4 {
        rig.transport.emit("MomentChanged", failed(SubscriptionFailureKind::Transport));
        tokio::time::sleep(Duration::from_secs(40)).await;
        assert_eq!(subscription.status().degraded, failures == 4, "after {failures} failures");
    }
    assert_eq!(rig.transport.call_count("MomentChanged"), 5);
    assert!(!subscription.status().stopped);
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "before ready"})));
    rig.transport.await_consumed("MomentChanged").await;
    assert!(subscription.status().degraded);
    rig.transport.emit("MomentChanged", SubscriptionEvent::Ready);
    rig.transport.await_consumed("MomentChanged").await;
    assert!(subscription.status().degraded);
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "healthy"})));
    rig.transport.await_consumed("MomentChanged").await;
    assert!(!subscription.status().degraded);
}

#[tokio::test(start_paused = true)]
async fn terminal_http_subscription_failures_stop_without_retry() {
    for status in [401, 403] {
        let rig = rig_with_subscription();
        let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
        rig.transport.emit("MomentChanged", SubscriptionEvent::Failed(SubscriptionFailure::from_error(crate::transport::TransportError::Http { status, body: "refused".into() })));
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert!(subscription.status().stopped, "HTTP {status} was retried");
        assert_eq!(rig.transport.call_count("MomentChanged"), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn independent_feeds_spread_reconnects() {
    let mut delays = Vec::new();
    for _ in 0..8 {
        let rig = rig_with_subscription();
        let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
        rig.transport.disconnect("MomentChanged");
        tokio::task::yield_now().await;
        let started = tokio::time::Instant::now();
        for _ in 0..1100 {
            if rig.transport.call_count("MomentChanged") == 2 { break; }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(rig.transport.call_count("MomentChanged"), 2);
        delays.push(started.elapsed());
    }
    assert!(delays.windows(2).any(|pair| pair[0] != pair[1]), "clients repeated together: {delays:?}");
}

#[tokio::test(start_paused = true)]
async fn inv_live_04_graphql_failure_stops_the_subscription() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", failed(SubscriptionFailureKind::Graphql));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(subscription.status().stopped);
    assert!(subscription.status().degraded);
    assert_eq!(rig.transport.call_count("MomentChanged"), 1);
}

#[tokio::test(start_paused = true)]
async fn terminal_session_failure_stops_even_while_offline() {
    for online in [false, true] {
        let rig = rig_with_subscription();
        let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
        rig.sync.set_online(online);
        let error = crate::transport::TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: "session_unknown".into() };
        rig.transport.emit("MomentChanged", SubscriptionEvent::Failed(crate::transport::SubscriptionFailure::from_error(error.clone())));
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert!(subscription.status().stopped, "online={online}");
        assert_eq!(subscription.status().failure.unwrap().error, error);
        assert_eq!(rig.transport.call_count("MomentChanged"), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn a_typed_session_refusal_during_subscribe_stops_without_retry() {
    struct Refusal;
    #[async_trait::async_trait]
    impl crate::transport::Transport for Refusal {
        async fn query(&self, _: crate::transport::GraphQLRequest) -> Result<crate::transport::Response, crate::transport::TransportError> { unreachable!() }
        async fn mutate(&self, _: crate::transport::GraphQLRequest) -> Result<crate::transport::Response, crate::transport::TransportError> { unreachable!() }
        async fn subscribe(&self, _: crate::transport::GraphQLRequest) -> Result<crate::transport::Subscription, crate::transport::TransportError> {
            Err(crate::transport::TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: "session_unknown".into() })
        }
        async fn put(&self, _: crate::transport::PutRequest) -> Result<crate::transport::PutResponse, crate::transport::TransportError> { unreachable!() }
    }
    let rig = rig_with_subscription();
    let sync = crate::sync::Sync::new(rig.db.clone(), std::sync::Arc::new(Refusal), std::sync::Arc::new(rig.clock.clone()), Default::default());
    *super::locked(&sync.inner.operations) = super::locked(&rig.sync.inner.operations).clone();
    let subscription = sync.subscribe(upsert_def()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(120)).await;
    assert!(subscription.status().stopped);
    assert!(matches!(subscription.status().failure.unwrap().error, crate::transport::TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, .. }));
}

#[tokio::test(start_paused = true)]
async fn inv_event_08_debounced_events_land_once_per_row_after_the_interval() {
    let rig = rig_with_subscription();
    put(&rig, "moments", json!({"id": "m1", "text": "start", "userId": null}));
    let _observer = rig.db.observe(Query::new("moments"), |_| {}).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let event = EventSpec::new("moment_changed").debounced(Duration::from_millis(500)).merged_by_row();
    let def = SubscriptionDef::new(event, "MomentChanged", "moments", EventEffect::Upsert);
    let _subscription = rig.sync.subscribe(def).await.unwrap();
    for text in ["one", "two", "three"] {
        rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": text})));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(text_of(&rig, "moments", "m1"), Some("start".to_string()));
    let waits = rig.transport.waits("MomentChanged");
    rig.clock.advance(Duration::from_millis(400));
    rig.transport.await_waits("MomentChanged", waits + 1).await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("three".to_string()));
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(rig.db.counters().observer_calls, 2);
}

#[tokio::test(start_paused = true)]
async fn event_spec_with_a_fault_is_rejected() {
    let rig = rig_with_subscription();
    let event = EventSpec::new("moment_changed").debounced(Duration::from_millis(500));
    let def = SubscriptionDef::new(event, "MomentChanged", "moments", EventEffect::Upsert);
    assert!(matches!(rig.sync.subscribe(def).await, Err(SyncError::Event(_))));
}

#[tokio::test(start_paused = true)]
async fn unknown_operation_is_rejected() {
    let rig = rig();
    let result = rig.sync.subscribe(upsert_def()).await;
    assert!(matches!(result, Err(SyncError::UnknownOperation(_))));
}

#[tokio::test(start_paused = true)]
async fn an_event_sets_the_declared_ephemeral_values_on_its_row() {
    let rig = rig_with_subscription();
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    let def = SubscriptionDef::new(EventSpec::new("typing"), "MomentChanged", "users", EventEffect::Patch).setting(params(json!({"typing": true})));
    let _subscription = rig.sync.subscribe(def).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "u1", "name": "Ann"})));
    rig.transport.await_consumed("MomentChanged").await;
    let user = rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
    assert_eq!(user.get("typing"), Some(&json!(true)));
}

#[tokio::test(start_paused = true)]
async fn a_destroying_event_sets_nothing() {
    let rig = rig_with_subscription();
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    let def = SubscriptionDef::new(EventSpec::new("typing"), "MomentChanged", "users", EventEffect::Destroy).setting(params(json!({"typing": true})));
    let _subscription = rig.sync.subscribe(def).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "u1"})));
    rig.transport.await_consumed("MomentChanged").await;
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    let user = rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
    assert_eq!(user.get("typing"), Some(&json!(false)));
}

#[tokio::test(start_paused = true)]
async fn inv_event_05_an_event_that_creates_a_row_does_not_reload_the_watched_lists_of_its_model() {
    let rig = rig_with_subscription();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1", "m2"], false, "c2"));
    let _watch = rig.sync.watch("moments", "feed", &serde_json::Map::new()).unwrap();
    rig.sync.load("moments", "feed", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m2", "text": "new", "userId": null})));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(member_ids(&rig, "moments", "feed", &serde_json::Map::new()), ["m1"]);
    assert_eq!(rig.transport.call_count("MomentsFeed"), 1);
}

fn user_list(ids: &[&str]) -> crate::transport::Reply {
    crate::transport::Reply::data(json!({"moments": ids.iter().map(|id| moment(id, id)).collect::<Vec<_>>()}))
}

#[tokio::test(start_paused = true)]
async fn a_refused_event_batch_reloads_the_watched_lists_of_its_model_or_leaves_a_gap_for_the_reader() {
    for reread_fails in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut specs = specs();
        let moments = specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap();
        moments.changes.push(crate::schema::ChangeRead { area: "MOMENT".into(), relation: "by_user".into(), parameter: None, removes_on: Vec::new(), observed: false });
        moments.change_feed = Some(change_feed());
        let rig = rig_on(Db::open(&directory.path().join("db.sqlite"), specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
        rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
        let params = serde_json::json!({"userId": "u1"}).as_object().unwrap().clone();
        rig.transport.enqueue("MomentsByUser", user_list(&["m1"]));
        let reread = if reread_fails { crate::transport::Reply::fail(crate::transport::TransportError::Http { status: 422, body: "rejected".into() }) } else { user_list(&["m1", "m2"]) };
        rig.transport.enqueue("MomentsByUser", reread);
        let _watch = rig.sync.watch("moments", "by_user", &params).unwrap();
        rig.sync.load("moments", "by_user", &params, crate::sync::LoadOptions::default()).await.unwrap();
        let events = rig.sync.model_events("moments").unwrap();
        rig.transport.await_calls("Changes", 1).await;
        let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
        let shared = rig.db.shared.clone();
        *rig.db.shared.store.before_write.lock().unwrap() = Some(Box::new(move || {
            let mut inner = shared.store.lock();
            inner.generation = inner.generation.next();
        }));
        rig.transport.emit("MomentChanged", next(json!({"id": "m9", "text": "t", "userId": "u1"})));
        tokio::time::sleep(Duration::from_secs(3)).await;
        let gap = tokio::time::timeout(Duration::from_secs(1), events.next()).await;
        let members = member_ids(&rig, "moments", "by_user", &params);
        assert_eq!(subscription.status().rejected, 1);
        assert_eq!(rig.transport.call_count("MomentsByUser"), 2);
        if reread_fails {
            assert_eq!(members, ["m1"]);
            assert!(matches!(gap, Ok(Some(super::ModelEventItem::Missed(1)))));
        } else {
            assert_eq!(members, ["m1", "m2"]);
            assert!(gap.is_err());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn inv_event_05_an_event_row_joins_the_loaded_list_that_declares_it_and_does_not_reload_it() {
    let rig = rig_with_subscription();
    let params = serde_json::json!({"userId": "u1"}).as_object().unwrap().clone();
    rig.transport.enqueue("MomentsByUser", user_list(&["m1"]));
    rig.transport.enqueue("MomentsByUser", user_list(&["m1"]));
    let _watch = rig.sync.watch("moments", "by_user", &params).unwrap();
    rig.sync.load("moments", "by_user", &params, crate::sync::LoadOptions::default()).await.unwrap();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m2", "text": "new", "userId": "u1"})));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(member_ids(&rig, "moments", "by_user", &params), ["m1", "m2"]);
    assert_eq!(rig.transport.call_count("MomentsByUser"), 1);
}

struct RefusingFirstSubscribe {
    inner: MockTransport,
    refusals: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::transport::Transport for RefusingFirstSubscribe {
    async fn query(&self, request: crate::transport::GraphQLRequest) -> Result<Response, crate::transport::TransportError> {
        self.inner.query(request).await
    }
    async fn mutate(&self, request: crate::transport::GraphQLRequest) -> Result<Response, crate::transport::TransportError> {
        self.inner.mutate(request).await
    }
    async fn subscribe(&self, request: crate::transport::GraphQLRequest) -> Result<crate::transport::Subscription, crate::transport::TransportError> {
        let refused = self.refusals.try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |left| left.checked_sub(1)).is_ok();
        if refused {
            return Err(crate::transport::TransportError::Network("down".to_string()));
        }
        self.inner.subscribe(request).await
    }
    async fn put(&self, request: crate::transport::PutRequest) -> Result<crate::transport::PutResponse, crate::transport::TransportError> {
        self.inner.put(request).await
    }
}

#[tokio::test(start_paused = true)]
async fn inv_live_02_a_refused_first_connection_is_retried_like_a_reconnect() {
    let inner = MockTransport::new();
    let transport = Arc::new(RefusingFirstSubscribe { inner: inner.clone(), refusals: 1.into() });
    let db = Db::open_in_memory(specs(), None).unwrap();
    let sync = Sync::new(db.clone(), transport, Arc::new(TestClock::new(Duration::from_millis(START_MS))), SyncConfig::default());
    register_operations(&sync);
    sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    let subscription = sync.subscribe(upsert_def()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(subscription.status().connects, 1);
    inner.emit("MomentChanged", next(json!({"id": "m1", "text": "late", "userId": null})));
    inner.await_consumed("MomentChanged").await;
    let row = db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap();
    assert_eq!(row.and_then(|row| row.text("text").map(str::to_string)), Some("late".to_string()));
}

#[tokio::test(start_paused = true)]
async fn inv_event_09_upsert_event_touches_the_parent_that_asks_for_it() {
    let mut moments = moments_spec();
    moments.associations.iter_mut().for_each(|association| association.touches_parent = true);
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    let user = crate::identity::RowId::new("u1").unwrap();
    let before = rig.db.find("users", &user).unwrap().unwrap().revision;
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "t", "userId": "u1"})));
    rig.transport.await_consumed("MomentChanged").await;
    assert!(rig.db.find("users", &user).unwrap().unwrap().revision > before);
}

#[tokio::test(start_paused = true)]
async fn an_event_for_a_child_whose_parent_is_not_stored_reloads_the_watched_lists_of_the_parent() {
    let mut users = users_spec();
    users.fields.push(FieldSpec::scalar("lastMomentId", ScalarKind::Id).nullable().optional());
    let mut moments = moments_spec();
    moments.associations[0].sets = [("lastMomentId".to_string(), "id".to_string())].into();
    let rig = rig_on(Db::open_in_memory(vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    for _ in 0..2 {
        rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me": {"id": "u1", "name": "Ann"}})));
    }
    let _watch = rig.sync.watch("users", "current", &serde_json::Map::new()).unwrap();
    rig.sync.load("users", "current", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m2", "text": "new", "userId": "u9"})));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(rig.transport.call_count("CurrentUser"), 2);
}

async fn join_cost(size: usize) -> usize {
    let rig = rig_with_subscription();
    let params = serde_json::json!({"userId": "u1"}).as_object().unwrap().clone();
    let ids: Vec<String> = (0..size).map(|index| format!("m{index:06}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    rig.transport.enqueue("MomentsByUser", user_list(&ids));
    let _watch = rig.sync.watch("moments", "by_user", &params).unwrap();
    rig.sync.load("moments", "by_user", &params, crate::sync::LoadOptions::default()).await.unwrap();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    let before = crate::identity::touches::member_reads();
    rig.transport.emit("MomentChanged", next(json!({"id": "joined", "text": "new", "userId": "u1"})));
    tokio::time::sleep(Duration::from_secs(3)).await;
    let cost = crate::identity::touches::member_reads() - before;
    assert_eq!(member_ids(&rig, "moments", "by_user", &params).last().map(String::as_str), Some("joined"));
    cost
}

#[tokio::test(start_paused = true)]
async fn an_event_row_joining_a_loaded_list_reads_the_same_members_at_every_list_size() {
    assert_eq!(join_cost(100).await, join_cost(10_000).await, "member reads at 100 and 10000 rows");
}
