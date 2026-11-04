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
async fn direct_subscribe_applies_the_passed_definition_instead_of_schema_declarations() {
    let rig = rig();
    put(&rig, "users", json!({"id":"u1", "name":"Ann"}));
    put(&rig, "moments", moment("u1", "keep"));
    let mut definition = SubscriptionDef::new(EventSpec::new("custom"), "MomentEvents", "users", EventEffect::Destroy);
    definition.root = Some("custom".into());
    definition.id = vec!["key".into()];
    let _subscription = rig.sync.subscribe(definition).await.unwrap();
    rig.transport.emit("MomentEvents", SubscriptionEvent::Next(Response::data(json!({"custom":{"key":"u1"}, "momentEvent":{"id":"u1", "text":"wrong"}}))));
    rig.transport.await_consumed("MomentEvents").await;
    assert!(rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().is_none());
    assert_eq!(text_of(&rig, "moments", "u1"), Some("keep".into()));
}

#[tokio::test(start_paused = true)]
async fn shared_batch_merges_nested_partial_rows_before_admission() {
    for stored in [false, true] {
        let mut users = users_spec();
        users.fields.push(FieldSpec::scalar("label", ScalarKind::Str).optional());
        let mut moments = moments_spec();
        moments.listens = vec![serde_json::from_value(json!({"subscription":"Signal", "root":"first"})).unwrap()];
        let mut mirror = moments.clone();
        mirror.key = crate::identity::ModelKey::new("mirror");
        mirror.listens[0].root = Some("second".into());
        let rig = rig_on(Db::open_in_memory(vec![users, moments, mirror], None).unwrap(), MockTransport::new(), SyncConfig::default());
        if stored { put(&rig, "users", json!({"id":"u", "name":"before", "label":"before"})); }
        rig.sync.register(super::OperationDef::new("Signal", "subscription Signal { first { id } second { id } }", "first"));
        rig.sync.inner.listen_all();
        rig.transport.await_calls("Signal", 1).await;
        let revision = rig.db.revision_seq().unwrap();
        rig.transport.emit("Signal", SubscriptionEvent::Next(Response::data(json!({"first":{"id":"a", "author":{"id":"u", "name":"Ann"}}, "second":{"id":"b", "author":{"id":"u", "label":"friend"}}}))));
        rig.transport.await_consumed("Signal").await;
        let row = rig.db.find("users", &crate::identity::RowId::new("u").unwrap()).unwrap().unwrap();
        assert_eq!((row.text("name"), row.text("label")), (Some("Ann"), Some("friend")));
        assert_eq!(rig.db.revision_seq().unwrap(), revision + 1);
    }
}

#[tokio::test(start_paused = true)]
async fn shared_changes_do_not_suppress_ephemeral_or_reread_definitions() {
    let mut users = users_spec();
    users.change_feed = Some(crate::schema::ChangeFeed { subscription:"Signal".into(), root:"signal.changes".into(), ..change_feed() });
    users.changes.push(crate::schema::ChangeRead { area:"USER".into(), relation:"current".into(), parameter:None, removes_on:vec![], observed:false, payload:None, id_of:None });
    users.listens = vec![
        serde_json::from_value(json!({"subscription":"Signal", "root":"signal.presence", "apply":{"effect":"ephemeral", "fields":{"typing":"active"}}})).unwrap(),
        serde_json::from_value(json!({"subscription":"Signal", "root":"signal.reread", "apply":{"effect":"reread", "via":"current"}})).unwrap(),
    ];
    let rig = rig_on(Db::open_in_memory(vec![users], None).unwrap(), MockTransport::new(), SyncConfig::default());
    put(&rig, "users", json!({"id":"u", "name":"before"}));
    rig.sync.register(super::OperationDef::new("Signal", "subscription Signal { signal { id } }", "signal"));
    for name in ["change", "effect"] { rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u", "name":name}}))); }
    rig.sync.inner.listen_all();
    rig.transport.await_calls("Signal", 1).await;
    rig.transport.emit("Signal", SubscriptionEvent::Next(Response::data(json!({"signal":{"changes":[{"topic":"USER", "action":"UPDATED", "recordId":"u"}], "presence":{"id":"u", "active":true}, "reread":{"id":"u"}}}))));
    rig.transport.await_consumed("Signal").await;
    let row = rig.db.find("users", &crate::identity::RowId::new("u").unwrap()).unwrap().unwrap();
    assert_eq!((rig.transport.call_count("CurrentUser"), row.text("name"), row.get("typing")), (2, Some("effect"), Some(&json!(true))));
}

#[tokio::test(start_paused = true)]
async fn model_ephemeral_and_reread_definitions_publish_before_and_after_events() {
    for effect in ["ephemeral", "reread"] {
        let mut users = users_spec();
        let apply = if effect == "ephemeral" { json!({"effect":effect, "fields":{"typing":"active"}}) } else { json!({"effect":effect, "via":"current"}) };
        users.listens.push(serde_json::from_value(json!({"subscription":"Signal", "apply":apply})).unwrap());
        let rig = rig_on(Db::open_in_memory(vec![users], None).unwrap(), MockTransport::new(), SyncConfig::default());
        put(&rig, "users", json!({"id":"u", "name":"before"}));
        rig.sync.register(super::OperationDef::new("Signal", "subscription Signal { signal { id active } }", "signal"));
        rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u", "name":"after"}})));
        let events = super::model_event_tests::open_heard(&rig.sync, "users").await.unwrap();
        rig.transport.emit("Signal", SubscriptionEvent::Next(Response::data(json!({"signal":{"id":"u", "active":true}}))));
        rig.transport.await_consumed("Signal").await;
        let item = tokio::time::timeout(Duration::from_secs(1), events.next()).await.unwrap().unwrap();
        let super::model_event_tests::HeardItem::Event(event) = item else { panic!("expected event, got {item:?}"); };
        assert_eq!((event.id.as_str(), event.area.as_str(), event.kind.as_str()), ("u", "Signal", "UPDATED"));
        let before = event.before.unwrap();
        let after = event.after.unwrap();
        if effect == "ephemeral" {
            assert_eq!((before.get("typing"), after.get("typing")), (Some(&json!(false)), Some(&json!(true))));
        } else {
            assert_eq!((before.text("name"), after.text("name")), (Some("before"), Some("after")));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn refused_shared_batch_rereads_every_model_and_records_each_failed_recovery() {
    for reread_fails in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut users = users_spec();
        users.listens.push(serde_json::from_value(json!({"subscription":"Signal", "root":"first"})).unwrap());
        let mut moments = moments_spec();
        moments.listens = vec![serde_json::from_value(json!({"subscription":"Signal", "root":"second"})).unwrap()];
        let rig = rig_on(Db::open(&directory.path().join("db.sqlite"), vec![users, moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(super::OperationDef::new("Signal", "subscription Signal { first { id } second { id } }", "first"));
        rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u", "name":"before"}})));
        rig.transport.enqueue("MomentsByUser", user_list(&["m1"]));
        for (operation, reply) in [("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u", "name":"after"}}))), ("MomentsByUser", user_list(&["m1", "m2"]))] {
            rig.transport.enqueue(operation, if reread_fails { crate::transport::Reply::fail(crate::transport::TransportError::Http { status:422, body:"refused".into() }) } else { reply });
        }
        let params = params(json!({"userId":"u"}));
        let _users = rig.sync.watch("users", "current", &Default::default()).unwrap();
        let _moments = rig.sync.watch("moments", "by_user", &params).unwrap();
        rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "current", &Default::default(), Default::default()).await.unwrap();
        rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "by_user", &params, Default::default()).await.unwrap();
        let users_events = super::model_event_tests::open_heard(&rig.sync, "users").await.unwrap();
        let moments_events = super::model_event_tests::open_heard(&rig.sync, "moments").await.unwrap();
        let shared = rig.db.shared.clone();
        *rig.db.shared.store.before_write.lock().unwrap() = Some(Box::new(move || shared.store.advance_generation_for_test()));
        rig.transport.emit("Signal", SubscriptionEvent::Next(Response::data(json!({"first":{"id":"u", "name":"event"}, "second":{"id":"m9", "text":"event", "userId":"u"}}))));
        rig.transport.await_consumed("Signal").await;
        assert_eq!((rig.transport.call_count("CurrentUser"), rig.transport.call_count("MomentsByUser")), (2, 2));
        assert!(rig.db.find("moments", &crate::identity::RowId::new("m9").unwrap()).unwrap().is_none());
        for events in [&users_events, &moments_events] {
            let item = tokio::time::timeout(Duration::from_millis(1), events.next()).await;
            if reread_fails { assert!(matches!(item, Ok(Some(super::model_event_tests::HeardItem::Lost(1))))); } else { assert!(item.is_err()); }
        }
        let name = rig.db.find("users", &crate::identity::RowId::new("u").unwrap()).unwrap().unwrap().text("name").unwrap().to_string();
        assert_eq!(name, if reread_fails { "before" } else { "after" });
        assert_eq!(member_ids(&rig, "moments", "by_user", &params), if reread_fails { vec!["m1"] } else { vec!["m1", "m2"] });
    }
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
    rig.transport.script_subscription("MomentChanged", vec![]);
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
        rig.db.update(&rig.db.admit().unwrap(), "moments", &crate::identity::RowId::new("m1").unwrap(), json!({"text":"pending"}).as_object().unwrap().clone()).unwrap();
        rig.db.update(&rig.db.admit().unwrap(), "users", &crate::identity::RowId::new("u1").unwrap(), json!({"name":"pending"}).as_object().unwrap().clone()).unwrap();
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
    specs.iter_mut().find(|spec| spec.key.as_str() == "chats").unwrap().changes.push(crate::schema::ChangeRead { area: "CHAT".into(), relation: "node".into(), parameter: None, removes_on: vec![], observed: false, payload: None, id_of: None });
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
        rig.sync.inner.listen_all();
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
    model.listens.push(serde_json::from_value(json!({"subscription":"PresenceSignal", "root":"presenceSignal", "id":["memberId"], "apply":{"effect":"ephemeral", "fields":{"typing":"active"}, "matches":{"kind":["ACTIVE"]}}})).unwrap());
    let rig = rig_on(Db::open_in_memory(vec![model], None).unwrap(), MockTransport::new(), SyncConfig::default());
    put(&rig, "users", json!({"id":"u1", "name":"Ann"}));
    rig.sync.register(super::OperationDef::new("PresenceSignal", "subscription PresenceSignal { presenceSignal { memberId active kind } }", "presenceSignal"));
    rig.sync.inner.listen_all();
    rig.transport.await_calls("PresenceSignal", 1).await;
    let row = || rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
    for (kind, active, expected) in [("ACTIVE", true, true), ("OTHER", false, true), ("ACTIVE", false, false), ("ACTIVE", true, true)] {
        rig.transport.emit("PresenceSignal", SubscriptionEvent::Next(Response::data(json!({"presenceSignal":{"memberId":"u1", "kind":kind, "active":active}}))));
        rig.transport.await_consumed("PresenceSignal").await;
        assert_eq!(row().get("typing"), Some(&json!(expected)));
    }
    rig.clock.advance(Duration::from_secs(6));
    rig.db.expire_ephemeral(&rig.db.admit().unwrap()).unwrap();
    assert_eq!(row().get("typing"), Some(&json!(false)));
    assert_eq!(row().text("name"), Some("Ann"));
}

#[tokio::test(start_paused = true)]
async fn a_shared_change_area_reads_each_declared_node_once() {
    let original = super::node_tests::node_rig();
    let mut specs: Vec<_> = original.db.shared.specs.values().cloned().collect();
    for spec in &mut specs {
        let relation = spec.relations.iter().find(|relation| relation.node == Some(crate::schema::NodeRead::One)).unwrap().name.clone();
        spec.changes.push(crate::schema::ChangeRead { area:"REACTION".into(), relation: relation.clone(), parameter:None, removes_on: vec![], observed: false, payload: None, id_of: None });
        spec.changes.push(crate::schema::ChangeRead { area:"MESSAGE".into(), relation, parameter:None, removes_on: vec!["DELETED".into(), "ACCESS_REVOKED".into()], observed: false, payload: None, id_of: None });
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
        rig.db.expire_ephemeral(&rig.db.admit().unwrap()).unwrap();
        assert!(rig.db.query(&Query::new("notices")).unwrap().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn a_server_event_of_equal_values_renews_a_memory_row_and_stays_silent_for_observers() {
    let mut model = crate::schema::ModelSpec::new("notices", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str).optional()]);
    model.storage = crate::schema::Storage::Memory;
    model.expire_after = Some("8.seconds".into());
    let rig = rig_on(Db::open_in_memory(vec![model], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "notices", EventEffect::Upsert)).await.unwrap();
    let notices = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = notices.clone();
    let _observer = rig.db.observe(Query::new("notices"), move |_| { counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }).unwrap();
    let id = crate::identity::RowId::new("n1").unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "n1", "body": "hello"})));
    rig.transport.await_consumed("MomentChanged").await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let seen = notices.load(std::sync::atomic::Ordering::SeqCst);
    rig.clock.advance(Duration::from_secs(5));
    rig.transport.emit("MomentChanged", next(json!({"id": "n1", "body": "hello"})));
    rig.transport.await_consumed("MomentChanged").await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    rig.clock.advance(Duration::from_secs(5));
    assert_eq!(rig.db.expire_ephemeral(&rig.db.admit().unwrap()).unwrap(), 0);
    assert!(rig.db.find("notices", &id).unwrap().is_some());
    assert_eq!(notices.load(std::sync::atomic::Ordering::SeqCst), seen);
    rig.clock.advance(Duration::from_secs(4));
    rig.db.expire_ephemeral(&rig.db.admit().unwrap()).unwrap();
    crate::test_wait::spin_until("the notice expires", || rig.db.find("notices", &id).unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn inv_live_03_repeated_ready_rereads_expired_watched_queries_after_cable_reconnect() {
    let rig = rig_with_subscription();
    rig.transport.script_subscription("MomentChanged", vec![]);
    for name in ["before", "after"] {
        rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u1", "name":name}})));
    }
    let _watch = rig.sync.watch("users", "current", &serde_json::Map::new()).unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "current", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
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
                rig.transport.script_subscription("MomentChanged", vec![]);
                let mut watches = Vec::new();
                for index in 0..count {
                    let id = format!("m{index}");
                    rig.transport.enqueue("MomentNode", crate::transport::Reply::data(json!({"node":{"id":id, "text":"before"}})));
                    rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "moments", "node", &id, false).await.unwrap();
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
    rig.transport.script_subscription("MomentChanged", vec![]);
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
        async fn query(&self, _admitted: &crate::lifecycle::Admitted, _: crate::transport::GraphQLRequest) -> Result<crate::transport::Response, crate::transport::TransportError> { unreachable!() }
        async fn mutate(&self, _admitted: &crate::lifecycle::Admitted, _: crate::transport::GraphQLRequest) -> Result<crate::transport::Response, crate::transport::TransportError> { unreachable!() }
        async fn subscribe(&self, _admitted: &crate::lifecycle::Admitted, _: crate::transport::GraphQLRequest) -> Result<crate::transport::ServerSubscription, crate::transport::TransportError> {
            Err(crate::transport::TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: "session_unknown".into() })
        }
        async fn put(&self, _admitted: &crate::lifecycle::Admitted, _: crate::transport::PutRequest) -> Result<crate::transport::PutResponse, crate::transport::TransportError> { unreachable!() }
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
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
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
        moments.changes.push(crate::schema::ChangeRead { area: "MOMENT".into(), relation: "by_user".into(), parameter: None, removes_on: Vec::new(), observed: false, payload: None, id_of: None });
        moments.change_feed = Some(change_feed());
        let rig = rig_on(Db::open(&directory.path().join("db.sqlite"), specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
        rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
        rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
        let params = serde_json::json!({"userId": "u1"}).as_object().unwrap().clone();
        rig.transport.enqueue("MomentsByUser", user_list(&["m1"]));
        let reread = if reread_fails { crate::transport::Reply::fail(crate::transport::TransportError::Http { status: 422, body: "rejected".into() }) } else { user_list(&["m1", "m2"]) };
        rig.transport.enqueue("MomentsByUser", reread);
        let _watch = rig.sync.watch("moments", "by_user", &params).unwrap();
        rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "by_user", &params, crate::sync::LoadOptions::default()).await.unwrap();
        let events = super::model_event_tests::open_heard(&rig.sync, "moments").await.unwrap();
        rig.transport.await_calls("Changes", 1).await;
        let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
        let shared = rig.db.shared.clone();
        *rig.db.shared.store.before_write.lock().unwrap() = Some(Box::new(move || {
            shared.store.advance_generation_for_test();
        }));
        rig.transport.emit("MomentChanged", next(json!({"id": "m9", "text": "t", "userId": "u1"})));
        tokio::time::sleep(Duration::from_secs(3)).await;
        let gap = tokio::time::timeout(Duration::from_secs(1), events.next()).await;
        let members = member_ids(&rig, "moments", "by_user", &params);
        assert_eq!(subscription.status().rejected, 1);
        assert_eq!(rig.transport.call_count("MomentsByUser"), 2);
        if reread_fails {
            assert_eq!(members, ["m1"]);
            assert!(matches!(gap, Ok(Some(super::model_event_tests::HeardItem::Lost(1)))));
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
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "by_user", &params, crate::sync::LoadOptions::default()).await.unwrap();
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
    async fn query(&self, _admitted: &crate::lifecycle::Admitted, request: crate::transport::GraphQLRequest) -> Result<Response, crate::transport::TransportError> {
        self.inner.query(_admitted, request).await
    }
    async fn mutate(&self, _admitted: &crate::lifecycle::Admitted, request: crate::transport::GraphQLRequest) -> Result<Response, crate::transport::TransportError> {
        self.inner.mutate(_admitted, request).await
    }
    async fn subscribe(&self, _admitted: &crate::lifecycle::Admitted, request: crate::transport::GraphQLRequest) -> Result<crate::transport::ServerSubscription, crate::transport::TransportError> {
        let refused = self.refusals.try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |left| left.checked_sub(1)).is_ok();
        if refused {
            return Err(crate::transport::TransportError::Network("down".to_string()));
        }
        self.inner.subscribe(_admitted, request).await
    }
    async fn put(&self, _admitted: &crate::lifecycle::Admitted, request: crate::transport::PutRequest) -> Result<crate::transport::PutResponse, crate::transport::TransportError> {
        self.inner.put(_admitted, request).await
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
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "current", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
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
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "by_user", &params, crate::sync::LoadOptions::default()).await.unwrap();
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

#[tokio::test(start_paused = true)]
async fn an_event_that_waits_for_its_debounce_never_commits_in_the_epoch_a_session_start_opened() {
    let rig = rig_with_subscription();
    let debounced = SubscriptionDef::new(EventSpec::new("moment_changed").debounced(Duration::from_secs(1)).merged_by_row(), "MomentChanged", "moments", EventEffect::Upsert);
    let _subscription = rig.sync.subscribe(debounced).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "of the old epoch", "userId": null})));
    rig.transport.await_consumed("MomentChanged").await;
    let life = rig.db.shared.store.lifecycle.clone();
    assert!(life.session_from(Some("another-account".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    rig.transport.await_calls("MomentChanged", 2).await;
    rig.transport.emit("MomentChanged", next(json!({"id": "m2", "text": "of the new epoch", "userId": null})));
    rig.transport.await_consumed("MomentChanged").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(text_of(&rig, "moments", "m2"), Some("of the new epoch".into()));
}

#[tokio::test(start_paused = true)]
async fn an_event_queued_before_a_session_start_never_commits_in_the_epoch_that_start_opened() {
    let rig = rig_with_subscription();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.await_consumed("MomentChanged").await;
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "of the old epoch", "userId": null})));
    let life = rig.db.shared.store.lifecycle.clone();
    assert!(life.session_from(Some("another-account".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    rig.transport.await_calls("MomentChanged", 2).await;
    rig.transport.emit("MomentChanged", next(json!({"id": "m2", "text": "of the new epoch", "userId": null})));
    rig.transport.await_consumed("MomentChanged").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(text_of(&rig, "moments", "m2"), Some("of the new epoch".into()));
}

#[tokio::test(start_paused = true)]
async fn a_delayed_callback_of_the_connection_a_session_start_replaced_changes_nothing() {
    let rig = rig_with_subscription();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.await_consumed("MomentChanged").await;
    let life = rig.db.shared.store.lifecycle.clone();
    assert!(life.session_from(Some("another-account".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    rig.transport.await_calls("MomentChanged", 2).await;
    rig.transport.emit_on("MomentChanged", 0, next(json!({"id": "m1", "text": "of the old connection", "userId": null})));
    rig.transport.emit_on("MomentChanged", 1, next(json!({"id": "m2", "text": "of the new connection", "userId": null})));
    rig.transport.await_consumed("MomentChanged").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!((text_of(&rig, "moments", "m1"), text_of(&rig, "moments", "m2")), (None, Some("of the new connection".into())));
}

#[tokio::test(start_paused = true)]
async fn an_event_enqueued_right_after_a_session_start_is_applied() {
    let rig = rig_with_subscription();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.await_consumed("MomentChanged").await;
    let life = rig.db.shared.store.lifecycle.clone();
    assert!(life.session_from(Some("another-account".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    rig.transport.await_calls("MomentChanged", 2).await;
    rig.transport.emit("MomentChanged", next(json!({"id": "m2", "text": "of the new epoch", "userId": null})));
    rig.transport.await_consumed("MomentChanged").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(text_of(&rig, "moments", "m2"), Some("of the new epoch".into()));
}

#[derive(Clone, Copy, Debug)]
enum Lost {
    Nothing,
    Queued,
    Debounced,
}

#[tokio::test(start_paused = true)]
async fn an_event_lost_at_a_session_start_reloads_the_fresh_window_once_and_nothing_lost_reloads_nothing() {
    for lost in [Lost::Nothing, Lost::Queued, Lost::Debounced] {
        let rig = rig_with_subscription();
        rig.transport.script_subscription("MomentChanged", vec![]);
        for name in ["before", "after"] {
            rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u1", "name":name}})));
        }
        let _watch = rig.sync.watch("users", "current", &serde_json::Map::new()).unwrap();
        rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "current", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
        let spec = match lost {
            Lost::Debounced => EventSpec::new("moment_changed").debounced(Duration::from_secs(60)).merged_by_row(),
            _ => EventSpec::new("moment_changed"),
        };
        let _subscription = rig.sync.subscribe(SubscriptionDef::new(spec, "MomentChanged", "users", EventEffect::Upsert)).await.unwrap();
        rig.transport.await_calls("MomentChanged", 1).await;
        rig.transport.await_consumed("MomentChanged").await;
        assert_eq!(rig.transport.call_count("CurrentUser"), 1);
        if !matches!(lost, Lost::Nothing) {
            rig.transport.emit("MomentChanged", next(json!({"id": "u1", "name": "lost"})));
        }
        if matches!(lost, Lost::Debounced) {
            rig.transport.await_consumed("MomentChanged").await;
        }
        let life = rig.db.shared.store.lifecycle.clone();
        assert!(life.session_from(Some("another-account".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
        tokio::time::sleep(Duration::from_secs(5)).await;
        let user = rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
        let expected = if matches!(lost, Lost::Nothing) { (1, Some("before")) } else { (2, Some("after")) };
        assert_eq!((rig.transport.call_count("CurrentUser"), user.text("name")), expected, "lost: {lost:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_lost_event_range_reads_the_open_window_once() {
    let rig = current_user_rig(None);
    let _watch = rig.sync.watch("users", "current", &serde_json::Map::new()).unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "current", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
    let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "users", EventEffect::Upsert)).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.await_consumed("MomentChanged").await;
    rig.transport.emit("MomentChanged", SubscriptionEvent::Lost { first: 1, last: 300 });
    rig.transport.await_calls("CurrentUser", 2).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!((rig.transport.call_count("CurrentUser"), user_name(&rig)), (2, Some("after".to_string())));
}

fn current_user_rig(directory: Option<&std::path::Path>) -> Rig {
    let rig = match directory {
        Some(directory) => {
            let rig = rig_on(Db::open(&directory.join("db.sqlite"), specs(), None).unwrap(), MockTransport::new(), SyncConfig::default());
            rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
            rig
        }
        None => rig_with_subscription(),
    };
    rig.transport.script_subscription("MomentChanged", vec![]);
    for name in ["before", "after"] {
        rig.transport.enqueue("CurrentUser", crate::transport::Reply::data(json!({"me":{"id":"u1", "name":name}})));
    }
    rig
}

fn user_name(rig: &Rig) -> Option<String> {
    rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().and_then(|row| row.text("name").map(str::to_string))
}

#[tokio::test(start_paused = true)]
async fn an_event_refused_at_its_sql_commit_by_a_session_start_reloads_the_fresh_window_once_in_the_new_epoch() {
    for debounced in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let rig = current_user_rig(Some(directory.path()));
        let _watch = rig.sync.watch("users", "current", &serde_json::Map::new()).unwrap();
        rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "current", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
        let spec = if debounced { EventSpec::new("moment_changed").debounced(Duration::from_secs(1)).merged_by_row() } else { EventSpec::new("moment_changed") };
        let _subscription = rig.sync.subscribe(SubscriptionDef::new(spec, "MomentChanged", "users", EventEffect::Upsert)).await.unwrap();
        rig.transport.await_calls("MomentChanged", 1).await;
        rig.transport.await_consumed("MomentChanged").await;
        let life = rig.db.shared.store.lifecycle.clone();
        let epoch = life.snapshot().session_epoch;
        *rig.db.shared.store.before_write.lock().unwrap() = Some(Box::new(move || { assert!(life.session_from(Some("another-account".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok()); }));
        rig.transport.emit("MomentChanged", next(json!({"id": "u1", "name": "lost"})));
        rig.transport.await_consumed("MomentChanged").await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!((rig.transport.call_count("CurrentUser"), user_name(&rig)), (2, Some("after".into())), "debounced: {debounced}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_lost_event_reloads_an_already_stale_window_once() {
    let rig = current_user_rig(None);
    let _watch = rig.sync.watch("users", "current", &serde_json::Map::new()).unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "users", "current", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
    let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "users", EventEffect::Upsert)).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.await_consumed("MomentChanged").await;
    {
        let admitted = rig.db.admit().unwrap();
        let keys: Vec<String> = rig.db.observed_reads().into_iter().map(|entry| entry.key).collect();
        rig.db.shared.store.invalidate_windows(&admitted.authority(), &keys, rig.sync.inner.next_issued().unwrap()).unwrap();
    }
    rig.transport.emit("MomentChanged", next(json!({"id": "u1", "name": "lost"})));
    let life = rig.db.shared.store.lifecycle.clone();
    assert!(life.session_from(Some("another-account".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!((rig.transport.call_count("CurrentUser"), user_name(&rig)), (2, Some("after".into())));
}

#[tokio::test(start_paused = true)]
async fn a_session_start_drops_the_replaced_connection_without_holding_a_library_lock() {
    let rig = rig_with_subscription();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.await_consumed("MomentChanged").await;
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "queued", "userId": null})));
    let life = rig.db.shared.store.lifecycle.clone();
    assert!(life.session_from(Some("another-account".into()), life.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(life.snapshot().session_epoch)).unwrap()).is_ok());
    rig.transport.await_cancellations("MomentChanged", 1).await;
    rig.transport.await_calls("MomentChanged", 2).await;
    rig.transport.emit("MomentChanged", next(json!({"id": "m2", "text": "of the new epoch", "userId": null})));
    rig.transport.await_consumed("MomentChanged").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(text_of(&rig, "moments", "m2"), Some("of the new epoch".into()));
}

#[tokio::test(start_paused = true)]
async fn a_terminal_failure_does_not_wait_for_the_reload_a_pending_event_starts() {
    let directory = tempfile::tempdir().unwrap();
    let rig = rig_on(Db::open(&directory.path().join("db.sqlite"), specs(), None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    let params = serde_json::json!({"userId": "u1"}).as_object().unwrap().clone();
    rig.transport.enqueue("MomentsByUser", user_list(&["m1"]).after(Duration::from_secs(100)));
    let key = rig.sync.inner.resolve(crate::lifecycle::Origin::Session, "moments", "by_user", &params).unwrap().window_key;
    let _observed = rig.db.register_observed_read(crate::db::ObservedRead { model: "moments".into(), relation: "by_user".into(), params: params.clone(), key });
    let debounced = SubscriptionDef::new(EventSpec::new("moment_changed").debounced(Duration::from_secs(60)).merged_by_row(), "MomentChanged", "moments", EventEffect::Upsert);
    let updates = rig.sync.subscribe(debounced).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.emit("MomentChanged", next(json!({"id": "m9", "text": "t", "userId": "u1"})));
    rig.transport.await_consumed("MomentChanged").await;
    let shared = rig.db.shared.clone();
    *rig.db.shared.store.before_write.lock().unwrap() = Some(Box::new(move || shared.store.advance_generation_for_test()));
    rig.transport.emit("MomentChanged", SubscriptionEvent::Failed(SubscriptionFailure::new(SubscriptionFailureKind::Graphql, "refused")));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let status = updates.status();
    assert!(status.failure.is_some() && status.stopped, "the failure waited for the reload: {status:?}");
    tokio::time::sleep(Duration::from_secs(200)).await;
    assert_eq!(rig.transport.call_count("MomentsByUser"), 1, "the pending event was still applied");
}

#[tokio::test(start_paused = true)]
async fn a_subscription_that_was_opened_but_not_polled_is_opening_for_a_second_reader() {
    let rig = rig_with_subscription();
    let updates = rig.sync.subscribe(upsert_def()).await.unwrap();
    let status = updates.status();
    assert!(status.opening && !status.connected, "{status:?}");
}

#[tokio::test(start_paused = true)]
async fn a_terminal_failure_is_published_as_stopped_before_its_listener_hears_it() {
    let rig = rig_with_subscription();
    let stopped_when_heard = Arc::new(Mutex::new(None));
    let heard = stopped_when_heard.clone();
    *super::locked(&rig.sync.inner.failure_probe) = Some(Box::new(move |stopped| *heard.lock().unwrap() = Some(stopped)));
    let updates = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.await_calls("MomentChanged", 1).await;
    rig.transport.emit("MomentChanged", SubscriptionEvent::Failed(SubscriptionFailure::new(SubscriptionFailureKind::Graphql, "refused")));
    crate::test_wait::yield_until("the failure reached the status", || updates.status().failure.is_some()).await;
    assert_eq!(*stopped_when_heard.lock().unwrap(), Some(true));
    assert!(updates.status().stopped);
}

#[tokio::test(start_paused = true)]
async fn every_terminal_subscription_failure_reaches_the_listener_once() {
    let session = crate::transport::TransportError::Session { code: crate::transport::SessionCode::StateLost, detail: "lost".into() };
    for (case, failure, expected) in [
        ("graphql", SubscriptionFailure::new(SubscriptionFailureKind::Graphql, "refused"), 1),
        ("internal", SubscriptionFailure::new(SubscriptionFailureKind::Internal, "undecodable"), 1),
        ("session", SubscriptionFailure::from_error(session), 1),
        ("network", SubscriptionFailure::new(SubscriptionFailureKind::Transport, "offline"), 0),
    ] {
        let rig = rig_with_subscription();
        let mut failures = rig.sync.inner.subscription_failures.subscribe();
        let updates = rig.sync.subscribe(upsert_def()).await.unwrap();
        rig.transport.await_calls("MomentChanged", 1).await;
        rig.transport.emit("MomentChanged", SubscriptionEvent::Failed(failure));
        crate::test_wait::yield_until("the failure reached the status", || updates.status().failure.is_some()).await;
        let mut heard = 0;
        while let Ok(notice) = failures.try_recv() { heard += usize::from(notice.is_some()); }
        assert_eq!(heard, expected, "{case}");
    }
}
