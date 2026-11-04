use std::time::Duration;

use serde_json::{json, Map};

use super::super::load::LoadOptions;
use super::super::rig::*;
use crate::transport::SubscriptionEvent;
use crate::transport::Response;

const KEY: &str = "MomentEvents|{}";

fn shared_rig(models: Vec<crate::schema::ModelSpec>) -> Rig {
    let rig = rig_on(crate::db::Db::open_in_memory(models, None).unwrap(), crate::transport::MockTransport::new(), super::super::SyncConfig::default());
    rig.sync.register(super::super::OperationDef::new("Signal", "subscription Signal { signal { id } }", "signal"));
    rig
}

#[tokio::test(start_paused = true)]
async fn shared_receivers_keep_their_debounce_in_either_open_order() {
    for reverse in [false, true] {
        let mut users = users_spec();
        users.relations[0].updates.push(serde_json::from_value(json!({"subscription":"Signal", "root":"first", "debounce":"100.milliseconds"})).unwrap());
        let mut moments = moments_spec();
        moments.relations.iter_mut().find(|relation| relation.name == "recent").unwrap().updates.push(serde_json::from_value(json!({"subscription":"Signal", "root":"second", "debounce":"300.milliseconds"})).unwrap());
        let rig = shared_rig(vec![users, moments]);
        let mut order = [("users", "current"), ("moments", "recent")];
        if reverse { order.reverse(); }
        let mut watches = Vec::new();
        for (model, relation) in order {
            watches.push(rig.sync.watch(model, relation, &none()).unwrap());
            rig.transport.await_calls("Signal", 1).await;
        }
        for value in ["one", "two"] {
            rig.transport.emit("Signal", SubscriptionEvent::Next(Response::data(json!({"first":{"id":"u", "name":value}, "second":{"id":"m", "text":value, "userId":null}}))));
            rig.transport.await_consumed("Signal").await;
        }
        assert_eq!(text_of(&rig, "moments", "m"), None);
        assert!(rig.db.find("users", &crate::identity::RowId::new("u").unwrap()).unwrap().is_none());
        let waits = rig.transport.waits("Signal");
        rig.clock.advance(Duration::from_millis(100));
        rig.transport.await_waits("Signal", waits + 1).await;
        assert_eq!(rig.db.find("users", &crate::identity::RowId::new("u").unwrap()).unwrap().unwrap().text("name"), Some("two"));
        assert_eq!(text_of(&rig, "moments", "m"), None);
        let waits = rig.transport.waits("Signal");
        rig.clock.advance(Duration::from_millis(200));
        rig.transport.await_waits("Signal", waits + 1).await;
        assert_eq!(text_of(&rig, "moments", "m"), Some("two".into()));
        assert_eq!(rig.transport.call_count("Signal"), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn debounced_definitions_of_one_model_apply_both_field_sets() {
    let mut users = users_spec();
    users.fields.push(crate::schema::FieldSpec::scalar("label", crate::value::ScalarKind::Str).optional());
    for root in ["first", "second"] {
        users.relations[0].updates.push(serde_json::from_value(json!({"subscription":"Signal", "root":root, "debounce":"100.milliseconds"})).unwrap());
    }
    let rig = shared_rig(vec![users]);
    let _watch = rig.sync.watch("users", "current", &none()).unwrap();
    rig.transport.await_calls("Signal", 1).await;
    rig.transport.emit("Signal", SubscriptionEvent::Next(Response::data(json!({"first":{"id":"u", "name":"Ann"}, "second":{"id":"u", "label":"friend"}}))));
    rig.transport.await_consumed("Signal").await;
    let waits = rig.transport.waits("Signal");
    rig.clock.advance(Duration::from_millis(100));
    rig.transport.await_waits("Signal", waits + 1).await;
    let row = rig.db.find("users", &crate::identity::RowId::new("u").unwrap()).unwrap().unwrap();
    assert_eq!((row.text("name"), row.text("label")), (Some("Ann"), Some("friend")));
}

#[tokio::test(start_paused = true)]
async fn shared_feed_applies_only_active_definitions_and_reports_failure_to_each_receiver() {
    let mut users = users_spec();
    for (relation, root) in [("incoming_a", "first"), ("incoming_b", "second")] {
        let mut scope = crate::schema::RelationSpec::global(relation);
        scope.updates.push(serde_json::from_value(json!({"subscription":"Signal", "root":root})).unwrap());
        users.relations.push(scope);
    }
    let rig = shared_rig(vec![users]);
    let mut first = rig.sync.watch("users", "incoming_a", &none()).unwrap();
    rig.transport.await_calls("Signal", 1).await;
    let emit = |name| rig.transport.emit("Signal", SubscriptionEvent::Next(Response::data(json!({"first":{"id":"a", "name":name}, "second":{"id":"b", "name":name}}))));
    emit("one");
    rig.transport.await_consumed("Signal").await;
    let row = |id| rig.db.find("users", &crate::identity::RowId::new(id).unwrap()).unwrap();
    assert_eq!(row("a").unwrap().text("name"), Some("one"));
    assert!(row("b").is_none());
    let mut second = rig.sync.watch("users", "incoming_b", &none()).unwrap();
    emit("two");
    rig.transport.await_consumed("Signal").await;
    assert_eq!(row("b").unwrap().text("name"), Some("two"));
    let mut failures = [first.failures().unwrap(), second.failures().unwrap()];
    let refusal = crate::transport::SubscriptionFailure::from_error(crate::transport::TransportError::Session { code: crate::transport::SessionCode::Unauthenticated, detail: "refused".into() });
    rig.transport.emit("Signal", SubscriptionEvent::Failed(refusal.clone()));
    rig.transport.await_cancellations("Signal", 1).await;
    for failure in &mut failures {
        let (source, seen) = tokio::time::timeout(Duration::from_secs(1), failure.receiver.recv()).await.unwrap().unwrap().unwrap();
        assert!(failure.sources.contains(&source));
        assert_eq!(seen, refusal);
        assert_eq!((failure.recheck)(), Some(refusal.clone()));
    }
    rig.sync.resume(&rig.db.admit().unwrap()).await.unwrap();
    rig.transport.await_calls("Signal", 2).await;
    drop(first);
    emit("three");
    rig.transport.await_consumed("Signal").await;
    assert_eq!(row("a").unwrap().text("name"), Some("two"));
    assert_eq!(row("b").unwrap().text("name"), Some("three"));
    assert_eq!(rig.transport.call_count("Signal"), 2);
}

#[tokio::test(start_paused = true)]
async fn memory_scope_events_follow_readers_and_expire_without_http() {
    use crate::schema::{FieldSpec, ModelSpec, RelationSpec, Storage, UpdatesSpec};
    use crate::value::ScalarKind;
    let mut model = ModelSpec::new("notices", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str)]);
    model.storage = Storage::Memory;
    model.expire_after = Some("5.seconds".into());
    let mut relation = RelationSpec::global("recent");
    relation.updates.push(UpdatesSpec { apply: None, subscription:"BulletinAdded".into(), deleted_by:None, deleted_unless:None, set:Map::new(), removes:false, debounce:None, root:None, id:Vec::new() });
    model.relations.push(relation);
    let rig = rig_on(crate::db::Db::open_in_memory(vec![model], None).unwrap(), crate::transport::MockTransport::new(), super::super::SyncConfig { live_linger:Duration::from_secs(1), ..Default::default() });
    rig.sync.register(super::super::OperationDef::new("BulletinAdded", "subscription BulletinAdded { bulletinAdded { body } }", "bulletinAdded"));
    let emit = || rig.transport.emit("BulletinAdded", SubscriptionEvent::Next(Response::data(json!({"bulletinAdded":{"body":"hello"}}))));
    emit();
    assert!(rig.db.query(&crate::query::Query::new("notices")).unwrap().is_empty());
    let first = rig.sync.watch("notices", "recent", &none()).unwrap();
    let second = rig.sync.watch("notices", "recent", &none()).unwrap();
    rig.transport.await_calls("BulletinAdded", 1).await;
    assert_eq!(rig.transport.call_count("BulletinAdded"), 1);
    emit();
    rig.transport.await_consumed("BulletinAdded").await;
    assert_eq!(rig.db.query(&crate::query::Query::new("notices")).unwrap()[0].text("body"), Some("hello"));
    rig.clock.advance(Duration::from_secs(6));
    rig.db.expire_ephemeral(&rig.db.admit().unwrap()).unwrap();
    assert!(rig.db.query(&crate::query::Query::new("notices")).unwrap().is_empty());
    drop(first);
    assert_eq!(rig.transport.cancellations("BulletinAdded"), 0);
    drop(second);
    tokio::time::sleep(Duration::from_secs(2)).await;
    rig.transport.await_cancellations("BulletinAdded", 1).await;
    emit();
    assert!(rig.db.query(&crate::query::Query::new("notices")).unwrap().is_empty());
    assert_eq!(rig.transport.cancellations("BulletinAdded"), 1);
    assert!(rig.transport.calls().iter().all(|call| call.kind == crate::transport::CallKind::Subscription));
}

#[tokio::test(start_paused = true)]
async fn changes_declarations_share_one_subscription_until_the_last_reader_leaves() {
    let mut spec = users_spec();
    spec.changes.push(crate::schema::ChangeRead { area: "USER".into(), relation: "current".into(), parameter: None, removes_on: vec![], observed: false, payload: None, id_of: None });
    let mut serialized = serde_json::to_value(spec).unwrap();
    serialized["change_feed"] = json!({"subscription":"RecordEvents", "root":"recordEvents.entries", "area":"topic", "kind":"action", "id":"recordId"});
    let spec = serde_json::from_value(serialized).unwrap();
    let rig = rig_on(crate::db::Db::open_in_memory(vec![spec], None).unwrap(), crate::transport::MockTransport::new(), super::super::SyncConfig { live_linger: Duration::from_secs(1), ..Default::default() });
    rig.sync.register(super::super::OperationDef::new("RecordEvents", "subscription RecordEvents { recordEvents { entries { topic action recordId } } }", "recordEvents"));
    let first = rig.sync.watch("users", "current", &none()).unwrap();
    let second = rig.sync.watch("users", "current", &none()).unwrap();
    rig.transport.await_calls("RecordEvents", 1).await;
    assert_eq!(rig.transport.call_count("RecordEvents"), 1);
    drop(first);
    assert_eq!(rig.transport.cancellations("RecordEvents"), 0);
    drop(second);
    tokio::time::advance(Duration::from_secs(10)).await;
    rig.transport.await_cancellations("RecordEvents", 1).await;
    assert_eq!(rig.transport.cancellations("RecordEvents"), 1);
}

#[tokio::test(start_paused = true)]
async fn dropping_sync_stops_subscriptions_at_every_lifetime_stage() {
    let mut results = Vec::new();
    for stage in ["starting", "active", "lingering", "clone", "direct"] {
        let rig = rig();
        let _direct = if stage == "direct" {
            Some(rig.sync.subscribe(super::SubscriptionDef::new(crate::transport::EventSpec::new("listen"), "MomentListen", "moments", super::EventEffect::Upsert)).await.unwrap())
        } else {
            rig.sync.inner.listen_all();
            None
        };
        if stage != "starting" { rig.transport.await_calls("MomentListen", 1).await; }
        if stage == "lingering" { rig.sync.inner.unlisten_all(); }
        let survivor = (stage == "clone").then(|| rig.sync.clone());
        let inner = std::sync::Arc::downgrade(&rig.sync.inner);
        drop(rig.sync);
        if survivor.is_some() { assert!(inner.upgrade().is_some()); }
        drop(survivor);
        await_tasks_ended().await;
        results.push((stage, inner.upgrade().is_none(), rig.transport.call_count("MomentListen"), rig.transport.cancellations("MomentListen")));
    }
    assert!(results.iter().all(|(_, released, opened, closed)| *released && opened == closed), "{results:?}");
}

fn none() -> Map<String, serde_json::Value> {
    Map::new()
}

fn event(node: serde_json::Value) -> SubscriptionEvent {
    SubscriptionEvent::Next(Response::data(json!({"momentEvent": node})))
}

#[tokio::test(start_paused = true)]
async fn inv_live_01_first_reader_starts_the_subscription_and_the_second_does_not_duplicate_it() {
    let rig = rig();
    let first = rig.sync.watch("moments", "pulse", &none()).unwrap();
    rig.transport.await_calls("MomentEvents", 1).await;
    let second = rig.sync.watch("moments", "pulse", &none()).unwrap();
    rig.transport.emit("MomentEvents", SubscriptionEvent::Ready);
    rig.transport.await_consumed("MomentEvents").await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
    assert_eq!(rig.sync.inner.live_status(KEY), Some((2, true)));
    drop((first, second));
}

#[tokio::test(start_paused = true)]
async fn watching_a_relation_without_updates_starts_nothing() {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    assert_eq!(rig.transport.call_count("MomentEvents"), 0);
    assert_eq!(rig.sync.inner.live_status(KEY), None);
}

#[tokio::test(start_paused = true)]
async fn inv_live_01_subscription_closes_after_the_last_reader_leaves_and_the_linger_passes() {
    let rig = rig();
    let watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    rig.transport.await_calls("MomentEvents", 1).await;
    drop(watch);
    tokio::time::sleep(Duration::from_secs(29)).await;
    assert_eq!(rig.sync.inner.live_status(KEY), Some((0, true)));
    assert_eq!(rig.transport.cancellations("MomentEvents"), 0);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(rig.sync.inner.live_status(KEY), None);
    assert_eq!(rig.transport.cancellations("MomentEvents"), 1);
}

#[tokio::test(start_paused = true)]
async fn inv_live_01_reader_returning_within_the_linger_keeps_the_same_subscription() {
    let rig = rig();
    drop(rig.sync.watch("moments", "pulse", &none()).unwrap());
    tokio::time::sleep(Duration::from_secs(10)).await;
    let _back = rig.sync.watch("moments", "pulse", &none()).unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(rig.sync.inner.live_status(KEY), Some((1, true)));
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
}

#[tokio::test(start_paused = true)]
async fn inv_live_03_dropped_connection_rereads_the_expired_watched_relation_after_it_reconnects() {
    let rig = rig();
    rig.transport.enqueue("MomentsPulse", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsPulse", feed_reply(&["m1", "m2"], false, "c2"));
    let _watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "pulse", &none(), LoadOptions::default()).await.unwrap();
    rig.transport.await_calls("MomentEvents", 1).await;
    assert_eq!(rig.transport.call_count("MomentsPulse"), 1);
    rig.transport.disconnect("MomentEvents");
    rig.clock.advance(Duration::from_secs(300));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 2);
    assert_eq!(rig.transport.call_count("MomentsPulse"), 2);
    assert_eq!(member_ids(&rig, "moments", "pulse", &none()), ["m1", "m2"]);
}

#[tokio::test(start_paused = true)]
async fn event_with_the_deleted_by_field_set_removes_the_row() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let _watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    rig.transport.await_calls("MomentEvents", 1).await;
    rig.transport.emit("MomentEvents", event(json!({"id": "m1", "text": "edited", "userId": null, "destroyed": false})));
    rig.transport.await_consumed("MomentEvents").await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("edited".to_string()));
    rig.transport.emit("MomentEvents", event(json!({"id": "m1", "text": "edited", "userId": null, "destroyed": true})));
    rig.transport.await_consumed("MomentEvents").await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn a_relation_with_two_subscriptions_starts_both_and_a_removing_one_deletes_the_row() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    rig.transport.await_calls("MomentEvents", 1).await;
    rig.transport.await_calls("MomentRemoved", 1).await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
    assert_eq!(rig.transport.call_count("MomentRemoved"), 1);
    rig.transport.emit("MomentRemoved", SubscriptionEvent::Next(Response::data(json!({"momentRemoved": {"id": "m1"}}))));
    rig.transport.await_consumed("MomentRemoved").await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    drop(watch);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("MomentRemoved"), 1);
}

#[tokio::test(start_paused = true)]
async fn listen_opens_the_model_subscriptions_without_a_reader_and_unlisten_closes_them() {
    let rig = rig();
    rig.sync.inner.listen_all();
    rig.transport.await_calls("MomentListen", 1).await;
    assert_eq!(rig.transport.call_count("MomentListen"), 1);
    rig.transport.emit("MomentListen", SubscriptionEvent::Next(Response::data(json!({"momentListen": {"id": "m9", "text": "pushed", "userId": null}}))));
    rig.transport.await_consumed("MomentListen").await;
    assert_eq!(text_of(&rig, "moments", "m9"), Some("pushed".to_string()));
    rig.sync.inner.unlisten_all();
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("MomentListen"), 1);
}

#[tokio::test(start_paused = true)]
async fn event_root_and_id_paths_find_the_row_in_a_wrapped_payload() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.sync.inner.listen_all();
    rig.transport.await_calls("MomentWrapped", 1).await;
    rig.transport.emit("MomentWrapped", SubscriptionEvent::Next(Response::data(json!({"momentWrapped": {"wrapped": {"key": "m1", "text": "from wrapper"}}}))));
    rig.transport.await_consumed("MomentWrapped").await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("from wrapper".to_string()));
}

#[tokio::test(start_paused = true)]
async fn a_debounced_listen_subscribes_and_lands_its_frame_after_the_interval() {
    let rig = rig();
    rig.sync.inner.listen_all();
    rig.transport.await_calls("MomentDebounced", 1).await;
    assert_eq!(rig.transport.call_count("MomentDebounced"), 1);
    rig.transport.emit("MomentDebounced", SubscriptionEvent::Next(Response::data(json!({"momentDebounced": {"id": "m7", "text": "later", "userId": null}}))));
    tokio::time::sleep(Duration::from_millis(400)).await;
    rig.transport.await_consumed("MomentDebounced").await;
    assert_eq!(text_of(&rig, "moments", "m7"), Some("later".to_string()));
}

fn vibe_sub_variables(rig: &Rig) -> Vec<serde_json::Value> {
    rig.transport.calls().into_iter().filter(|call| call.operation_name == "VibeEvents").map(|call| call.variables).collect()
}

#[tokio::test]
async fn subscriptions_reopen_and_deliver_after_a_data_reset() {
    for (next, keep_old) in [("a", false), ("b", false), ("a", true)] {
        let rig = rig();
        let mut old = Some(rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "a"}))).unwrap());
        crate::test_wait::within_secs(5, rig.transport.await_calls("VibeEvents", 1)).await;
        if !keep_old { drop(old.take()); }
        rig.db.reset_all().unwrap();
        let _new = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": next}))).unwrap();
        crate::test_wait::within_secs(5, rig.transport.await_calls("VibeEvents", if next == "a" { 2 } else { 3 })).await;
        assert!(vibe_sub_variables(&rig).contains(&json!({"vibeId": next})), "{next}, {keep_old}");
        rig.transport.emit("VibeEvents", SubscriptionEvent::Next(Response::data(json!({"momentEvent": moment("m9", "after reset")}))));
        crate::test_wait::within_secs(5, rig.transport.await_consumed("VibeEvents")).await;
        assert_eq!(text_of(&rig, "moments", "m9"), Some("after reset".to_string()), "{next}, {keep_old}");
    }
}

#[tokio::test(start_paused = true)]
async fn readers_with_different_params_open_separate_subscriptions_with_those_variables() {
    let rig = rig();
    let a = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "a"}))).unwrap();
    let b = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "b"}))).unwrap();
    let a_again = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "a"}))).unwrap();
    rig.transport.await_calls("VibeEvents", 2).await;
    let mut variables = vibe_sub_variables(&rig);
    variables.sort_by_key(|value| value.to_string());
    assert_eq!(variables, [json!({"vibeId": "a"}), json!({"vibeId": "b"})]);
    drop((a, b, a_again));
}

#[tokio::test(start_paused = true)]
async fn last_reader_of_a_param_set_closes_only_its_subscription_and_the_event_lands() {
    let rig = rig();
    let a = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "a"}))).unwrap();
    let a_again = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "a"}))).unwrap();
    let b = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "b"}))).unwrap();
    rig.transport.await_calls("VibeEvents", 2).await;
    drop(a);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("VibeEvents"), 0);
    drop(a_again);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("VibeEvents"), 1);
    rig.transport.emit("VibeEvents", SubscriptionEvent::Next(Response::data(json!({"momentEvent": moment("m9", "from b")}))));
    rig.transport.await_consumed("VibeEvents").await;
    assert_eq!(rig.db.query(&crate::query::Query::new("moments")).unwrap().iter().filter(|row| row.text("text") == Some("from b")).count(), 1);
    drop(b);
}

#[tokio::test(start_paused = true)]
async fn a_committed_credential_starts_the_model_subscriptions_and_the_end_of_the_session_stops_them() {
    let rig = rig();
    rig.transport.set_session(None);
    rig.sync.commit_session_for_test();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.transport.call_count("MomentListen"), 0, "no credential");
    rig.transport.set_session(Some("first"));
    rig.sync.commit_session_for_test();
    rig.transport.await_calls("MomentListen", 1).await;
    rig.sync.end_session_for_test();
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("MomentListen"), 1, "logout");
    rig.transport.set_session(Some("second"));
    rig.sync.commit_session_for_test();
    rig.transport.await_calls("MomentListen", 2).await;
    assert_eq!(rig.transport.call_count("MomentListen"), 2, "next sign-in");
}

#[tokio::test(start_paused = true)]
async fn a_denied_listening_keeps_a_committed_credential_from_opening_subscriptions_until_allowed() {
    let rig = rig();
    rig.sync.set_listening(false);
    rig.transport.set_session(Some("stored"));
    rig.sync.commit_session_for_test();
    rig.transport.set_session(Some("signed-in"));
    rig.sync.commit_session_for_test();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.transport.call_count("MomentListen"), 0, "stored credential and sign-in while denied");
    rig.sync.set_listening(true);
    rig.sync.set_listening(true);
    rig.transport.await_calls("MomentListen", 1).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.transport.call_count("MomentListen"), 1, "allowed twice starts them once");
    rig.sync.set_listening(false);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("MomentListen"), 1, "denied again");
    rig.sync.set_listening(true);
    rig.transport.await_calls("MomentListen", 2).await;
}

#[tokio::test(start_paused = true)]
async fn allowing_listening_without_a_credential_opens_nothing_and_logout_stops_the_allowed_listen() {
    let rig = rig();
    rig.transport.set_session(None);
    rig.sync.commit_session_for_test();
    rig.sync.set_listening(false);
    rig.sync.set_listening(true);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.transport.call_count("MomentListen"), 0, "no credential");
    rig.transport.set_session(Some("first"));
    rig.sync.commit_session_for_test();
    rig.transport.await_calls("MomentListen", 1).await;
    rig.sync.end_session_for_test();
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("MomentListen"), 1, "logout");
    rig.sync.set_listening(true);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.transport.call_count("MomentListen"), 1, "allowed after logout needs a new credential");
}
