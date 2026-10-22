use std::time::Duration;

use serde_json::{json, Map};

use super::super::load::LoadOptions;
use super::super::rig::*;
use crate::transport::SubscriptionEvent;
use crate::transport::Response;

const KEY: &str = "moments|MomentEvents";

#[tokio::test(start_paused = true)]
async fn memory_scope_events_follow_readers_and_expire_without_http() {
    use crate::schema::{FieldSpec, ModelSpec, RelationSpec, Storage, UpdatesSpec};
    use crate::value::ScalarKind;
    let mut model = ModelSpec::new("notices", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("body", ScalarKind::Str)]);
    model.storage = Storage::Memory;
    model.expire_after = Some("5.seconds".into());
    let mut relation = RelationSpec::global("recent");
    relation.updates.push(UpdatesSpec { apply: None, subscription:"BulletinAdded".into(), deleted_by:None, set:Map::new(), removes:false, debounce:None, root:None, id:None });
    model.relations.push(relation);
    let rig = rig_on(crate::db::Db::open_in_memory(vec![model], None).unwrap(), crate::transport::MockTransport::new(), super::super::SyncConfig { live_linger:Duration::from_secs(1), ..Default::default() });
    rig.sync.register(super::super::OperationDef::new("BulletinAdded", "subscription BulletinAdded { bulletinAdded { body } }", "bulletinAdded"));
    let emit = || rig.transport.emit("BulletinAdded", SubscriptionEvent::Next(Response::data(json!({"bulletinAdded":{"body":"hello"}}))));
    emit();
    settle().await;
    assert!(rig.db.query(&crate::query::Query::new("notices")).unwrap().is_empty());
    let first = rig.sync.watch("notices", "recent", &none()).unwrap();
    let second = rig.sync.watch("notices", "recent", &none()).unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("BulletinAdded"), 1);
    emit();
    settle().await;
    assert_eq!(rig.db.query(&crate::query::Query::new("notices")).unwrap()[0].text("body"), Some("hello"));
    rig.clock.advance(Duration::from_secs(6));
    rig.db.expire_ephemeral().unwrap();
    assert!(rig.db.query(&crate::query::Query::new("notices")).unwrap().is_empty());
    drop(first);
    settle().await;
    assert_eq!(rig.transport.cancellations("BulletinAdded"), 0);
    drop(second);
    tokio::time::sleep(Duration::from_secs(2)).await;
    emit();
    settle().await;
    assert!(rig.db.query(&crate::query::Query::new("notices")).unwrap().is_empty());
    assert_eq!(rig.transport.cancellations("BulletinAdded"), 1);
    assert!(rig.transport.calls().iter().all(|call| call.kind == crate::transport::CallKind::Subscription));
}

#[tokio::test(start_paused = true)]
async fn changes_declarations_share_one_subscription_until_the_last_reader_leaves() {
    let mut spec = users_spec();
    spec.changes.push(crate::schema::ChangeRead { area: "USER".into(), relation: "current".into(), parameter: None, removes_on: vec![] });
    let mut serialized = serde_json::to_value(spec).unwrap();
    serialized["change_feed"] = json!({"subscription":"RecordEvents", "root":"recordEvents.entries", "area":"topic", "kind":"action", "id":"recordId"});
    let spec = serde_json::from_value(serialized).unwrap();
    let rig = rig_on(crate::db::Db::open_in_memory(vec![spec], None).unwrap(), crate::transport::MockTransport::new(), super::super::SyncConfig { live_linger: Duration::from_secs(1), ..Default::default() });
    rig.sync.register(super::super::OperationDef::new("RecordEvents", "subscription RecordEvents { recordEvents { entries { topic action recordId } } }", "recordEvents"));
    let first = rig.sync.watch("users", "current", &none()).unwrap();
    let second = rig.sync.watch("users", "current", &none()).unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("RecordEvents"), 1);
    drop(first);
    settle().await;
    assert_eq!(rig.transport.cancellations("RecordEvents"), 0);
    drop(second);
    settle().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    settle().await;
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
            rig.sync.listen();
            None
        };
        if stage != "starting" { settle().await; }
        if stage == "lingering" { rig.sync.unlisten(); }
        let survivor = (stage == "clone").then(|| rig.sync.clone());
        let inner = std::sync::Arc::downgrade(&rig.sync.inner);
        drop(rig.sync);
        settle().await;
        if survivor.is_some() { assert!(inner.upgrade().is_some()); }
        drop(survivor);
        settle().await;
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
async fn first_reader_starts_the_subscription_and_the_second_does_not_duplicate_it() {
    let rig = rig();
    let first = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    let second = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
    assert_eq!(rig.sync.inner.live_state(KEY), Some((2, true)));
    drop((first, second));
}

#[tokio::test(start_paused = true)]
async fn watching_a_relation_without_updates_starts_nothing() {
    let rig = rig();
    let _watch = rig.sync.watch("moments", "feed", &none()).unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 0);
    assert_eq!(rig.sync.inner.live_state(KEY), None);
}

#[tokio::test(start_paused = true)]
async fn subscription_closes_after_the_last_reader_leaves_and_the_linger_passes() {
    let rig = rig();
    let watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    drop(watch);
    tokio::time::sleep(Duration::from_secs(29)).await;
    assert_eq!(rig.sync.inner.live_state(KEY), Some((0, true)));
    assert_eq!(rig.transport.cancellations("MomentEvents"), 0);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(rig.sync.inner.live_state(KEY), None);
    assert_eq!(rig.transport.cancellations("MomentEvents"), 1);
}

#[tokio::test(start_paused = true)]
async fn reader_returning_within_the_linger_keeps_the_same_subscription() {
    let rig = rig();
    drop(rig.sync.watch("moments", "pulse", &none()).unwrap());
    settle().await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let _back = rig.sync.watch("moments", "pulse", &none()).unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(rig.sync.inner.live_state(KEY), Some((1, true)));
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
}

#[tokio::test(start_paused = true)]
async fn dropped_connection_rereads_the_expired_watched_relation_after_it_reconnects() {
    let rig = rig();
    rig.transport.enqueue("MomentsPulse", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsPulse", feed_reply(&["m1", "m2"], false, "c2"));
    let _watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    rig.sync.load("moments", "pulse", &none(), LoadOptions::default()).await.unwrap();
    settle().await;
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
    settle().await;
    rig.transport.emit("MomentEvents", event(json!({"id": "m1", "text": "edited", "userId": null, "destroyed": false})));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("edited".to_string()));
    rig.transport.emit("MomentEvents", event(json!({"id": "m1", "text": "edited", "userId": null, "destroyed": true})));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn a_relation_with_two_subscriptions_starts_both_and_a_removing_one_deletes_the_row() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    let watch = rig.sync.watch("moments", "pulse", &none()).unwrap();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentEvents"), 1);
    assert_eq!(rig.transport.call_count("MomentRemoved"), 1);
    rig.transport.emit("MomentRemoved", SubscriptionEvent::Next(Response::data(json!({"momentRemoved": {"id": "m1"}}))));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    drop(watch);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("MomentRemoved"), 1);
}

#[tokio::test(start_paused = true)]
async fn listen_opens_the_model_subscriptions_without_a_reader_and_unlisten_closes_them() {
    let rig = rig();
    rig.sync.listen();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentListen"), 1);
    rig.transport.emit("MomentListen", SubscriptionEvent::Next(Response::data(json!({"momentListen": {"id": "m9", "text": "pushed", "userId": null}}))));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m9"), Some("pushed".to_string()));
    rig.sync.unlisten();
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("MomentListen"), 1);
}

#[tokio::test(start_paused = true)]
async fn event_root_and_id_paths_find_the_row_in_a_wrapped_payload() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "t"));
    rig.sync.listen();
    settle().await;
    rig.transport.emit("MomentWrapped", SubscriptionEvent::Next(Response::data(json!({"momentWrapped": {"wrapped": {"key": "m1", "text": "from wrapper"}}}))));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("from wrapper".to_string()));
}

#[tokio::test(start_paused = true)]
async fn a_debounced_listen_subscribes_and_lands_its_frame_after_the_interval() {
    let rig = rig();
    rig.sync.listen();
    settle().await;
    assert_eq!(rig.transport.call_count("MomentDebounced"), 1);
    rig.transport.emit("MomentDebounced", SubscriptionEvent::Next(Response::data(json!({"momentDebounced": {"id": "m7", "text": "later", "userId": null}}))));
    tokio::time::sleep(Duration::from_millis(400)).await;
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m7"), Some("later".to_string()));
}

fn vibe_sub_variables(rig: &Rig) -> Vec<serde_json::Value> {
    rig.transport.calls().into_iter().filter(|call| call.operation_name == "VibeEvents").map(|call| call.variables).collect()
}

#[tokio::test(start_paused = true)]
async fn readers_with_different_params_open_separate_subscriptions_with_those_variables() {
    let rig = rig();
    let a = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "a"}))).unwrap();
    let b = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "b"}))).unwrap();
    let a_again = rig.sync.watch("moments", "vibe_feed", &params(json!({"vibeId": "a"}))).unwrap();
    settle().await;
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
    settle().await;
    drop(a);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("VibeEvents"), 0);
    drop(a_again);
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(rig.transport.cancellations("VibeEvents"), 1);
    rig.transport.emit("VibeEvents", SubscriptionEvent::Next(Response::data(json!({"momentEvent": moment("m9", "from b")}))));
    settle().await;
    assert_eq!(rig.db.query(&crate::query::Query::new("moments")).unwrap().iter().filter(|row| row.text("text") == Some("from b")).count(), 1);
    drop(b);
}
