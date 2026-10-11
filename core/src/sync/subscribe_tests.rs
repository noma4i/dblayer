use super::error::SyncError;
use super::subscribe::{EventEffect, SubscriptionDef};
use super::rig::*;
use crate::db::Db;
use crate::pacing::ManualClock;
use crate::query::Query;
use crate::schema::{AssociationKind, AssociationSpec, Dependent, FieldSpec};
use crate::value::ScalarKind;
use crate::sync::{Sync, SyncConfig};
use crate::transport::MockTransport;
use crate::transport::{EventSpec, Response, SubscriptionEvent, SubscriptionFailure, SubscriptionFailureKind};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

fn failed(kind: SubscriptionFailureKind) -> SubscriptionEvent {
    SubscriptionEvent::Failed(SubscriptionFailure::new(kind, "boom"))
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
    rig.db.settle();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "after"})));
    settle().await;
    rig.db.settle();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("after".to_string()));
    assert_eq!(*seen.lock().unwrap(), ["before", "after"]);
    assert_eq!(subscription.status().applied, 1);
}

#[tokio::test(start_paused = true)]
async fn upsert_event_lands_nested_author() {
    let rig = rig_with_subscription();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m9", "text": "t", "author": {"id": "u7", "name": "Zed"}})));
    settle().await;
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
    settle().await;
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
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn dropping_the_subscription_cancels_the_transport() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    settle().await;
    drop(subscription);
    settle().await;
    assert_eq!(rig.transport.cancellations("MomentChanged"), 1);
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "late"})));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn dropped_connection_is_reopened_after_a_delay() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.disconnect("MomentChanged");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(rig.transport.call_count("MomentChanged"), 2);
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "again"})));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("again".to_string()));
    assert_eq!(subscription.status().connects, 2);
}

#[tokio::test(start_paused = true)]
async fn offline_subscription_waits_without_spending_attempts_and_reconnects_online() {
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
    settle().await;
    assert_eq!(rig.transport.call_count("MomentChanged"), 2);
    assert_eq!(subscription.status().connects, 2);
}

#[tokio::test(start_paused = true)]
async fn repeated_transport_failures_report_degraded_once_then_ready_recovers() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    for _ in 0..4 {
        rig.transport.emit("MomentChanged", failed(SubscriptionFailureKind::Transport));
        tokio::time::sleep(Duration::from_secs(40)).await;
    }
    assert_eq!(rig.transport.call_count("MomentChanged"), 5);
    assert!(subscription.status().degraded);
    assert!(!subscription.status().stopped);
    rig.transport.emit("MomentChanged", SubscriptionEvent::Ready);
    settle().await;
    assert!(!subscription.status().degraded);
}

#[tokio::test(start_paused = true)]
async fn graphql_failure_stops_the_subscription() {
    let rig = rig_with_subscription();
    let subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", failed(SubscriptionFailureKind::Graphql));
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(subscription.status().stopped);
    assert!(subscription.status().degraded);
    assert_eq!(rig.transport.call_count("MomentChanged"), 1);
}

#[tokio::test(start_paused = true)]
async fn debounced_events_land_once_per_row_after_the_interval() {
    let rig = rig_with_subscription();
    put(&rig, "moments", json!({"id": "m1", "text": "start", "userId": null}));
    let _observer = rig.db.observe(Query::new("moments"), |_| {}).unwrap();
    rig.db.settle();
    let event = EventSpec::new("moment_changed").debounced(Duration::from_millis(500)).merged_by_row();
    let def = SubscriptionDef::new(event, "MomentChanged", "moments", EventEffect::Upsert);
    let _subscription = rig.sync.subscribe(def).await.unwrap();
    for text in ["one", "two", "three"] {
        rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": text})));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(text_of(&rig, "moments", "m1"), Some("start".to_string()));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(text_of(&rig, "moments", "m1"), Some("three".to_string()));
    rig.db.settle();
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
    settle().await;
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
    settle().await;
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    let user = rig.db.find("users", &crate::identity::RowId::new("u1").unwrap()).unwrap().unwrap();
    assert_eq!(user.get("typing"), Some(&json!(false)));
}

#[tokio::test(start_paused = true)]
async fn upsert_event_keeps_the_fields_it_does_not_select() {
    let rig = rig_with_subscription();
    put(&rig, "moments", json!({"id": "m1", "text": "keep", "userId": "u1"}));
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "likedByMe": true})));
    settle().await;
    let row = rig.db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap().unwrap();
    assert_eq!((row.text("text"), row.text("userId"), row.get("likedByMe")), (Some("keep"), Some("u1"), Some(&json!(true))));
}

#[tokio::test(start_paused = true)]
async fn destroy_event_removes_the_dependent_children() {
    let mut users = users_spec();
    let mut moments = AssociationSpec::new("moments", AssociationKind::HasMany, "moments", "userId");
    moments.dependent = Some(Dependent::Destroy);
    users.associations.push(moments);
    let rig = rig_on(Db::open_in_memory(vec![users, moments_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    put(&rig, "moments", json!({"id": "m1", "text": "child", "userId": "u1"}));
    let def = SubscriptionDef::new(EventSpec::new("user_gone"), "MomentChanged", "users", EventEffect::Destroy);
    let _subscription = rig.sync.subscribe(def).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "u1"})));
    settle().await;
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn event_that_creates_a_row_refreshes_the_watched_lists_of_its_model() {
    let rig = rig_with_subscription();
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1"], false, "c"));
    rig.transport.enqueue("MomentsFeed", feed_reply(&["m1", "m2"], false, "c2"));
    let _watch = rig.sync.watch("moments", "feed", &serde_json::Map::new()).unwrap();
    rig.sync.load("moments", "feed", &serde_json::Map::new(), crate::sync::LoadOptions::default()).await.unwrap();
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m2", "text": "new", "userId": null})));
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(member_ids(&rig, "moments", "feed", &serde_json::Map::new()), ["m1", "m2"]);
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
async fn a_refused_first_connection_is_retried_like_a_reconnect() {
    let inner = MockTransport::new();
    let transport = Arc::new(RefusingFirstSubscribe { inner: inner.clone(), refusals: 1.into() });
    let db = Db::open_in_memory(specs(), None).unwrap();
    let sync = Sync::new(db.clone(), transport, Arc::new(ManualClock::new(Duration::from_millis(START_MS))), SyncConfig::default());
    register_operations(&sync);
    sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    let subscription = sync.subscribe(upsert_def()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(subscription.status().connects, 1);
    inner.emit("MomentChanged", next(json!({"id": "m1", "text": "late", "userId": null})));
    settle().await;
    let row = db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap();
    assert_eq!(row.and_then(|row| row.text("text").map(str::to_string)), Some("late".to_string()));
}

#[tokio::test(start_paused = true)]
async fn upsert_event_touches_the_parent_that_asks_for_it() {
    let mut moments = moments_spec();
    moments.associations.iter_mut().for_each(|association| association.touches_parent = true);
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), moments], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    let user = crate::identity::RowId::new("u1").unwrap();
    let before = rig.db.find("users", &user).unwrap().unwrap().revision;
    let _subscription = rig.sync.subscribe(upsert_def()).await.unwrap();
    rig.transport.emit("MomentChanged", next(json!({"id": "m1", "text": "t", "userId": "u1"})));
    settle().await;
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
