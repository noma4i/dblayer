use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};

use super::node_tests::{node_rig, stored};
use super::rig::*;
use super::subscribe::{EventEffect, SubscriptionDef, Updates};
use super::model_event_tests::HeardItem;
use super::SyncConfig;
use crate::db::Db;
use crate::schema::{Coverage, FieldSpec, Membership, RelationSpec, RemoteShape};
use crate::transport::{EventSpec, MockTransport, Reply, Response, SubscriptionEvent};
use crate::value::ScalarKind;

async fn payload_rig(chat: bool, message: bool) -> (Rig, Updates) {
    let original = node_rig();
    let mut specs: Vec<_> = original.db.shared.specs.values().cloned().collect();
    let chats = specs.iter_mut().find(|spec| spec.key.as_str() == "chats").unwrap();
    chats.fields = vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str), FieldSpec { from: Some("revision".into()), ..FieldSpec::scalar("serverRevision", ScalarKind::Int) }, FieldSpec::list("lists", ScalarKind::Str)];
    chats.server_revision = Some("serverRevision".into());
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    list.membership.insert("list".into(), Membership::Contains("lists".into()));
    list.joins_node = true;
    list.joins_events = true;
    list.remote = Some(remote("ChatList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    chats.relations.push(list);
    chats.changes.push(serde_json::from_value(json!({"area":"MESSAGE", "relation":"node", "payload":chat.then_some("chat"), "id_of":(!chat).then_some(json!({"model":"messages", "field":"chatId"}))})).unwrap());
    chats.changes.push(serde_json::from_value(json!({"area":"CHAT", "relation":"node", "removes_on":["DELETED", "ACCESS_REVOKED"]})).unwrap());
    chats.change_feed = Some(super::subscribe_tests::change_feed());
    let messages = specs.iter_mut().find(|spec| spec.key.as_str() == "messages").unwrap();
    messages.fields.retain(|field| field.name != "media");
    messages.relations.iter_mut().find(|relation| relation.name == "transcode_status").unwrap().remote.as_mut().unwrap().operation = "MessageNode".into();
    messages.changes.push(serde_json::from_value(json!({"area":"MESSAGE", "relation":"transcode_status", "payload":message.then_some("message"), "removes_on":["DELETED", "ACCESS_REVOKED"]})).unwrap());
    let rig = rig_on(Db::open_in_memory(specs, None).unwrap(), MockTransport::new(), SyncConfig::default());
    for operation in original.sync.inner.operations.lock().unwrap().values() { rig.sync.register((**operation).clone()); }
    rig.sync.register(super::OperationDef::from_document("MessageNode", "query MessageNode($messageId: ID!) { node(id: $messageId) { ... on Message { id chatId text } } }").unwrap());
    rig.sync.register(super::OperationDef::new("ChatList", "query ChatList { chats { id title revision lists } }", "chats"));
    rig.sync.register(super::OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId chat { id title revision lists } message { id chatId text } } } }", "recordEvents"));
    let updates = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changes"), "Changes", "chats", EventEffect::Upsert)).await.unwrap();
    (rig, updates)
}

fn chat(id: &str, title: &str, revision: i64, list: &str) -> Value {
    json!({"id":id, "title":title, "revision":revision, "lists":[list]})
}

fn emit(rig: &Rig, item: Value) {
    rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[item]}}))));
}

fn message(chat: Value, message: Value) -> Value {
    json!({"topic":"MESSAGE", "action":"UPDATED", "recordId":"m1", "chat":chat, "message":message})
}

async fn load_list(rig: &Rig, list: &str) -> Map<String, Value> {
    let params = Map::from_iter([("list".into(), json!(list))]);
    rig.transport.enqueue("ChatList", Reply::data(json!({"chats":[]})));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "chats", "list", &params, super::load::LoadOptions::default()).await.unwrap();
    params
}

fn members(rig: &Rig, params: &Map<String, Value>) -> Vec<String> {
    member_ids(rig, "chats", "list", params)
}

#[test]
fn message_chat_request_cost() { crate::test_wait::paused(async {
    for (full_chat, full_message, expected) in [(false, false, (1, 1)), (true, false, (1, 0)), (true, true, (0, 0))] {
        let (rig, updates) = payload_rig(full_chat, full_message).await;
        let mut chat = chat("c1", "new last message", 9007199254740993, "PRIMARY");
        if full_chat { chat["revision"] = json!("9007199254740993"); }
        let row = json!({"id":"m1", "chatId":"c1", "text":"received"});
        if !full_message { rig.transport.enqueue("MessageNode", Reply::data(json!({"node":row}))); }
        if !full_chat { rig.transport.enqueue("ChatNode", Reply::data(json!({"node":chat}))); }
        emit(&rig, message(chat, row));
        rig.transport.await_consumed("Changes").await;
        assert_eq!((rig.transport.call_count("MessageNode"), rig.transport.call_count("ChatNode")), expected);
        let all: Vec<_> = rig.transport.calls().into_iter().map(|call| call.operation_name).collect();
        let mut declared = vec!["Changes".to_string()];
        if !full_message { declared.push("MessageNode".into()); }
        if !full_chat { declared.push("ChatNode".into()); }
        assert_eq!(all, declared, "all declared targets are counted");
        let row = stored(&rig, "chats", "c1").unwrap();
        assert_eq!(row.text("title"), Some("new last message"));
        assert_eq!(row.get("serverRevision"), Some(&json!(9007199254740993i64)));
        assert_eq!(stored(&rig, "messages", "m1").unwrap().text("text"), Some("received"));
        assert_eq!(updates.status().rejected, 0);
    }
}) }

#[test]
fn message_moves_chat_targets() { crate::test_wait::paused(async {
    let (rig, updates) = payload_rig(false, true).await;
    put(&rig, "messages", json!({"id":"m1", "chatId":"c1", "text":"old"}));
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node":chat("c2", "new", 2, "PRIMARY")})));
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node":chat("c1", "removed last", 3, "PRIMARY")})));
    emit(&rig, message(Value::Null, json!({"id":"m1", "chatId":"c2", "text":"moved"})));
    rig.transport.await_consumed("Changes").await;
    let ids: Vec<_> = rig.transport.calls().iter().filter(|call| call.operation_name == "ChatNode").map(|call| call.variables["id"].clone()).collect();
    assert_eq!(ids, vec![json!("c2"), json!("c1")]);
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("removed last"));
    assert_eq!(stored(&rig, "chats", "c2").unwrap().text("title"), Some("new"));
    assert_eq!(rig.transport.call_count("MessageNode"), 0);
    assert_eq!(updates.status().rejected, 0);
}) }

#[test]
fn chat_payload_revision() { crate::test_wait::paused(async {
    let (rig, updates) = payload_rig(true, true).await;
    let primary = load_list(&rig, "PRIMARY").await;
    let secondary = load_list(&rig, "SECONDARY").await;
    for (revision, title, list, expected, primary_ids) in [
        (20, "new", "SECONDARY", "new", Vec::<String>::new()),
        (19, "late", "PRIMARY", "new", vec![]),
        (20, "equal", "PRIMARY", "equal", vec!["c1".into()]),
        (21, "higher", "SECONDARY", "higher", vec![]),
    ] {
        emit(&rig, message(chat("c1", title, revision, list), json!({"id":"m1", "chatId":"c1", "text":"text"})));
        rig.transport.await_consumed("Changes").await;
        assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some(expected));
        assert_eq!(members(&rig, &primary), primary_ids);
    }
    assert_eq!(members(&rig, &secondary), ["c1"]);
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node":chat("c1", "older node", 18, "PRIMARY")})));
    let _ = rig.sync.node(&rig.sync.inner.db.admit().unwrap(), "chats", "node", "c1", true).await.unwrap();
    assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("higher"));
    assert!(members(&rig, &primary).is_empty());
    assert_eq!(updates.status().rejected, 0);
    rig.transport.enqueue("ChatNode", Reply::data(json!({"node":chat("c1", "late after delete", 30, "PRIMARY")})).after(Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let pending = tokio::spawn(async move { sync.node(&sync.inner.db.admit().unwrap(), "chats", "node", "c1", true).await });
    rig.transport.await_calls("ChatNode", 2).await;
    emit(&rig, json!({"topic":"CHAT", "action":"DELETED", "recordId":"c1"}));
    rig.transport.await_consumed("Changes").await;
    assert!(stored(&rig, "chats", "c1").is_none());
    pending.await.unwrap().unwrap();
    assert!(stored(&rig, "chats", "c1").is_none(), "a versioned answer cannot bypass the tombstone");
    assert!(members(&rig, &primary).is_empty());
}) }

#[test]
fn payload_errors_and_terminal_events_never_fall_back_to_http() { crate::test_wait::paused(async {
    let (rig, updates) = payload_rig(true, true).await;
    for payload in [Value::Null, json!({"id":"c1", "title":"incomplete"}), json!({"id":"c1", "title":"bad type", "revision":"bad", "lists":[]})] {
        emit(&rig, message(payload, json!({"id":"m1", "chatId":"c1", "text":"valid"})));
    }
    rig.transport.await_consumed("Changes").await;
    assert!(stored(&rig, "chats", "c1").is_none());
    assert_eq!(updates.status().rejected, 3);
    for kind in ["DELETED", "ACCESS_REVOKED"] {
        put(&rig, "messages", json!({"id":"m1", "chatId":"c1", "text":"exists"}));
        let mut event = message(chat("c1", "after removal", 30, "PRIMARY"), Value::Null);
        event["action"] = json!(kind);
        emit(&rig, event);
        rig.transport.await_consumed("Changes").await;
        assert!(stored(&rig, "messages", "m1").is_none());
        assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("after removal"));
    }
    assert_eq!(rig.transport.calls().iter().map(|call| call.operation_name.as_str()).collect::<Vec<_>>(), ["Changes"]);
}) }

#[test]
fn payload_events_use_current_readers_and_reject_the_connection_after_reset() { crate::test_wait::paused(async {
    let (rig, updates) = payload_rig(true, true).await;
    emit(&rig, message(chat("c1", "before reader", 1, "PRIMARY"), json!({"id":"m1", "chatId":"c1", "text":"old"})));
    rig.transport.await_consumed("Changes").await;
    updates.cancel();
    rig.transport.await_cancellations("Changes", 1).await;
    let events = super::model_event_tests::open_heard(&rig.sync, "chats").await.unwrap();
    assert!(crate::test_wait::timeout(Duration::from_millis(1), events.next()).await.is_err(), "no replay");
    emit(&rig, message(chat("c1", "after reader", 2, "PRIMARY"), json!({"id":"m1", "chatId":"c1", "text":"new"})));
    let item = crate::test_wait::timeout(Duration::from_secs(1), events.next()).await.unwrap().unwrap();
    let HeardItem::Event(event) = item else { panic!("expected event") };
    assert_eq!(event.before.unwrap().text("title"), Some("before reader"));
    assert_eq!(event.after.unwrap().text("title"), Some("after reader"));
    rig.db.reset_all().unwrap();
    emit(&rig, message(chat("c1", "stale connection", 99, "PRIMARY"), json!({"id":"m1", "chatId":"c1", "text":"stale"})));
    rig.transport.await_consumed("Changes").await;
    assert!(stored(&rig, "chats", "c1").is_none());
    assert!(stored(&rig, "messages", "m1").is_none());
    events.close();
}) }

struct EventClock {
    clock: crate::pacing::TestClock,
    wakes: std::sync::atomic::AtomicUsize,
}

impl crate::pacing::Clock for EventClock {
    fn now(&self) -> Duration { self.clock.now() }
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.clock.sleep(duration).await;
            self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
    }
}

#[test]
fn chat_event_work() { crate::test_wait::paused(async {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for count in [1, 100] {
        let (original, original_updates) = payload_rig(true, true).await;
        let mut specs: Vec<_> = original.db.shared.specs.values().cloned().collect();
        for index in 0..count {
            let mut spec = specs[0].clone();
            spec.key = crate::identity::ModelKey::new(&format!("unrelated{index}"));
            spec.change_feed = None;
            spec.changes = vec![serde_json::from_value(json!({"area":format!("UNRELATED{index}"), "relation":"node"})).unwrap()];
            specs.push(spec);
        }
        let clock = Arc::new(EventClock { clock: crate::test_wait::clock(Duration::from_millis(START_MS)), wakes: AtomicUsize::new(0) });
        let db = Db::open_in_memory(specs, None).unwrap();
        let transport = MockTransport::new();
        let sync = super::Sync::new(db.clone(), Arc::new(transport.clone()), clock.clone(), SyncConfig::default());
        for operation in original.sync.inner.operations.lock().unwrap().values() { sync.register((**operation).clone()); }
        original_updates.cancel();
        let rig = Rig { sync, db, transport, clock: clock.clock.clone() };
        let _updates = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("changes"), "Changes", "chats", EventEffect::Upsert)).await.unwrap();
        emit(&rig, message(chat("c1", "target only", 1, "PRIMARY"), json!({"id":"m1", "chatId":"c1", "text":"new"})));
        rig.transport.await_consumed("Changes").await;
        for index in 0..count { assert!(stored(&rig, &format!("unrelated{index}"), "c1").is_none()); }
        assert_eq!(stored(&rig, "chats", "c1").unwrap().text("title"), Some("target only"));
        let baseline = (rig.transport.calls().len(), rig.transport.waits("Changes"), clock.wakes.load(Ordering::SeqCst));
        rig.clock.advance(Duration::from_secs(86400));
        crate::test_wait::pass(Duration::from_secs(86400)).await;
        tokio::task::yield_now().await;
        assert_eq!((rig.transport.calls().len(), rig.transport.waits("Changes"), clock.wakes.load(Ordering::SeqCst)), baseline, "idle requests, stream wakeups and timer wakeups");
    }
}) }
