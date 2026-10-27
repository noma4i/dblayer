use std::time::Duration;

use serde_json::{json, Value};

use super::rig::*;
use super::{LoadOptions, OperationDef, SyncConfig};
use crate::db::Db;
use crate::query::Query;
use crate::schema::{Coverage, FieldSpec, Membership, ModelSpec, RelationSpec, RemoteShape, Storage};
use crate::transport::{MockTransport, Reply, Response, SubscriptionEvent};
use crate::value::ScalarKind;

fn action_model(name: &str, matches: Value, lifetime: &str) -> ModelSpec {
    let mut model = ModelSpec::new(name, vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("kind", ScalarKind::Str), FieldSpec::scalar("active", ScalarKind::Bool)]);
    model.storage = Storage::Memory;
    model.expire_after = Some(lifetime.into());
    model.listens.push(serde_json::from_value(json!({"subscription":"Signal", "root":"signal", "id":["chatId","userId"], "deleted_unless":"active", "apply":{"effect":"upsert", "matches":{"kind":matches}}})).unwrap());
    model
}

fn signal(rig: &Rig, kind: Value, active: bool) {
    let mut node = json!({"chatId":"c", "userId":"u", "active":active});
    if kind != json!("MISSING") { node["kind"] = kind; }
    rig.transport.emit("Signal", SubscriptionEvent::Next(Response::data(json!({"signal":node}))));
}

#[tokio::test(start_paused = true)]
async fn chat_action_routes_share_one_stream_commit_and_keep_independent_lifetimes() {
    let kinds = json!(["TYPING","RECORDING_AUDIO","RECORDING_VIDEO","UPLOADING_PHOTO","UPLOADING_VIDEO"]);
    let models = vec![action_model("actions", kinds.clone(), "8.seconds"), action_model("mirror", kinds, "8.seconds"), action_model("notices", json!(["SCREENSHOT"]), "1.minute")];
    let rig = rig_on(Db::open_in_memory(models, None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("Signal", "subscription Signal { signal { chatId userId kind active } }", "signal"));
    rig.sync.inner.listen_all();
    rig.transport.await_calls("Signal", 1).await;
    let events = rig.sync.open_model_events("notices").await.unwrap();
    assert_eq!(rig.transport.call_count("Signal"), 1);
    for kind in ["TYPING","RECORDING_AUDIO","RECORDING_VIDEO","UPLOADING_PHOTO","UPLOADING_VIDEO"] {
        let revision = rig.db.revision_seq().unwrap();
        signal(&rig, json!(kind), true);
        rig.transport.await_consumed("Signal").await;
        for model in ["actions", "mirror"] { assert_eq!(rig.db.query(&Query::new(model)).unwrap()[0].text("kind"), Some(kind)); }
        assert_eq!(rig.db.revision_seq().unwrap(), revision + 1, "both models land in one commit");
    }
    rig.clock.advance(Duration::from_secs(7));
    signal(&rig, json!("UPLOADING_VIDEO"), true);
    rig.transport.await_consumed("Signal").await;
    rig.clock.advance(Duration::from_secs(2));
    rig.db.expire_ephemeral().unwrap();
    assert_eq!(rig.db.query(&Query::new("actions")).unwrap().len(), 1, "equal writes renew the lifetime");
    signal(&rig, json!("SCREENSHOT"), true);
    rig.transport.await_consumed("Signal").await;
    let item = tokio::time::timeout(Duration::from_secs(1), events.next()).await.unwrap().unwrap();
    assert!(matches!(item, super::ModelEventItem::Event(event) if event.kind == "SCREENSHOT" && event.after.is_some()));
    let late = rig.sync.open_model_events("notices").await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(1), late.next()).await.is_err(), "events are not replayed");
    signal(&rig, json!("UNKNOWN"), true);
    rig.transport.await_consumed("Signal").await;
    assert_eq!(rig.db.query(&Query::new("notices")).unwrap().len(), 1);
    signal(&rig, json!("UPLOADING_VIDEO"), false);
    rig.transport.await_consumed("Signal").await;
    assert!(rig.db.query(&Query::new("actions")).unwrap().is_empty());
    assert!(rig.db.query(&Query::new("mirror")).unwrap().is_empty());
    rig.clock.advance(Duration::from_secs(9));
    rig.db.expire_ephemeral().unwrap();
    assert_eq!(rig.db.query(&Query::new("notices")).unwrap().len(), 1);
    assert_eq!(rig.transport.call_count("Signal"), 1, "idle makes no requests");
}

#[tokio::test(start_paused = true)]
async fn inv_dsl_10_apply_not_values_require_a_present_string_before_any_write_or_delete() {
    let rig = rig_on(Db::open_in_memory(vec![action_model("actions", json!({"not":["SCREENSHOT"]}), "8.seconds")], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("Signal", "subscription Signal { signal { chatId userId kind active } }", "signal"));
    rig.sync.inner.listen_all();
    rig.transport.await_calls("Signal", 1).await;
    signal(&rig, json!("TYPING"), true);
    rig.transport.await_consumed("Signal").await;
    for (kind, expected) in [(json!("SCREENSHOT"), true), (Value::Null, true), (json!("MISSING"), true), (json!(1), true), (json!("RECORDING_AUDIO"), false)] {
        signal(&rig, kind, false);
        rig.transport.await_consumed("Signal").await;
        assert_eq!(!rig.db.query(&Query::new("actions")).unwrap().is_empty(), expected);
    }
}

#[tokio::test(start_paused = true)]
async fn inv_dsl_11_unchanged_event_node_joins_loaded_membership_heads_and_manual_nodes_do_not() {
    let original = super::node_tests::node_rig();
    let mut models: Vec<_> = original.db.shared.specs.values().cloned().collect();
    let chat = models.iter_mut().find(|model| model.key.as_str() == "chats").unwrap();
    chat.fields.push(FieldSpec::list("lists", ScalarKind::Str).optional());
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    list.membership.insert("list".into(), Membership::Contains("lists".into()));
    list.joins_node = true;
    list.joins_events = true;
    list.remote = Some(remote("Chats", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    chat.relations.push(list);
    let mut scalar = RelationSpec::new("scalar", &[]);
    scalar.text_params.push("name".into());
    scalar.membership.insert("name".into(), Membership::Equals("title".into()));
    scalar.joins_node = true;
    scalar.remote = Some(remote("Chats", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    chat.relations.push(scalar);
    chat.change_feed = Some(super::subscribe_tests::change_feed());
    chat.changes.push(serde_json::from_value(json!({"area":"CHAT","relation":"node"})).unwrap());
    chat.changes.push(serde_json::from_value(json!({"area":"MESSAGE","relation":"node","id_of":{"model":"messages","field":"chatId"}})).unwrap());
    models.iter_mut().find(|model| model.key.as_str() == "messages").unwrap().changes.push(serde_json::from_value(json!({"area":"MESSAGE","relation":"transcode_status"})).unwrap());
    let rig = rig_on(Db::open_in_memory(models, None).unwrap(), MockTransport::new(), SyncConfig::default());
    for operation in original.sync.inner.operations.lock().unwrap().values() { rig.sync.register((**operation).clone()); }
    rig.sync.register(OperationDef::new("Chats", "query Chats { chats { id title lists } }", "chats"));
    rig.sync.register(OperationDef::new("Changes", "subscription Changes { recordEvents { changes { topic action recordId } } }", "recordEvents"));
    for _ in 0..2 {
        rig.transport.enqueue("Chats", Reply::data(json!({"chats":[{"id":"anchor", "title":"anchor", "lists":["PRIMARY","SECONDARY"]}]})));
    }
    rig.transport.enqueue("Chats", Reply::data(json!({"chats":[]})));
    for name in ["PRIMARY", "SECONDARY"] {
        rig.sync.load("chats", "list", json!({"list":name}).as_object().unwrap(), LoadOptions::default()).await.unwrap();
    }
    rig.sync.load("chats", "scalar", json!({"name":"same"}).as_object().unwrap(), LoadOptions::default()).await.unwrap();
    let listed = |name: &str| {
        let key = rig.db.spec("chats").unwrap().selection_key("list", json!({"list":name}).as_object().unwrap()).unwrap();
        rig.db.query(&Query::new("chats").in_scope(key)).unwrap().iter().map(|row| row.id.to_string()).collect::<Vec<_>>()
    };
    let cases = [(Some(json!(["PRIMARY"])), vec!["c1","anchor"], vec!["anchor"]), (Some(json!(["PRIMARY","SECONDARY"])), vec!["c1","anchor"], vec!["c1","anchor"]), (None, vec!["c1","anchor"], vec!["c1","anchor"]), (Some(json!(["SECONDARY"])), vec!["anchor"], vec!["c1","anchor"]), (Some(json!([])), vec!["anchor"], vec!["anchor"])];
    let responses = std::iter::once(json!({"id":"c1","title":"same","lists":["PRIMARY"]})).chain(cases.iter().map(|(lists, _, _)| {
        let mut node = json!({"id":"c1","title":"same"});
        if let Some(lists) = lists { node["lists"] = lists.clone(); }
        node
    })).chain(std::iter::once(json!({"id":"c2","title":"new","lists":["PRIMARY","THIRD"]})));
    for node in responses {
        rig.transport.enqueue("ChatNode", Reply::data(json!({"node":node})));
    }
    rig.sync.node("chats", "node", "c1", true).await.unwrap();
    assert_eq!(listed("PRIMARY"), ["anchor"]);
    let _events = rig.sync.open_model_events("chats").await.unwrap();
    for (index, (_, primary, secondary)) in cases.into_iter().enumerate() {
        let (topic, id) = if index == 0 {
            rig.transport.enqueue("MessageTranscodeStatus", Reply::data(json!({"node":{"id":"m1","chatId":"c1","text":"new"}})));
            ("MESSAGE", "m1")
        } else { ("CHAT", "c1") };
        rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[{"topic":topic,"action":"UPDATED","recordId":id}]}}))));
        rig.transport.await_consumed("Changes").await;
        assert_eq!(listed("PRIMARY"), primary);
        assert_eq!(listed("SECONDARY"), secondary);
    }
    rig.transport.emit("Changes", SubscriptionEvent::Next(Response::data(json!({"recordEvents":{"changes":[{"topic":"CHAT","action":"UPDATED","recordId":"c2"}]}}))));
    rig.transport.await_consumed("Changes").await;
    assert_eq!(listed("PRIMARY"), ["c2", "anchor"]);
    assert!(listed("THIRD").is_empty(), "unloaded windows are not created");
    let scalar = rig.db.spec("chats").unwrap().selection_key("scalar", json!({"name":"same"}).as_object().unwrap()).unwrap();
    assert_eq!(rig.db.query(&Query::new("chats").in_scope(scalar)).unwrap().iter().map(|row| row.id.to_string()).collect::<Vec<_>>(), ["c1"]);
    assert_eq!(rig.transport.call_count("Chats"), 3, "events do not reread lists");
}
