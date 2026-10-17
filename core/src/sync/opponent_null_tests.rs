use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::load::LoadOptions;
use super::rig::*;
use super::subscribe::{EventEffect, SubscriptionDef};
use super::{OperationDef, SyncConfig};
use crate::db::Db;
use crate::identity::RowId;
use crate::schema::{ActionDeclaration, AssociationKind, AssociationSpec, Coverage, FieldSpec, ModelSpec, NodeRead, PageDirection, RelationSpec, RemoteShape, RootShape, VariableSource, NODE_PARAM};
use crate::transport::{EventSpec, MockTransport, Reply, Response, SubscriptionEvent};
use crate::value::ScalarKind;

fn chats_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "chats",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("title", ScalarKind::Str).optional(),
            FieldSpec::scalar("opponentId", ScalarKind::Id).nullable().optional().indexed(),
        ],
    );
    spec.associations.push(AssociationSpec::new("opponent", AssociationKind::BelongsTo, "users", "opponentId"));
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.to_string()];
    node.node = Some(NodeRead::One);
    let mut node_remote = remote("ChatNode", RemoteShape::Single, "5.minutes");
    node_remote.variables = BTreeMap::from([("id".to_string(), VariableSource::Param { name: NODE_PARAM.to_string() })]);
    node.remote = Some(node_remote);
    let mut recent = RelationSpec::global("recent");
    recent.remote = Some(remote("ChatsPage", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes"));
    spec.relations.extend([node, recent]);
    let mut update = ActionDeclaration::new("update", RootShape::Upsert);
    update.mutation = Some("UpdateChat".to_string());
    spec.actions.push(update);
    spec
}

fn chat_rig() -> Rig {
    let rig = rig_on(Db::open_in_memory(vec![users_spec(), chats_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    for (name, root) in [("ChatNode", "node"), ("ChatsPage", "chats"), ("UpdateChat", "updateChat"), ("ChatChanged", "chatChanged")] {
        rig.sync.register(OperationDef::new(name, &format!("operation {name} {{ x }}"), root).paged(2));
    }
    rig
}

fn opponent_id(rig: &Rig) -> Option<String> {
    let chat = rig.db.find("chats", &RowId::new("c1").unwrap()).unwrap().unwrap();
    chat.get("opponentId").and_then(Value::as_str).map(str::to_string)
}

fn title(rig: &Rig) -> Option<String> {
    rig.db.find("chats", &RowId::new("c1").unwrap()).unwrap().and_then(|chat| chat.text("title").map(str::to_string))
}

fn user_name(rig: &Rig, id: &str) -> Option<String> {
    rig.db.find("users", &RowId::new(id).unwrap()).unwrap().and_then(|user| user.text("name").map(str::to_string))
}

fn answer(rig: &Rig, operation: &'static str, data: Value) {
    rig.transport.respond_with(move |request| (request.operation_name == operation).then(|| Reply::data(data.clone())));
}

async fn page(rig: &Rig, chat: Value) {
    answer(rig, "ChatsPage", json!({"chats": {"nodes": [chat], "pageInfo": {"hasNextPage": false, "endCursor": "e"}}}));
    rig.sync.load("chats", "recent", &Default::default(), LoadOptions { force: true }).await.unwrap();
}

async fn node(rig: &Rig, chat: Value) {
    answer(rig, "ChatNode", json!({"node": chat}));
    rig.sync.node("chats", "node", "c1", true).await.unwrap();
}

async fn mutation(rig: &Rig, chat: Value) {
    rig.db.act("chats", "update", &RowId::new("c1").unwrap(), params(json!({})), params(json!({}))).unwrap();
    answer(rig, "UpdateChat", json!({"updateChat": chat}));
    rig.sync.flush().await.unwrap();
}

async fn event(rig: &Rig, chat: Value) {
    rig.transport.emit("ChatChanged", SubscriptionEvent::Next(Response::data(json!({"chatChanged": chat}))));
    settle().await;
}

macro_rules! opponent_cases {
    ($name:ident, $apply:ident, $prepare:expr) => {
        #[tokio::test(start_paused = true)]
        async fn $name() {
            let rig = chat_rig();
            let prepare: fn(&Rig) = $prepare;
            prepare(&rig);
            let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("chat_changed"), "ChatChanged", "chats", EventEffect::Upsert)).await.unwrap();
            $apply(&rig, json!({"id": "c1", "title": "a", "opponent": {"id": "u1", "name": "Ann"}})).await;
            assert_eq!(opponent_id(&rig), Some("u1".to_string()), "object sets the key");
            assert_eq!(user_name(&rig, "u1"), Some("Ann".to_string()));
            $apply(&rig, json!({"id": "c1", "title": "b"})).await;
            assert_eq!(opponent_id(&rig), Some("u1".to_string()), "absent key leaves the link");
            $apply(&rig, json!({"id": "c1", "title": "c", "opponent": null})).await;
            assert_eq!(title(&rig), Some("c".to_string()), "the answer landed");
            assert_eq!(opponent_id(&rig), None, "explicit null resets the key");
            assert_eq!(user_name(&rig, "u1"), Some("Ann".to_string()), "the user row stays");
            $apply(&rig, json!({"id": "c1", "title": "d", "opponent": {"id": "u2", "name": "Bob"}})).await;
            assert_eq!(opponent_id(&rig), Some("u2".to_string()), "a later object sets the key again");
        }
    };
}

opponent_cases!(page_answer_resets_the_link_on_explicit_null_only, page, |_| {});
opponent_cases!(node_answer_resets_the_link_on_explicit_null_only, node, |_| {});
opponent_cases!(event_resets_the_link_on_explicit_null_only, event, |_| {});
opponent_cases!(mutation_answer_resets_the_link_on_explicit_null_only, mutation, |rig| put(rig, "chats", json!({"id": "c1", "title": "seed"})));
