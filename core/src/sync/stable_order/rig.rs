use serde_json::{json, Map, Value};

use crate::db::{Change, Db, WindowSpec};
use crate::db::fixtures::{changes, recorder, ChangeLog};
use crate::query::Query;
use crate::schema::{ActionDeclaration, Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RootShape, Sort, SortField};
use crate::transport::{EventSpec, MockTransport, Reply, Response, SubscriptionEvent};
use crate::value::{ScalarKind, SortDirection};

use super::super::rig::*;
use super::super::subscribe::{EventEffect, SubscriptionDef};
use super::super::{LoadOptions, OperationDef, SyncConfig};

pub const CHAT: &str = "c1";

pub fn messages_spec(stable: bool) -> ModelSpec {
    let mut spec = ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("text", ScalarKind::Str).optional(),
            FieldSpec::scalar("sequenceNumber", ScalarKind::Int).nullable().optional(),
            FieldSpec::scalar("createdAt", ScalarKind::Date).optional(),
        ],
    );
    let mut thread = RelationSpec::new("thread", &["chatId"]);
    thread.remote = Some(remote("MessagesThread", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "1.minute"));
    thread.joins_events = true;
    thread.order_stable = stable;
    thread.sort = Sort::Fields(vec![
        SortField { column: "sequenceNumber".to_string(), direction: SortDirection::AscNullsLast },
        SortField { column: "createdAt".to_string(), direction: SortDirection::Asc },
    ]);
    spec.relations.push(thread);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateMessage".to_string());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.variables.insert("chatId".to_string(), "chatId".to_string());
    create.variables.insert("text".to_string(), "text".to_string());
    spec.actions.push(create);
    spec
}

pub fn chat_rig_on(db: Db) -> Rig {
    let rig = rig_without_operations(db, MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("MessagesThread", "operation MessagesThread { x }", "chat").paged(100));
    rig.sync.register(OperationDef::new("CreateMessage", "mutation CreateMessage { x }", "createMessage"));
    rig.sync.register(OperationDef::new("MessageChanged", "subscription MessageChanged { x }", "messageChanged"));
    rig
}

pub fn chat_rig(stable: bool) -> Rig {
    chat_rig_on(Db::open_in_memory(vec![messages_spec(stable)], None).unwrap())
}

pub fn chat_ordered_by_time() -> Rig {
    let mut spec = messages_spec(true);
    spec.relations[0].sort = Sort::Fields(vec![SortField { column: "createdAt".to_string(), direction: SortDirection::Asc }]);
    chat_rig_on(Db::open_in_memory(vec![spec], None).unwrap())
}

pub fn chat_params() -> Map<String, Value> {
    params(json!({"chatId": CHAT}))
}

pub fn message(id: &str, sequence: Option<i64>, second: u32) -> Value {
    json!({"id": id, "chatId": CHAT, "text": id, "sequenceNumber": sequence, "createdAt": format!("2026-01-01T00:00:{second:02}.000Z")})
}

pub fn thread_page(nodes: Vec<Value>, has_next: bool, cursor: &str) -> Reply {
    Reply::data(json!({"chat": {"nodes": nodes, "pageInfo": {"hasNextPage": has_next, "endCursor": cursor}}}))
}

pub fn enqueue_page(rig: &Rig, nodes: Vec<Value>, has_next: bool, cursor: &str) {
    rig.transport.enqueue("MessagesThread", thread_page(nodes, has_next, cursor));
}

pub async fn load_first_page(rig: &Rig) {
    rig.sync.load("messages", "thread", &chat_params(), LoadOptions { force: true }).await.unwrap();
    rig.db.settle();
}

pub async fn load_next_page(rig: &Rig) {
    rig.sync.load_more("messages", "thread", &chat_params()).await.unwrap();
    rig.db.settle();
}

pub async fn load_page(rig: &Rig, nodes: Vec<Value>, has_next: bool, cursor: &str) {
    enqueue_page(rig, nodes, has_next, cursor);
    load_first_page(rig).await;
}

pub async fn subscribe_to_events(rig: &Rig) -> super::super::subscribe::EventSubscription {
    let definition = SubscriptionDef::new(EventSpec::new("message_changed"), "MessageChanged", "messages", EventEffect::Upsert);
    rig.sync.subscribe(definition).await.unwrap()
}

pub async fn event(rig: &Rig, node: Value) {
    rig.transport.emit("MessageChanged", SubscriptionEvent::Next(Response::data(json!({"messageChanged": node}))));
    settle().await;
    rig.db.settle();
}

pub fn thread_query(rig: &Rig) -> Query {
    rig.db.remote_relation_query("messages", "thread", &chat_params()).unwrap().unwrap()
}

pub fn watch_thread(rig: &Rig, size: usize) -> (ChangeLog, crate::db::WindowHandle) {
    let (log, callback) = recorder();
    let handle = rig.db.observe_window(thread_query(rig), WindowSpec::Tail { size }, callback).unwrap();
    rig.db.settle();
    (log, handle)
}

pub fn shown(log: &ChangeLog) -> Vec<String> {
    changes(log).pop().map(|change| crate::db::fixtures::ids(&change)).unwrap_or_default()
}

pub fn moves_since(log: &ChangeLog, from: usize) -> Vec<String> {
    changes(log).iter().skip(from).flat_map(|change: &Change| change.diff.moved.iter().map(ToString::to_string)).collect()
}

pub fn member_keys(rig: &Rig) -> Vec<(String, String)> {
    let key = thread_query(rig).scope.unwrap();
    rig.db.shared.store.scope_members("messages", key.as_str()).unwrap()
}
