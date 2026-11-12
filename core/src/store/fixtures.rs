use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::store::{BatchPlan, DestroyReason, EntityOp, ScopeOp, ScopeRef};
use crate::identity::{ModelKey, RowId, ListKey};
use crate::runtime::ManualClock;
use crate::schema::{ActionDeclaration, AssociationKind, AssociationSpec, Dependent, FieldSpec, ModelSpec, Newer, RootShape};
use crate::value::ScalarKind;

use super::Store;

pub fn failed_restore(store: &Store) -> Vec<super::restore::RestoreFailedRow> {
    super::restore::read_failed(&store.lock().connection).unwrap()
}

pub fn messages_spec() -> ModelSpec {
    ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("body", ScalarKind::Str).optional(),
            FieldSpec::scalar("seen", ScalarKind::Bool).optional().indexed(),
        ],
    )
}

pub fn items_spec() -> ModelSpec {
    ModelSpec::new(
        "items",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("createdAt", ScalarKind::Date).optional().indexed()],
    )
}

pub fn booted_store() -> (Store, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(Duration::from_millis(1_000_000)));
    let store = Store::open_in_memory(clock.clone()).unwrap();
    store.boot(&[messages_spec(), items_spec()], None).unwrap();
    store.mark_ready();
    (store, clock)
}

pub fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

pub fn id(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

pub fn model(name: &str) -> ModelKey {
    ModelKey::new(name)
}

pub fn scope(model_name: &str, name: &str) -> ScopeRef {
    ScopeRef::new(model(model_name), ListKey::compose(&[model_name, name, "1"]))
}

pub fn upsert(model_name: &str, payload: Value) -> EntityOp {
    let row = row(payload);
    let row_id = id(row["id"].as_str().unwrap());
    EntityOp::Upsert { model: model(model_name), id: row_id, row }
}

pub fn destroy(model_name: &str, row_id: &str, reason: DestroyReason) -> EntityOp {
    EntityOp::Destroy { model: model(model_name), id: id(row_id), reason }
}

pub fn append(target: &ScopeRef, row_id: &str, order_key: &str) -> ScopeOp {
    ScopeOp::Append { scope: target.clone(), row: id(row_id), order_key: order_key.to_string() }
}

pub fn envelope(store: &Store, entity_ops: Vec<EntityOp>, scope_ops: Vec<ScopeOp>) -> BatchPlan {
    let mut envelope = BatchPlan::from_source(store.generation(), crate::schema::RowSource::Event);
    envelope.entity_ops = entity_ops;
    envelope.scope_ops = scope_ops;
    envelope
}

pub fn message(row_id: &str, chat: &str) -> Value {
    json!({"id": row_id, "chatId": chat})
}

pub fn count(store: &Store, sql: &str) -> i64 {
    store.lock().connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

pub fn text(store: &Store, sql: &str) -> Option<String> {
    rusqlite::OptionalExtension::optional(store.lock().connection.query_row(sql, [], |row| row.get(0))).unwrap()
}

pub fn exec(store: &Store, sql: &str) {
    store.lock().connection.execute_batch(sql).unwrap();
}

pub fn users_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "users",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional(), FieldSpec::array("friendIds").optional()],
    );
    let mut moments = AssociationSpec::new("moments", AssociationKind::HasMany, "moments", "userId");
    moments.dependent = Some(Dependent::Destroy);
    let latest = AssociationSpec::new("latest", AssociationKind::HasOne, "moments", "userId");
    let friends = AssociationSpec::new("friends", AssociationKind::References, "users", "friendIds");
    spec.associations.extend([moments, latest, friends]);
    spec
}

pub fn moments_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "moments",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("userId", ScalarKind::Id).nullable().indexed(),
            FieldSpec::scalar("text", ScalarKind::Str).optional().indexed(),
            FieldSpec::scalar("createdAt", ScalarKind::Date).optional().indexed(),
            FieldSpec::file("media").optional(),
            FieldSpec::file("cover").optional(),
        ],
    );
    let mut author = AssociationSpec::new("author", AssociationKind::BelongsTo, "users", "userId");
    author.touches_parent = true;
    let mut vibes = AssociationSpec::new("vibes", AssociationKind::HasMany, "vibes", "momentId");
    vibes.dependent = Some(Dependent::Destroy);
    spec.associations.extend([author, vibes]);
    spec
}

pub fn vibes_spec() -> ModelSpec {
    ModelSpec::new(
        "vibes",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("momentId", ScalarKind::Id).indexed(), FieldSpec::scalar("kind", ScalarKind::Str).optional(), FieldSpec::file("media").optional()],
    )
}

pub fn chats_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "chats",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("title", ScalarKind::Str).optional(),
            FieldSpec::scalar("lastMessageId", ScalarKind::Id).nullable().optional().indexed(),
            FieldSpec::scalar("lastMessageAt", ScalarKind::Date).nullable().optional(),
            FieldSpec::scalar("lastSequenceNumber", ScalarKind::Int).nullable().optional(),
            FieldSpec::scalar("lastMessageBody", ScalarKind::Str).nullable().optional(),
        ],
    );
    let mut messages = AssociationSpec::new("messages", AssociationKind::HasMany, "messages", "chatId");
    messages.dependent = Some(Dependent::Destroy);
    let last = AssociationSpec::new("lastMessage", AssociationKind::BelongsTo, "messages", "lastMessageId");
    spec.associations.extend([messages, last]);
    spec
}

pub fn revisioned_chats_spec() -> ModelSpec {
    let mut spec = chats_spec();
    spec.server_revision = Some("rev".into());
    spec.fields.push(FieldSpec::scalar("rev", ScalarKind::Int).optional());
    spec
}

pub fn chat_messages_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("body", ScalarKind::Str).optional().indexed(),
            FieldSpec::scalar("createdAt", ScalarKind::Date).optional(),
            FieldSpec::scalar("sequenceNumber", ScalarKind::Int).nullable().optional(),
            FieldSpec::array("reactions").optional(),
        ],
    );
    spec.actions.push(ActionDeclaration::new("react", RootShape::Upsert));
    let mut chat = AssociationSpec::new("chat", AssociationKind::BelongsTo, "chats", "chatId");
    chat.touches_parent = true;
    chat.sets = [("lastMessageId", "id"), ("lastMessageAt", "createdAt"), ("lastSequenceNumber", "sequenceNumber"), ("lastMessageBody", "body")].map(|(parent, child)| (parent.to_string(), child.to_string())).into();
    chat.newer = [("sequenceNumber", "lastSequenceNumber"), ("createdAt", "lastMessageAt")].map(|(child, parent)| Newer { child: child.to_string(), parent: parent.to_string() }).into();
    spec.associations.push(chat);
    spec
}

pub fn all_specs() -> Vec<ModelSpec> {
    vec![users_spec(), moments_spec(), vibes_spec(), chats_spec(), chat_messages_spec()]
}

pub fn booted() -> Arc<Store> {
    let db = Arc::new(Store::open_models(None, all_specs(), None, Arc::new(ManualClock::new(Duration::from_millis(1_000_000)))).unwrap());
    db
}

pub fn put(db: &Store, model: &str, value: Value) {
    let event = serde_json::json!({"op": "upsert", "id": value["id"], "row": value}).to_string();
    db.land_events_with_pin(&[(model.to_string(), event)], None).unwrap();
}

pub(crate) fn list_view(query: &crate::store::query::Query) -> crate::owners::read::deliveries::View {
    use crate::owners::read::deliveries::{Kind, View};
    use crate::owners::read::windows::WindowKey;
    let model = query.model.to_string();
    View { kind: Kind::List, tables: vec![crate::store::ddl::table_name(&model)], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: model.clone(), name: "list".into() }, subject: String::new(), deferred_rows: Vec::new(), query: Some(Arc::new(query.clone())), model }
}

pub(crate) fn operations_view() -> crate::owners::read::deliveries::View {
    use crate::owners::read::deliveries::{Kind, View};
    use crate::owners::read::windows::WindowKey;
    View { kind: Kind::Operations, tables: vec![crate::store::operation::QUEUE_TABLE.into()], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), deferred_rows: Vec::new(), query: None, model: String::new() }
}

impl Store {
    pub(crate) fn write_queue_ops(&self, ops: &[crate::owners::store::StoreOp]) -> Result<super::batch_write::BatchWritten, super::StoreError> {
        self.write_queue_ops_with_pin(ops, None)
    }

    pub(crate) fn write_queue_ops_with_pin(&self, ops: &[crate::owners::store::StoreOp], min_pin_revision: Option<u64>) -> Result<super::batch_write::BatchWritten, super::StoreError> {
        let batch = crate::owners::store::StoreBatch { author: crate::owners::contract::BatchAuthor::Outbox, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: ops.to_vec() };
        self.write_batch(&batch, self.files(), min_pin_revision)
    }
}

impl Store {
    /// Lands feed events the way the owner Feed hands them to Store.
    pub(crate) fn land_events_with_pin(&self, events: &[(String, String)], min_pin_revision: Option<u64>) -> Result<super::batch_write::BatchWritten, super::StoreError> {
        let batch = crate::owners::store::StoreBatch { author: crate::owners::contract::BatchAuthor::Feed, id: 1, stamp: crate::runtime::EpochStamp::Epoch(1), basis: Default::default(), rows: events.to_vec(), intents: 0, credential: None, ops: Vec::new() };
        self.write_batch(&batch, self.files(), min_pin_revision)
    }
}

pub fn posts_spec() -> ModelSpec {
    use crate::schema::{Coverage, RelationSpec, RemoteShape};
    let mut spec = ModelSpec::new(
        "posts",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional(), FieldSpec::scalar("groupId", ScalarKind::Str).nullable().indexed()],
    );
    let window = |name: &str, events: bool, columns: bool| {
        let mut relation = RelationSpec::global(name);
        relation.joins_events = events;
        if columns {
            relation.is_global = false;
            relation.by_columns = vec!["groupId".into()];
        }
        relation.remote = Some(crate::schema_fixtures::remote("Feed", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
        relation
    };
    spec.relations.extend([window("pager", false, false), window("evt", true, false), window("cols", true, true)]);
    spec
}

pub fn snapshot_specs() -> Vec<ModelSpec> {
    vec![chats_spec(), chat_messages_spec(), posts_spec()]
}
