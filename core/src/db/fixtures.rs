use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::pacing::ManualClock;
use crate::schema::{ActionDeclaration, RootShape, AssociationKind, AssociationSpec, Dependent, FieldSpec, ModelSpec, Newer};
use crate::value::ScalarKind;

use super::{Change, Db, EntityChange, Row};

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
        ],
    );
    let mut messages = AssociationSpec::new("messages", AssociationKind::HasMany, "messages", "chatId");
    messages.dependent = Some(Dependent::Destroy);
    let last = AssociationSpec::new("lastMessage", AssociationKind::BelongsTo, "messages", "lastMessageId");
    spec.associations.extend([messages, last]);
    spec
}

pub fn messages_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::scalar("body", ScalarKind::Str).optional().indexed(),
            FieldSpec::scalar("createdAt", ScalarKind::Date).optional(),
            FieldSpec::array("reactions").optional(),
        ],
    );
    spec.actions.push(ActionDeclaration::new("react", RootShape::Upsert));
    let mut chat = AssociationSpec::new("chat", AssociationKind::BelongsTo, "chats", "chatId");
    chat.touches_parent = true;
    chat.sets = [("lastMessageId", "id"), ("lastMessageAt", "createdAt")].map(|(parent, child)| (parent.to_string(), child.to_string())).into();
    chat.newer = vec![Newer { child: "createdAt".to_string(), parent: "lastMessageAt".to_string() }];
    spec.associations.push(chat);
    spec
}

pub fn all_specs() -> Vec<ModelSpec> {
    vec![users_spec(), moments_spec(), vibes_spec(), chats_spec(), messages_spec()]
}

pub fn booted() -> Db {
    let db = Db::open_in_memory(all_specs(), None).unwrap();
    db.set_clock(Arc::new(ManualClock::new(Duration::from_millis(1_000_000))));
    db
}

pub fn poison<T: Send>(mutex: &Mutex<T>) {
    std::thread::scope(|scope| {
        let _ = scope.spawn(|| {
            let _held = mutex.lock().unwrap();
            panic!("poisoning the lock on purpose");
        }).join();
    });
    assert!(mutex.is_poisoned());
}

pub fn obj(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

pub fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

pub fn put(db: &Db, model: &str, value: Value) {
    let row = obj(value);
    let id = rid(row["id"].as_str().unwrap());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
    db.land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
}

pub type ChangeLog = Arc<Mutex<Vec<Change>>>;

#[derive(Default)]
pub struct Whole(Mutex<Vec<Row>>);

impl Whole {
    pub fn of(&self, change: &Change) -> Vec<Row> {
        let mut rows = self.0.lock().unwrap();
        change.apply_to(&mut rows);
        rows.clone()
    }
}

pub fn recorder() -> (ChangeLog, impl Fn(&Change) + Send + Sync + 'static) {
    let log: ChangeLog = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let whole = Whole::default();
    (log, move |change: &Change| sink.lock().unwrap().push(Change { rows: whole.of(change), full: true, ..change.clone() }))
}

pub fn calls(log: &ChangeLog) -> Vec<usize> {
    log.lock().unwrap().iter().map(|change| change.rows.len()).collect()
}

pub fn changes(log: &ChangeLog) -> Vec<Change> {
    log.lock().unwrap().clone()
}

pub fn texts(change: &Change) -> Vec<String> {
    change.rows.iter().filter_map(|row| row.text("text")).map(str::to_string).collect()
}

pub fn ids(change: &Change) -> Vec<String> {
    change.rows.iter().map(|row| row.id.to_string()).collect()
}

pub fn seed_tree(db: &Db) {
    put(db, "users", json!({"id": "u1", "name": "Ann"}));
    put(db, "users", json!({"id": "u2", "name": "Bob"}));
    put(db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
    put(db, "moments", json!({"id": "m2", "userId": "u2", "text": "two"}));
    put(db, "vibes", json!({"id": "v1", "momentId": "m1", "kind": "like"}));
    put(db, "vibes", json!({"id": "v2", "momentId": "m1", "kind": "wow"}));
}

impl Db {
    pub fn execute_for_test(&self, sql: &str) {
        self.shared.store.lock().connection.execute_batch(sql).unwrap();
    }
}

pub type EntityLog = Arc<Mutex<Vec<EntityChange>>>;

pub fn entity_recorder() -> (EntityLog, impl Fn(&EntityChange) + Send + Sync + 'static) {
    let log: EntityLog = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    (log, move |change: &EntityChange| sink.lock().unwrap().push(change.clone()))
}
