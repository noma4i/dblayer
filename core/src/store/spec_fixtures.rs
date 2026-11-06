//! Models the tests of the owners and the bridge share: users with moments, chats with messages.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value};

use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::pacing::ManualClock;
use crate::schema::{ActionDeclaration, RootShape, AssociationKind, AssociationSpec, Dependent, FieldSpec, ModelSpec, Newer};
use crate::value::ScalarKind;
use crate::store::Store;


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

pub fn booted() -> Arc<Store> {
    let db = Arc::new(Store::open_models(None, all_specs(), None).unwrap());
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

pub fn put(db: &Store, model: &str, value: Value) {
    let row = obj(value);
    let id = rid(row["id"].as_str().unwrap());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
    db.commit(envelope).unwrap();
}

impl Store {
    pub fn execute_for_test(&self, sql: &str) {
        self.lock().connection.execute_batch(sql).unwrap();
    }
}


impl Store {
    /// Lands an envelope the way an owner batch would, without the owners.
    pub fn land_in_tests(&self, envelope: CommitEnvelope) -> Result<crate::commit::CommitResult, super::StoreError> {
        self.commit(envelope)
    }

    pub(crate) fn write_queue_ops(&self, ops: &[crate::owners::store::StoreOp]) -> Result<super::batch_write::BatchWritten, super::StoreError> {
        self.write_queue_ops_with_pin(ops, None)
    }

    pub(crate) fn write_queue_ops_with_pin(&self, ops: &[crate::owners::store::StoreOp], min_pin_revision: Option<u64>) -> Result<super::batch_write::BatchWritten, super::StoreError> {
        let batch = crate::owners::store::StoreBatch { author: crate::owners::contract::BatchAuthor::Outbox, id: 1, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops: ops.to_vec() };
        self.write_batch(&batch, self.files(), self.pointer_guards(), min_pin_revision)
    }
}

impl Store {
    /// Lands feed events the way the owner Feed hands them to Store.
    pub(crate) fn land_events_with_pin(&self, events: &[(String, String)], min_pin_revision: Option<u64>) -> Result<super::batch_write::BatchWritten, super::StoreError> {
        let batch = crate::owners::store::StoreBatch { author: crate::owners::contract::BatchAuthor::Feed, id: 1, epoch: 1, basis: Default::default(), rows: events.to_vec(), intents: 0, credential: None, ops: Vec::new() };
        self.write_batch(&batch, self.files(), self.pointer_guards(), min_pin_revision)
    }
}
