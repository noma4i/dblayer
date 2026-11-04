use serde_json::json;

use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::db::Db;
use crate::identity::{ModelKey, RowId};
use crate::schema::{ActionDeclaration, AssociationKind, AssociationSpec, Dependent, FieldSpec, ModelSpec, RootShape};
use crate::transport::{MockTransport, Reply};
use crate::value::ScalarKind;

use super::super::rig::*;
use super::super::{OperationDef, SyncConfig};

const LARGE: usize = 300 * 1024;

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn with_media() -> Vec<ModelSpec> {
    let mut moments = moments_spec();
    moments.fields.push(FieldSpec::file("media").optional());
    let mut vibes = ModelSpec::new(
        "vibes",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("momentId", ScalarKind::Id).indexed(), FieldSpec::file("media").optional()],
    );
    vibes.associations.push(AssociationSpec::new("moment", AssociationKind::BelongsTo, "moments", "momentId"));
    let mut owned = AssociationSpec::new("vibes", AssociationKind::HasMany, "vibes", "momentId");
    owned.dependent = Some(Dependent::Destroy);
    moments.associations.push(owned);
    vec![users_spec(), moments, vibes]
}

fn on_disk() -> (tempfile::TempDir, Rig) {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(&root.path().join("42.sqlite"), with_media(), None).unwrap();
    (root, rig_on(db, MockTransport::new(), SyncConfig::default()))
}

fn feed_scope(rig: &Rig) -> ScopeRef {
    let spec = rig.db.spec("moments").unwrap();
    let relation = spec.relations.iter().find(|relation| relation.name == "feed").unwrap();
    ScopeRef::new(ModelKey::new("moments"), relation.scope_key("moments", &Default::default()).unwrap())
}

fn land_in_feed(rig: &Rig, id: &str) {
    let scope = feed_scope(rig);
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(id), row: params(moment(id, "one")) });
    envelope.scope_ops.push(ScopeOp::Append { scope, row: rid(id), order_key: "a".into() });
    rig.db.land(&rig.db.admit().unwrap(), envelope).unwrap();
}

fn feed_members(rig: &Rig) -> usize {
    rig.db.shared.store.scope_members("moments", feed_scope(rig).key.as_str()).unwrap().len()
}

fn file_rows(rig: &Rig) -> i64 {
    rig.db.shared.store.lock().connection.query_row("SELECT COUNT(*) FROM file", [], |row| row.get(0)).unwrap()
}

fn large() -> Vec<u8> {
    (0..LARGE).map(|index| (index % 251) as u8).collect()
}

fn seed_with_files(rig: &Rig) {
    land_in_feed(rig, "m1");
    put(rig, "vibes", json!({"id": "v1", "momentId": "m1"}));
    rig.db.attach(&rig.db.admit().unwrap(), "moments", &rid("m1"), "media", b"small", "a.bin", "a/b").unwrap();
    rig.db.attach(&rig.db.admit().unwrap(), "vibes", &rid("v1"), "media", &large(), "v.bin", "a/b").unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_refused_destroy_brings_back_rows_files_and_list_membership() {
    let (_root, rig) = on_disk();
    seed_with_files(&rig);
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("denied", "FORBIDDEN"));
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &rid("m1")).unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(rig.db.find("vibes", &rid("v1")).unwrap().is_some());
    assert_eq!(rig.db.read("moments", &rid("m1"), "media").unwrap(), Some(b"small".to_vec()));
    assert_eq!(rig.db.read("vibes", &rid("v1"), "media").unwrap(), Some(large()));
    assert_eq!(feed_members(&rig), 1);
}

#[tokio::test(start_paused = true)]
async fn a_confirmed_destroy_removes_the_files_of_the_row_and_its_children() {
    let (_root, rig) = on_disk();
    seed_with_files(&rig);
    rig.transport.enqueue("DeleteMoment", Reply::data(json!({"deleteMoment": {"id": "m1"}})));
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &rid("m1")).unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(file_rows(&rig), 0);
    assert!(rig.db.shared.files.directory_entries().unwrap().is_empty());
}

fn held_file_id(rig: &Rig, row: &str) -> String {
    rig.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT id FROM file WHERE row_id = ?1", [row], |found| found.get::<_, String>(0))?)).unwrap()
}

#[tokio::test(start_paused = true)]
async fn a_confirmed_destroy_discards_held_files_off_the_async_executor() {
    let (_root, rig) = on_disk();
    seed_with_files(&rig);
    let id = held_file_id(&rig, "v1");
    crate::files::watch_file_io(&id);
    rig.transport.enqueue("DeleteMoment", Reply::data(json!({"deleteMoment": {"id": "m1"}})));
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &rid("m1")).unwrap();
    let executor = std::thread::current().id();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    rig.sync.wait_uploads().await;
    let events = crate::files::take_file_io(&id);
    assert_eq!(events.iter().map(|(operation, _)| *operation).collect::<Vec<_>>(), ["discard"]);
    assert!(events.iter().all(|(_, thread)| *thread != executor), "discard used executor {executor:?}: {events:?}");
    assert!(rig.db.shared.files.directory_entries().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_server_destroy_event_discards_held_files_off_the_async_executor() {
    use crate::sync::subscribe::{EventEffect, SubscriptionDef};
    use crate::transport::{EventSpec, Response, SubscriptionEvent};
    let (_root, rig) = on_disk();
    rig.sync.register(OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
    seed_with_files(&rig);
    let id = held_file_id(&rig, "v1");
    crate::files::watch_file_io(&id);
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &rid("m1")).unwrap();
    let def = SubscriptionDef::new(EventSpec::new("moment_gone"), "MomentChanged", "moments", EventEffect::Destroy);
    let _subscription = rig.sync.subscribe(def).await.unwrap();
    let executor = std::thread::current().id();
    rig.transport.emit("MomentChanged", SubscriptionEvent::Next(Response::data(json!({"momentChanged": {"id": "m1"}}))));
    rig.transport.await_consumed("MomentChanged").await;
    rig.sync.wait_uploads().await;
    let events = crate::files::take_file_io(&id);
    assert_eq!(events.iter().map(|(operation, _)| *operation).collect::<Vec<_>>(), ["discard"]);
    assert!(events.iter().all(|(_, thread)| *thread != executor), "discard used executor {executor:?}: {events:?}");
    assert!(rig.db.shared.files.directory_entries().unwrap().is_empty());
}

fn with_notes() -> Vec<ModelSpec> {
    let mut notes = ModelSpec::new("notes", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("momentId", ScalarKind::Id).indexed()]);
    notes.associations.push(AssociationSpec::new("moment", AssociationKind::BelongsTo, "moments", "momentId"));
    notes.actions.push(ActionDeclaration::new("create", RootShape::Upsert));
    vec![users_spec(), moments_spec(), notes]
}

fn notes_rig() -> Rig {
    rig_on(Db::open_in_memory(with_notes(), None).unwrap(), MockTransport::new(), SyncConfig::default())
}

fn replaced_parent(rig: &Rig) {
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m1", "text": "parent", "userId": null}})));
}

#[tokio::test(start_paused = true)]
async fn replacing_a_temp_parent_id_rewrites_child_rows_and_queued_child_inputs() {
    let rig = notes_rig();
    replaced_parent(&rig);
    let parent = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "parent"}))).unwrap();
    let child = rig.db.create(&rig.db.admit().unwrap(), "notes", params(json!({"momentId": parent.id.as_str()}))).unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.db.find("notes", &child.id).unwrap().unwrap().get("momentId"), Some(&json!("m1")));
    let queued = rig.db.outbox().unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].input["momentId"], json!("m1"));
}

#[tokio::test(start_paused = true)]
async fn replacing_a_temp_parent_id_rewrites_the_rows_held_by_a_queued_destroy() {
    let rig = notes_rig();
    replaced_parent(&rig);
    let parent = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "parent"}))).unwrap();
    let child = rig.db.create(&rig.db.admit().unwrap(), "notes", params(json!({"id": "n1", "momentId": parent.id.as_str()}))).unwrap();
    rig.db.destroy(&rig.db.admit().unwrap(), "notes", &child.id).unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    let held = rig.db.outbox().unwrap().into_iter().find(|entry| entry.operation.action_key == "notes:destroy").unwrap();
    assert!(held.rollback.unwrap().to_string().contains("\"momentId\":\"m1\""));
}

fn with_chat_pointer() -> Vec<ModelSpec> {
    let mut chats = ModelSpec::new(
        "chats",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("lastMessageId", ScalarKind::Id).nullable().optional().indexed(), FieldSpec::scalar("lastMessageAt", ScalarKind::Date).nullable().optional()],
    );
    chats.associations.push(AssociationSpec::new("lastMessage", AssociationKind::BelongsTo, "messages", "lastMessageId"));
    let mut messages = ModelSpec::new(
        "messages",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), FieldSpec::scalar("createdAt", ScalarKind::Date).optional()],
    );
    let mut chat = AssociationSpec::new("chat", AssociationKind::BelongsTo, "chats", "chatId");
    chat.sets = [("lastMessageId", "id"), ("lastMessageAt", "createdAt")].map(|(parent, child)| (parent.to_string(), child.to_string())).into();
    messages.associations.push(chat);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateMessage".to_string());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.variables.insert("chatId".to_string(), "chatId".to_string());
    messages.actions.push(create);
    vec![chats, messages]
}

#[tokio::test(start_paused = true)]
async fn the_server_answer_moves_the_chat_pointer_from_the_temp_id_to_the_real_one() {
    let rig = rig_on(Db::open_in_memory(with_chat_pointer(), None).unwrap(), MockTransport::new(), SyncConfig::default());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("chats"), id: rid("c1"), row: params(json!({"id": "c1"})) });
    rig.db.land(&rig.db.admit().unwrap(), envelope).unwrap();
    rig.sync.register(OperationDef::new("CreateMessage", "operation CreateMessage { x }", "createMessage"));
    rig.transport.enqueue("CreateMessage", Reply::data(json!({"createMessage": {"id": "m7", "chatId": "c1", "createdAt": "2026-01-02T00:00:00.000Z"}})));
    let message = rig.db.create(&rig.db.admit().unwrap(), "messages", params(json!({"chatId": "c1", "createdAt": "2026-01-02T00:00:00.000Z"}))).unwrap();
    assert_eq!(rig.db.find("chats", &rid("c1")).unwrap().unwrap().text("lastMessageId"), Some(message.id.as_str()));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.db.find("chats", &rid("c1")).unwrap().unwrap().text("lastMessageId"), Some("m7"));
}

#[tokio::test(start_paused = true)]
async fn the_confirmed_message_moves_the_chat_fields_to_the_server_values() {
    let rig = rig_on(Db::open_in_memory(with_chat_pointer(), None).unwrap(), MockTransport::new(), SyncConfig::default());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("chats"), id: rid("c1"), row: params(json!({"id": "c1"})) });
    rig.db.land(&rig.db.admit().unwrap(), envelope).unwrap();
    rig.sync.register(OperationDef::new("CreateMessage", "operation CreateMessage { x }", "createMessage"));
    rig.transport.enqueue("CreateMessage", Reply::data(json!({"createMessage": {"id": "m7", "chatId": "c1", "createdAt": "2026-01-02T00:00:03.000Z"}})));
    rig.db.create(&rig.db.admit().unwrap(), "messages", params(json!({"chatId": "c1", "createdAt": "2026-01-02T00:00:00.000Z"}))).unwrap();
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(rig.db.find("chats", &rid("c1")).unwrap().unwrap().text("lastMessageAt"), Some("2026-01-02T00:00:03.000Z"));
}
