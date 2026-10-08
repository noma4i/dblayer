use serde_json::json;

use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::db::Db;
use crate::identity::{ModelKey, RowId};
use crate::schema::{ActionDeclaration, AssociationKind, AssociationSpec, Dependent, FieldSpec, ModelSpec, RootShape};
use crate::transport::{MockTransport, Reply};
use crate::value::ScalarKind;

use super::super::rig::*;
use super::super::SyncConfig;

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
    rig.db.land(envelope).unwrap();
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
    rig.db.attach("moments", &rid("m1"), "media", b"small", "a.bin", "a/b").unwrap();
    rig.db.attach("vibes", &rid("v1"), "media", &large(), "v.bin", "a/b").unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_refused_destroy_brings_back_rows_files_and_list_membership() {
    let (_root, rig) = on_disk();
    seed_with_files(&rig);
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("denied", "FORBIDDEN"));
    rig.db.destroy("moments", &rid("m1")).unwrap();
    rig.sync.flush().await.unwrap();
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
    rig.db.destroy("moments", &rid("m1")).unwrap();
    rig.sync.flush().await.unwrap();
    assert_eq!(file_rows(&rig), 0);
    assert!(rig.db.shared.files.directory_entries().is_empty());
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
    let parent = rig.db.create("moments", params(json!({"text": "parent"}))).unwrap();
    let child = rig.db.create("notes", params(json!({"momentId": parent.id.as_str()}))).unwrap();
    rig.sync.flush().await.unwrap();
    assert_eq!(rig.db.find("notes", &child.id).unwrap().unwrap().get("momentId"), Some(&json!("m1")));
    let queued = rig.db.outbox().unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].input["momentId"], json!("m1"));
}

#[tokio::test(start_paused = true)]
async fn replacing_a_temp_parent_id_rewrites_the_rows_held_by_a_queued_destroy() {
    let rig = notes_rig();
    replaced_parent(&rig);
    let parent = rig.db.create("moments", params(json!({"text": "parent"}))).unwrap();
    let child = rig.db.create("notes", params(json!({"momentId": parent.id.as_str()}))).unwrap();
    rig.db.destroy("notes", &child.id).unwrap();
    rig.sync.flush().await.unwrap();
    let held = rig.db.outbox().unwrap().into_iter().find(|entry| entry.operation.action_key == "notes:destroy").unwrap();
    assert!(held.rollback.unwrap().to_string().contains("\"momentId\":\"m1\""));
}
