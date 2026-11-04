use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::identity::ModelKey;
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use serde_json::json;

fn feed() -> Db {
    let spec = ModelSpec::new("posts", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::enumeration("kind", &["text", "image"]).indexed()]);
    Db::open_in_memory(vec![spec], None).unwrap()
}

#[test]
fn a_server_enum_value_outside_values_is_stored_and_read_back_unchanged() {
    let db = feed();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("posts"), id: rid("p1"), row: obj(json!({"id": "p1", "kind": "voice_note"})) });
    db.land(&db.admit().unwrap(), envelope).unwrap();
    assert_eq!(db.find("posts", &rid("p1")).unwrap().unwrap().text("kind"), Some("voice_note"));
}

#[test]
fn a_local_write_keeps_an_enum_text_outside_values() {
    let db = feed();
    let row = db.create(&db.admit().unwrap(), "posts", obj(json!({"kind": "voice_note"}))).unwrap();
    assert_eq!(db.find("posts", &row.id).unwrap().unwrap().text("kind"), Some("voice_note"));
}
