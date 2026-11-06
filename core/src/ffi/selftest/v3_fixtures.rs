use serde_json::{json, Value};

use super::rig_tests::Rig;
use crate::ffi::wire::{wire_fields, Cell, WireRow};
use crate::schema::{ActionDeclaration, RootShape, AssociationKind, AssociationSpec, FieldSpec, ModelSpec, Newer, RelationSpec};
use crate::value::ScalarKind;

fn optional(name: &str, kind: ScalarKind) -> FieldSpec { FieldSpec::scalar(name, kind).nullable().optional() }

/// A chat that points at its last message and keeps counters, as the chat rows of the V3 apps do.
pub(super) fn chat_spec() -> ModelSpec {
    let mut chat = ModelSpec::new("Chat", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("name", ScalarKind::Str).optional(),
        optional("lastMessageId", ScalarKind::Id),
        optional("lastMessageAt", ScalarKind::Int),
        optional("lastActivityAt", ScalarKind::Int),
        optional("unreadCount", ScalarKind::Int),
        optional("pinned", ScalarKind::Bool),
        optional("muted", ScalarKind::Bool),
    ]);
    chat.relations.push(RelationSpec::global("all"));
    for (name, mutation, root, arg) in [("pin", "ChatPin", "chatPin", Some("pinned")), ("mute", "ChatMute", "chatMute", Some("muted")), ("mark_read", "ChatMarkRead", "chatMarkRead", None)] {
        let mut action = ActionDeclaration::new(name, RootShape::Upsert);
        action.mutation = Some(mutation.into());
        action.root = Some(root.into());
        action.variables.insert("id".into(), "id".into());
        match arg {
            Some(arg) => {
                action.args.insert(arg.into(), "bool".into());
                action.variables.insert(arg.into(), arg.into());
            }
            None => {}
        }
        chat.actions.push(action);
    }
    chat
}

/// The models of the plain rig plus the chat, and the message pointing the chat at its last message (the older message leaves the pointer alone).
pub(super) fn add_chat(models: &mut Value) {
    let models = models.as_array_mut().unwrap();
    models.push(serde_json::to_value(chat_spec()).unwrap());
    let message = models.iter_mut().find(|model| model["key"] == "Message").unwrap();
    let mut chat = AssociationSpec::new("chat", AssociationKind::BelongsTo, "Chat", "chatId");
    chat.sets = [("lastMessageId", "id"), ("lastMessageAt", "at"), ("lastActivityAt", "at")].map(|(parent, child)| (parent.to_string(), child.to_string())).into();
    chat.newer = vec![Newer { child: "at".to_string(), parent: "lastMessageAt".to_string() }];
    message["associations"].as_array_mut().unwrap().push(serde_json::to_value(chat).unwrap());
}

/// A row of the server arrives as an event: it lands the way a change of the feed does.
pub(super) fn land_event(rig: &Rig, model: &str, row: Value) {
    use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
    use crate::identity::{ModelKey, RowId};
    let db = &rig.database.db;
    let row = row.as_object().unwrap().clone();
    let id = RowId::new(row["id"].as_str().unwrap()).unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new(model), id, row });
    db.land(&db.admit().unwrap(), envelope).unwrap();
}

/// The cell of a field of a row of `model`.
pub(super) fn cell(rig: &Rig, model: &str, row: &WireRow, field: &str) -> Cell {
    let spec = rig.database.db.spec(model).unwrap();
    let at = wire_fields(&spec).iter().position(|declared| declared.name == field).unwrap_or_else(|| panic!("{model} has no field {field}"));
    row.cells[at].clone()
}

pub(super) fn text(value: &str) -> Cell { Cell::Text { value: value.into() } }

pub(super) fn int(value: i64) -> Cell { Cell::Int { value } }

pub(super) fn chat_row(id: &str, last_message_at: i64) -> Value { json!({"id": id, "name": id, "lastMessageId": null, "lastMessageAt": last_message_at, "lastActivityAt": last_message_at, "unreadCount": 0, "pinned": false, "muted": false}) }
