use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::commit::{ScopeOp, ScopeRef};
use crate::db::Db;
use crate::identity::{ListKey, ModelKey, RowId};
use crate::schema::{
    ActionDeclaration, ActionEffect, ActionLanding, Coverage, EffectSource, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RootShape, Selection,
};
use crate::sync::rig::remote;
use crate::value::ScalarKind;

pub const ROOM_ORDER: &str = "first";

fn arg(name: &str) -> EffectSource {
    EffectSource::Arg(name.to_string())
}

fn list(operation: &str) -> crate::schema::RemoteSpec {
    remote(operation, RemoteShape::List { coverage: Coverage::Complete }, "5.minutes")
}

fn action(name: &str, mutation: &str, args: &[(&str, &str)], effects: Vec<ActionEffect>) -> ActionDeclaration {
    let mut declared = ActionDeclaration::new(name, RootShape::Upsert);
    declared.mutation = Some(mutation.to_string());
    declared.args = args.iter().map(|(name, kind)| (name.to_string(), kind.to_string())).collect();
    declared.effects = effects;
    declared
}

fn selection(model: &str, scope: &str, rows: Option<&str>, with: &[(&str, EffectSource)]) -> Selection {
    Selection { model: model.to_string(), scope: scope.to_string(), rows: rows.map(str::to_string), with: with.iter().map(|(name, source)| (name.to_string(), source.clone())).collect() }
}

pub fn people_spec() -> ModelSpec {
    let mut spec = ModelSpec::new("people", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("name", ScalarKind::Str).optional()]);
    let mut blocked = RelationSpec::global("blocked");
    blocked.remote = Some(list("BlockedPeople"));
    let mut roster = RelationSpec::new("roster", &[]);
    roster.text_params = vec!["roomId".to_string(), "order".to_string()];
    roster.text_defaults = BTreeMap::from([("order".to_string(), ROOM_ORDER.to_string())]);
    roster.remote = Some(list("RoomRoster"));
    spec.relations.extend([blocked, roster]);
    let mut block = action("block", "BlockPerson", &[], vec![ActionEffect::Add(selection("people", "blocked", None, &[]))]);
    block.ignore_response = true;
    let mut unblock = action("unblock", "UnblockPerson", &[], vec![ActionEffect::Remove(selection("people", "blocked", None, &[]))]);
    unblock.ignore_response = true;
    let mut gift = action(
        "send_gift",
        "SendGift",
        &[("giftId", "string"), ("roomId", "id"), ("price", "int?")],
        vec![
            ActionEffect::Create { model: "notes".to_string(), values: BTreeMap::from([("roomId".to_string(), arg("roomId")), ("body".to_string(), EffectSource::Literal(json!("gift")))]) },
            ActionEffect::Change { model: "wallets".to_string(), row: None, values: BTreeMap::from([("balance".to_string(), EffectSource::Minus("price".to_string()))]) },
        ],
    );
    gift.wrap = Some("input".to_string());
    gift.variables = BTreeMap::from([("personId".to_string(), "id".to_string()), ("giftId".to_string(), "giftId".to_string())]);
    gift.root = Some("sendGift.person".to_string());
    gift.lands.push(ActionLanding { model: "notes".to_string(), root: "sendGift.note".to_string() });
    gift.result.insert("successMessage".to_string(), "string".to_string());
    block.variables.insert("personId".to_string(), "id".to_string());
    unblock.variables.insert("personId".to_string(), "id".to_string());
    spec.actions.extend([block, unblock, gift]);
    spec
}

pub fn rooms_spec() -> ModelSpec {
    let mut spec = ModelSpec::new("rooms", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str).optional()]);
    let mut open = RelationSpec::new("open", &[]);
    open.text_params = vec!["folder".to_string()];
    open.remote = Some(list("OpenRooms"));
    spec.relations.push(open);
    let room = ("roomId", EffectSource::Owner("id".to_string()));
    let mut leave = action("leave", "LeaveRoom", &[], vec![ActionEffect::Remove(selection("rooms", "open", None, &[]))]);
    let mut add = action("add_members", "AddMembers", &[("personIds", "ids")], vec![ActionEffect::Add(selection("people", "roster", Some("personIds"), &[room.clone()]))]);
    let mut remove = action(
        "remove_members",
        "RemoveMembers",
        &[("personIds", "ids")],
        vec![ActionEffect::Remove(selection("people", "roster", Some("personIds"), &[room, ("order", EffectSource::Literal(json!(ROOM_ORDER)))]))],
    );
    for declared in [&mut leave, &mut add, &mut remove] {
        declared.ignore_response = true;
        declared.variables.insert("roomId".to_string(), "id".to_string());
    }
    add.variables.insert("personIds".to_string(), "personIds".to_string());
    remove.variables.insert("personIds".to_string(), "personIds".to_string());
    spec.actions.extend([leave, add, remove]);
    spec
}

pub fn notes_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "notes",
        vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("roomId", ScalarKind::Id).indexed(), FieldSpec::scalar("body", ScalarKind::Str).optional()],
    );
    let mut thread = RelationSpec::new("thread", &["roomId"]);
    thread.remote = Some(list("NoteThread"));
    spec.relations.push(thread);
    let mut forward = action(
        "forward",
        "ForwardNote",
        &[("targetRoom", "id")],
        vec![ActionEffect::Create { model: "notes".to_string(), values: BTreeMap::from([("roomId".to_string(), arg("targetRoom")), ("body".to_string(), EffectSource::Owner("body".to_string()))]) }],
    );
    forward.variables = BTreeMap::from([("noteId".to_string(), "id".to_string()), ("roomId".to_string(), "targetRoom".to_string())]);
    forward.root = Some("forwardNote.note".to_string());
    spec.actions.push(forward);
    spec
}

pub fn wallets_spec() -> ModelSpec {
    let mut spec = ModelSpec::new("wallets", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("balance", ScalarKind::Int)]);
    let mut current = RelationSpec::global("current");
    current.remote = Some(remote("Wallet", RemoteShape::Single, "5.minutes"));
    spec.relations.push(current);
    spec
}

pub fn specs() -> Vec<ModelSpec> {
    vec![people_spec(), rooms_spec(), notes_spec(), wallets_spec()]
}

pub fn scope_key(db: &Db, model: &str, relation: &str, params: Value) -> ListKey {
    db.spec(model).unwrap().selection_key(relation, params.as_object().unwrap()).unwrap()
}

pub fn join(db: &Db, model: &str, key: &ListKey, id: &str, order_key: &str) {
    use crate::commit::{CommitEnvelope, WriteOrigin};
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops.push(ScopeOp::Append { scope: ScopeRef::new(ModelKey::new(model), key.clone()), row: RowId::new(id).unwrap(), order_key: order_key.to_string() });
    db.land(envelope).unwrap();
}

pub fn members(db: &Db, model: &str, key: &ListKey) -> Vec<String> {
    db.shared.store.scope_members(model, key.as_str()).unwrap().into_iter().map(|(id, _)| id).collect()
}
