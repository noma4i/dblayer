use serde_json::{json, Value};

use super::rig_tests::Rig;
use crate::ffi::wire::{wire_fields, Cell, WireRow};
use crate::schema::{ActionDeclaration, RootShape, AssociationKind, AssociationSpec, FieldSpec, ModelSpec, RelationSpec};
use crate::value::ScalarKind;

/// The stored members of one selection of `model`: row id and place key, in list order.
pub(super) fn selection_members(rig: &Rig, model: &str, selection: &str, params: Value) -> Vec<(String, String)> {
    let key = rig.database.db.spec(model).unwrap().selection_key(selection, params.as_object().unwrap()).unwrap();
    rig.database.db.scope_members(model, key.as_str()).unwrap()
}

fn optional(name: &str, kind: ScalarKind) -> FieldSpec { FieldSpec::scalar(name, kind).nullable().optional() }

/// A chat that points at its last message and keeps counters, as chat rows of a messenger do.
pub(super) fn chat_spec() -> ModelSpec {
    let mut chat = ModelSpec::new("Chat", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("name", ScalarKind::Str).optional(),
        optional("lastMessageId", ScalarKind::Id).derived(),
        optional("lastMessageAt", ScalarKind::Int).derived(),
        optional("lastActivityAt", ScalarKind::Int).derived(),
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
        if let Some(arg) = arg {
            action.args.insert(arg.into(), "bool".into());
            action.variables.insert(arg.into(), arg.into());
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
    chat.newer = vec!["at".to_string()];
    message["fields"].as_array_mut().unwrap().push(serde_json::to_value(optional("status", ScalarKind::Str)).unwrap());
    for field in [optional("authorId", ScalarKind::Id), optional("isForwarded", ScalarKind::Bool), optional("forwardedFromUserId", ScalarKind::Id)] {
        message["fields"].as_array_mut().unwrap().push(serde_json::to_value(field).unwrap());
    }
    let forward = message["actions"].as_array_mut().unwrap().iter_mut().find(|action| action["name"] == "forward").unwrap();
    forward["copies_receiver_to"]["set"] = json!({
        "authorId": {"source": {"row": {"model": "User", "field": "id"}}},
        "isForwarded": {"literal": true},
        "forwardedFromUserId": {"source": {"owner": "authorId"}},
    });
    let mut edit = ActionDeclaration::new("update", RootShape::Upsert);
    edit.mutation = Some("MessageEdit".into());
    edit.root = Some("messageEdit".into());
    edit.variables.insert("id".into(), "id".into());
    edit.variables.insert("text".into(), "text".into());
    edit.args.insert("text".into(), "string".into());
    let mut destroy = ActionDeclaration::new("destroy", RootShape::Destroy);
    destroy.mutation = Some("MessageDelete".into());
    destroy.variables.insert("id".into(), "id".into());
    message["actions"].as_array_mut().unwrap().extend([serde_json::to_value(edit).unwrap(), serde_json::to_value(destroy).unwrap()]);
    message["associations"].as_array_mut().unwrap().push(serde_json::to_value(chat).unwrap());
}

/// The cell of a field of a row of `model`.
pub(super) fn cell(rig: &Rig, model: &str, row: &WireRow, field: &str) -> Cell {
    let spec = rig.database.db.spec(model).unwrap();
    let at = wire_fields(spec).iter().position(|declared| declared.name == field).unwrap_or_else(|| panic!("{model} has no field {field}"));
    row.cells[at].clone()
}

pub(super) fn text(value: &str) -> Cell { Cell::Text { value: value.into() } }

pub(super) fn int(value: i64) -> Cell { Cell::Int { value } }

pub(super) fn chat_row(id: &str, last_message_at: i64) -> Value { json!({"id": id, "name": id, "lastMessageId": null, "lastMessageAt": last_message_at, "lastActivityAt": last_message_at, "unreadCount": 0, "pinned": false, "muted": false}) }

/// The reactor of a reaction is the row of a user: the nested `user` links to `User` by `userId`.
pub(super) fn add_reactor_user(models: &mut Value) {
    let message = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Message").unwrap();
    let reactions = message["fields"].as_array_mut().unwrap().iter_mut().find(|field| field["name"] == "reactions").unwrap();
    let mut user = FieldSpec::object("user").nullable();
    user.link = Some(crate::schema::FieldLink { model: "User".into(), id: "userId".into() });
    user.fields = vec![FieldSpec::scalar("name", ScalarKind::Str).nullable().optional()];
    reactions["fields"].as_array_mut().unwrap().push(serde_json::to_value(user).unwrap());
}

/// Every text a cell holds, nested cells included.
pub(super) fn texts(cell: &Cell) -> Vec<String> {
    match cell {
        Cell::Text { value } => vec![value.clone()],
        Cell::Seq { items } => items.iter().flat_map(texts).collect(),
        Cell::Keyed { entries } => entries.iter().flat_map(|entry| texts(&entry.value)).collect(),
        _ => Vec::new(),
    }
}

/// The user row of the account and of other users: counters of the account, the connection with a chat, the blocked list, and the actions on them.
pub(super) fn add_profile(models: &mut Value) {
    let models = models.as_array_mut().unwrap();
    let chat = models.iter_mut().find(|model| model["key"] == "Chat").unwrap();
    chat["fields"].as_array_mut().unwrap().push(serde_json::to_value(optional("connectionStatus", ScalarKind::Str)).unwrap());
    let user = models.iter_mut().find(|model| model["key"] == "User").unwrap();
    {
    let name = "connectionStatus"; user["fields"].as_array_mut().unwrap().push(serde_json::to_value(optional(name, ScalarKind::Str)).unwrap()); }
    for name in ["unreadChatsCount", "secondaryChatsCount", "premiumSecondaryChatsCount", "unreadSecondaryChatsCount"] { user["fields"].as_array_mut().unwrap().push(serde_json::to_value(optional(name, ScalarKind::Int)).unwrap()); }
    let mut blocked = user["relations"].as_array().unwrap().iter().find(|relation| relation["name"] == "all").unwrap().clone();
    blocked["name"] = json!("blocked");
    blocked["remote"]["operation"] = json!("BlockedUsers");
    user["relations"].as_array_mut().unwrap().push(blocked);
    let mut update = ActionDeclaration::new("update", RootShape::Upsert);
    update.mutation = Some("ProfileUpdate".into());
    update.variables.insert("name".into(), "name".into());
    update.args.insert("name".into(), "string".into());
    update.ignore_response = true;
    update.refetch.push("User.profile".into());
    let mut answer = ActionDeclaration::new("answer_connection", RootShape::Upsert);
    answer.mutation = Some("AnswerConnection".into());
    answer.root = Some("answerConnection.user".into());
    answer.variables.insert("userId".into(), "id".into());
    answer.variables.insert("chatId".into(), "chatId".into());
    answer.args.insert("chatId".into(), "id".into());
    answer.lands.push(crate::schema::ActionLanding { model: "Chat".into(), root: "answerConnection.chat".into() });
    answer.refetch.push("User.profile".into());
    let mut actions = vec![update, answer];
    for (name, mutation) in [("block", "BlockUser"), ("unblock", "UnblockUser")] {
        let mut action = ActionDeclaration::new(name, RootShape::Upsert);
        action.mutation = Some(mutation.into());
        action.variables.insert("id".into(), "id".into());
        action.ignore_response = true;
        action.refetch.push("User.blocked".into());
        actions.push(action);
    }
    user["actions"].as_array_mut().unwrap().extend(actions.into_iter().map(|action| serde_json::to_value(action).unwrap()));
}

/// A moment with a reaction counter, an unread counter of the compass, and the actions on them.
pub(super) fn add_moment(models: &mut Value) {
    let models = models.as_array_mut().unwrap();
    let user = models.iter_mut().find(|model| model["key"] == "User").unwrap();
    user["fields"].as_array_mut().unwrap().push(serde_json::to_value(optional("unreadCompassCount", ScalarKind::Int)).unwrap());
    let mut moment = ModelSpec::new("Moment", vec![
        FieldSpec::scalar("id", ScalarKind::Id),
        FieldSpec::scalar("userId", ScalarKind::Id).indexed(),
        FieldSpec::scalar("text", ScalarKind::Str).optional(),
        optional("reacted", ScalarKind::Bool),
        optional("visitsCount", ScalarKind::Int),
        optional("unreadSimilarMomentsCount", ScalarKind::Int),
    ]);
    moment.relations.push(RelationSpec::global("feed"));
    let mut react = ActionDeclaration::new("react", RootShape::Upsert);
    react.mutation = Some("MomentReact".into());
    react.root = Some("momentReact".into());
    react.variables.insert("id".into(), "id".into());
    let mut read = ActionDeclaration::new("read_compass", RootShape::Upsert);
    read.mutation = Some("MomentReadCompass".into());
    read.root = Some("momentReadCompass".into());
    read.variables.insert("id".into(), "id".into());
    read.refetch.push("User.profile".into());
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("MomentCreate".into());
    create.root = Some("momentCreate".into());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    for name in ["userId", "text"] { create.variables.insert(name.into(), name.into()); }
    let mut destroy = ActionDeclaration::new("destroy", RootShape::Destroy);
    destroy.mutation = Some("MomentDelete".into());
    destroy.variables.insert("id".into(), "id".into());
    moment.actions.extend([react, read, create, destroy]);
    models.push(serde_json::to_value(moment).unwrap());
}

/// The balance of the account, the history of the wallet and the gift that moves both and posts a message.
pub(super) fn add_wallet(models: &mut Value) {
    let models = models.as_array_mut().unwrap();
    let user = models.iter_mut().find(|model| model["key"] == "User").unwrap();
    user["fields"].as_array_mut().unwrap().push(serde_json::to_value(optional("balance", ScalarKind::Int)).unwrap());
    let mut gift = ActionDeclaration::new("send_gift", RootShape::Upsert);
    gift.mutation = Some("SendGift".into());
    gift.root = Some("sendGift.user".into());
    gift.variables.insert("chatId".into(), "chatId".into());
    gift.variables.insert("amount".into(), "amount".into());
    gift.args.insert("chatId".into(), "id".into());
    gift.args.insert("amount".into(), "int".into());
    gift.lands.push(crate::schema::ActionLanding { model: "WalletTransaction".into(), root: "sendGift.transaction".into() });
    gift.lands.push(crate::schema::ActionLanding { model: "Message".into(), root: "sendGift.message".into() });
    user["actions"].as_array_mut().unwrap().push(serde_json::to_value(gift).unwrap());
    let mut transaction = ModelSpec::new("WalletTransaction", vec![FieldSpec::scalar("id", ScalarKind::Id), optional("amount", ScalarKind::Int)]);
    transaction.relations.push(RelationSpec::global("history"));
    models.push(serde_json::to_value(transaction).unwrap());
}

/// The search of chats returns rows of the chat model itself.
pub(super) fn add_search(models: &mut Value) {
    let models = models.as_array_mut().unwrap();
    let chat = models.iter_mut().find(|model| model["key"] == "Chat").unwrap();
    let mut search = RelationSpec::new("search", &[]);
    search.text_params = vec!["query".to_string()];
    search.remote = Some(crate::schema_fixtures::remote("ChatSearch", crate::schema::RemoteShape::List { coverage: crate::schema::Coverage::Complete }, "1.minute"));
    chat["relations"].as_array_mut().unwrap().push(serde_json::to_value(search).unwrap());
}

/// Who is typing in a chat: a row in memory that expires after 8 seconds.
pub(super) fn add_typing(models: &mut Value) {
    let mut action = ModelSpec::new("ChatAction", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("chatId", ScalarKind::Id).indexed(), optional("userId", ScalarKind::Id), optional("kind", ScalarKind::Str)]);
    action.storage = crate::schema::Storage::Memory;
    action.expire_after = Some("8.seconds".into());
    action.removed_by.push(crate::schema::RemovedBy { model: "Message".into(), matches: [("chatId".to_string(), "chatId".to_string()), ("userId".to_string(), "authorId".to_string())].into() });
    models.as_array_mut().unwrap().push(serde_json::to_value(action).unwrap());
}

/// Media of a message with merge rules (a server URL is never emptied, progress never falls), a local preview, a gift with a local opened flag, and the local media counters of a chat.
pub(super) fn add_media(models: &mut Value) {
    use crate::schema::MergeRule;
    let models = models.as_array_mut().unwrap();
    let chat = models.iter_mut().find(|model| model["key"] == "Chat").unwrap();
    for name in ["mediaSequence", "mediaPhotoCount"] {
        let local = FieldSpec { merge: Some(MergeRule::Local), ..optional(name, ScalarKind::Int) };
        chat["fields"].as_array_mut().unwrap().push(serde_json::to_value(local).unwrap());
    }
    let message = models.iter_mut().find(|model| model["key"] == "Message").unwrap();
    let mut media = FieldSpec::object("media").nullable().optional();
    media.fields = vec![optional("fileUrl", ScalarKind::Str), optional("transcodeProgress", ScalarKind::Int)];
    media.merge = Some(MergeRule::Keys { keys: [("fileUrl".to_string(), MergeRule::NonEmpty), ("transcodeProgress".to_string(), MergeRule::Monotonic)].into() });
    let mut gift = FieldSpec::object("gift").nullable().optional();
    gift.fields = vec![optional("status", ScalarKind::Str)];
    let fields = [
        media,
        gift,
        FieldSpec { merge: Some(MergeRule::Local), ..optional("localPreviewUrl", ScalarKind::Str) },
        FieldSpec { merge: Some(MergeRule::Local), ..optional("giftOpened", ScalarKind::Bool) },
    ];
    message["fields"].as_array_mut().unwrap().extend(fields.map(|field| serde_json::to_value(field).unwrap()));
}

/// The inbox lists of the chat: a chat belongs to the lists it names, the node read of a chat puts it into them, the group chat is renamed by `update`.
pub(super) fn add_lists(models: &mut Value) {
    use crate::schema::{Coverage, Membership, NodeRead, RemoteShape, VariableSource, NODE_PARAM};
    let models = models.as_array_mut().unwrap();
    let chat = models.iter_mut().find(|model| model["key"] == "Chat").unwrap();
    chat["fields"].as_array_mut().unwrap().push(serde_json::to_value(FieldSpec::list("lists", ScalarKind::Str).nullable().optional()).unwrap());
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.into()];
    node.node = Some(NodeRead::One);
    let mut remote = crate::schema_fixtures::remote("ChatNode", RemoteShape::Single, "5.minutes");
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    list.membership.insert("list".into(), Membership::Contains("lists".into()));
    list.joins_node = true;
    list.joins_events = true;
    list.joins_created = true;
    let mut listed = crate::schema_fixtures::remote("ChatList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes");
    listed.variables.insert("list".into(), VariableSource::Param { name: "list".into() });
    list.remote = Some(listed);
    chat["relations"].as_array_mut().unwrap().extend([serde_json::to_value(node).unwrap(), serde_json::to_value(list).unwrap()]);
    let mut update = ActionDeclaration::new("update", RootShape::Upsert);
    update.mutation = Some("ChatUpdate".into());
    update.root = Some("chatUpdate".into());
    update.variables.insert("id".into(), "id".into());
    update.variables.insert("name".into(), "name".into());
    update.args.insert("name".into(), "string".into());
    chat["actions"].as_array_mut().unwrap().push(serde_json::to_value(update).unwrap());
}

/// Places that hold the same change end on one revision: a reader that pulls after a second commit takes both commits in one value at the revision of the last (P-DLV-17, dlv_live_read_unacknowledged), so the first revision of a change is only bounded below by its commit.
pub(super) async fn one_revision(handle: &impl crate::test_wait::Quiet, places: &[&crate::ffi::tape::Rows]) {
    crate::test_wait::settled(handle, "every place ends on one revision", || {
        let last: Vec<Option<i64>> = places.iter().map(|rows| rows.last().map(|seen| seen.revision)).collect();
        last[0].is_some() && last.iter().all(|revision| *revision == last[0])
    }).await
}
