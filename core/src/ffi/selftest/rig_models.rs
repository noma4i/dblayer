use serde_json::{json, Value};
use crate::ffi::tests::lifecycle_tests::models;

const OPERATIONS: &str = r#"[
    {"name":"MyProfile","document":"query MyProfile { me { id name } }"},
    {"name":"UserNode","document":"query UserNode($id: ID!) { node(id: $id) { ... on User { id name } } }"},
    {"name":"Users","document":"query Users { users { id name } }"},
    {"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"},
    {"name":"UnregisterDevice","document":"mutation UnregisterDevice { unregisterDevice { ok } }"},
    {"name":"Thread","document":"query Thread($chatId: ID!) { thread(chatId: $chatId) { id chatId text at } }"},
    {"name":"SendMessage","document":"mutation SendMessage($chatId: ID!, $text: String!, $at: Int!) { sendMessage(chatId: $chatId, text: $text, at: $at) { id chatId text at } }"},
    {"name":"CreateGroup","document":"mutation CreateGroup($name: String!) { createGroup(name: $name) { id name } }"},
    {"name":"LeaveGroup","document":"mutation LeaveGroup($groupId: ID!) { leaveGroup(groupId: $groupId) { id name left membership } }"},
    {"name":"AddMember","document":"mutation AddMember($groupId: ID!, $userIds: [ID!]!) { addMember(groupId: $groupId, userIds: $userIds) { id name left membership } }"},
    {"name":"RemoveMember","document":"mutation RemoveMember($groupId: ID!, $userIds: [ID!]!) { removeMember(groupId: $groupId, userIds: $userIds) { id name left membership } }"},
    {"name":"GroupMembers","document":"query GroupMembers($groupId: ID!, $order: String!) { groupMembers(groupId: $groupId, order: $order) { id name } }"},
    {"name":"MessageForward","document":"mutation MessageForward($input: MessageForwardInput!) { messageForward(input: $input) { message { id chatId text at authorId isForwarded forwardedFromUserId } chat { id updatedAt } } }"},
    {"name":"MessageReact","document":"mutation MessageReact($id: ID!, $body: String!, $userId: ID!) { messageReact(id: $id, body: $body, userId: $userId) { id chatId text at reactions { body userId } } }"},
    {"name":"MemberChanged","document":"subscription MemberChanged { memberChanged { id name } }"},
    {"name":"ProfileUpdate","document":"mutation ProfileUpdate($name: String!) { profileUpdate(name: $name) { ok } }"},
    {"name":"AnswerConnection","document":"mutation AnswerConnection($userId: ID!, $chatId: ID!) { answerConnection(userId: $userId, chatId: $chatId) { user { id connectionStatus } chat { id connectionStatus name lists } } }"},
    {"name":"BlockUser","document":"mutation BlockUser($id: ID!) { blockUser(id: $id) { id } }"},
    {"name":"UnblockUser","document":"mutation UnblockUser($id: ID!) { unblockUser(id: $id) { id } }"},
    {"name":"BlockedUsers","document":"query BlockedUsers { blockedUsers { id name } }"},
    {"name":"MomentReact","document":"mutation MomentReact($id: ID!) { momentReact(id: $id) { id reacted visitsCount } }"},
    {"name":"MomentReadCompass","document":"mutation MomentReadCompass($id: ID!) { momentReadCompass(id: $id) { id unreadSimilarMomentsCount } }"},
    {"name":"MomentCreate","document":"mutation MomentCreate($userId: ID!, $text: String!) { momentCreate(userId: $userId, text: $text) { id userId text } }"},
    {"name":"MomentDelete","document":"mutation MomentDelete($id: ID!) { momentDelete(id: $id) { id } }"},
    {"name":"SendGift","document":"mutation SendGift($chatId: ID!, $amount: Int!) { sendGift(chatId: $chatId, amount: $amount) { user { id balance } transaction { id amount } message { id chatId text at } } }"},
    {"name":"ChatSearch","document":"query ChatSearch($query: String!) { chatSearch(query: $query) { id name pinned } }"},
    {"name":"ChatList","document":"query ChatList($list: String) { chats(list: $list) { id name lists } }"},
    {"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id name lists } } }"},
    {"name":"ChatUpdate","document":"mutation ChatUpdate($id: ID!, $name: String!) { chatUpdate(id: $id, name: $name) { id name } }"},
    {"name":"ChatPin","document":"mutation ChatPin($id: ID!, $pinned: Boolean!) { chatPin(id: $id, pinned: $pinned) { id pinned } }"},
    {"name":"ChatMute","document":"mutation ChatMute($id: ID!, $muted: Boolean!) { chatMute(id: $id, muted: $muted) { id muted } }"},
    {"name":"ChatMarkRead","document":"mutation ChatMarkRead($id: ID!) { chatMarkRead(id: $id) { id unreadCount } }"},
    {"name":"MessageEdit","document":"mutation MessageEdit($id: ID!, $text: String!) { messageEdit(id: $id, text: $text) { id chatId text at } }"},
    {"name":"MessageDelete","document":"mutation MessageDelete($id: ID!) { messageDelete(id: $id) { id } }"},
    {"name":"DeleteGroup","document":"mutation DeleteGroup($id: ID!) { deleteGroup(id: $id) { id } }"},
    {"name":"SendMessageFile","document":"mutation SendMessageFile($chatId: ID!, $text: String!, $at: Int!, $signedId: String!) { sendMessageFile(chatId: $chatId, text: $text, at: $at, signedId: $signedId) { id chatId text at } }","rootPath":"sendMessageFile"},
    {"name":"MomentCreateFile","document":"mutation MomentCreateFile($userId: ID!, $text: String!, $signedId: String!) { momentCreateFile(userId: $userId, text: $text, signedId: $signedId) { id userId text } }","rootPath":"momentCreateFile"},
    {"name":"CreateBlob","document":"mutation CreateBlob { createBlob { url headers signedId blobId } }","rootPath":"createBlob"},
    {"name":"InitMultipart","document":"mutation InitMultipart { initMultipart { key uploadId } }","rootPath":"initMultipart"},
    {"name":"PresignParts","document":"mutation PresignParts { presignParts { parts { partNumber url headers } } }","rootPath":"presignParts"},
    {"name":"CompleteMultipart","document":"mutation CompleteMultipart { completeMultipart { signedId blobId } }","rootPath":"completeMultipart"},
    {"name":"AbortMultipart","document":"mutation AbortMultipart { abortMultipart { ok } }","rootPath":"abortMultipart"},
    {"name":"ChatUpdatePhoto","document":"mutation ChatUpdatePhoto($id: ID!, $signedId: String!) { chatUpdatePhoto(id: $id, signedId: $signedId) { id logoUrl } }","rootPath":"chatUpdatePhoto"}
]"#;

/// The plain harness also holds a memory model that a server subscription feeds.
pub(super) fn plain_models() -> String {
    let mut models: Vec<crate::schema::ModelSpec> = serde_json::from_str(&models()).unwrap();
    add_user_ephemeral_and_nodes(&mut models[0]);
    let mut notice = crate::schema::ModelSpec::new("Notice", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    notice.storage = crate::schema::Storage::Memory;
    let mut relation = crate::schema::RelationSpec::global("recent");
    relation.updates.push(crate::schema::UpdatesSpec { apply: None, subscription: "Notices".into(), deleted_by: None, deleted_unless: None, set: serde_json::Map::new(), removes: false, debounce: None, root: None, id: Vec::new() });
    notice.relations.push(relation);
    models.push(notice);
    models.push(message_spec());
    models.push(group_spec());
    models.push(member_spec());
    serde_json::to_string(&models).unwrap()
}

/// The admission matrix reads `typing` through `set_ephemeral` and the many-node relation `nodes` through `nodes`.
fn add_user_ephemeral_and_nodes(user: &mut crate::schema::ModelSpec) {
    use crate::schema::{Coverage, EphemeralSpec, FieldSpec, NodeRead, RelationSpec, RemoteShape};
    let mut typing = FieldSpec::scalar("typing", crate::value::ScalarKind::Bool);
    typing.default = Some(json!(false));
    user.ephemeral.push(EphemeralSpec::new(typing));
    let mut nodes = RelationSpec::global("nodes");
    nodes.remote = Some(crate::schema_fixtures::remote("UsersByIds", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    nodes.node = Some(NodeRead::Many);
    user.relations.push(nodes);
}

/// The chat message of the bridge sequences: a thread read from the server and a create that works offline.
fn message_spec() -> crate::schema::ModelSpec {
    use crate::schema::{ActionDeclaration, Coverage, FieldSpec, RelationSpec, RemoteShape, RootShape};
    let mut message = crate::schema::ModelSpec::new("Message", vec![
        FieldSpec::scalar("id", crate::value::ScalarKind::Id),
        FieldSpec::scalar("chatId", crate::value::ScalarKind::Id).indexed(),
        FieldSpec::scalar("text", crate::value::ScalarKind::Str),
        FieldSpec::scalar("at", crate::value::ScalarKind::Int),
    ]);
    let mut thread = RelationSpec::new("thread", &["chatId"]);
    thread.remote = Some(crate::schema_fixtures::remote("Thread", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    message.relations.push(thread);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("SendMessage".into());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    for field in ["chatId", "text", "at"] { create.variables.insert(field.into(), field.into()); }
    message.actions.push(create);
    let mut forward = ActionDeclaration::new("forward", RootShape::Upsert);
    forward.mutation = Some("MessageForward".into());
    forward.args.insert("targetChatId".into(), "string".into());
    forward.variables.insert("messageId".into(), "id".into());
    forward.variables.insert("chatId".into(), "targetChatId".into());
    forward.wrap = Some("input".into());
    forward.root = Some("messageForward.message".into());
    forward.copies_receiver_to = Some(crate::schema::ReceiverCopy { arg: "targetChatId".into(), field: "chatId".into(), set: Default::default() });
    message.actions.push(forward);
    let mut edit = ActionDeclaration::new("edit", RootShape::Upsert);
    edit.mutation = Some("MessageEdit".into());
    edit.variables.insert("id".into(), "id".into());
    edit.variables.insert("text".into(), "text".into());
    message.actions.push(edit);
    let mut upload = ActionDeclaration::new("upload", RootShape::Upsert);
    upload.mutation = Some("SendMessageFile".into());
    upload.upload = true;
    upload.root = Some("sendMessageFile".into());
    message.actions.push(upload);
    message.upload_steps = Some(crate::owners::write::upload::UploadOperations::test_steps());
    message
}

/// A model whose create goes to the server and is not durable: it needs the network and the answer of the server.
fn group_spec() -> crate::schema::ModelSpec {
    use crate::schema::{ActionDeclaration, FieldSpec, RootShape};
    let mut left = FieldSpec::scalar("left", crate::value::ScalarKind::Bool);
    left.nullable = true;
    let mut membership = FieldSpec::scalar("membership", crate::value::ScalarKind::Str);
    membership.nullable = true;
    let mut group = crate::schema::ModelSpec::new("Group", vec![FieldSpec::scalar("id", crate::value::ScalarKind::Id), FieldSpec::scalar("name", crate::value::ScalarKind::Str), left, membership]);
    let mut create = ActionDeclaration::new("create", RootShape::Upsert);
    create.mutation = Some("CreateGroup".into());
    create.has_optimistic = true;
    create.has_temp_row_retention = true;
    create.variables.insert("name".into(), "name".into());
    group.actions.push(create);
    group.relations.push(crate::schema::RelationSpec::global("all"));
    let mut leave = ActionDeclaration::new("leave", RootShape::Upsert);
    leave.mutation = Some("LeaveGroup".into());
    leave.variables.insert("groupId".into(), "id".into());
    leave.ignore_response = true;
    leave.removes_receiver = true;
    group.actions.push(leave);
    for (name, mutation, root, kind) in [("add_member", "AddMember", "addMember", crate::schema::ScopeEffectKind::Join), ("remove_member", "RemoveMember", "removeMember", crate::schema::ScopeEffectKind::Leave)] {
        let mut action = ActionDeclaration::new(name, RootShape::Upsert);
        action.mutation = Some(mutation.into());
        action.args.insert("userIds".into(), "ids".into());
        action.variables.insert("groupId".into(), "id".into());
        action.variables.insert("userIds".into(), "userIds".into());
        action.root = Some(root.into());
        action.refetch.push("Member.group_members".into());
        action.scope_effects.push(crate::schema::ScopeEffect {
            kind,
            model: "Member".into(),
            scope: "group_members".into(),
            rows: "userIds".into(),
            with: [("groupId".to_string(), "id".to_string())].into_iter().collect(),
            order_param: "order".into(),
            orders: vec!["JOINED".into(), "OWNER_FIRST".into()],
        });
        group.actions.push(action);
    }
    group
}

/// The member of a group: two lists of the same rows, one ordered by the join and one with the owner first.
fn member_spec() -> crate::schema::ModelSpec {
    use crate::schema::{FieldSpec, RelationSpec};
    let mut member = crate::schema::ModelSpec::new("Member", vec![FieldSpec::scalar("id", crate::value::ScalarKind::Id), FieldSpec::scalar("name", crate::value::ScalarKind::Str)]);
    let mut members = RelationSpec::new("group_members", &[]);
    members.text_params = vec!["groupId".to_string(), "order".to_string()];
    members.remote = Some(crate::schema_fixtures::remote("GroupMembers", crate::schema::RemoteShape::List { coverage: crate::schema::Coverage::Complete }, "5.minutes"));
    member.relations.push(members);
    member
}

pub(super) fn plain_operations() -> String {
    let mut operations: Vec<Value> = serde_json::from_str(OPERATIONS).unwrap();
    operations.push(json!({"name":"RigEvents","document":"subscription RigEvents { rigEvents { id } }","rootPath":"rigEvents"}));
    operations.push(json!({"name":"Notices","document":"subscription Notices { notices { id } }","rootPath":"notices"}));
    operations.push(json!({"name":"UsersByIds","document":"query UsersByIds($ids: [ID!]!) { nodes(ids: $ids) { ... on User { id name } } }"}));
    json!(operations).to_string()
}

pub(super) fn add_reactions(models: &mut Value) {
    let message = models.as_array_mut().unwrap().iter_mut().find(|model| model["key"] == "Message").unwrap();
    let mut field = crate::schema::FieldSpec::array("reactions");
    field.app_type = Some("Reaction".into());
    field.default = Some(json!([]));
    field.fields = vec![crate::schema::FieldSpec::scalar("body", crate::value::ScalarKind::Str), crate::schema::FieldSpec::scalar("userId", crate::value::ScalarKind::Id)];
    message["fields"].as_array_mut().unwrap().push(serde_json::to_value(field).unwrap());
    let mut action = crate::schema::ActionDeclaration::new("react", crate::schema::RootShape::Upsert);
    action.mutation = Some("MessageReact".into());
    action.args.insert("body".into(), "string".into());
    action.args.insert("userId".into(), "id".into());
    action.variables.insert("id".into(), "id".into());
    action.variables.insert("body".into(), "body".into());
    action.variables.insert("userId".into(), "userId".into());
    action.optimistic.insert("reactions".into(), json!({"$keyed":{"by":"userId","element":{"body":{"$arg":"body"},"userId":{"$arg":"userId"}}}}));
    message["actions"].as_array_mut().unwrap().push(serde_json::to_value(action).unwrap());
}
