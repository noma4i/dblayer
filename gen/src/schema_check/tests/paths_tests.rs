use std::fs;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query {
  node(id: ID!): Node
  chats: ChatConnection!
  roster: [Chat!]!
  pricing: Pricing
}
interface Node { id: ID! }
type Chat implements Node { id: ID! title: String messages: [Message!]! members: UserConnection! membersCount: Int! unseenCount: Int! }
type User implements Node { id: ID! name: String }
type Message { id: ID! body: String }
type PageInfo { hasNextPage: Boolean! }
type ChatConnection { nodes: [Chat!]! pageInfo: PageInfo! }
type UserConnection { edges: [UserEdge!]! pageInfo: PageInfo! totalCount: Int! lastSequence: Int }
type UserEdge { cursor: String! node: User! }
type ChatGroupPayload { chat: Chat message: Message removedChatId: ID note: String }
type Failure { reason: String! }
union ChatResult = ChatGroupPayload | Failure
input ChatGroupInput { title: String }
type Mutation {
  createChat(input: ChatGroupInput!): ChatGroupPayload
  updateChat(input: ChatGroupInput!): ChatGroupPayload
  leaveChat(chatId: ID!): ChatGroupPayload
  tryChat(input: ChatGroupInput!): ChatResult
}
type Pricing { gifts: [Gift!]! flag: Boolean }
type Gift { id: ID! name: String }
type Subscription { chatChanged: ChatGroupPayload }
"#;

const DOCUMENTS: &str = r#"
query Chats { chats { nodes { id title } } }
query ChatMembers($id: ID!, $first: Int, $after: String) { node(id: $id) { ... on Chat { membersCount members(first: $first, after: $after) { edges { cursor node { id name } } pageInfo { hasNextPage } totalCount lastSequence } } } }
query ChatMessages($id: ID!) { node(id: $id) { ... on Chat { title } } }
query Pricing { pricing { gifts { id name } flag } }
query Roster { roster { id title } }
query Aliased { found: roster { id label: title } }
mutation CreateChat($input: ChatGroupInput!) { createChat(input: $input) { chat { id title } message { id } note } }
mutation UpdateChat($input: ChatGroupInput!) { updateChat(input: $input) { chat { id title } } }
mutation LeaveChat($chatId: ID!) { leaveChat(chatId: $chatId) { removedChatId } }
mutation AliasChat($input: ChatGroupInput!) { made: createChat(input: $input) { room: chat { id title } } }
mutation SpreadChat($input: ChatGroupInput!) { createChat(input: $input) { ...Landed } }
mutation PlainInlineChat($input: ChatGroupInput!) { createChat(input: $input) { ... { chat { id title } } } }
mutation TryChat($input: ChatGroupInput!) { tryChat(input: $input) { __typename ... on ChatGroupPayload { chat { id title } } ... on Failure { reason } } }
subscription ChatChanged { chatChanged { chat { id title } } }
fragment Landed on ChatGroupPayload { chat { id title } }
"#;

fn model(lines: &str) -> String {
    format!("model Chat do\n  attribute :title, :string, null: true\n  scope :all do\n    query \"Chats\"\n    paginate size: 20, direction: :forward\n  end\n{lines}end\n")
}

fn standard(name: &str, mutation: &str, root: &str) -> String {
    format!("  operation :{name} do\n    mutation \"{mutation}\"\n    wrap \"input\"\n    response from: \"{root}\"\n  end\n")
}

fn action(name: &str, mutation: &str, lines: &str) -> String {
    format!("  operation :{name} do\n    mutation \"{mutation}\"\n    optimistic false\n{lines}  end\n")
}

fn members(options: &str) -> String {
    format!("model User do\n  attribute :name, :string, null: true\n  scope :members do\n    parameter :chatId, :string\n    query \"ChatMembers\"{options}\n    paginate size: 30, direction: :forward\n  end\nend\n")
}

fn run(source: &str) -> Result<(), Vec<(usize, String)>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("documents")).unwrap();
    fs::write(directory.path().join("documents/all.graphql"), DOCUMENTS).unwrap();
    let file = directory.path().join("chat.dbl");
    fs::write(&file, source).unwrap();
    let mut files = vec![file];
    if !source.contains("model Message") {
        let message = directory.path().join("message.dbl");
        fs::write(&message, "model Message do\n  attribute :body, :string, null: true\nend\n").unwrap();
        files.push(message);
    }
    check_models_against_schema(&schema, &[directory.path().join("documents")], &files).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| (error.error.line, error.error.message)).collect())
}

fn line_of(source: &str, marker: &str) -> usize {
    source.lines().position(|line| line.contains(marker)).map(|index| index + 1).unwrap_or_else(|| panic!("no line with {marker}"))
}

fn refused(name: &str, source: &str, marker: &str, expected: &[&str]) {
    let line = line_of(source, marker);
    match run(source) {
        Ok(()) => panic!("{name}: accepted"),
        Err(errors) => assert!(errors.iter().any(|(found, text)| *found == line && expected.iter().all(|part| text.contains(part))), "{name}: line {line} {expected:?} not in {errors:?}"),
    }
}

#[test]
fn stale_root_prefixes_of_model_operations_are_refused() {
    let create = model(&standard("create", "CreateChat", "chatGroupCreate.chat"));
    refused("create with the old prefix", &create, "response from: \"chatGroupCreate.chat\"", &["response from `chatGroupCreate.chat`", "`CreateChat`", "no `chatGroupCreate`", "top level", "createChat"]);
    let update = model(&standard("update", "UpdateChat", "chatGroupUpdate.chat"));
    refused("update with the old prefix", &update, "response from: \"chatGroupUpdate.chat\"", &["no `chatGroupUpdate`", "updateChat"]);
    let leave = model(&action("leave", "LeaveChat", "    wrap \"input\"\n    response from: \"leaveChat.chat\"\n"));
    refused("answer carries removedChatId", &leave, "response from: \"leaveChat.chat\"", &["no `chat` under `leaveChat`", "removedChatId"]);
}

#[test]
fn a_root_inside_the_answer_is_accepted_through_every_form_of_selection() {
    let cases = [
        ("plain", model(&standard("create", "CreateChat", "createChat.chat"))),
        ("alias on both levels", model(&standard("create", "AliasChat", "made.room"))),
        ("fragment spread", model(&standard("create", "SpreadChat", "createChat.chat"))),
        ("inline fragment without a type", model(&standard("create", "PlainInlineChat", "createChat.chat"))),
        ("union branch", model(&standard("create", "TryChat", "tryChat.chat"))),
        ("update", model(&standard("update", "UpdateChat", "updateChat.chat"))),
        ("second row of the payload", model(&action("send", "CreateChat", "    wrap \"input\"\n    response from: \"createChat.chat\"\n    upsert Message, from: \"createChat.message\"\n"))),
    ];
    for (name, source) in cases {
        if let Err(errors) = run(&source) {
            panic!("{name}: {errors:?}");
        }
    }
}

#[test]
fn a_root_that_names_the_schema_field_behind_an_alias_is_refused() {
    let source = model(&standard("create", "AliasChat", "made.chat"));
    refused("schema name instead of the alias", &source, "response from: \"made.chat\"", &["no `chat` under `made`", "room"]);
}

#[test]
fn a_landing_path_the_answer_lacks_is_refused() {
    let stale = model(&action("send", "CreateChat", "    wrap \"input\"\n    response from: \"createChat.chat\"\n    upsert Message, from: \"chatGroupCreate.message\"\n"));
    refused("landing with the old prefix", &stale, "upsert Message", &["upsert `chatGroupCreate.message`", "no `chatGroupCreate`", "createChat"]);
    let missing = model(&action("send", "UpdateChat", "    wrap \"input\"\n    response from: \"updateChat.chat\"\n    upsert Message, from: \"updateChat.message\"\n"));
    refused("landing the document does not select", &missing, "upsert Message", &["no `message` under `updateChat`", "chat"]);
}

#[test]
fn result_names_must_be_keys_of_the_payload() {
    let good = model(&action("send", "CreateChat", "    wrap \"input\"\n    response from: \"createChat.chat\"\n    returns do\n      attribute :note, :string\n    end\n"));
    run(&good).unwrap();
    let bad = model(&action("send", "CreateChat", "    wrap \"input\"\n    response from: \"createChat.chat\"\n    returns do\n      attribute :successMessage, :string\n    end\n"));
    refused("result key the payload lacks", &bad, "attribute :successMessage", &["returns `successMessage`", "no `successMessage` under `createChat`", "note"]);
}

#[test]
fn a_fetch_root_the_document_does_not_select_is_refused() {
    let broken = "model Message do\n  attribute :body, :string, null: true\n  scope :messages do\n    parameter :chatId, :string\n    query \"ChatMessages\"\n    response from: \"node.messages\", shape: :list\n  end\nend\n";
    refused("root past what the document selects", broken, "response from: \"node.messages\"", &["response from `node.messages`", "no `messages` under `node`", "title"]);
    let renamed = "model Chat do\n  attribute :title, :string, null: true\n  scope :found do\n    parameter :chatId, :string\n    query \"Aliased\"\n    response from: \"found\", shape: :list\n  end\nend\n";
    run(renamed).unwrap();
    let schema_name = renamed.replace("from: \"found\"", "from: \"roster\"");
    refused("schema name of an aliased root", &schema_name, "query \"Aliased\"", &["must start with the first field `found`"]);
}

#[test]
fn a_fetch_root_through_node_edges_and_nodes_is_accepted() {
    run(&members("\n    response from: \"node.members\"")).unwrap();
    run(&model("")).unwrap();
}

#[test]
fn total_and_cursor_paths_follow_the_same_selection() {
    let total = |path: &str| members("\n    response from: \"node.members\"").replace("    paginate size: 30, direction: :forward\n", &format!("    paginate size: 30, direction: :forward\n    total from: \"{path}\"\n"));
    run(&total("node.membersCount")).unwrap();
    refused("total the document does not select", &total("node.unseenCount"), "total from", &["total `node.unseenCount`", "no `unseenCount` under `node`", "membersCount"]);
    let cursor = |path: &str| members("\n    response from: \"node.members\"").replace("paginate size: 30, direction: :forward", &format!("paginate size: 30, direction: :forward, cursor: {{ parameter: \"afterSequence\", from: \"{path}\", type: :int }}"));
    run(&cursor("lastSequence")).unwrap();
    refused("cursor the connection lacks", &cursor("lastSequenceNumber"), "paginate size: 30, direction: :forward", &["cursor from `lastSequenceNumber`", "no `lastSequenceNumber`", "lastSequence"]);
}

#[test]
fn subscription_roots_follow_the_subscription_document() {
    let updates = |root: &str| format!("model Chat do\n  attribute :title, :string, null: true\n  scope :all do\n    query \"Chats\"\n    paginate size: 20, direction: :forward\n    subscribe \"ChatChanged\", from: \"{root}\"\n  end\nend\n");
    run(&updates("chatChanged.chat")).unwrap();
    refused("updates root past the event", &updates("chatChanged.room"), "subscribe \"ChatChanged\"", &["subscribe from `chatChanged.room`", "no `room` under `chatChanged`", "chat"]);
}

#[test]
fn an_empty_path_or_segment_is_refused() {
    for root in ["", "createChat.", ".chat", "createChat..chat"] {
        let source = model(&standard("create", "CreateChat", root));
        refused(&format!("root {root:?}"), &source, "response from: \"", &["from", "empty"]);
    }
}

#[test]
fn destroy_listen_and_event_id_paths_follow_the_documents() {
    let destroy = model(&standard("destroy", "LeaveChat", "leaveChat.chat"));
    refused("destroy root", &destroy, "response from: \"leaveChat.chat\"", &["no `chat` under `leaveChat`", "removedChatId"]);
    let listen = |line: &str| model(&format!("  subscribe \"ChatChanged\"{line}\n"));
    run(&listen(", from: \"chatChanged.chat\", id: \"id\"")).unwrap();
    refused("listen root", &listen(", from: \"chatChanged.room\""), "subscribe \"ChatChanged\"", &["subscribe from `chatChanged.room`", "no `room` under `chatChanged`"]);
    refused("listen id", &listen(", from: \"chatChanged.chat\", id: \"chatId\""), "subscribe \"ChatChanged\"", &["subscribe id `chatId`", "no `chatId` under `chatChanged.chat`", "id, title"]);
}

#[test]
fn standalone_result_keys_follow_the_payload_down_to_nested_objects() {
    let call = |result: &str| format!("operation :pricing_load do\n  query \"Pricing\"\n  returns do\n{result}  end\nend\n");
    let gifts = |inner: &str| format!("    attribute :gifts, [PricingGifts] do\n{inner}    end\n");
    run(&call(&format!("{}    attribute :flag, :bool\n", gifts("      attribute :id, :id\n      attribute :name, :string\n")))).unwrap();
    refused("top level key", &call("    attribute :ghost, :string\n"), "attribute :ghost", &["returns `ghost`", "no `ghost` under `pricing`", "flag, gifts"]);
    refused("key inside a list of objects", &call(&gifts("      attribute :id, :id\n      attribute :title, :string\n")), "attribute :title", &["returns `title`", "no `title` under `pricing.gifts`", "id, name"]);
}
