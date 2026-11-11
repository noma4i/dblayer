use std::fs;

use crate::compile_files;

const CHAT: &str = "model Chat do\n  attribute :title, :string\n  attribute :lists, [:enum], values: [\"PRIMARY\", \"SECONDARY\"], as: ChatList\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"ChatNode\"\n    variables id: :id\n  end\n  scope :list, insert_on: [:event, :response] do\n    parameter :list, ChatList\n    membership list: { contains: :lists }\n    query \"ChatList\"\n    variables list: :list\n    response from: \"chats\", shape: :list\n  end\n  operation :rename do\n    mutation \"ChatRename\"\n    optimistic false\n    parameter :title, :string\n    variables id: :id, title: :title\n    response from: \"chatRename.chat\"\n  end\n  operation :archive do\n    mutation \"ChatArchive\"\n    optimistic false\n    variables id: :id\n  end\n  operation :destroy do\n    mutation \"ChatDestroy\"\n    variables id: :id\n    response from: \"chatDestroy.chat\"\n  end\nend\n";
const NOTE: &str = "model Note do\n  attribute :text, :string\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"NoteNode\"\n    variables id: :id\n  end\n  operation :pin do\n    mutation \"NotePin\"\n    optimistic false\n    variables id: :id\n    upsert Chat, from: \"notePin.chat\"\n    response from: \"notePin.note\"\n  end\nend\n";
const SCHEMA: &str = "interface Node { id: ID! }\nenum ChatList { PRIMARY SECONDARY }\ntype Chat implements Node { id: ID! title: String! lists: [ChatList!]! }\ntype Note implements Node { id: ID! text: String! }\ntype ChatPayload { chat: Chat }\ntype NotePayload { note: Note chat: Chat }\ntype Query { node(id: ID!): Node chats(list: ChatList!): [Chat!]! }\ntype Mutation { chatRename(id: ID!, title: String!): ChatPayload chatArchive(id: ID!): Chat chatDestroy(id: ID!): ChatPayload notePin(id: ID!): NotePayload }\n";
const DOCUMENTS: &str = "query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title lists } } }\nquery NoteNode($id: ID!) { node(id: $id) { ... on Note { id text } } }\nquery ChatList($list: ChatList!) { chats(list: $list) { id title lists } }\nmutation ChatRename($id: ID!, $title: String!) { chatRename(id: $id, title: $title) { chat { id title lists } } }\nmutation ChatArchive($id: ID!) { chatArchive(id: $id) { id title lists } }\nmutation ChatDestroy($id: ID!) { chatDestroy(id: $id) { chat { id } } }\nmutation NotePin($id: ID!) { notePin(id: $id) { note { id text } chat { id title lists } } }\n";

/// M75: a row of an operation response that can enter a `membership` scope with `insert_on: [:event, :response]` selects the membership field; an operation that lands no row asks nothing.
#[test]
fn response_rows_select_the_membership_field_of_their_scopes() {
    let directory = tempfile::tempdir().unwrap();
    let model = directory.path().join("models.dbl");
    let schema = directory.path().join("schema.graphql");
    let documents = directory.path().join("operations.graphql");
    let models = format!("{CHAT}{NOTE}");
    let nested = |text: &str| text.replace("lists: [ChatList!]! }", "viewer: Viewer! }\ntype Viewer { lists: [ChatList!]! }").replace("title lists", "title viewer { lists }");
    let nested_models = models.replace("as: ChatList\n", "as: ChatList, from: \"viewer.lists\"\n");
    let mut wrong = Vec::new();
    for (case, source, graph, operations, expected) in [
        ("every row response selects the field, `destroy` selects the id alone", models.clone(), SCHEMA.to_string(), DOCUMENTS.to_string(), None),
        ("`response from` without the field", models.clone(), SCHEMA.to_string(), DOCUMENTS.replace("{ chat { id title lists } } }\nmutation ChatArchive", "{ chat { id title } } }\nmutation ChatArchive"), Some("response field `lists`: operation `ChatRename`")),
        ("the default root without the field", models.clone(), SCHEMA.to_string(), DOCUMENTS.replace("chatArchive(id: $id) { id title lists }", "chatArchive(id: $id) { id title }"), Some("response field `lists`: operation `ChatArchive`")),
        ("`upsert from` without the field", models.clone(), SCHEMA.to_string(), DOCUMENTS.replace("note { id text } chat { id title lists }", "note { id text } chat { id title }"), Some("response field `lists`: operation `NotePin`")),
        ("a scope without `insert_on` asks nothing", models.replace("scope :list, insert_on: [:event, :response] do", "scope :list do"), SCHEMA.to_string(), DOCUMENTS.replace("{ chat { id title lists } } }\nmutation ChatArchive", "{ chat { id title } } }\nmutation ChatArchive"), None),
        ("the field under the `from:` path of its attribute", nested_models.clone(), nested(SCHEMA), nested(DOCUMENTS), None),
        ("`response ignore` with no field in the selection", models.replace("    response from: \"chatRename.chat\"\n", "    response :ignore\n"), SCHEMA.to_string(), DOCUMENTS.replace("{ chat { id title lists } } }\nmutation ChatArchive", "{ chat { id title } } }\nmutation ChatArchive"), None),
        ("`destroy` on the default root with no field in the selection", models.replace("    response from: \"chatDestroy.chat\"\n", ""), SCHEMA.replace("chatDestroy(id: ID!): ChatPayload", "chatDestroy(id: ID!): Chat"), DOCUMENTS.replace("chatDestroy(id: $id) { chat { id } }", "chatDestroy(id: $id) { id }"), None),
        ("the membership field is a local attribute", models.replace("as: ChatList\n", "as: ChatList, optional: true, local: true\n"), SCHEMA.to_string(), DOCUMENTS.replace(" title lists", " title"), None),
        ("`response from` without the `from:` path of the field", nested_models.clone(), nested(SCHEMA), nested(DOCUMENTS).replace("{ chat { id title viewer { lists } } } }\nmutation ChatArchive", "{ chat { id title } } }\nmutation ChatArchive"), Some("response field `viewer.lists`: operation `ChatRename`")),
    ] {
        fs::write(&model, source).unwrap();
        fs::write(&schema, graph).unwrap();
        fs::write(&documents, operations).unwrap();
        let errors: Vec<String> = compile_files(&[&model], Some(&schema), std::slice::from_ref(&documents)).err().unwrap_or_default().iter().map(|error| error.error.message.clone()).collect();
        let fits = match expected {
            Some(expected) => errors.len() == 1 && errors[0].contains(expected),
            None => errors.is_empty(),
        };
        if !fits {
            wrong.push(format!("{case}: expected {expected:?}, got {errors:?}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong cases:\n{}", wrong.len(), wrong.join("\n"));
}
