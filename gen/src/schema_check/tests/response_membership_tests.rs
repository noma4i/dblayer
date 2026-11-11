use std::fs;

use crate::compile_files;

const CHAT: &str = "model Chat do\n  attribute :title, :string\n  attribute :lists, [:enum], values: [\"PRIMARY\", \"SECONDARY\"], as: ChatList\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"ChatNode\"\n    variables id: :id\n  end\n  scope :list, insert_on: [:event, :response] do\n    parameter :list, ChatList\n    membership list: { contains: :lists }\n    query \"ChatList\"\n    variables list: :list\n    response from: \"chats\", shape: :list\n  end\n  operation :rename do\n    mutation \"ChatRename\"\n    optimistic false\n    parameter :title, :string\n    variables id: :id, title: :title\n    response from: \"chatRename.chat\"\n  end\nend\n";
const NOTE: &str = "model Note do\n  attribute :text, :string\n  scope :node, node: :one do\n    parameter :id, :id\n    query \"NoteNode\"\n    variables id: :id\n  end\n  operation :pin do\n    mutation \"NotePin\"\n    optimistic false\n    variables id: :id\n    upsert Chat, from: \"notePin.chat\"\n    response from: \"notePin.note\"\n  end\nend\n";
const SCHEMA: &str = "interface Node { id: ID! }\nenum ChatList { PRIMARY SECONDARY }\ntype Chat implements Node { id: ID! title: String! lists: [ChatList!]! }\ntype Note implements Node { id: ID! text: String! }\ntype ChatPayload { chat: Chat }\ntype NotePayload { note: Note chat: Chat }\ntype Query { node(id: ID!): Node chats(list: ChatList!): [Chat!]! }\ntype Mutation { chatRename(id: ID!, title: String!): ChatPayload notePin(id: ID!): NotePayload }\n";
const DOCUMENTS: &str = "query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title lists } } }\nquery NoteNode($id: ID!) { node(id: $id) { ... on Note { id text } } }\nquery ChatList($list: ChatList!) { chats(list: $list) { id title lists } }\nmutation ChatRename($id: ID!, $title: String!) { chatRename(id: $id, title: $title) { chat { id title lists } } }\nmutation NotePin($id: ID!) { notePin(id: $id) { note { id text } chat { id title lists } } }\n";

/// A row of an operation response that can enter a `membership` scope with `insert_on: [:event, :response]` selects the membership field.
#[test]
fn response_rows_select_the_membership_field_of_their_scopes() {
    let directory = tempfile::tempdir().unwrap();
    let model = directory.path().join("models.dbl");
    let schema = directory.path().join("schema.graphql");
    let documents = directory.path().join("operations.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    let models = format!("{CHAT}{NOTE}");
    let mut wrong = Vec::new();
    for (case, source, operations, expected) in [
        ("every response selects the field", models.clone(), DOCUMENTS.to_string(), None),
        ("`response from` without the field", models.clone(), DOCUMENTS.replace("{ chat { id title lists } } }\nmutation NotePin", "{ chat { id title } } }\nmutation NotePin"), Some("response field `lists`")),
        ("`upsert from` without the field", models.clone(), DOCUMENTS.replace("note { id text } chat { id title lists }", "note { id text } chat { id title }"), Some("response field `lists`")),
        ("a scope without `insert_on` asks nothing", models.replace("scope :list, insert_on: [:event, :response] do", "scope :list do"), DOCUMENTS.replace("{ chat { id title lists } } }\nmutation NotePin", "{ chat { id title } } }\nmutation NotePin"), None),
    ] {
        fs::write(&model, source).unwrap();
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
