use std::fs;

use crate::{check_models_against_schema, parse_models};

const SCHEMA: &str = r#"
type Query { node(id: ID!): Node }
interface Node { id: ID! }
scalar Cursor
type Chat implements Node { id: ID! name: String membersCount: Int! title: String members(first: Int = 30, after: Cursor): ChatMemberConnection! }
type User implements Node { id: ID! fullName: String }
type ChatMemberConnection { edges: [ChatMemberEdge!]! pageInfo: PageInfo! totalCount: Int! }
type ChatMemberEdge { cursor: Cursor! node: User! }
type PageInfo { hasNextPage: Boolean! endCursor: Cursor }
"#;

const USER: &str = "model User do\n  attribute :fullName, :string, null: true\n  scope :chat_members do\n    parameter :chatId, :string\n    query \"ChatMembers\"\n    response from: \"node.members\"\n    paginate size: 30, direction: :forward\n    freshness expires_in: 5.minutes\n    total from: \"node.membersCount\"\n  end\nend\n";

const OPERATION: &str = "query ChatMembers($chatId: ID!, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { membersCount members(first: $first, after: $after) { edges { cursor node { id fullName } } pageInfo { hasNextPage endCursor } } } } }\n";

fn run(user: &str, operation: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/members.graphql"), operation).unwrap();
    let model = directory.path().join("user.dbl");
    fs::write(&model, user).unwrap();
    check_models_against_schema(&schema, &[directory.path().join("queries")], &[model]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn a_selected_int_field_beside_the_connection_is_a_valid_total_source() {
    run(USER, OPERATION).unwrap();
}

#[test]
fn a_total_path_that_breaks_the_schema_is_refused() {
    let cases = [
        ("node.membersTotal", "membersTotal"),
        ("node.title", "must be an `Int`"),
        ("node.members", "must be an `Int`"),
        ("chat.membersCount", "must start with the first field"),
    ];
    for (path, expected) in cases {
        let errors = run(&USER.replace("node.membersCount", path), OPERATION).unwrap_err();
        assert!(errors.iter().any(|text| text.contains(expected)), "{path}: {errors:?}");
    }
}

#[test]
fn a_document_that_does_not_select_the_total_field_is_refused() {
    let errors = run(USER, &OPERATION.replace("membersCount ", "")).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("total `node.membersCount`") && text.contains("no `membersCount` under `node`")), "{errors:?}");
}

#[test]
fn total_needs_paginate_and_a_from_path() {
    let without_paginate = parse_models(&USER.replace("    paginate size: 30, direction: :forward\n", "").replace("response from: \"node.members\"", "response from: \"node.members\", shape: :list")).unwrap_err();
    assert!(without_paginate.iter().any(|error| error.message.contains("`total` needs `paginate`")), "{without_paginate:?}");
    let without_from = parse_models(&USER.replace("total from: \"node.membersCount\"", "total")).unwrap_err();
    assert!(without_from.iter().any(|error| error.message.contains("from")), "{without_from:?}");
}
