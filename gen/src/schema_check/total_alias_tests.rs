use std::fs;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { node(id: ID!): Node }
interface Node { id: ID! }
scalar Cursor
type Chat implements Node { id: ID! membersCount: Int! title: String members(first: Int = 30, after: Cursor): ChatMemberConnection! }
type User implements Node { id: ID! fullName: String }
type ChatMemberConnection { edges: [ChatMemberEdge!]! pageInfo: PageInfo! }
type ChatMemberEdge { cursor: Cursor! node: User! }
type PageInfo { hasNextPage: Boolean! endCursor: Cursor }
"#;

const USER: &str = "model User do\n  attribute :fullName, :string, null: true\n  scope :chat_members do\n    parameter :chatId, :string\n    query \"ChatMembers\"\n    response from: \"node.members\"\n    paginate size: 30, direction: :forward\n    total from: \"node.count\"\n  end\nend\n";

const OPERATION: &str = "query ChatMembers($chatId: ID!, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { count: membersCount members(first: $first, after: $after) { edges { cursor node { id fullName } } pageInfo { hasNextPage endCursor } } } } }\n";

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
fn a_total_path_names_the_response_key_so_an_alias_reaches_its_schema_field() {
    run(USER, OPERATION).unwrap();
}

#[test]
fn an_aliased_total_is_still_checked_against_the_type_of_the_field_behind_the_alias() {
    let errors = run(USER, &OPERATION.replace("count: membersCount", "count: title")).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("must be an `Int`")), "{errors:?}");
    let errors = run(&USER.replace("node.count", "node.membersCount"), OPERATION).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("membersCount")), "{errors:?}");
}
