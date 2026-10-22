use std::fs;

use crate::check_models_against_schema;

const SCHEMA: &str = r#"
type Query { node(id: ID!): Node }
interface Node { id: ID! }
scalar Cursor
type Chat implements Node { id: ID! name: String members(first: Int = 30, after: Cursor): ChatMemberConnection! }
type User implements Node { id: ID! fullName: String }
type ChatMemberConnection { edges: [ChatMemberEdge!]! pageInfo: PageInfo! }
type ChatMemberEdge { cursor: Cursor! node: User! }
type PageInfo { hasNextPage: Boolean! endCursor: Cursor }
"#;

const USER: &str = "model User do\n  attribute :fullName, :string, null: true\n  scope :chat_members do\n    parameter :chatId, :string\n    query \"ChatMembers\"\n    response from: \"node.members\"\n    paginate size: 30, direction: :forward\n    freshness expires_in: 5.minutes\n  end\nend\n";

const OPERATION: &str = "query ChatMembers($chatId: ID!, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { members(first: $first, after: $after) { edges { cursor node { id fullName } } pageInfo { hasNextPage endCursor } } } } }\n";

fn run(user: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/members.graphql"), OPERATION).unwrap();
    let model = directory.path().join("user.dbl");
    fs::write(&model, user).unwrap();
    check_models_against_schema(&schema, &[directory.path().join("queries")], &[model]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn members_of_a_chat_are_checked_through_node_and_the_member_connection() {
    run(USER).unwrap();
    let errors = run(&USER.replace("node.members", "node.participants")).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("participants")), "{errors:?}");
}
