use std::fs;

use crate::{compile_files, emit_kotlin, emit_swift, Declarations};

const SCHEMA: &str = r#"
type Query { node(id: ID!): Node }
scalar Cursor
enum ChatMemberOrder { JOINED OWNER_FIRST }
interface Node { id: ID! }
type Chat implements Node { id: ID! membersCount: Int! members(first: Int, after: Cursor, order: ChatMemberOrder = JOINED): UserConnection! }
type User { id: ID! name: String! }
type UserConnection { edges: [UserEdge!]! pageInfo: PageInfo! }
type UserEdge { cursor: Cursor! node: User! }
type PageInfo { hasNextPage: Boolean! endCursor: Cursor }
"#;

const OPERATIONS: &str = "query chatMembers($chatId: ID!, $order: ChatMemberOrder = JOINED, $first: Int, $after: Cursor) { node(id: $chatId) { ... on Chat { membersCount members(first: $first, after: $after, order: $order) { edges { cursor node { id name } } pageInfo { hasNextPage endCursor } } } } }\n";

const MODEL: &str = "model User do\n  attribute :name, :string\n  scope :chat_members do\n    parameter :chatId, :string\n    parameter :order, ChatMemberOrder, default: \"JOINED\"\n    query \"chatMembers\"\n    response from: \"node.members\"\n    paginate size: 30, direction: :forward\n    total from: \"node.membersCount\"\n  end\nend\n";

fn compile(source: &str) -> Result<Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("user.dbl");
    fs::write(&file, source).unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/members.graphql"), OPERATIONS).unwrap();
    compile_files(&[file], Some(&schema), &[directory.path().join("queries")]).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn an_enum_scope_param_is_labeled_and_takes_its_default_only_when_one_is_declared_on_both_platforms() {
    let cases = [
        (
            "with a default",
            MODEL.to_string(),
            "public static func chatMembers(_ chatId: String, order: ChatMemberOrder = .joined) throws -> Query<Self> {",
            "fun chatMembers(chatId: String, order: ChatMemberOrder = ChatMemberOrder.Joined): Query<User> {",
        ),
        (
            "without a default",
            MODEL.replace(", default: \"JOINED\"", ""),
            "public static func chatMembers(_ chatId: String, order: ChatMemberOrder) throws -> Query<Self> {",
            "fun chatMembers(chatId: String, order: ChatMemberOrder): Query<User> {",
        ),
    ];
    for (case, model, swift_signature, kotlin_signature) in cases {
        let declared = compile(&model).unwrap();
        let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
        assert!(swift.contains(swift_signature), "{case}: {swift}");
        let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
        assert!(kotlin.contains(kotlin_signature), "{case}: {kotlin}");
        if case == "with a default" {
            assert!(swift.contains("\"order\": try JSONValue.encode(order)"), "{swift}");
        }
    }
}

#[test]
fn a_default_must_name_a_value_of_its_enum() {
    let errors = compile(&MODEL.replace("\"JOINED\"", "\"NEWEST\"")).expect_err("value outside the enum");
    assert!(errors.iter().any(|text| text.contains("default `NEWEST` is not a value of `ChatMemberOrder`")), "{errors:?}");
}
