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

const MODEL: &str = "model User do\n  field :name, type: :string\n\n  scope :chat_members, text: [:chatId, order: :ChatMemberOrder], defaults: { order: :JOINED } do\n    fetch \"chatMembers\", root: \"node.members\"\n    paginate 30\n    total from: \"node.membersCount\"\n  end\nend\n";

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
fn an_enum_scope_param_is_labeled_and_takes_its_default_on_both_platforms() {
    let declared = compile(MODEL).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    assert!(swift.contains("public static func chatMembers(_ chatId: String, order: ChatMemberOrder = .joined) -> Query<Self> {"), "{swift}");
    assert!(swift.contains("\"order\": JSONValue.encode(order)"), "{swift}");
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    assert!(kotlin.contains("fun chatMembers(chatId: String, order: ChatMemberOrder = ChatMemberOrder.Joined): Query<User> {"), "{kotlin}");
}

#[test]
fn an_enum_param_without_a_default_is_labeled_and_required() {
    let declared = compile(&MODEL.replace(", defaults: { order: :JOINED }", "")).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    assert!(swift.contains("public static func chatMembers(_ chatId: String, order: ChatMemberOrder) -> Query<Self> {"), "{swift}");
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    assert!(kotlin.contains("fun chatMembers(chatId: String, order: ChatMemberOrder): Query<User> {"), "{kotlin}");
}

#[test]
fn a_default_must_name_a_typed_param_and_a_value_of_its_enum() {
    for (case, model, error) in [
        ("value outside the enum", MODEL.replace(":JOINED", ":NEWEST"), "default `NEWEST` is not a value of `ChatMemberOrder`"),
        ("string param", MODEL.replace("order: :JOINED", "chatId: :JOINED"), "default for `chatId` needs an enum param"),
        ("unknown param", MODEL.replace("order: :JOINED", "ghost: :JOINED"), "default for `ghost` names no param"),
    ] {
        let errors = compile(&model).expect_err(case);
        assert!(errors.iter().any(|text| text.contains(error)), "{case}: {errors:?}");
    }
}
