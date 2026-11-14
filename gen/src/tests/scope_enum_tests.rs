use std::fs;

use crate::{compile_files, emit_kotlin, emit_swift, Declarations};

const SCHEMA: &str = r#"
type Query { chats(list: ChatList, first: Int, after: Cursor): ChatConnection!, byOwner(ownerId: ID!, list: ChatList!): [Chat!]! }
scalar Cursor
enum ChatList { PRIMARY SECONDARY }
input ChatFilter { list: ChatList }
type Chat { id: ID! title: String! }
type ChatConnection { edges: [ChatEdge!]! pageInfo: PageInfo! }
type ChatEdge { cursor: Cursor! node: Chat! }
type PageInfo { hasNextPage: Boolean! endCursor: Cursor }
"#;

const OPERATIONS: &str = "query chats($list: ChatList, $first: Int, $after: Cursor) { chats(list: $list, first: $first, after: $after) { edges { cursor node { id title } } pageInfo { hasNextPage endCursor } } }\nquery byOwner($ownerId: ID!, $list: ChatList!) { byOwner(ownerId: $ownerId, list: $list) { id title } }\n";

const MODEL: &str = "model Chat do\n  attribute :title, :string\n  scope :list do\n    parameter :list, ChatList\n    query \"chats\"\n    response from: \"chats\"\n    paginate size: 25, direction: :forward\n  end\nend\n";

fn compile(source: &str, operations: &str, schema: Option<&str>) -> Result<Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("chat.dbl");
    fs::write(&file, source).unwrap();
    let schema_file = schema.map(|text| {
        let path = directory.path().join("schema.graphql");
        fs::write(&path, text).unwrap();
        fs::create_dir(directory.path().join("queries")).unwrap();
        fs::write(directory.path().join("queries/chats.graphql"), operations).unwrap();
        path
    });
    let documents = vec![directory.path().join("queries")];
    compile_files(&[file], schema_file.as_deref(), if schema_file.is_some() { &documents } else { &[] }).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn a_scope_param_of_a_schema_enum_is_a_typed_argument_on_both_platforms() {
    let declared = compile(MODEL, OPERATIONS, Some(SCHEMA)).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "public static func list(list: ChatList) throws -> Query<Self> {",
        "let built_ = Query<Self>(relation: \"list\", params: [\"list\": try JSONValue.encode(list)])",
        "public enum ChatList: Codable, Sendable, Hashable {",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "fun list(list: ChatList): Query<Chat> {",
        "put(\"list\", jsonValue(list))",
        "sealed class ChatList(override val raw: String) : RawEnum {",
        "data object Primary : ChatList(\"PRIMARY\")",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn string_and_enum_text_params_mix_in_declared_order() {
    let source = "model Chat do\n  attribute :title, :string\n  scope :owned do\n    parameter :ownerId, :string\n    parameter :list, ChatList\n    query \"byOwner\"\n    response from: \"byOwner\", shape: :list\n  end\nend\n";
    let declared = compile(source, OPERATIONS, Some(SCHEMA)).unwrap();
    assert_eq!(declared.models[0].relations[0].text_params, ["ownerId", "list"]);
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    assert!(swift.contains("public static func owned(_ ownerId: String, list: ChatList) throws -> Query<Self> {"), "{swift}");
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    assert!(kotlin.contains("fun owned(ownerId: String, list: ChatList): Query<Chat> {"), "{kotlin}");
}

#[test]
fn a_scope_enum_param_names_its_schema_type_and_is_checked_against_the_document() {
    let without = compile(MODEL, OPERATIONS, None).unwrap_err();
    assert!(without.iter().any(|text| text.contains("`ChatList`") && text.contains("--schema")), "{without:?}");
    for (case, model, operations, error) in [
        ("missing type", MODEL.replace("parameter :list, ChatList", "parameter :list, Ghost"), OPERATIONS.to_string(), "`Ghost`"),
        ("input object", MODEL.replace("parameter :list, ChatList", "parameter :list, ChatFilter"), OPERATIONS.to_string(), "names `ChatFilter`, which is not an enum"),
        ("variable of another type", MODEL.to_string(), OPERATIONS.replace("query chats($list: ChatList,", "query chats($list: String,"), "param `list` is `ChatList` but the variable `$list` of `chats` is `String`"),
        ("variable missing", MODEL.to_string(), OPERATIONS.replace("$list: ChatList, $first", "$first").replace("chats(list: $list, first", "chats(first"), "has no variable `$list`"),
        ("nullable param over a required variable is fine only the other way", MODEL.to_string(), OPERATIONS.replace("$list: ChatList, $first", "$list: ChatList!, $first"), ""),
    ] {
        let result = compile(&model, &operations, Some(SCHEMA));
        if error.is_empty() {
            result.expect(case);
        } else {
            let errors = result.expect_err(case);
            assert!(errors.iter().any(|text| text.contains(error)), "{case}: {errors:?}");
        }
    }
}

#[test]
fn the_variables_line_names_the_document_variable_of_an_enum_param() {
    let model = MODEL.replace("    paginate size: 25, direction: :forward", "    variables filter: :list\n    paginate size: 25, direction: :forward");
    let operations = OPERATIONS.replace("query chats($list: ChatList,", "query chats($filter: ChatList,").replace("chats(list: $list", "chats(list: $filter");
    compile(&model, &operations, Some(SCHEMA)).unwrap();
    let wrong = operations.replace("$filter: ChatList", "$filter: Int");
    let errors = compile(&model, &wrong, Some(SCHEMA)).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("variable `$filter`")), "{errors:?}");
}

#[test]
fn joins_events_needs_columns_to_join_by() {
    let model = MODEL.replace("scope :list do", "scope :list, insert_on: :server do");
    let errors = compile(&model, OPERATIONS, Some(SCHEMA)).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("insert_on: :server") && text.contains("`where`")), "{errors:?}");
    let strings = "model Chat do\n  attribute :title, :string\n  scope :search, insert_on: :server do\n    parameter :query, :string\n    query \"chats\"\n    response from: \"chats\"\n    paginate size: 25, direction: :forward\n  end\nend\n";
    let errors = compile(strings, OPERATIONS, None).unwrap_err();
    assert!(errors.iter().any(|text| text.contains("insert_on: :server") && text.contains("`where`")), "{errors:?}");
}
