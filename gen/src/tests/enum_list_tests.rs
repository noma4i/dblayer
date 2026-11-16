use std::fs;

use crate::{compile_files, emit_kotlin, emit_swift, parse_models};

const CHAT: &str = "model Chat do\n  attribute :title, :string\n  attribute :lists, [:enum], values: [\"PRIMARY\", \"SECONDARY\"], as: ChatList\n  attribute :extras, [:enum], values: [\"a_b\", \"c\"], as: ChatExtra, null: true\nend\n";

#[test]
fn a_list_of_enum_values_is_a_typed_list_on_both_platforms() {
    let models = parse_models(CHAT).unwrap();
    let swift = emit_swift(&models, &[], &Default::default());
    for line in [
        "public let lists: [ChatList]",
        "public let extras: [ChatExtra]?",
        "self.lists = try r.list(1, \\.asText).map(ChatList.init(rawValue:))",
        "self.extras = try r.listOpt(2, \\.asText)?.map(ChatExtra.init(rawValue:))",
        "public static let lists = Column<Chat, [ChatList]>(\"lists\")",
        "values_[\"lists\"] = try JSONValue.encode(lists)",
        "public enum ChatList: Codable, Sendable, Hashable {",
        "public enum ChatExtra: Codable, Sendable, Hashable {",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&models, &[], &Default::default());
    for line in [
        "val lists: List<ChatList>,",
        "val extras: List<ChatExtra>? = null,",
        "lists = r.list(1, Cell::asText).map { ChatList.of(it) },",
        "extras = r.listOpt(2, Cell::asText)?.map { ChatExtra.of(it) },",
        "val lists = Column<Chat, List<ChatList>>(\"lists\")",
        "put(\"lists\", jsonValue(lists))",
        "sealed class ChatList(override val raw: String) : RawEnum {",
        "data object Primary : ChatList(\"PRIMARY\")",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn a_list_of_enum_values_keeps_the_storage_of_a_list_of_strings() {
    let listed = parse_models(CHAT).unwrap();
    let chat = &listed[0];
    let lists = chat.field("lists").unwrap();
    assert_eq!(serde_json::to_value(&lists.kind).unwrap(), serde_json::json!({"type": "list", "element": "str"}));
    assert_eq!(lists.app_type.as_deref(), Some("ChatList"));
    assert_eq!(lists.values, ["PRIMARY", "SECONDARY"]);
}

#[test]
fn a_list_of_enum_values_stops_the_run_on_a_bad_declaration() {
    for (case, source, error) in [
        ("collision", CHAT.replace("[\"PRIMARY\", \"SECONDARY\"]", "[\"a_b\", \"aB\"]"), "both become the Swift case `aB`"),
        ("no values", CHAT.replace("values: [\"PRIMARY\", \"SECONDARY\"], ", ""), "enum attribute `lists` needs `values:`"),
        ("string list with values", CHAT.replace("[:enum], values: [\"a_b\", \"c\"], as: ChatExtra", "[:string], values: [\"a_b\"]"), "`values:` belongs to `:enum` and `[:enum]`"),
    ] {
        let errors = parse_models(&source).expect_err(case);
        assert!(errors.iter().any(|found| found.message.contains(error)), "{case}: {errors:?}");
    }
}

const SCHEMA: &str = "type Query { chat: Chat }\nenum ChatList { PRIMARY SECONDARY }\ntype Chat { id: ID! title: String! lists: [ChatList!]! maybe: [ChatList!] }\n";
const OPERATION: &str = "query chat { chat { id title lists maybe } }\n";
const SCOPE: &str = "  scope :all do\n    query \"chat\"\n    response from: \"chat\"\n  end\n";

fn check(fields: &str) -> Result<(), Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("queries")).unwrap();
    fs::write(directory.path().join("queries/chat.graphql"), OPERATION).unwrap();
    let model = directory.path().join("chat.dbl");
    fs::write(&model, format!("model Chat do\n  attribute :title, :string\n{fields}{SCOPE}end\n")).unwrap();
    compile_files(&[model], Some(&schema), &[directory.path().join("queries")]).map(|_| ()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn a_list_of_enum_values_is_checked_against_the_schema_enum_list() {
    let good = "  attribute :lists, [:enum], as: ChatList\n  attribute :maybe, [:enum], as: ChatList, null: true\n";
    check(good).unwrap();
    for (case, fields, error) in [
        ("values of a schema enum", good.replace("[:enum], as", "[:enum], values: [\"PRIMARY\"], as"), "enum `ChatList` takes its values from the schema; omit `values:`"),
        ("not a list in the declaration", good.replace("[:enum], as", ":enum, as"), "does not fit"),
        ("nullable list without null", good.replace(", null: true", ", null: false"), "is nullable in the schema"),
    ] {
        let errors = check(&fields).expect_err(case);
        assert!(errors.iter().any(|text| text.contains(error)), "{case}: {errors:?}");
    }
}
