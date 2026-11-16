use std::fs;

use serde_json::{json, Value};

use crate::compile_files;

const SCHEMA: &str = r#"
type Query { chat(id: ID!): Chat }
type Mutation { chatPin(input: ChatPinInput!): ChatPinPayload chatRename(chatId: ID!, title: String!, note: String): Chat }
input ChatPinInput { chatId: ID! pinned: Boolean! clientMutationId: String }
type ChatPinPayload { chat: Chat }
type Chat { id: ID! title: String! pinned: Boolean! }
"#;

const DOCUMENTS: &str = "mutation ChatPin($input: ChatPinInput!) { chatPin(input: $input) { chat { id title pinned } } }\nmutation ChatRename($chatId: ID!, $title: String!, $note: String) { chatRename(chatId: $chatId, title: $title, note: $note) { id title pinned } }\n";

fn model(operations: &str) -> String {
    format!("model Chat do\n  attribute :title, :string\n  attribute :pinned, :bool\n{operations}end\n")
}

const PIN: &str = "  operation :pin do\n    mutation \"ChatPin\"\n    parameter :pinned, :bool\n    response from: \"chatPin.chat\"\n  end\n";
const RENAME: &str = "  operation :rename do\n    mutation \"ChatRename\"\n    parameter :title, :string\n    optimistic false\n  end\n";

fn generate(source: &str, schema: bool) -> Result<Value, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema_path = directory.path().join("schema.graphql");
    fs::write(&schema_path, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("mutations")).unwrap();
    fs::write(directory.path().join("mutations/all.graphql"), DOCUMENTS).unwrap();
    let model_path = directory.path().join("chat.dbl");
    fs::write(&model_path, source).unwrap();
    let operations = [directory.path().join("mutations")];
    let found = if schema { compile_files(&[model_path], Some(&schema_path), &operations) } else { compile_files(&[model_path], None, &[]) };
    found.map(|declarations| serde_json::to_value(&declarations.models[0]).unwrap()).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

fn action<'a>(model: &'a Value, name: &str) -> &'a Value {
    model["actions"].as_array().unwrap().iter().find(|action| action["name"] == name).unwrap_or_else(|| panic!("no action {name}: {model}"))
}

fn refused(source: &str, expected: &str) -> Option<String> {
    match generate(source, true) {
        Ok(_) => Some(format!("accepted, wanted `{expected}`:\n{source}")),
        Err(errors) if errors.iter().any(|error| error.contains(expected)) => None,
        Err(errors) => Some(format!("wanted `{expected}`, got {errors:?}")),
    }
}

/// D-483 M64: the body goes under the one input variable of the document; a document with plain variables sends them as they are; `wrap` is no word.
#[test]
fn wrap_comes_from_the_operation_document() {
    let model = generate(&model(&format!("{PIN}{RENAME}")), true).unwrap_or_else(|errors| panic!("{errors:?}"));
    assert_eq!(action(&model, "pin")["wrap"], "input");
    assert_eq!(action(&model, "rename")["wrap"], Value::Null);
    assert_eq!(refused(&model(&PIN.replace("    parameter", "    wrap \"input\"\n    parameter")), "unknown word `wrap`"), None);
}

/// D-483 M80: a field of the input takes the parameter, then the attribute, then the row id by its name; `variables` only renames.
#[test]
fn variables_bind_by_name() {
    let model = generate(&model(&format!("{PIN}{RENAME}")), true).unwrap_or_else(|errors| panic!("{errors:?}"));
    assert_eq!(action(&model, "pin")["variables"], json!({"chatId": "id", "pinned": "pinned"}));
    assert_eq!(action(&model, "rename")["variables"], json!({"chatId": "id", "title": "title"}), "the nullable `note` without a source is not sent");
    let missed: Vec<String> = [
        (model(&PIN.replace("    parameter :pinned, :bool\n", "    parameter :pinned, :bool\n    variables pinned: :pinned\n")), "repeats the name"),
        (model(&PIN.replace("    parameter :pinned, :bool\n", "")).replace("  attribute :pinned, :bool\n", ""), "pinned"),
        (model(&PIN.replace("    parameter :pinned, :bool\n", "    parameter :pinned, :bool\n    variables missing: :pinned\n")), "missing"),
    ].iter().filter_map(|(source, expected)| refused(source, expected)).collect();
    assert!(missed.is_empty(), "{missed:#?}");
}

/// D-483 M81 (owner 2026-11-16, optimistic by default): an operation without `optimistic` writes each parameter named like an attribute at once; `optimistic false` writes nothing.
#[test]
fn an_operation_writes_its_attribute_parameters_at_once() {
    let model = generate(&model(&format!("{PIN}{RENAME}")), true).unwrap_or_else(|errors| panic!("{errors:?}"));
    assert_eq!(action(&model, "pin")["optimistic"], json!({"pinned": {"$arg": "pinned"}}));
    assert_eq!(action(&model, "rename")["optimistic"], json!({}));
}

/// D-483 M79: the generator stops when a file declares an operation and no schema is given.
#[test]
fn generation_without_a_schema_stops_at_the_first_operation() {
    let errors = generate(&model(PIN), false).unwrap_err();
    assert!(errors.iter().any(|error| error.contains("operation `pin` needs `--schema`")), "{errors:?}");
}
