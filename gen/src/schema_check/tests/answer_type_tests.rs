use std::fs;

use serde_json::Value;

use crate::compile_files;

const SCHEMA: &str = r#"
type Query { chat(id: ID!): Chat }
type Mutation { callEnd(input: CallEndInput!): CallEndPayload chatPin(input: ChatPinInput!): ChatPinPayload }
input CallEndInput { callId: ID! }
input ChatPinInput { chatId: ID! pinned: Boolean! }
enum CallStatus { ACTIVE ENDED }
type CallEndPayload { call: Call }
type Call { id: ID! status: CallStatus! endedAt: String duration: Int! }
type ChatPinPayload { chat: Chat pinnedCount: Int! }
type Chat { id: ID! pinned: Boolean! }
"#;

const DOCUMENTS: &str = "mutation CallEnd($input: CallEndInput!) { callEnd(input: $input) { call { id status finishedAt: endedAt } } }\nmutation ChatPin($input: ChatPinInput!) { chatPin(input: $input) { chat { id pinned } pinnedCount } }\n";

const CALL_END: &str = "operation :call_end do\n  mutation \"CallEnd\"\n  parameter :input, CallEndInput\nend\n";

fn chat(returns: &str) -> String {
    format!("model Chat do\n  attribute :pinned, :bool\n  operation :pin do\n    mutation \"ChatPin\"\n    parameter :pinned, :bool\n    response from: \"chatPin.chat\"\n{returns}  end\nend\n")
}

fn compile(sources: &[&str]) -> Result<crate::Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema = directory.path().join("schema.graphql");
    fs::write(&schema, SCHEMA).unwrap();
    fs::create_dir(directory.path().join("mutations")).unwrap();
    fs::write(directory.path().join("mutations/all.graphql"), DOCUMENTS).unwrap();
    let paths: Vec<_> = sources.iter().enumerate().map(|(index, source)| {
        let path = directory.path().join(format!("f{index}.dbl"));
        fs::write(&path, source).unwrap();
        path
    }).collect();
    compile_files(&paths, Some(&schema), &[directory.path().join("mutations")]).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

fn fields(spec: &Value) -> Vec<(String, bool)> {
    spec["fields"].as_array().unwrap_or_else(|| panic!("no fields: {spec}")).iter().map(|field| (field["name"].as_str().unwrap().to_owned(), field["nullable"] == true)).collect()
}

/// D-484 M53: the answer type of an operation is the selection of its document under the root, typed by the schema, every field nullable, named by M57.
#[test]
fn the_answer_type_comes_from_the_document() {
    let declarations = compile(&[CALL_END]).unwrap_or_else(|errors| panic!("{errors:?}"));
    let result = serde_json::to_value(declarations.operations[0].result.as_ref().expect("a typed answer")).unwrap();
    assert_eq!(result["app_type"], "CallEndResult");
    assert_eq!(fields(&result), vec![("call".to_owned(), true)]);
    let call = &result["fields"][0];
    assert_eq!(call["app_type"], "CallEndResultCall");
    assert_eq!(fields(call), vec![("id".to_owned(), true), ("status".to_owned(), true), ("finishedAt".to_owned(), true)], "aliases name the field; non-null schema fields are nullable in the answer");
    assert_eq!(call["fields"][1]["values"], serde_json::json!(["ACTIVE", "ENDED"]), "the enum takes the values of the schema");
    let errors = compile(&[&CALL_END.replace("end\n", "  returns do\n    attribute :call, :string\n  end\nend\n")]).err().unwrap_or_default();
    assert!(errors.iter().any(|error| error.contains("takes no block")), "{errors:?}");
}

/// D-484 M68: a model operation answers with the typed answer only when it says `returns`; otherwise it gives the receipt of the row.
#[test]
fn a_model_operation_answers_only_with_returns() {
    let plain = compile(&[&chat("")]).unwrap_or_else(|errors| panic!("{errors:?}"));
    let action = |declarations: &crate::Declarations| serde_json::to_value(&declarations.models[0].actions[0]).unwrap();
    assert_eq!(action(&plain)["returns"], Value::Null);
    let typed = compile(&[&chat("    returns\n")]).unwrap_or_else(|errors| panic!("{errors:?}"));
    let returns = &action(&typed)["returns"];
    assert_eq!(returns["app_type"], "ChatPinResult");
    assert_eq!(fields(returns), vec![("chat".to_owned(), true), ("pinnedCount".to_owned(), true)]);
}
