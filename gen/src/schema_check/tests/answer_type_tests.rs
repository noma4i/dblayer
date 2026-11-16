use std::fs;

use serde_json::Value;

use crate::compile_files;

const SCHEMA: &str = r#"
type Query { chat(id: ID!): Chat }
type Mutation { callEnd(input: CallEndInput!): CallEndPayload callPeer(input: CallEndInput!): CallEndPayload chatPin(input: ChatPinInput!): ChatPinPayload }
input CallEndInput { callId: ID! }
input ChatPinInput { chatId: ID! pinned: Boolean! }
enum CallStatus { ACTIVE ENDED }
type CallEndPayload { call: Call }
type Call { id: ID! status: CallStatus! endedAt: String duration: Int! peer: Peer }
union Peer = Person | Robot
type Person { id: ID! name: String! }
type Robot { id: ID! serial: String! }
type Contact { id: ID! peer: Peer }
type ChatPinPayload { chat: Chat pinnedCount: Int! }
type Chat { id: ID! pinned: Boolean! }
"#;

const DOCUMENTS: &str = "mutation CallEnd($input: CallEndInput!) { callEnd(input: $input) { call { id status finishedAt: endedAt } } }\nmutation ChatPin($input: ChatPinInput!) { chatPin(input: $input) { chat { id pinned } pinnedCount } }\nmutation CallPeer($input: CallEndInput!) { callPeer(input: $input) { call { peer { __typename ... on Person { id name } ... on Robot { id serial } } } } }\n";

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
    assert_eq!(call["fields"][1]["kind"]["values"], serde_json::json!(["ACTIVE", "ENDED"]), "the enum takes the values of the schema");
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

/// D-484 M53: a variant of a union in an answer is named by its union field type and its variant, so a model that declares `on Person` keeps its name.
#[test]
fn an_answer_variant_does_not_take_the_name_of_a_model_variant() {
    let contact = "model Contact, storage: :local do\n  attribute :peer, Peer, null: true do\n    on Person do\n      attribute :name, :string\n    end\n    on Robot do\n      attribute :serial, :string\n    end\n  end\nend\n";
    let call_peer = "operation :call_peer do\n  mutation \"CallPeer\"\n  parameter :input, CallEndInput\nend\n";
    let declarations = compile(&[contact, call_peer]).unwrap_or_else(|errors| panic!("{errors:?}"));
    let result = serde_json::to_value(declarations.operations.iter().find(|call| call.name == "call_peer").unwrap().result.as_ref().unwrap()).unwrap();
    let peer = &result["fields"][0]["fields"][0];
    let variants: Vec<&str> = peer["fields"].as_array().unwrap_or_else(|| panic!("no variants: {peer}")).iter().map(|variant| variant["app_type"].as_str().unwrap()).collect();
    assert_eq!(variants, vec!["CallPeerResultCallPeerPerson", "CallPeerResultCallPeerRobot"]);
}

/// D-484 M82: an enum the schema knows takes the values of the schema; a hand `values:` list of it stops the generator.
#[test]
fn enum_values_come_from_the_schema() {
    let call = |values: &str| format!("model Call, storage: :local do\n  attribute :status, :enum, {values}as: CallStatus\nend\n");
    let declarations = compile(&[&call("")]).unwrap_or_else(|errors| panic!("{errors:?}"));
    let model = serde_json::to_value(&declarations.models[0]).unwrap();
    let status = model["fields"].as_array().unwrap().iter().find(|field| field["name"] == "status").unwrap();
    assert_eq!(status["kind"]["values"], serde_json::json!(["ACTIVE", "ENDED"]));
    let errors = compile(&[&call("values: [\"ACTIVE\"], ")]).err().unwrap_or_default();
    assert!(errors.iter().any(|error| error.contains("takes its values from the schema")), "{errors:?}");
}

/// D-485 M84: `upsert Model, from:` of a standalone operation declares the landing of its answer rows; an unknown model or a path outside the selection stops the generator.
#[test]
fn a_standalone_upsert_declares_its_landing() {
    let chat = "model Chat do\n  attribute :pinned, :bool\nend\n";
    let call = |upsert: &str| format!("operation :pin_chat do\n  mutation \"ChatPin\"\n  parameter :input, ChatPinInput\n  {upsert}\nend\n");
    let declarations = compile(&[chat, &call("upsert Chat, from: \"chatPin.chat\"")]).unwrap_or_else(|errors| panic!("{errors:?}"));
    let lands = &declarations.operations.iter().find(|call| call.name == "pin_chat").unwrap().lands;
    assert_eq!(lands.iter().map(|landing| (landing.model.as_str(), landing.root.as_str())).collect::<Vec<_>>(), [("Chat", "chatPin.chat")]);
    for (upsert, expected) in [("upsert Absent, from: \"chatPin.chat\"", "unknown model `Absent`"), ("upsert Chat, from: \"chatPin.missing\"", "missing")] {
        let errors = compile(&[chat, &call(upsert)]).err().unwrap_or_default();
        assert!(errors.iter().any(|error| error.contains(expected)), "{upsert}: {errors:?}");
    }
}
