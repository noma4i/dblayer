use std::fs;
use std::path::PathBuf;

use crate::{compile_files, emit_kotlin, emit_swift};

const SCHEMA: &str = "type Query { a: Int }\nenum ConnectionAction { ACCEPT DECLINE }\nenum ConnectionStatus { CONNECTED NEVER_MET }\ninput NoteInput { text: String! }\ninput ConnectionInput { userId: ID!, action: ConnectionAction!, note: NoteInput }\ntype Mutation { connectionUpdate(input: ConnectionInput!): Int }\n";
const OPERATIONS: &str = "mutation ConnectionUpdate($input: ConnectionInput!) { connectionUpdate(input: $input) }\n";
const MODEL: &str = "model Sample do\n  attribute :name, :string\n  attribute :connectionStatus, :enum, as: ConnectionStatus, null: true\n  operation :answer_connection do\n    mutation \"ConnectionUpdate\"\n    parameter :decision, ConnectionAction\n    parameter :status, ConnectionStatus\n    parameter :note, NoteInput, null: true\n    variables userId: :id, action: :decision\n    optimistic connectionStatus: :status\n  end\nend\n";

fn compile(source: &str, schema: Option<&str>) -> Result<crate::Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sample.dbl");
    fs::write(&path, source).unwrap();
    let schema_path = schema.map(|text| {
        let file = directory.path().join("schema.graphql");
        fs::write(&file, text).unwrap();
        fs::create_dir(directory.path().join("mutations")).unwrap();
        fs::write(directory.path().join("mutations/connection.graphql"), OPERATIONS).unwrap();
        file
    });
    let operations: Vec<PathBuf> = schema_path.iter().map(|_| directory.path().join("mutations")).collect();
    compile_files(&[path], schema_path.as_deref(), &operations).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn a_model_operation_takes_schema_enums_and_input_objects_as_typed_args() {
    let declared = compile(MODEL, Some(SCHEMA)).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "nonisolated(nonsending) public func answerConnection(decision: ConnectionAction, note: NoteInput? = nil, status: ConnectionStatus, in scope_: AccountScope? = nil) async throws -> MutationReceipt<Self> {",
        "public enum ConnectionAction: Codable, Sendable, Hashable {",
        "public struct NoteInput",
        "values_[\"connectionStatus\"] = try JSONValue.encode(status)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "suspend fun answerConnection(decision: ConnectionAction, note: NoteInput? = null, status: ConnectionStatus, account: AccountScope? = null): MutationReceipt<Sample> = this.act(\"answer_connection\"",
        "sealed class ConnectionAction(override val raw: String) : RawEnum {",
        "data class NoteInput(",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn a_model_operation_names_the_schema_type_it_needs_and_checks_its_kind() {
    let wrong = compile(&MODEL.replace("parameter :decision, ConnectionAction", "parameter :decision, :string"), Some(SCHEMA)).unwrap_err();
    assert!(wrong.iter().any(|message| message.contains("does not fit `action: ConnectionAction!`")), "{wrong:?}");
    let missing = compile(&MODEL.replace("parameter :decision, ConnectionAction", "parameter :decision, Ghost"), Some(SCHEMA)).unwrap_err();
    assert!(missing.iter().any(|message| message.contains("`Ghost`")), "{missing:?}");
}

#[test]
fn a_related_row_removal_accepts_only_a_declared_id_and_enum_value() {
    let schema = SCHEMA.replace("ACCEPT DECLINE", "ACCEPT DECLINE REMOVE");
    let model = format!("model Conversation do\n  attribute :title, :string\nend\n{}", MODEL
        .replace("  attribute :name, :string", "  attribute :name, :string\n  attribute :conversationId, :id, null: true")
        .replace("    optimistic connectionStatus: :status", "    optimistic connectionStatus: :status\n    removes_row Conversation, id: :conversationId, when: { decision: \"REMOVE\" }"));
    let declared = compile(&model, Some(&schema)).unwrap();
    let sample = declared.models.iter().find(|model| model.key.as_str() == "Sample").unwrap();
    let action = serde_json::to_value(&sample.actions[0]).unwrap();
    assert_eq!(action["removes_row"], serde_json::json!({"model":"Conversation","id_field":"conversationId","parameter":"decision","equals":"REMOVE"}));
    for (source, expected) in [
        (model.replace("id: :conversationId", "id: :name"), "scalar ID"),
        (model.replace("\"REMOVE\"", "\"UNKNOWN\""), "UNKNOWN"),
        (model.replace("removes_row Conversation", "removes_row Missing"), "different persisted server model"),
    ] {
        let errors = compile(&source, Some(&schema)).unwrap_err();
        assert!(errors.iter().any(|message| message.contains(expected)), "{expected}: {errors:?}");
    }
}
