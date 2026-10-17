use std::fs;
use std::path::PathBuf;

use crate::{compile_files, emit_kotlin, emit_swift};

const SCHEMA: &str = "type Query { a: Int }\nenum ConnectionAction { ACCEPT DECLINE }\nenum ConnectionStatus { CONNECTED NEVER_MET }\ninput NoteInput { text: String! }\ninput ConnectionInput { userId: ID!, action: ConnectionAction!, note: NoteInput }\ntype Mutation { connectionUpdate(input: ConnectionInput!): Int }\n";
const OPERATIONS: &str = "mutation ConnectionUpdate($input: ConnectionInput!) { connectionUpdate(input: $input) }\n";
const MODEL: &str = "model Sample do\n\n  field :name, type: :string\n  field :connectionStatus, type: :enum, values: [:CONNECTED, :NEVER_MET], as: ConnectionStatus, null: true\n\n  operation :answer_connection do\n    mutation   ConnectionUpdate\n    args       decision: :ConnectionAction, status: :ConnectionStatus, note: :NoteInput?\n    input      sampleId: :id, action: :decision, note: :note\n    optimistic connectionStatus: :status\n    wrap       :input\n  end\nend\n";

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
        "public func answerConnection(decision: ConnectionAction, note: NoteInput? = nil, status: ConnectionStatus) async throws -> Self {",
        "public enum ConnectionAction: Codable, Sendable, Hashable {",
        "public struct NoteInput",
        "values[\"connectionStatus\"] = JSONValue.encode(status)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &declared.inputs);
    for line in [
        "suspend fun answerConnection(decision: ConnectionAction, note: NoteInput? = null, status: ConnectionStatus): Sample = act(\"answer_connection\"",
        "sealed class ConnectionAction(override val raw: String) : RawEnum {",
        "data class NoteInput(",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn a_model_operation_names_the_schema_type_it_needs_and_checks_its_kind() {
    let without = compile(MODEL, None).unwrap_err();
    assert!(without.iter().any(|message| message.contains("`ConnectionAction`") && message.contains("--schema")), "{without:?}");
    let wrong = compile(&MODEL.replace("decision: :ConnectionAction", "decision: :string"), Some(SCHEMA)).unwrap_err();
    assert!(wrong.iter().any(|message| message.contains("does not fit `action: ConnectionAction!`")), "{wrong:?}");
    let missing = compile(&MODEL.replace(":ConnectionAction", ":Ghost"), Some(SCHEMA)).unwrap_err();
    assert!(missing.iter().any(|message| message.contains("`:Ghost`")), "{missing:?}");
}
