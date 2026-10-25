use crate::SchemaTypes;
use crate::{emit_kotlin, emit_swift, parse_all};

const QR: &str = "operation :qr_code_create do\n  mutation \"QrCodeCreate\"\n  wrap \"input\"\n  parameter :kind, :string\n  parameter :size, :int, null: true\n  returns do\n    attribute :url, :string\n  end\nend\n\noperation :sign_out do\n  mutation \"SignOut\"\nend\n";

#[test]
fn an_operation_without_a_model_becomes_a_typed_call_on_both_platforms() {
    let declared = parse_all(&[QR]).unwrap();
    assert!(declared.models.is_empty());
    assert!(!emit_swift(&declared.models, &declared.operations, &SchemaTypes::default()).contains("modelsJSON") && !emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default()).contains("modelsJSON"), "a file without models has no models constant");
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "public struct QrCodeCreateResult: Codable, Sendable, Equatable {\n    public let url: String?",
        "public static func qrCodeCreate(kind: String, size: Int? = nil) async throws -> QrCodeCreateResult {",
        "if let value = size { variables[\"size\"] = try JSONValue.encode(value) }",
        "return try await DB.run(\"QrCodeCreate\", variables: [\"input\": JSONValue.object(variables)], as: QrCodeCreateResult.self)",
        "public static func signOut() async throws {\n        let variables: [String: JSONValue] = [:]",
        "try await DB.run(\"SignOut\", variables: variables)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "data class QrCodeCreateResult(\n    val url: String? = null,\n)",
        "suspend fun qrCodeCreate(kind: String, size: Long? = null): QrCodeCreateResult {",
        "if (size != null) variables[\"size\"] = jsonValue(size)",
        "return DBlayer.run(\"QrCodeCreate\", JsonObject(mapOf(\"input\" to JsonObject(variables))), QrCodeCreateResult.serializer())",
        "suspend fun signOut() {",
        "DBlayer.run(\"SignOut\", JsonObject(variables))",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn a_wrong_operation_declaration_is_named_with_its_line() {
    let cases = [
        ("operation :a do\n  mutation \"A\"\n  returns do\n    attribute :first, :string\n  end\n  returns do\n    attribute :second, :int\n  end\nend\n", "`returns` is declared twice"),
        ("operation :a do\n  mutation \"A\"\n  wrap \"first\"\n  wrap \"second\"\nend\n", "`wrap` is declared twice"),
        ("operation :a do\n  mutation \"A\"\n  parameter :x, :string\n  parameter :x, :int\nend\n", "parameter `x` is declared twice"),
        ("operation :a do\n  parameter :x, :id\nend\n", "needs exactly one `mutation` or `query` line"),
        ("operation :a do\n  mutation \"A\"\n  query \"B\"\nend\n", "needs exactly one `mutation` or `query` line"),
        ("operation :a do\n  mutation \"A\"\n  parameter :x, :uuid\nend\n", "the name of a schema input object or enum like QrCodeInput"),
        ("operation :a do\n  mutation \"A\"\n  returns do\n    attribute :x, :date\n  end\nend\n", "a returned attribute is :bool, :int, :num, :string, :id, :json"),
        ("operation :a do\n  mutation \"A\"\n  frob :x\nend\n", "unknown line `frob`"),
        ("operation :a do\n  mutation \"A\"\n  args x: :string\nend\n", "unknown line `args`"),
        ("operation :a do\n  mutation \"A\"\n  result x: :string\nend\n", "unknown line `result`"),
        ("operation :a do\n  mutation \"A\"\nend\n\noperation :a do\n  mutation \"B\"\nend\n", "operation `a` is declared twice"),
        ("operation :a\n", "needs a `do ... end` block"),
        ("operation :a do\n  mutation A\nend\n", "the operation name must be a string"),
        ("operation :a do\n  mutation \"A\", extra: 1\nend\n", "unknown key `extra` for `mutation`"),
        ("operation :a do\n  mutation \"A\"\n  wrap :input\nend\n", "the variable path must be a string"),
        ("operation :a do\n  mutation \"A\"\n  response from: \"x\"\nend\n", "has `response shape: :list` only"),
    ];
    for (source, message) in cases {
        let errors = parse_all(&[source]).err().unwrap_or_else(|| panic!("accepted: {source}"));
        assert!(errors.iter().any(|(_, error)| error.message.contains(message)), "{source}: {errors:?}");
    }
}

const GIPHY: &str = "operation :giphy_load do\n  query \"Giphy\"\n  response shape: :list\n  parameter :type, :string\n  parameter :limit, :int, null: true\n  returns do\n    attribute :id, :id\n    attribute :url, :string\n  end\nend\n";

#[test]
fn a_list_operation_returns_one_result_type_per_element_on_both_platforms() {
    let declared = parse_all(&[GIPHY]).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "public struct GiphyLoadResult: Codable, Sendable, Equatable {",
        "public static func giphyLoad(type: String, limit: Int? = nil) async throws -> [GiphyLoadResult] {",
        "return try await DB.run(\"Giphy\", variables: variables, as: [GiphyLoadResult].self)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "suspend fun giphyLoad(type: String, limit: Long? = null): List<GiphyLoadResult> {",
        "return DBlayer.run(\"Giphy\", JsonObject(variables), kotlinx.serialization.builtins.ListSerializer(GiphyLoadResult.serializer()))",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn a_list_line_without_a_result_is_named_with_its_line() {
    let errors = parse_all(&["operation :a do\n  query \"A\"\n  response shape: :list\nend\n"]).err().expect("accepted");
    assert!(errors.iter().any(|(_, error)| error.message.contains("`response shape: :list` needs a `returns` block")), "{errors:?}");
}
