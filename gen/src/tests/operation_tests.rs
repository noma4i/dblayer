use crate::SchemaTypes;
use crate::{emit_kotlin, emit_swift, parse_all};
use super::result_tests::compile_calls;

const QR_SCHEMA: &str = "type Query { a: Int }\ntype Mutation { qrCodeCreate(kind: String!, size: Int): QrCode, signOut: Boolean }\ntype QrCode { url: String }\n";
const QR_DOCUMENTS: &str = "mutation QrCodeCreate($kind: String!, $size: Int) { qrCodeCreate(kind: $kind, size: $size) { url } }\nmutation SignOut { signOut }\n";
const QR: &str = "operation :qr_code_create do\n  mutation \"QrCodeCreate\"\n  parameter :kind, :string\n  parameter :size, :int, null: true\nend\n\noperation :sign_out do\n  mutation \"SignOut\"\nend\n";

#[test]
fn an_operation_without_a_model_becomes_a_typed_call_on_both_platforms() {
    let declared = compile_calls(QR_SCHEMA, QR_DOCUMENTS, QR).unwrap_or_else(|errors| panic!("{errors:?}"));
    assert!(declared.models.is_empty());
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "public struct QrCodeCreateResult: Codable, Sendable, Equatable {\n    public let url: String?",
        "public static func qrCodeCreate(kind: String, size: Int? = nil, in scope_: AccountScope? = nil) async throws -> QrCodeCreateResult {",
        "if let value = size { variables_[\"size\"] = try JSONValue.encode(value) }",
        "return try await DB.call(\"QrCodeCreate\", variables: variables_, as: QrCodeCreateResult.self, in: scope_)",
        "public static func signOut(in scope_: AccountScope? = nil) async throws {\n        let variables_: [String: JSONValue] = [:]",
        "try await DB.call(\"SignOut\", variables: variables_, in: scope_)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "data class QrCodeCreateResult(\n    val url: String? = null,\n)",
        "suspend fun qrCodeCreate(kind: String, size: Long? = null, account: AccountScope? = null): QrCodeCreateResult {",
        "if (size != null) variables_[\"size\"] = jsonValue(size)",
        "DBlayer.call(\"QrCodeCreate\", JsonObject(variables_), QrCodeCreateResult.serializer(), account)",
        "suspend fun signOut(account: AccountScope? = null) {",
        "DBlayer.call(\"SignOut\", JsonObject(variables_), account)",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn a_wrong_operation_declaration_is_named_with_its_line() {
    let cases = [
        ("operation :a do\n  mutation \"A\"\n  returns do\n    attribute :first, :string\n  end\nend\n", "`returns` takes no block: the answer type comes from the document"),
        ("operation :a do\n  mutation \"A\"\n  returns\nend\n", "always answers with the type of its document: delete `returns`"),
        ("operation :a do\n  mutation \"A\"\n  parameter :x, :string\n  parameter :x, :int\nend\n", "parameter `x` is declared twice"),
        ("operation :a do\n  parameter :x, :id\nend\n", "needs exactly one `mutation` or `query` line"),
        ("operation :a do\n  mutation \"A\"\n  query \"B\"\nend\n", "needs exactly one `mutation` or `query` line"),
        ("operation :a do\n  mutation \"A\"\n  parameter :x, :uuid\nend\n", "the name of a schema input object or enum like QrCodeInput"),
        ("operation :a do\n  mutation \"A\"\n  frob :x\nend\n", "unknown line `frob`"),
        ("operation :a do\n  mutation \"A\"\n  args x: :string\nend\n", "unknown line `args`"),
        ("operation :a do\n  mutation \"A\"\n  result x: :string\nend\n", "unknown line `result`"),
        ("operation :a do\n  mutation \"A\"\nend\n\noperation :a do\n  mutation \"B\"\nend\n", "operation `a` is declared twice"),
        ("operation :a\n", "needs a `do ... end` block"),
        ("operation :a do\n  mutation A\nend\n", "the operation name must be a string"),
        ("operation :a do\n  mutation \"A\", extra: 1\nend\n", "unknown key `extra` for `mutation`"),
        ("operation :a do\n  mutation \"A\"\n  response from: \"x\"\nend\n", "has `response shape: :list` only"),
    ];
    for (source, message) in cases {
        let errors = parse_all(&[source]).err().unwrap_or_else(|| panic!("accepted: {source}"));
        assert!(errors.iter().any(|(_, error)| error.message.contains(message)), "{source}: {errors:?}");
    }
}

const GIPHY: &str = "operation :giphy_load do\n  query \"Giphy\"\n  response shape: :list\n  parameter :type, :string\n  parameter :limit, :int, null: true\nend\n";
const GIPHY_SCHEMA: &str = "type Query { giphy(type: String!, limit: Int): [Giphy!]! }\ntype Giphy { id: ID!, url: String! }\n";
const GIPHY_DOCUMENTS: &str = "query Giphy($type: String!, $limit: Int) { giphy(type: $type, limit: $limit) { id url } }\n";

#[test]
fn a_list_operation_returns_one_result_type_per_element_on_both_platforms() {
    let declared = compile_calls(GIPHY_SCHEMA, GIPHY_DOCUMENTS, GIPHY).unwrap_or_else(|errors| panic!("{errors:?}"));
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "public struct GiphyLoadResult: Codable, Sendable, Equatable {",
        "public static func giphyLoad(type: String, limit: Int? = nil, in scope_: AccountScope? = nil) async throws -> [GiphyLoadResult] {",
        "return try await DB.call(\"Giphy\", variables: variables_, as: [GiphyLoadResult].self, in: scope_)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "suspend fun giphyLoad(type: String, limit: Long? = null, account: AccountScope? = null): List<GiphyLoadResult> {",
        "DBlayer.call(\"Giphy\", JsonObject(variables_), ListSerializer(GiphyLoadResult.serializer()), account)",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn a_list_line_without_a_result_is_named_with_its_line() {
    let errors = parse_all(&["operation :a do\n  query \"A\"\n  response shape: :list\nend\n"]).expect_err("accepted");
    assert!(errors.iter().any(|(_, error)| error.message.contains("`response shape: :list` needs the document to select the fields of one element")), "{errors:?}");
}

/// M64: a standalone operation whose document takes one input object sends its body under that variable.
#[test]
fn an_operation_wrapped_by_its_document_sends_the_body_under_the_variable() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    std::fs::write(root.join("schema.graphql"), "type Query { a: Int }\ntype Mutation { qrCodeCreate(input: QrCodeInput!): QrCode }\ninput QrCodeInput { kind: String!, size: Int }\ntype QrCode { url: String }\n").unwrap();
    std::fs::create_dir(root.join("mutations")).unwrap();
    std::fs::write(root.join("mutations/all.graphql"), "mutation QrCodeCreate($input: QrCodeInput!) { qrCodeCreate(input: $input) { url } }\n").unwrap();
    std::fs::write(root.join("call.dbl"), QR.split("\n\noperation :sign_out").next().unwrap().to_string() + "\n").unwrap();
    let declared = crate::compile_files(&[root.join("call.dbl")], Some(&root.join("schema.graphql")), &[root.join("mutations")]).unwrap_or_else(|errors| panic!("{errors:?}"));
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
    assert!(swift.contains("return try await DB.call(\"QrCodeCreate\", variables: [\"input\": JSONValue.object(variables_)], as: QrCodeCreateResult.self, in: scope_)"), "{swift}");
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default());
    assert!(kotlin.contains("DBlayer.call(\"QrCodeCreate\", JsonObject(mapOf(\"input\" to JsonObject(variables_))), QrCodeCreateResult.serializer(), account)"), "{kotlin}");
}
