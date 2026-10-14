use crate::{emit_kotlin, emit_swift, parse_all};

const QR: &str = "operation :qr_code_create do\n  mutation QrCodeCreate\n  wrap :input\n  args kind: :string, size: :int?\n  result url: :string\nend\n\noperation :sign_out do\n  mutation SignOut\nend\n";

#[test]
fn an_operation_without_a_model_becomes_a_typed_call_on_both_platforms() {
    let declared = parse_all(&[QR]).unwrap();
    assert!(declared.models.is_empty());
    let swift = emit_swift(&declared.models, &declared.operations);
    for line in [
        "public struct QrCodeCreateResult: Codable, Sendable {\n    public let url: String?\n}",
        "public static func qrCodeCreate(kind: String, size: Int? = nil) async throws -> QrCodeCreateResult {",
        "if let value = size { variables[\"size\"] = JSONValue.encode(value) }",
        "return try await DB.run(\"QrCodeCreate\", variables: [\"input\": JSONValue.object(variables)], as: QrCodeCreateResult.self)",
        "public static func signOut() async throws {",
        "try await DB.run(\"SignOut\", variables: variables)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations);
    for line in [
        "data class QrCodeCreateResult(val url: String? = null)",
        "suspend fun qrCodeCreate(kind: String, size: Int? = null): QrCodeCreateResult {",
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
        ("operation :a do\n  args x: :id\nend\n", "needs exactly one `mutation` or `query` line"),
        ("operation :a do\n  mutation A\n  query B\nend\n", "needs exactly one `mutation` or `query` line"),
        ("operation :a do\n  mutation A\n  args x: :uuid\nend\n", "type must be :bool, :int, :num, :string, :id or :ids"),
        ("operation :a do\n  mutation A\n  frob :x\nend\n", "unknown line `frob`"),
        ("operation :a do\n  mutation A\nend\noperation :a do\n  mutation B\nend\n", "operation `a` is declared twice"),
        ("operation :a\n", "needs a `do ... end` block"),
    ];
    for (source, message) in cases {
        let errors = parse_all(&[source]).err().unwrap_or_else(|| panic!("accepted: {source}"));
        assert!(errors.iter().any(|(_, error)| error.message.contains(message)), "{source}: {errors:?}");
    }
}
