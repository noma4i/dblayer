use crate::{emit_kotlin, emit_swift, parse_all};

const PRICING: &str = "operation :pricing_load do\n  query Pricing\n  result gifts: [{ id: :id, name: :string, price: :int }], videoIntro: { enabled: :bool, price: :int }, tags: [:string], spec: :json, wallet: { balance: :int }\nend\n";

#[test]
fn a_result_with_objects_lists_and_raw_json_becomes_nested_types_on_both_platforms() {
    let declared = parse_all(&[PRICING]).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &[]);
    for line in [
        "public struct PricingLoadResult: Codable, Sendable, Equatable {",
        "    public let gifts: [PricingLoadResultGifts]?",
        "    public let videoIntro: PricingLoadResultVideoIntro?",
        "    public let tags: [String]?",
        "    public let spec: JSONValue?",
        "public struct PricingLoadResultGifts: Codable, Sendable, Equatable {",
        "    public let price: Int?",
        "public struct PricingLoadResultWallet: Codable, Sendable, Equatable {",
        "return try await DB.run(\"Pricing\", variables: variables, as: PricingLoadResult.self)",
    ] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &[]);
    for line in [
        "data class PricingLoadResult(",
        "    val gifts: List<PricingLoadResultGifts>? = null,",
        "    val videoIntro: PricingLoadResultVideoIntro? = null,",
        "    val tags: List<String>? = null,",
        "    val spec: JsonElement? = null,",
        "data class PricingLoadResultGifts(",
        "    @Serializable(with = IDText::class) val id: String? = null,",
        "return DBlayer.run(\"Pricing\", JsonObject(variables), PricingLoadResult.serializer())",
    ] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
}

#[test]
fn a_wrong_result_declaration_is_named_with_its_line() {
    let cases = [
        ("result x: :uuid", "type must be"),
        ("result x: :string?", "type must be"),
        ("result x: [:string, :int]", "a list holds one type"),
        ("result x: []", "a list holds one type"),
        ("result x: {}", "an object needs at least one field"),
        ("result x: { a: :Sticker }", "type must be"),
        ("result x: { a: :int, a: :string }", "declared twice"),
    ];
    for (line, message) in cases {
        let source = format!("operation :a do\n  mutation A\n  {line}\nend\n");
        let errors = parse_all(&[&source]).err().unwrap_or_else(|| panic!("accepted: {line}"));
        assert!(errors.iter().any(|(_, error)| error.message.contains(message)), "{line}: {errors:?}");
    }
}
