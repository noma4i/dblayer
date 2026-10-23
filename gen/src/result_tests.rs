use crate::SchemaTypes;
use crate::{emit_kotlin, emit_swift, parse_all};

const PRICING: &str = "operation :pricing_load do\n  query \"Pricing\"\n  returns do\n    attribute :gifts, [PricingLoadResultGifts] do\n      attribute :id, :id\n      attribute :name, :string\n      attribute :price, :int\n    end\n    attribute :videoIntro, PricingLoadResultVideoIntro do\n      attribute :enabled, :bool\n      attribute :price, :int\n    end\n    attribute :tags, [:string]\n    attribute :spec, :json\n    attribute :wallet, PricingLoadResultWallet do\n      attribute :balance, :int\n    end\n  end\nend\n";

#[test]
fn a_result_with_objects_lists_and_raw_json_becomes_nested_types_on_both_platforms() {
    let declared = parse_all(&[PRICING]).unwrap();
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
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
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in [
        "data class PricingLoadResult(",
        "    val gifts: List<PricingLoadResultGifts>? = null,",
        "    val videoIntro: PricingLoadResultVideoIntro? = null,",
        "    val price: Long? = null,",
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
fn a_wrong_returns_declaration_is_named_with_its_line() {
    let cases = [
        ("attribute :x, :uuid", "an attribute type is"),
        ("attribute :x, :date", "a returned attribute is"),
        ("attribute :x, :string, null: true", "every returned attribute is nullable"),
        ("attribute :x, [:string, :int]", "an attribute type is"),
        ("attribute :x, Thing do\n    end", "block needs at least 1 attribute"),
        ("attribute :x, Thing do\n      attribute :a, :sticker\n    end", "an attribute type is"),
        ("attribute :x, Thing do\n      attribute :a, :int\n      attribute :a, :string\n    end", "declared twice"),
        ("attribute :x, :int\n    attribute :x, :string", "declared twice"),
        ("attribute :x, :file", "a returned attribute is"),
        ("attribute :x, :enum, values: [\"A\"]", "a returned attribute is"),
    ];
    for (line, message) in cases {
        let source = format!("operation :a do\n  mutation \"A\"\n  returns do\n    {line}\n  end\nend\n");
        let errors = parse_all(&[&source]).err().unwrap_or_else(|| panic!("accepted: {line}"));
        assert!(errors.iter().any(|(_, error)| error.message.contains(message)), "{line}: {errors:?}");
    }
}
