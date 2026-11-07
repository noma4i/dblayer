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
        "return try await DB.call(\"Pricing\", variables: variables_, as: PricingLoadResult.self, in: scope_)",
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
        "    val id: String? = null,",
        "DBlayer.call(\"Pricing\", JsonObject(variables_), PricingLoadResult.serializer(), account)",
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

#[test]
fn the_specs_hold_the_returns_of_every_operation_and_a_void_one_is_raw() {
    let void = "operation :sign_out do\n  mutation \"SignOut\"\nend\n";
    let declared = parse_all(&[PRICING, void]).unwrap();
    let specs = serde_json::from_str::<serde_json::Value>(&crate::specs_text(&declared.models, &declared.operations)).unwrap();
    let operations = specs["operations"].as_array().unwrap();
    assert_eq!(operations.iter().map(|operation| operation["name"].as_str().unwrap()).collect::<Vec<_>>(), ["Pricing", "SignOut"]);
    assert_eq!(operations[0]["returns"]["kind"]["type"], "object");
    assert_eq!(operations[0]["returns"]["fields"][0]["name"], "gifts");
    assert_eq!(operations[1]["returns"]["kind"]["type"], "raw");
    for source in [emit_swift(&declared.models, &declared.operations, &SchemaTypes::default()), emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default())] {
        assert!(source.contains("modelsJSON") && source.contains(r#""operations":[{"name":"Pricing""#), "an operations-only project still has its specs\n{source}");
        assert!(!source.contains("Returns"), "no constant carries a declaration");
    }
}
