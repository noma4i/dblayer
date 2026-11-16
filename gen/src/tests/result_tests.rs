use std::fs;

use crate::SchemaTypes;
use crate::{compile_files, emit_kotlin, emit_swift, Declarations};

pub(crate) const SCHEMA: &str = "type Query { pricing: Pricing }\ntype Mutation { signOut: Boolean }\nscalar JSON\nscalar Mystery\ntype Pricing { gifts: [Gift!]!, videoIntro: VideoIntro, tags: [String!], spec: JSON, wallet: Wallet }\ntype Gift { id: ID! name: String! price: Int! }\ntype VideoIntro { enabled: Boolean! price: Int! }\ntype Wallet { balance: Int! }\n";
pub(crate) const DOCUMENTS: &str = "query Pricing { pricing { gifts { id name price } videoIntro { enabled price } tags spec wallet { balance } } }\nmutation SignOut { signOut }\n";
const PRICING: &str = "operation :pricing_load do\n  query \"Pricing\"\nend\n";

pub(crate) fn compile_calls(schema: &str, documents: &str, source: &str) -> Result<Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let schema_file = directory.path().join("schema.graphql");
    fs::write(&schema_file, schema).unwrap();
    fs::create_dir(directory.path().join("documents")).unwrap();
    fs::write(directory.path().join("documents/all.graphql"), documents).unwrap();
    let file = directory.path().join("calls.dbl");
    fs::write(&file, source).unwrap();
    compile_files(&[file], Some(&schema_file), &[directory.path().join("documents")]).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn a_result_with_objects_lists_and_raw_json_becomes_nested_types_on_both_platforms() {
    let declared = compile_calls(SCHEMA, DOCUMENTS, PRICING).unwrap_or_else(|errors| panic!("{errors:?}"));
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

/// D-484 M53: an answer field the schema cannot type is refused with its operation.
#[test]
fn an_answer_field_the_schema_cannot_type_is_refused_with_its_operation() {
    let schema = format!("{SCHEMA}type Grid {{ cells: [[Int]] }}\ntype Face {{ id: ID! }}\ntype Dog {{ id: ID! }}\nunion Pet = Face | Dog\n");
    let schema = schema.replace("spec: JSON,", "spec: JSON, mystery: Mystery, grid: Grid, pet: Pet,");
    let cases = [
        ("query Pricing { pricing { mystery } }", "Pricing", "custom scalar `Mystery` has no mapping"),
        ("query Pricing { pricing { grid { cells } } }", "Pricing", "list of lists"),
        ("query Pricing { pricing { pet { ... on Face { id } ... on Dog { id } } } }", "Pricing", "must select `__typename`"),
    ];
    for (document, name, message) in cases {
        let source = format!("operation :pricing_load do\n  query \"{name}\"\nend\n");
        let errors = compile_calls(&schema, document, &source).expect_err(document);
        assert!(errors.iter().any(|error| error.contains(message) && error.contains("operation `pricing_load`")), "{document}: {errors:?}");
    }
}

#[test]
fn the_specs_hold_the_returns_of_every_operation_and_a_void_one_is_raw() {
    let void = "operation :sign_out do\n  mutation \"SignOut\"\nend\n";
    let declared = compile_calls(SCHEMA, DOCUMENTS, &format!("{PRICING}\n{void}")).unwrap_or_else(|errors| panic!("{errors:?}"));
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

/// D-484 M53: a union answer is typed by the prefixed variant names and decoded by the typename of the server.
#[test]
fn a_union_answer_decodes_by_the_wire_typename_and_is_typed_by_the_prefixed_name() {
    let schema = "type Query { peer: Holder }\ntype Holder { peer: Peer }\ntype Person { name: String }\ntype Robot { serial: String }\nunion Peer = Person | Robot\n";
    let documents = "query Peer { peer { peer { __typename ... on Person { name } ... on Robot { serial } } } }\n";
    let declared = compile_calls(schema, documents, "operation :peer_load do\n  query \"Peer\"\nend\n").unwrap_or_else(|errors| panic!("{errors:?}"));
    let swift = emit_swift(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in ["case \"Person\": self = .person(try PeerLoadResultPeerPerson(from: decoder))", "case person = \"Person\"", "public struct PeerLoadResultPeerPerson:"] {
        assert!(swift.contains(line), "swift lacks {line}\n{swift}");
    }
    assert!(!swift.contains("init(cells:)"), "an answer is never read from cells\n{swift}");
    let kotlin = emit_kotlin(&declared.models, &declared.operations, &SchemaTypes::default());
    for line in ["\"Robot\" -> PeerLoadResultPeer.Robot(dbJson.decodeFromJsonElement(PeerLoadResultPeerRobot.serializer(), body))", "data class PeerLoadResultPeerRobot("] {
        assert!(kotlin.contains(line), "kotlin lacks {line}\n{kotlin}");
    }
    assert!(!kotlin.contains("fromCells"), "an answer is never read from cells\n{kotlin}");
}
