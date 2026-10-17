use crate::{emit_kotlin, emit_swift, parse_models, SchemaTypes};

const MODEL: &str = "model Moment do\n  field :text, type: :string, null: true\n  scope :feed do\n    fetch MomentsFeed\n    paginate 20\n    edge :rank, type: :enum, values: [:A, :B], as: EdgeRank, null: true\n  end\nend\n";

#[test]
fn an_enum_declared_only_on_an_edge_gets_its_type() {
    let models = parse_models(MODEL).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    assert!(swift.contains("public let rank: EdgeRank?"), "{swift}");
    assert!(swift.contains("enum EdgeRank"), "Swift uses EdgeRank without declaring it:\n{swift}");
    assert!(kotlin.contains("sealed class EdgeRank"), "Kotlin uses EdgeRank without declaring it:\n{kotlin}");
}
