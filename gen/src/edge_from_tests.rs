use crate::{emit_kotlin, emit_swift, parse_models, SchemaTypes};

const MODEL: &str = "model Moment do\n  field :text, type: :string, null: true\n  scope :feed do\n    fetch MomentsFeed\n    paginate 20\n    edge :score, type: :int, null: true, from: \"impressionScore\"\n    edge :uuid, type: :string, null: true\n  end\nend\n";

#[test]
fn an_edge_value_is_read_back_under_the_name_it_was_stored_with() {
    let models = parse_models(MODEL).unwrap();
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    assert!(swift.contains("self.score = values[\"score\"]?.intValue"), "{swift}");
    assert!(kotlin.contains("(values[\"score\"] as? JsonPrimitive)?.longOrNull"), "{kotlin}");
    for (platform, source) in [("Swift", &swift), ("Kotlin", &kotlin)] {
        assert!(!source.contains("values[\"impressionScore\"]"), "{platform} reads the response key of a stored edge value:\n{source}");
    }
}
