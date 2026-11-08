use crate::parse_models;

#[test]
fn json_parsing_is_declared_only_for_raw_fields_with_canonical_defaults() {
    for body in [
        "attribute :value, :json, parse: :json, null: true",
        "attribute :meta, Meta do\nattribute :value, :json, parse: :json, default: {enabled: true}\nend",
        "attribute :items, [Item] do\nattribute :value, :json, parse: :json, null: true, default: nil\nend",
        "ephemeral do\nattribute :value, :json, parse: :json, default: {enabled: true}\nend",
    ] {
        let source = format!("model Sample do\n{body}\nend\n");
        let models = parse_models(&source).unwrap();
        assert!(serde_json::to_string(&models).unwrap().contains("\"parse_json\":true"), "{source}");
    }
    for body in [
        "attribute :value, :string, parse: :json",
        "attribute :value, :json, parse: :text",
        "attribute :value, :json, parse: true",
        "attribute :value, :json, parse: \"json\"",
        "attribute :value, :json, parse: :json, default: \"{}\"",
        "attribute :meta, Meta do\nattribute :value, :json, parse: :json, default: []\nend",
        "ephemeral do\nattribute :value, :json, parse: :json, default: \"{}\"\nend",
        "attribute :value, :json, parse: :json, default: 12",
    ] {
        let source = format!("model Sample do\n{body}\nend\n");
        assert!(parse_models(&source).is_err(), "{source}");
    }
}

#[test]
fn edge_fields_reject_json_parsing_with_an_explicit_error() {
    let source = "model Sample do\n  attribute :name, :string\n  scope :all do\n    query \"SampleFeed\"\n    paginate size: 20, direction: :forward\n    edge do\n      attribute :data, :json, parse: :json, null: true\n    end\n  end\nend\n";
    let errors = parse_models(source).unwrap_err();
    assert!(errors.iter().any(|error| error.message == "edge `data` cannot set `parse`"), "{errors:?}");
}
