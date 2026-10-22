use crate::parse_models;

#[test]
fn json_parsing_is_declared_only_for_raw_fields_with_canonical_defaults() {
    for body in [
        "field :value, type: :raw, parse: :json, null: true",
        "field :meta, type: Meta do\nfield :value, type: :raw, parse: :json, default: {enabled: true}\nend",
        "field :items, type: [Item] do\nfield :value, type: :raw, parse: :json, null: true, default: nil\nend",
        "ephemeral do\nfield :value, type: :raw, parse: :json, default: {enabled: true}\nend",
    ] {
        let source = format!("model Sample do\n{body}\nend\n");
        let models = parse_models(&source).unwrap();
        assert!(serde_json::to_string(&models).unwrap().contains("\"parse_json\":true"), "{source}");
    }
    for body in [
        "field :value, type: :string, parse: :json",
        "field :value, type: :raw, parse: :text",
        "field :value, type: :raw, parse: true",
        "field :value, type: :raw, parse: \"json\"",
        "field :value, type: :raw, parse: :json, default: \"{}\"",
        "field :meta, type: Meta do\nfield :value, type: :raw, parse: :json, default: []\nend",
        "ephemeral do\nfield :value, type: :raw, parse: :json, default: \"{}\"\nend",
        "field :value, type: :raw, parse: :json, default: 12",
    ] {
        let source = format!("model Sample do\n{body}\nend\n");
        assert!(parse_models(&source).is_err(), "{source}");
    }
}

#[test]
fn edge_fields_reject_json_parsing_with_an_explicit_error() {
    let source = "model Sample do\nfield :name, type: :string\nscope :all do\nfetch SampleFeed\npaginate 20\nedge :data, type: :raw, parse: :json, null: true\nend\nend\n";
    let errors = parse_models(source).unwrap_err();
    assert!(errors.iter().any(|error| error.message == "edge `data` cannot set `parse`"), "{errors:?}");
}
