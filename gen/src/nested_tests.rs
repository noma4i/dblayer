use crate::{emit_kotlin, emit_swift, parse_models};
use dblayer::db::Db;
use serde_json::json;

#[test]
fn nested_types_keep_optional_fields_and_work_in_local_models() {
    let models = crate::parse_sources(&[include_str!("../../fixtures/nested_reference.dbl"), include_str!("../../fixtures/nested.dbl")]).unwrap();
    for (source, read) in [(emit_swift(&models, &[], &[]), "note = try container.decodeIfPresent(String.self, forKey: .note)"), (emit_kotlin(&models, &[], &[]), "val note: String? = null")] {
        assert!(source.contains(read), "optional note was lost: {read}");
    }
    let db = Db::open_in_memory(models, None).unwrap();
    let value = json!({"title": "local", "note": "kept", "child": {"body": "nested"}});
    let row = db.create("NestedSample", json!({"value": value, "values": []}).as_object().unwrap().clone()).unwrap();
    assert_eq!(db.find("NestedSample", &row.id).unwrap().unwrap().data["value"], value);
}

#[test]
fn nested_declarations_reject_invalid_fields_names_references_and_defaults_at_their_line() {
    for (fields, error) in [
        ("field :v, type: Missing", "not declared"),
        ("field :v, type: :object do\nend", "at least 1 field"),
        ("field :v, type: :object do\nfield :a, type: :string\nfield :a, type: :string\nend", "declared twice"),
        ("field :v, type: Sample do\nfield :a, type: :string\nend", "name of a model"),
        ("field :v, type: :object do\nfield :a, type: :string, default: 3\nend", "default must"),
        ("field :v, type: :object do\nfield :a, type: :num, from: \"\"\nend", "non-empty"),
        ("field :v, type: :object do\nfield :a, type: :enum, values: [:yes], default: :no\nend", "default must"),
        ("field :v, type: :object do\nfield :a, type: [:bool], default: [1]\nend", "default must"),
        ("field :v, type: :object do\nfield :child, type: :object, default: {} do\nfield :required, type: :string\nend\nend", "default must"),
        ("field :v, type: Recursive do\nfield :child, type: Recursive, null: true\nend", "recursive objects"),
        ("field :v, type: :object do\nfield :a, type: :file\nend", "unknown word"),
        ("field :v, type: :object do\nfield :a, type: :string, index: true\nend", "declare indexes"),
        ("field :v, type: [\"[Value]\"]", "type name"),
        ("field :v, type: :raw, as: Value", "declare named objects and lists"),
        ("field :v, type: Value do\nfield :a, type: :string\nend\nfield :w, type: Value do\nfield :a, type: :string\nend", "declared twice"),
    ] {
        let source = format!("model Sample do\n\n{fields}\n\nend");
        let errors = parse_models(&source).expect_err(&source);
        assert!(errors.iter().any(|found| found.line >= 3 && found.message.contains(error)), "{source}: {errors:?}");
    }
}
