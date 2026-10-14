use crate::{emit_kotlin, emit_swift, parse_models};
use dblayer::db::Db;
use serde_json::json;

#[test]
fn nested_types_keep_optional_fields_and_work_in_local_models() {
    let models = crate::parse_sources(&[include_str!("../../fixtures/nested_reference.dbl"), include_str!("../../fixtures/nested.dbl")]).unwrap();
    for (source, read) in [(emit_swift(&models), "note = try container.decodeIfPresent(String.self, forKey: .note)"), (emit_kotlin(&models), "val note: String? = null")] {
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
        ("object :v, as: Missing", "not declared"),
        ("object :v do\nend", "at least 1 field"),
        ("object :v do\nstring :a\nstring :a\nend", "declared twice"),
        ("object :v, as: Sample do\nstring :a\nend", "name of a model"),
        ("object :v do\nstring :a, default: 3\nend", "default must"),
        ("object :v do\nnum :a, from: \"\"\nend", "non-empty"),
        ("object :v do\nenum :a, values: [:yes], default: :no\nend", "default must"),
        ("object :v do\nlist :a, of: :bool, default: [1]\nend", "default must"),
        ("object :v do\nobject :child, default: {} do\nstring :required\nend\nend", "default must"),
        ("object :v, as: Recursive do\nobject :child, as: Recursive, null: true\nend", "recursive objects"),
        ("object :v do\nfile :a\nend", "unknown word"),
        ("object :v do\nstring :a, index: true\nend", "unknown key"),
        ("array :v, as: \"[Value]\"", "type name"),
        ("raw :v, as: Value", "unknown key"),
        ("object :v, as: Value do\nstring :a\nend\nobject :w, as: Value do\nstring :a\nend", "declared twice"),
    ] {
        let source = format!("model Sample do\nschema do\n{fields}\nend\nend");
        let errors = parse_models(&source).expect_err(&source);
        assert!(errors.iter().any(|found| found.line >= 3 && found.message.contains(error)), "{source}: {errors:?}");
    }
}
