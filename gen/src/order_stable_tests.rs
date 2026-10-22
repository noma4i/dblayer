use std::fs;

use crate::{compile_files, emit_kotlin, emit_swift, Declarations};

const MODEL: &str = "model Message do\n  attribute :chatId, :id\n  index :chatId\n  attribute :sequenceNumber, :int, null: true\n  attribute :createdAt, :date\n  scope :thread, insert_on: [:event, :response], stable_order: true do\n    parameter :chatId, :id\n    where chatId: :chatId\n    order sequenceNumber: :asc_nulls_last, createdAt: :asc\n    query \"messages\"\n    response from: \"chat.messages\"\n    paginate size: 30, direction: :forward\n  end\nend\n";

fn compile(source: &str) -> Result<Declarations, Vec<String>> {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("message.dbl");
    fs::write(&file, source).unwrap();
    compile_files(&[file], None, &[]).map_err(|errors| errors.into_iter().map(|error| error.error.message).collect())
}

#[test]
fn a_stable_scope_reaches_the_spec_on_both_platforms() {
    let declared = compile(MODEL).unwrap();
    assert!(declared.models[0].relations.iter().find(|relation| relation.name == "thread").unwrap().order_stable);
    for emitted in [emit_swift(&declared.models, &declared.operations, &declared.inputs), emit_kotlin(&declared.models, &declared.operations, &declared.inputs)] {
        assert!(emitted.contains(r#""order_stable":true"#), "the model specification carries the option");
    }
    let plain = compile(&MODEL.replace(", stable_order: true", "")).unwrap();
    assert!(!emit_swift(&plain.models, &plain.operations, &plain.inputs).contains("order_stable"));
}

#[test]
fn a_stable_scope_needs_an_order_a_where_and_no_text_params() {
    let without = |line: &str| MODEL.replace(line, "");
    for (name, source) in [
        ("order", without("    order sequenceNumber: :asc_nulls_last, createdAt: :asc\n")),
        ("where", without("    where chatId: :chatId\n")),
        ("text", MODEL.replace("    order sequenceNumber", "    parameter :q, :string\n    order sequenceNumber")),
    ] {
        let errors = compile(&source).unwrap_err();
        assert!(errors.iter().any(|text| text.contains("stable_order: true")), "{name}: {errors:?}");
    }
}
