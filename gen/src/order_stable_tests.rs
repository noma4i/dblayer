use std::fs;

use crate::{compile_files, emit_kotlin, emit_swift, Declarations};

const MODEL: &str = "model Message do\n  field :chatId, type: :id\n  index :chatId\n  field :sequenceNumber, type: :int, null: true\n  field :createdAt, type: :date\n\n  scope :thread, joins: :events, order_stable: true do\n    order sequenceNumber: :asc_nulls_last, createdAt: :asc\n    where :chatId\n    fetch \"messages\", root: \"chat.messages\"\n    paginate 30\n  end\nend\n";

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
    let plain = compile(&MODEL.replace(", order_stable: true", "")).unwrap();
    assert!(!emit_swift(&plain.models, &plain.operations, &plain.inputs).contains("order_stable"));
}

#[test]
fn a_stable_scope_needs_an_order_a_where_and_no_text_params() {
    let without = |line: &str| MODEL.replace(line, "");
    for (name, source) in [
        ("order", without("    order sequenceNumber: :asc_nulls_last, createdAt: :asc\n")),
        ("where", without("    where :chatId\n")),
        ("text", MODEL.replace("order_stable: true", "order_stable: true, text: [:chatId]")),
    ] {
        let errors = compile(&source).unwrap_err();
        assert!(errors.iter().any(|text| text.contains("order_stable: true")), "{name}: {errors:?}");
    }
}
