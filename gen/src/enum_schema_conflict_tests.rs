use crate::enum_tests::{compile, SCALARS, SCHEMA, SEND};

#[test]
fn a_model_enum_that_differs_from_a_schema_enum_of_the_same_name_stops_the_run() {
    let widget = "model Widget do\n  field :kind, type: :enum, values: [:other], as: WidgetKind\nend\n";
    let errors = compile(&[SCALARS, SEND, widget], SCHEMA).unwrap_err();
    assert!(errors.iter().any(|message| message.contains("enum `WidgetKind` is declared with different values")), "{errors:?}");
    let same = "model Widget do\n  field :kind, type: :enum, values: [:EMOJI, :GIF], as: WidgetKind\nend\n";
    compile(&[SCALARS, SEND, same], SCHEMA).unwrap();
}
