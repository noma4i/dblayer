use crate::tests::enum_tests::{compile, SCALARS, SCHEMA, SEND};

/// D-484 M82: an enum the schema knows takes its values from the schema, so a hand `values:` list of it stops the run, equal or not.
#[test]
fn a_model_enum_of_a_schema_type_takes_its_values_from_the_schema() {
    for values in ["[\"other\"]", "[\"EMOJI\", \"GIF\"]"] {
        let widget = format!("model Widget do\n  attribute :kind, :enum, values: {values}, as: WidgetKind\nend\n");
        let errors = compile(&[SCALARS, SEND, &widget], SCHEMA).unwrap_err();
        assert!(errors.iter().any(|message| message.contains("enum `WidgetKind` takes its values from the schema; omit `values:`")), "{errors:?}");
    }
    let same = "model Widget do\n  attribute :kind, :enum, as: WidgetKind\nend\n";
    compile(&[SCALARS, SEND, same], SCHEMA).unwrap();
}
