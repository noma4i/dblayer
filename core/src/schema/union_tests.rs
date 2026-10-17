use super::*;

fn wide(variants: usize, members: usize) -> ModelSpec {
    let variants = (0..variants)
        .map(|at| FieldSpec::variant(&format!("v{at}"), &format!("Variant{at}"), (0..members).map(|member| FieldSpec::scalar(&format!("m{member}"), ScalarKind::Str).nullable()).collect()))
        .collect();
    ModelSpec::new("messages", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::union("attachment", "Attachment", variants).nullable()])
}

#[test]
fn a_union_wider_than_the_sqlite_column_limit_is_rejected_with_the_table_and_count() {
    let error = wide(40, 60).validate(&[]).unwrap_err();
    assert_eq!(error, DeclarationError::TooManyColumns { table: "row_messages".to_string(), count: 2405 });
    assert_eq!(error.to_string(), "table row_messages would have 2405 columns, above the SQLite limit of 2000");
    assert_eq!(wide(33, 60).validate(&[]), Ok(()));
}

#[test]
fn a_union_declares_each_variant_and_case_once() {
    let mut model = wide(2, 1);
    model.fields[1].fields[1].name = "v0".to_string();
    assert_eq!(model.validate(&[]), Err(DeclarationError::DuplicateVariant("attachment".to_string())));
    let mut model = wide(2, 1);
    model.fields[1].fields[1].app_type = Some("Variant0".to_string());
    assert_eq!(model.validate(&[]), Err(DeclarationError::DuplicateVariant("attachment".to_string())));
}
