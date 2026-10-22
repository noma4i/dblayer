use super::screens_fixture::{elements, screen_variants, screens_spec};
use super::*;
use crate::value::ScalarKind;

fn wide(variants: usize, members: usize) -> ModelSpec {
    let variants = (0..variants)
        .map(|at| FieldSpec::variant(&format!("v{at}"), &format!("Variant{at}"), (0..members).map(|member| FieldSpec::scalar(&format!("m{member}"), ScalarKind::Str).nullable()).collect()))
        .collect();
    let list = FieldSpec::union_array("items", "Item", variants).with_shared(vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    ModelSpec::new("screens", vec![FieldSpec::scalar("id", ScalarKind::Id), list])
}

#[test]
fn a_list_of_variants_wider_than_the_sqlite_limit_is_rejected_with_the_element_table() {
    let error = wide(40, 60).validate(&[]).unwrap_err();
    assert_eq!(error, DeclarationError::TooManyColumns { table: "row_screens__items".to_string(), count: 2406 });
    assert_eq!(wide(33, 60).validate(&[]), Ok(()));
}

#[test]
fn the_real_screen_list_fits_one_element_table() {
    assert_eq!(screens_spec().validate(&[]), Ok(()));
    let widths = crate::ddl::table_widths(&screens_spec());
    assert_eq!(widths.iter().find(|(table, _)| table == "row_screens__spec__elements").map(|(_, count)| *count), Some(18));
}

#[test]
fn a_shared_field_cannot_repeat_a_variant_field_or_another_shared_field() {
    let mut variants = screen_variants();
    variants[1].fields.push(FieldSpec::scalar("id", ScalarKind::Str).nullable());
    let model = ModelSpec::new("screens", vec![FieldSpec::scalar("id", ScalarKind::Id), elements(variants)]);
    assert_eq!(model.validate(&[]), Err(DeclarationError::DuplicateField("id".to_string())));
}

#[test]
fn a_reference_without_a_block_takes_the_variants_and_the_shared_fields_of_the_declaration() {
    let mut reused = FieldSpec::union_array("more", "SampleElement", vec![]);
    reused.fields.clear();
    let mut model = ModelSpec::new("screens", vec![FieldSpec::scalar("id", ScalarKind::Id), elements(screen_variants()), reused]);
    resolve_types(std::slice::from_mut(&mut model));
    assert_eq!((model.fields[2].fields.len(), model.fields[2].shared.len()), (4, 3));
}

#[test]
fn a_variant_named_without_members_takes_them_from_its_declaration_and_an_empty_declared_variant_keeps_its_type() {
    let reused = FieldSpec::union("alias", "OtherAction", vec![FieldSpec::variant("navigate", "SampleNavigateAction", vec![]), FieldSpec::variant("pressable", "SamplePressable", vec![])]);
    let mut model = ModelSpec::new("screens", vec![FieldSpec::scalar("id", ScalarKind::Id), elements(screen_variants()), crate::schema::screens_fixture::action(), reused]);
    resolve_types(std::slice::from_mut(&mut model));
    let alias = &model.fields[3].fields;
    assert_eq!((alias[0].fields.len(), alias[1].app_type.as_deref()), (2, Some("SamplePressable")));
}
