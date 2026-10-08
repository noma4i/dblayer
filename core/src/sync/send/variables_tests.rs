use super::variables::mutation_variables;
use crate::identity::RowId;
use crate::schema::{ActionDeclaration, ActionMode, RootShape};
use serde_json::json;

fn declaration(variables: &[(&str, &str)]) -> ActionDeclaration {
    let mut declared = ActionDeclaration::new("like", ActionMode::Request, RootShape::Upsert);
    declared.variables = variables.iter().map(|(variable, field)| (variable.to_string(), field.to_string())).collect();
    declared
}

#[test]
fn mapping_renames_fields_to_variables_and_skips_missing_ones() {
    let variables = mutation_variables(&declaration(&[("momentId", "id"), ("note", "note")]), &json!({"id": "m1", "text": "x"}), None);
    assert_eq!(variables, json!({"momentId": "m1"}));
}

#[test]
fn without_mapping_names_pass_through_and_the_temp_id_is_dropped() {
    let temp = RowId::new("temp-1").unwrap();
    let input = json!({"id": "temp-1", "text": "x"});
    assert_eq!(mutation_variables(&declaration(&[]), &input, Some(&temp)), json!({"text": "x"}));
    assert_eq!(mutation_variables(&declaration(&[]), &json!({"id": "m1", "text": "x"}), None), json!({"id": "m1", "text": "x"}));
}
