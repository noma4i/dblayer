use super::variables::mutation_variables;
use crate::identity::RowId;
use crate::schema::{ActionDeclaration, RootShape};
use serde_json::json;

fn no_ids(_: &str) -> bool {
    false
}

fn declaration(variables: &[(&str, &str)]) -> ActionDeclaration {
    let mut declared = ActionDeclaration::new("like", RootShape::Upsert);
    declared.variables = variables.iter().map(|(variable, field)| (variable.to_string(), field.to_string())).collect();
    declared
}

#[test]
fn mapping_renames_fields_to_variables_and_skips_missing_ones() {
    let variables = mutation_variables(&declaration(&[("momentId", "id"), ("note", "note")]), &json!({"id": "m1", "text": "x"}), None, &no_ids);
    assert_eq!(variables, json!({"momentId": "m1"}));
}

#[test]
fn without_mapping_names_pass_through_and_the_temp_id_is_dropped() {
    let temp = RowId::new("temp-1").unwrap();
    let input = json!({"id": "temp-1", "text": "x"});
    assert_eq!(mutation_variables(&declaration(&[]), &input, Some(&temp), &no_ids), json!({"text": "x"}));
    assert_eq!(mutation_variables(&declaration(&[]), &json!({"id": "m1", "text": "x"}), None, &no_ids), json!({"id": "m1", "text": "x"}));
}

#[test]
fn integer_ids_travel_as_numbers_so_int_and_id_arguments_both_accept_them() {
    let is_id = |field: &str| field == "replyToId" || field == "chatId";
    let input = json!({"chatId": "427014", "replyToId": "477867", "body": "12"});
    let variables = mutation_variables(&declaration(&[]), &input, None, &is_id);
    assert_eq!(variables, json!({"chatId": 427014, "replyToId": 477867, "body": "12"}));
    let named = mutation_variables(&declaration(&[("replyTo", "replyToId")]), &input, None, &is_id);
    assert_eq!(named, json!({"replyTo": 477867}));
    let keeps = mutation_variables(&declaration(&[]), &json!({"replyToId": "temp-1", "chatId": "007"}), None, &is_id);
    assert_eq!(keeps, json!({"replyToId": "temp-1", "chatId": "007"}));
}
