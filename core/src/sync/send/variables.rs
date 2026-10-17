use serde_json::{Map, Value};

use crate::identity::RowId;
use crate::schema::ActionDeclaration;

pub(crate) fn mutation_variables(declaration: &ActionDeclaration, input: &Value, temp: Option<&RowId>, is_id: &dyn Fn(&str) -> bool) -> Value {
    let is_id = |name: &str| declaration.args.get(name).map_or_else(|| is_id(name), |kind| kind.starts_with("id") && !kind.starts_with("ids"));
    let variables = plain_variables(declaration, input, temp, &is_id);
    wrap_variables(declaration, variables)
}

fn wrap_variables(declaration: &ActionDeclaration, variables: Value) -> Value {
    match &declaration.wrap {
        Some(key) => Value::Object(Map::from_iter([(key.clone(), variables)])),
        None => variables,
    }
}

pub(crate) fn idempotent_variables(declaration: &ActionDeclaration, input: &Value, mut variables: Value, key: &str) -> Value {
    if declaration.client_mutation_id {
        let target = match &declaration.wrap {
            Some(wrap) => variables.get_mut(wrap),
            None => variables.get_mut("input"),
        };
        if let Some(target) = target.and_then(Value::as_object_mut) {
            let explicit = input.get("clientMutationId").or_else(|| input.get("input").and_then(|input| input.get("clientMutationId")));
            target.entry("clientMutationId").or_insert_with(|| explicit.cloned().unwrap_or_else(|| Value::String(key.to_string())));
        }
    }
    variables
}

pub(crate) fn upload_variables(declaration: &ActionDeclaration, input: Value, key: &str) -> Value {
    idempotent_variables(declaration, &input, wrap_variables(declaration, input.clone()), key)
}

fn plain_variables(declaration: &ActionDeclaration, input: &Value, temp: Option<&RowId>, is_id: &dyn Fn(&str) -> bool) -> Value {
    let Some(input) = input.as_object() else { return Value::Object(Map::new()) };
    if declaration.variables.is_empty() {
        let is_temp = |value: &Value| temp.is_some_and(|temp| value.as_str() == Some(temp.as_str()));
        return Value::Object(input.iter().filter(|(name, value)| !(*name == "id" && is_temp(value))).map(|(name, value)| (name.clone(), wire(name, value, is_id))).collect());
    }
    Value::Object(
        declaration
            .variables
            .iter()
            .filter_map(|(variable, field)| input.get(field).map(|value| (variable.clone(), wire(field, value, is_id))))
            .collect(),
    )
}

/// Integer ids go out as JSON numbers: a GraphQL `ID` accepts them and so does an `Int`, while a string only fits `ID`.
fn wire(field: &str, value: &Value, is_id: &dyn Fn(&str) -> bool) -> Value {
    let Some(text) = value.as_str().filter(|_| is_id(field)) else { return value.clone() };
    let plain = !text.is_empty() && text.len() <= 15 && text.bytes().all(|byte| byte.is_ascii_digit()) && (text == "0" || !text.starts_with('0'));
    text.parse::<i64>().ok().filter(|_| plain).map_or_else(|| value.clone(), Value::from)
}
