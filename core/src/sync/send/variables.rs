use serde_json::{Map, Value};

use crate::identity::RowId;
use crate::schema::ActionDeclaration;

pub(crate) fn mutation_variables(declaration: &ActionDeclaration, input: &Value, temp: Option<&RowId>, is_id: &dyn Fn(&str) -> bool) -> Value {
    let variables = plain_variables(declaration, input, temp, is_id);
    match &declaration.wrap {
        Some(key) => Value::Object(Map::from_iter([(key.clone(), variables)])),
        None => variables,
    }
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
