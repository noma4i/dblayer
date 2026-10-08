use serde_json::{Map, Value};

use crate::identity::RowId;
use crate::schema::ActionDeclaration;

pub(crate) fn mutation_variables(declaration: &ActionDeclaration, input: &Value, temp: Option<&RowId>) -> Value {
    let Some(input) = input.as_object() else { return Value::Object(Map::new()) };
    if declaration.variables.is_empty() {
        let is_temp = |value: &Value| temp.is_some_and(|temp| value.as_str() == Some(temp.as_str()));
        return Value::Object(input.iter().filter(|(name, value)| !(*name == "id" && is_temp(value))).map(|(name, value)| (name.clone(), value.clone())).collect());
    }
    Value::Object(
        declaration
            .variables
            .iter()
            .filter_map(|(variable, field)| input.get(field).map(|value| (variable.clone(), value.clone())))
            .collect(),
    )
}
