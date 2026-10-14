use std::path::Path;

use super::graphql::{Schema, TypeKind};

/// One field of a schema input object: `base` is a scalar kind (`string`, `int`, `num`, `bool`) or the name of another input object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputField {
    pub name: String,
    pub base: String,
    pub list: bool,
    pub item_optional: bool,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputObject {
    pub name: String,
    pub fields: Vec<InputField>,
}

fn scalar_kind(schema: &Schema, owner: &str, field: &str, named: &str, queue: &mut Vec<String>) -> Result<String, String> {
    match named {
        "String" | "ID" => Ok("string".to_string()),
        "Int" => Ok("int".to_string()),
        "Float" => Ok("num".to_string()),
        "Boolean" => Ok("bool".to_string()),
        other => match schema.types.get(other) {
            Some(TypeKind::Scalar | TypeKind::Enum(_)) => Ok("string".to_string()),
            Some(TypeKind::Input(_)) => {
                if !queue.iter().any(|queued| queued == other) {
                    queue.push(other.to_string());
                }
                Ok(other.to_string())
            }
            _ => Err(format!("field `{field}` of `{owner}` has the type `{other}`, which is not an input type of the schema")),
        },
    }
}

pub(crate) fn load_input_objects(path: &Path, wanted: &[String]) -> Result<Vec<InputObject>, String> {
    let schema = Schema::load(path)?;
    let mut queue = wanted.to_vec();
    let mut found = Vec::new();
    let mut index = 0;
    while index < queue.len() {
        let name = queue[index].clone();
        index += 1;
        let Some(declared) = schema.input_fields_in_order(&name) else {
            return Err(format!("`{name}` is not an input object of the schema"));
        };
        let mut fields = Vec::new();
        for (field, ty) in declared {
            let base = scalar_kind(&schema, &name, field, &ty.name, &mut queue)?;
            fields.push(InputField { name: field.clone(), base, list: ty.list, item_optional: ty.list && ty.item_nullable, optional: ty.nullable });
        }
        found.push(InputObject { name, fields });
    }
    Ok(found)
}
