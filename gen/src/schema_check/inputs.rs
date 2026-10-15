use super::graphql::{Schema, TypeKind};
pub use super::graphql::{Ty, TyKind};

#[derive(Debug, Clone, PartialEq)]
pub struct InputField {
    pub name: String,
    pub ty: Ty,
}

#[derive(Debug, Clone, PartialEq)]
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
        "JSON" if matches!(schema.types.get("JSON"), Some(TypeKind::Scalar)) => Ok("json".to_string()),
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

fn input_type(schema: &Schema, owner: &str, field: &str, ty: &Ty, queue: &mut Vec<String>) -> Result<Ty, String> {
    let kind = match &ty.kind {
        TyKind::Named(name) => TyKind::Named(scalar_kind(schema, owner, field, name, queue)?),
        TyKind::List(item) => TyKind::List(Box::new(input_type(schema, owner, field, item, queue)?)),
    };
    Ok(Ty { kind, nullable: ty.nullable, has_default: ty.has_default })
}

pub(crate) fn load_input_objects(schema: &Schema, wanted: &[String]) -> Result<Vec<InputObject>, String> {
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
            fields.push(InputField { name: field.clone(), ty: input_type(schema, &name, field, ty, &mut queue)? });
        }
        found.push(InputObject { name, fields });
    }
    Ok(found)
}
