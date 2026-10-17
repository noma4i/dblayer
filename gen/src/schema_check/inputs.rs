use super::graphql::{Schema, TypeKind};
use crate::enums::EnumDecl;
use crate::scalars::{self, Scalars};
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
    pub one_of: bool,
}

/// The schema types an operation names: input objects with everything they nest, and the enums they reach.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaTypes {
    pub inputs: Vec<InputObject>,
    pub enums: Vec<EnumDecl>,
}

struct Walk<'a> {
    schema: &'a Schema,
    scalars: &'a Scalars,
    queue: Vec<String>,
}

impl Walk<'_> {
    fn kind(&mut self, owner: &str, field: &str, named: &str) -> Result<String, String> {
        match named {
            "String" | "ID" => Ok("string".to_string()),
            "Int" => Ok("int".to_string()),
            "Float" => Ok("num".to_string()),
            "Boolean" => Ok("bool".to_string()),
            "JSON" if matches!(self.schema.types.get("JSON"), Some(TypeKind::Scalar)) => Ok("json".to_string()),
            other => match self.schema.types.get(other) {
                Some(TypeKind::Scalar) => self.scalars.get(other).map(|kind| if kind == "date" { "string".to_string() } else { kind.clone() }).ok_or_else(|| format!("field `{field}` of `{owner}`: {}", scalars::unmapped(other))),
                Some(TypeKind::Enum(_) | TypeKind::Input(_)) => {
                    if !self.queue.iter().any(|queued| queued == other) {
                        self.queue.push(other.to_string());
                    }
                    Ok(other.to_string())
                }
                _ => Err(format!("field `{field}` of `{owner}` has the type `{other}`, which is not an input type of the schema")),
            },
        }
    }

    fn ty(&mut self, owner: &str, field: &str, ty: &Ty) -> Result<Ty, String> {
        let kind = match &ty.kind {
            TyKind::Named(name) => TyKind::Named(self.kind(owner, field, name)?),
            TyKind::List(item) => TyKind::List(Box::new(self.ty(owner, field, item)?)),
        };
        Ok(Ty { kind, nullable: ty.nullable, has_default: ty.has_default })
    }
}

pub(crate) fn load_schema_types(schema: &Schema, scalars: &Scalars, wanted: &[String]) -> Result<SchemaTypes, String> {
    let mut walk = Walk { schema, scalars, queue: wanted.to_vec() };
    let mut found = SchemaTypes::default();
    let mut index = 0;
    while index < walk.queue.len() {
        let name = walk.queue[index].clone();
        index += 1;
        if let Some(TypeKind::Enum(values)) = schema.types.get(&name) {
            let declared = EnumDecl::new(&name, values);
            declared.check()?;
            found.enums.push(declared);
            continue;
        }
        let Some(declared) = schema.input_fields_in_order(&name) else {
            return Err(format!("`{name}` is not an input object or enum of the schema"));
        };
        let mut fields = Vec::new();
        for (field, ty) in declared {
            fields.push(InputField { name: field.clone(), ty: walk.ty(&name, field, ty)? });
        }
        let one_of = schema.one_of.contains(&name);
        found.inputs.push(InputObject { name, fields, one_of });
    }
    Ok(found)
}
