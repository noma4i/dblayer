use std::collections::HashMap;
use std::fs;
use std::path::Path;

use graphql_parser::query::{self, Definition as QueryDefinition, OperationDefinition, Selection};
use graphql_parser::schema::{self, Definition, Type, TypeDefinition};

const BUILTIN_SCALARS: [&str; 5] = ["String", "Int", "Float", "Boolean", "ID"];

#[derive(Debug, Clone, PartialEq)]
pub struct Ty {
    pub name: String,
    pub list: bool,
    pub nullable: bool,
    pub item_nullable: bool,
}

impl Ty {
    fn of(declared: &Type<'_, String>) -> Self {
        match declared {
            Type::NonNullType(inner) => Self { nullable: false, ..Self::of(inner) },
            Type::ListType(inner) => {
                let item = Self::of(inner);
                Self { name: item.name, list: true, nullable: true, item_nullable: item.nullable }
            }
            Type::NamedType(name) => Self { name: name.clone(), list: false, nullable: true, item_nullable: false },
        }
    }

    pub fn describe(&self) -> String {
        let named = if self.list { format!("[{}]", self.name) } else { self.name.clone() };
        if self.nullable { named } else { format!("{named}!") }
    }
}

#[derive(Debug)]
pub enum TypeKind {
    Fields(HashMap<String, Ty>),
    Enum(Vec<String>),
    Union,
    Scalar,
    Input(Vec<(String, Ty)>),
}

#[derive(Debug, Default)]
pub struct Schema {
    pub types: HashMap<String, TypeKind>,
    pub query: String,
}

impl Schema {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        let document = schema::parse_schema::<String>(&text).map_err(|error| error.to_string())?;
        let mut loaded = Self { query: "Query".to_string(), ..Self::default() };
        for definition in &document.definitions {
            match definition {
                Definition::SchemaDefinition(declared) => {
                    if let Some(name) = &declared.query {
                        loaded.query = name.clone();
                    }
                }
                Definition::TypeDefinition(declared) => loaded.add(declared),
                _ => {}
            }
        }
        Ok(loaded)
    }

    fn add(&mut self, declared: &TypeDefinition<'_, String>) {
        let (name, kind) = match declared {
            TypeDefinition::Object(object) => (&object.name, TypeKind::Fields(fields_of(&object.fields))),
            TypeDefinition::Interface(interface) => (&interface.name, TypeKind::Fields(fields_of(&interface.fields))),
            TypeDefinition::Enum(declared) => (&declared.name, TypeKind::Enum(declared.values.iter().map(|value| value.name.clone()).collect())),
            TypeDefinition::Union(union) => (&union.name, TypeKind::Union),
            TypeDefinition::Scalar(scalar) => (&scalar.name, TypeKind::Scalar),
            TypeDefinition::InputObject(input) => (&input.name, TypeKind::Input(input.fields.iter().map(|field| (field.name.clone(), Ty { nullable: field.default_value.is_some() || Ty::of(&field.value_type).nullable, ..Ty::of(&field.value_type) })).collect())),
        };
        self.types.insert(name.clone(), kind);
    }

    pub fn is_builtin_scalar(name: &str) -> bool {
        BUILTIN_SCALARS.contains(&name)
    }

    pub fn field(&self, owner: &str, name: &str) -> Result<Ty, String> {
        match self.types.get(owner) {
            Some(TypeKind::Fields(fields)) => fields.get(name).cloned().ok_or_else(|| format!("type `{owner}` has no field `{name}`")),
            Some(_) => Err(format!("type `{owner}` has no fields to look up `{name}`")),
            None => Err(format!("type `{owner}` is not in the schema")),
        }
    }

    pub fn input_fields(&self, owner: &str) -> Option<HashMap<String, Ty>> {
        self.input_fields_in_order(owner).map(|fields| fields.iter().cloned().collect())
    }

    pub fn input_fields_in_order(&self, owner: &str) -> Option<&[(String, Ty)]> {
        match self.types.get(owner) {
            Some(TypeKind::Input(fields)) => Some(fields),
            _ => None,
        }
    }

    pub fn fields(&self, owner: &str) -> Option<&HashMap<String, Ty>> {
        match self.types.get(owner) {
            Some(TypeKind::Fields(fields)) => Some(fields),
            _ => None,
        }
    }
}

fn fields_of(fields: &[schema::Field<'_, String>]) -> HashMap<String, Ty> {
    fields.iter().map(|field| (field.name.clone(), Ty::of(&field.field_type))).collect()
}

#[derive(Debug, Clone, PartialEq)]
pub struct Root {
    pub key: String,
    pub field: String,
}

#[derive(Debug, Default)]
pub struct Operations {
    pub roots: HashMap<String, Root>,
    pub mutations: HashMap<String, HashMap<String, Ty>>,
}

impl Operations {
    pub fn load(&mut self, path: &Path) -> Result<(), String> {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        let document = query::parse_query::<String>(&text).map_err(|error| error.to_string())?;
        for definition in &document.definitions {
            if let QueryDefinition::Operation(OperationDefinition::Mutation(mutation)) = definition {
                if let Some(name) = &mutation.name {
                    let variables = mutation.variable_definitions.iter().map(|variable| (variable.name.clone(), Ty { nullable: variable.default_value.is_some() || Ty::of(&variable.var_type).nullable, ..Ty::of(&variable.var_type) })).collect();
                    self.mutations.insert(name.clone(), variables);
                }
                continue;
            }
            let QueryDefinition::Operation(OperationDefinition::Query(operation)) = definition else { continue };
            let Some(name) = &operation.name else { continue };
            let first = operation.selection_set.items.iter().find_map(|selection| match selection {
                Selection::Field(field) => Some(field),
                _ => None,
            });
            if let Some(field) = first {
                let key = field.alias.clone().unwrap_or_else(|| field.name.clone());
                self.roots.insert(name.clone(), Root { key, field: field.name.clone() });
            }
        }
        Ok(())
    }
}
