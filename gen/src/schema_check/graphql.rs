use std::collections::HashMap;
use std::fs;
use std::path::Path;

use graphql_parser::query::{self, Definition as QueryDefinition, OperationDefinition, Selection};
use graphql_parser::schema::{self, Definition, Type, TypeDefinition};

const BUILTIN_SCALARS: [&str; 5] = ["String", "Int", "Float", "Boolean", "ID"];

#[derive(Debug, Clone, PartialEq)]
pub struct Ty {
    pub kind: TyKind,
    pub nullable: bool,
    pub has_default: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TyKind {
    Named(String),
    List(Box<Ty>),
}

impl Ty {
    fn of(declared: &Type<'_, String>) -> Self {
        match declared {
            Type::NonNullType(inner) => Self { nullable: false, ..Self::of(inner) },
            Type::ListType(inner) => Self { kind: TyKind::List(Box::new(Self::of(inner))), nullable: true, has_default: false },
            Type::NamedType(name) => Self { kind: TyKind::Named(name.clone()), nullable: true, has_default: false },
        }
    }

    pub fn name(&self) -> &str {
        match &self.kind { TyKind::Named(name) => name, TyKind::List(item) => item.name() }
    }

    pub fn is_list(&self) -> bool { matches!(self.kind, TyKind::List(_)) }

    pub fn optional(&self) -> bool { self.nullable || self.has_default }

    pub fn into_list(self) -> Self {
        Self { nullable: self.nullable, has_default: false, kind: TyKind::List(Box::new(self)) }
    }

    pub fn describe(&self) -> String {
        let named = match &self.kind { TyKind::Named(name) => name.clone(), TyKind::List(item) => format!("[{}]", item.describe()) };
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
            TypeDefinition::InputObject(input) => (&input.name, TypeKind::Input(input.fields.iter().map(|field| (field.name.clone(), Ty { has_default: field.default_value.is_some(), ..Ty::of(&field.value_type) })).collect())),
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
    pub named: HashMap<String, Operation>,
}

#[derive(Debug)]
pub struct Operation {
    pub kind: &'static str,
    pub root: Option<Root>,
    pub variables: HashMap<String, Ty>,
}

impl Operations {
    pub fn load(&mut self, path: &Path) -> Result<(), String> {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        let document = query::parse_query::<String>(&text).map_err(|error| error.to_string())?;
        for definition in &document.definitions {
            let (kind, name, variables, selection) = match definition {
                QueryDefinition::Operation(OperationDefinition::Query(operation)) => ("query", &operation.name, &operation.variable_definitions, &operation.selection_set),
                QueryDefinition::Operation(OperationDefinition::Mutation(operation)) => ("mutation", &operation.name, &operation.variable_definitions, &operation.selection_set),
                QueryDefinition::Operation(OperationDefinition::Subscription(operation)) => ("subscription", &operation.name, &operation.variable_definitions, &operation.selection_set),
                _ => continue,
            };
            let Some(name) = name else { return Err("operation documents need named operations".into()) };
            let first = selection.items.iter().find_map(|selection| match selection {
                Selection::Field(field) => Some(field),
                _ => None,
            });
            let root = first.map(|field| {
                let key = field.alias.clone().unwrap_or_else(|| field.name.clone());
                Root { key, field: field.name.clone() }
            });
            let variables = variables.iter().map(|variable| (variable.name.clone(), Ty { has_default: variable.default_value.is_some(), ..Ty::of(&variable.var_type) })).collect();
            if self.named.insert(name.clone(), Operation { kind, root, variables }).is_some() {
                return Err(format!("operation `{name}` is declared twice in operation documents"));
            }
        }
        Ok(())
    }
}
