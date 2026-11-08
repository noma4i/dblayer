use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::Path;

use graphql_parser::query::{self, Definition as QueryDefinition, OperationDefinition, Selection, SelectionSet};
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
    Union(Vec<String>),
    Scalar,
    Input(Vec<(String, Ty)>),
}

#[derive(Debug, Default)]
pub struct Schema {
    pub types: HashMap<String, TypeKind>,
    pub query: String,
    pub mutation: String,
    pub subscription: String,
    pub implementers: HashMap<String, Vec<String>>,
    pub one_of: std::collections::HashSet<String>,
}

impl Schema {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        let document = schema::parse_schema::<String>(&text).map_err(|error| error.to_string())?;
        let mut loaded = Self { query: "Query".to_string(), mutation: "Mutation".to_string(), subscription: "Subscription".to_string(), ..Self::default() };
        for definition in &document.definitions {
            match definition {
                Definition::SchemaDefinition(declared) => {
                    if let Some(name) = &declared.query {
                        loaded.query = name.clone();
                    }
                    if let Some(name) = &declared.mutation { loaded.mutation = name.clone(); }
                    if let Some(name) = &declared.subscription { loaded.subscription = name.clone(); }
                }
                Definition::TypeDefinition(declared) => loaded.add(declared),
                _ => {}
            }
        }
        Ok(loaded)
    }

    fn add(&mut self, declared: &TypeDefinition<'_, String>) {
        let implemented = match declared {
            TypeDefinition::Object(object) => Some((&object.name, &object.implements_interfaces)),
            TypeDefinition::Interface(interface) => Some((&interface.name, &interface.implements_interfaces)),
            _ => None,
        };
        if let Some((name, interfaces)) = implemented {
            for interface in interfaces {
                self.implementers.entry(interface.clone()).or_default().push(name.clone());
            }
        }
        let (name, kind) = match declared {
            TypeDefinition::Object(object) => (&object.name, TypeKind::Fields(fields_of(&object.fields))),
            TypeDefinition::Interface(interface) => (&interface.name, TypeKind::Fields(fields_of(&interface.fields))),
            TypeDefinition::Enum(declared) => (&declared.name, TypeKind::Enum(declared.values.iter().map(|value| value.name.clone()).collect())),
            TypeDefinition::Union(union) => (&union.name, TypeKind::Union(union.types.clone())),
            TypeDefinition::Scalar(scalar) => (&scalar.name, TypeKind::Scalar),
            TypeDefinition::InputObject(input) => (&input.name, TypeKind::Input(input.fields.iter().map(|field| (field.name.clone(), Ty { has_default: field.default_value.is_some(), ..Ty::of(&field.value_type) })).collect())),
        };
        if matches!(declared, TypeDefinition::InputObject(input) if input.directives.iter().any(|directive| directive.name == "oneOf")) {
            self.one_of.insert(name.clone());
        }
        self.types.insert(name.clone(), kind);
    }

    pub fn is_interface(&self, name: &str) -> bool {
        self.implementers.contains_key(name)
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

    pub fn find_field(&self, owner: &str, name: &str) -> Option<Ty> {
        match self.types.get(owner) {
            Some(TypeKind::Fields(fields)) => fields.get(name).cloned(),
            _ => None,
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

pub struct Miss {
    pub at: usize,
    pub keys: BTreeSet<String>,
}

pub struct Followed<'b> {
    pub names: Vec<&'b str>,
    pub selects: Vec<&'b Select>,
}

#[derive(Debug, Default)]
pub struct Operations {
    pub named: HashMap<String, Operation>,
    pub fragments: HashMap<String, (String, Select)>,
}

#[derive(Debug)]
pub struct Operation {
    pub file: String,
    pub kind: &'static str,
    pub root: Option<Root>,
    pub variables: HashMap<String, Ty>,
    pub select: Select,
}

#[derive(Debug, Default, Clone)]
pub struct Select {
    pub fields: Vec<SelectedField>,
    pub typename: bool,
    pub on: Vec<(String, Select)>,
    pub spreads: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SelectedField {
    pub key: String,
    pub name: String,
    pub select: Select,
}

fn select_of(set: &SelectionSet<'_, String>) -> Select {
    let mut found = Select::default();
    for selection in &set.items {
        match selection {
            Selection::Field(field) if field.name == "__typename" => found.typename = true,
            Selection::Field(field) => found.fields.push(SelectedField { key: field.alias.clone().unwrap_or_else(|| field.name.clone()), name: field.name.clone(), select: select_of(&field.selection_set) }),
            Selection::InlineFragment(fragment) => match &fragment.type_condition {
                Some(query::TypeCondition::On(name)) => found.on.push((name.clone(), select_of(&fragment.selection_set))),
                None => {
                    let inner = select_of(&fragment.selection_set);
                    found.typename |= inner.typename;
                    found.fields.extend(inner.fields);
                    found.on.extend(inner.on);
                    found.spreads.extend(inner.spreads);
                }
            },
            Selection::FragmentSpread(spread) => found.spreads.push(spread.fragment_name.clone()),
        }
    }
    found
}

impl Operations {
    pub fn each_select<'b>(&'b self, select: &'b Select, on: Option<&'b str>, visit: &mut impl FnMut(&'b Select, Option<&'b str>), seen: &mut Vec<&'b str>) {
        visit(select, on);
        for (on, inner) in &select.on {
            self.each_select(inner, Some(on), visit, seen);
        }
        for spread in &select.spreads {
            if let (false, Some((on, inner))) = (seen.contains(&spread.as_str()), self.fragments.get(spread)) {
                seen.push(spread);
                self.each_select(inner, Some(on), visit, seen);
            }
        }
    }

    pub fn fields_in<'b>(&'b self, select: &'b Select, key: &str, apart: &[String]) -> Vec<&'b SelectedField> {
        let mut found = Vec::new();
        self.each_select(select, None, &mut |inner, on| {
            if !on.is_some_and(|on| apart.iter().any(|other| other == on)) {
                found.extend(inner.fields.iter().filter(|field| field.key == key));
            }
        }, &mut Vec::new());
        found
    }

    pub fn keys_in(&self, selects: &[&Select]) -> BTreeSet<String> {
        let mut keys = BTreeSet::new();
        for select in selects {
            self.each_select(select, None, &mut |inner, _| keys.extend(inner.fields.iter().map(|field| field.key.clone())), &mut Vec::new());
        }
        keys
    }

    pub fn follow<'b>(&'b self, starts: &[&'b Select], path: &[&str]) -> Result<Followed<'b>, Miss> {
        let mut here: Vec<&'b Select> = starts.to_vec();
        let mut names = Vec::new();
        for (at, key) in path.iter().enumerate() {
            let found: Vec<&'b SelectedField> = here.iter().flat_map(|select| self.fields_in(select, key, &[])).collect();
            let Some(first) = found.first() else { return Err(Miss { at, keys: self.keys_in(&here) }) };
            names.push(first.name.as_str());
            here = found.iter().map(|field| &field.select).collect();
        }
        Ok(Followed { names, selects: here })
    }

    pub fn missing_spreads(&self) -> BTreeSet<&str> {
        let mut missing = BTreeSet::new();
        let roots = self.named.values().map(|operation| &operation.select).chain(self.fragments.values().map(|(_, select)| select));
        roots.for_each(|select| self.collect_missing(select, &mut missing));
        missing
    }

    fn collect_missing<'b>(&'b self, select: &'b Select, missing: &mut BTreeSet<&'b str>) {
        missing.extend(select.spreads.iter().map(String::as_str).filter(|spread| !self.fragments.contains_key(*spread)));
        select.fields.iter().for_each(|field| self.collect_missing(&field.select, missing));
        select.on.iter().for_each(|(_, inner)| self.collect_missing(inner, missing));
    }

    pub fn load(&mut self, path: &Path) -> Result<(), String> {
        let text = fs::read_to_string(path).map_err(|error| error.to_string())?;
        let document = query::parse_query::<String>(&text).map_err(|error| error.to_string())?;
        for definition in &document.definitions {
            if let QueryDefinition::Fragment(fragment) = definition {
                let query::TypeCondition::On(on) = &fragment.type_condition;
                self.fragments.insert(fragment.name.clone(), (on.clone(), select_of(&fragment.selection_set)));
                continue;
            }
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
            if self.named.insert(name.clone(), Operation { file: path.display().to_string(), kind, root, variables, select: select_of(selection) }).is_some() {
                return Err(format!("operation `{name}` is declared twice in operation documents"));
            }
        }
        Ok(())
    }
}
