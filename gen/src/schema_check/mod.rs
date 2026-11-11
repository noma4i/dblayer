use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::decl::{AssociationKind, AttributeBlock, AttributeDecl, CallDecl, CallKind, ListElement, ModelDecl, OperationDecl, OptimisticDecl, OptimisticValue, ParamBase, ParameterDecl, Pos, ScopeDecl, TypeRef, VariantDecl, VariableValue};
use crate::scalars::{self, Scalars};
use dblayer::schema::NodeRead;
use dblayer::value::ScalarKind;
use crate::{FileError, ParseError};

mod documents;
#[cfg(test)]
mod tests;
mod fields;
mod graphql;
mod operation;
mod selection;
mod inputs;
mod paths;
mod media;
mod standalone;

pub use inputs::{InputField, InputObject, SchemaTypes, Ty, TyKind};
pub(crate) use inputs::load_schema_types;

use graphql::{Operations, Select};
pub(crate) use graphql::{Schema, TypeKind};

#[derive(Clone, Copy)]
enum Body<'a> {
    Fields(&'a [AttributeDecl]),
    Union(Option<&'a [AttributeDecl]>, &'a [VariantDecl]),
}

type Declared<'a> = HashMap<&'a str, (usize, Body<'a>)>;

pub struct Outcome {
    pub variables: HashMap<String, HashSet<String>>,
    pub notes: Vec<String>,
    pub idempotent_inputs: HashSet<(String, String)>,
}

pub fn check_schema(schema: &Schema, scalars: &Scalars, operations: &[PathBuf], files: &[String], nodes: &[ModelDecl], standalone: &[CallDecl]) -> Result<Outcome, Vec<FileError>> {
    let failed = |file: &Path, message: String| vec![FileError { file: file.display().to_string(), error: ParseError::new(1, 1, message) }];
    let mut documents = Operations::default();
    for path in operation_files(operations).map_err(|(path, message)| failed(&path, message))? {
        documents.load(&path).map_err(|message| failed(&path, message))?;
    }
    if let Some(spread) = documents.missing_spreads().into_iter().next() {
        let first = operations.first().expect("a missing fragment comes from a loaded document");
        return Err(failed(first, format!("fragment `{spread}` is not in the operation documents")));
    }
    let mismatched = documents.check_against(schema);
    if !mismatched.is_empty() {
        return Err(mismatched);
    }
    let mut declared = Declared::new();
    for node in nodes {
        collect_declared(&node.attributes, node.file, &mut declared);
    }
    let mut run = Run { schema, scalars, operations: &documents, files, declared, found: Vec::new(), notes: Vec::new(), edge: None };
    for node in nodes.iter().filter(|node| node.storage != dblayer::schema::Storage::Local) {
        run.check_model(node, nodes);
        run.check_change_payloads(node, nodes);
    }
    for node in standalone { run.check_standalone(node); }
    let mut found = run.found;
    found.sort_by(|left, right| (&left.file, left.error.line, &left.error.message).cmp(&(&right.file, right.error.line, &right.error.message)));
    found.dedup();
    if !found.is_empty() { return Err(found); }
    let idempotent_inputs = documents.named.iter().filter(|(_, operation)| operation.kind == "mutation")
        .flat_map(|(name, operation)| operation.variables.iter().flat_map(move |(variable, ty)| {
            schema.input_fields(ty.name()).filter(|fields| fields.contains_key("clientMutationId"))
                .map(|_| (name.clone(), variable.clone()))
        })).collect();
    let variables = documents.named.iter().map(|(name, operation)| (name.clone(), operation.variables.keys().cloned().collect())).collect();
    Ok(Outcome { variables, notes: run.notes, idempotent_inputs })
}

fn operation_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>, (PathBuf, String)> {
    let mut found = Vec::new();
    for path in paths {
        if !path.is_dir() {
            found.push(path.clone());
            continue;
        }
        let entries = fs::read_dir(path).map_err(|error| (path.clone(), format!("cannot read the operation directory: {error}")))?;
        let mut inside = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| (path.clone(), format!("cannot read the operation directory: {error}")))?;
            if entry.path().extension().is_some_and(|extension| extension == "graphql") {
                inside.push(entry.path());
            }
        }
        inside.sort();
        found.extend(inside);
    }
    Ok(found)
}

fn collect_declared<'a>(attributes: &'a [AttributeDecl], file: usize, declared: &mut Declared<'a>) {
    for attribute in attributes {
        let (TypeRef::Named(name) | TypeRef::NamedList(name)) = &attribute.ty else { continue };
        match &attribute.block {
            AttributeBlock::None => {}
            AttributeBlock::Fields(inner) => {
                declared.entry(name).or_insert((file, Body::Fields(inner)));
                collect_declared(inner, file, declared);
            }
            AttributeBlock::Union { shared, variants } => {
                declared.entry(name).or_insert((file, Body::Union(shared.as_ref().map(|shared| shared.item.as_slice()), variants)));
                if let Some(shared) = shared {
                    collect_declared(&shared.item, file, declared);
                }
                for variant in variants {
                    if let Some(fields) = &variant.fields {
                        declared.entry(&variant.type_name).or_insert((file, Body::Fields(fields)));
                        collect_declared(fields, file, declared);
                    }
                }
            }
        }
    }
}

fn fit_word(attribute: &AttributeDecl) -> (&'static str, Option<&'static str>) {
    let scalar = |kind: ScalarKind| match kind {
        ScalarKind::Str => "string",
        ScalarKind::Num => "num",
        ScalarKind::Int => "int",
        ScalarKind::Bool => "bool",
        ScalarKind::Date => "date",
        ScalarKind::Id => "id",
    };
    match &attribute.ty {
        TypeRef::Scalar(kind) => (scalar(*kind), None),
        TypeRef::Enum => ("enum", None),
        TypeRef::Json => ("raw", None),
        TypeRef::File => ("file", None),
        TypeRef::Named(_) if attribute.is_union() => ("union", None),
        TypeRef::Named(_) => ("object", None),
        TypeRef::NamedList(_) => ("array", None),
        TypeRef::List(ListElement::Scalar(kind)) => ("list", Some(scalar(*kind))),
        TypeRef::List(ListElement::Enum) => ("list", Some("enum")),
    }
}

fn exact_word(named: &str) -> Option<&'static str> {
    match named {
        "Int" => Some("int"),
        "Float" => Some("num"),
        "Boolean" => Some("bool"),
        _ => None,
    }
}

struct Run<'a> {
    schema: &'a Schema,
    scalars: &'a Scalars,
    operations: &'a Operations,
    files: &'a [String],
    declared: Declared<'a>,
    found: Vec<FileError>,
    notes: Vec<String>,
    edge: Option<String>,
}

#[derive(Clone, Copy)]
struct Spot<'l> {
    file: usize,
    at: Pos,
    label: &'l str,
}

struct UnionAt<'r, 'a> {
    node: &'a ModelDecl,
    attribute: &'a AttributeDecl,
    name: &'r str,
    shared: Option<&'a [AttributeDecl]>,
    variants: &'a [VariantDecl],
    file: usize,
    selects: &'r [&'a Select],
}

struct Scope<'a> {
    file: usize,
    keys: HashSet<String>,
    top: bool,
    lists: bool,
    selects: Vec<&'a Select>,
    apart: Vec<String>,
}

struct Walked<'a> {
    ty: Ty,
    nullable_hop: bool,
    first: String,
    selects: Vec<&'a Select>,
}

impl<'a> Scope<'a> {
    fn inside(file: usize, selects: Vec<&'a Select>) -> Self {
        Self { file, keys: HashSet::new(), top: false, lists: false, selects, apart: Vec::new() }
    }
}

fn query_of(scope: &ScopeDecl) -> &str {
    scope.query.as_ref().map(|query| query.operation.as_str()).expect("a remote scope has a query")
}

fn root_path(scope: &ScopeDecl) -> Option<&str> {
    scope.response.as_ref().and_then(|read| read.from.as_ref()).map(|found| found.item.as_str())
}










