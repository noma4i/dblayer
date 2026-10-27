use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::decl::{AssociationKind, AttributeBlock, AttributeDecl, CallDecl, CallKind, ListElement, ModelDecl, OperationDecl, OptimisticDecl, OptimisticValue, ParamBase, ParameterDecl, Pos, ScopeDecl, TypeRef, VariantDecl, VariableValue};
use crate::scalars::{self, Scalars};
use dblayer::schema::NodeRead;
use dblayer::value::ScalarKind;
use crate::{FileError, ParseError};

mod graphql;
mod inputs;
#[cfg(test)]
mod idempotency_tests;
#[cfg(test)]
mod event_payload_tests;
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
    let failed = |file: &Path, message: String| vec![FileError { file: file.display().to_string(), error: ParseError::new(0, 0, message) }];
    let mut documents = Operations::default();
    for path in operation_files(operations) {
        documents.load(&path).map_err(|message| failed(&path, message))?;
    }
    if let Some(spread) = documents.missing_spreads().into_iter().next() {
        return Err(vec![FileError { file: "operation documents".to_string(), error: ParseError::new(0, 0, format!("fragment `{spread}` is not in the operation documents")) }]);
    }
    let mut declared = Declared::new();
    for node in nodes {
        collect_declared(&node.attributes, node.file, &mut declared);
    }
    let mut run = Run { schema, scalars, operations: &documents, files, declared, found: Vec::new(), notes: Vec::new(), edge: None };
    for node in nodes.iter().filter(|node| node.storage != dblayer::schema::Storage::Local) {
        run.check_model(node);
        run.check_change_payloads(node, nodes);
    }
    for node in standalone { run.check_standalone(node); }
    let mut found = run.found;
    found.sort_by(|left, right| (&left.file, left.error.line, &left.error.message).cmp(&(&right.file, right.error.line, &right.error.message)));
    found.dedup();
    if !found.is_empty() { return Err(found); }
    let idempotent_inputs = documents.named.iter().filter(|(_, operation)| operation.kind == "mutation")
        .flat_map(|(name, operation)| operation.variables.iter().filter_map(move |(variable, ty)| {
            schema.input_fields(ty.name()).filter(|fields| fields.contains_key("clientMutationId"))
                .map(|_| (name.clone(), variable.clone()))
        })).collect();
    let variables = documents.named.iter().map(|(name, operation)| (name.clone(), operation.variables.keys().cloned().collect())).collect();
    Ok(Outcome { variables, notes: run.notes, idempotent_inputs })
}

fn operation_files(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut found = Vec::new();
    for path in paths {
        match fs::read_dir(path) {
            Ok(entries) => {
                let mut inside: Vec<PathBuf> = entries.filter_map(|entry| entry.ok().map(|entry| entry.path())).filter(|entry| entry.extension().is_some_and(|extension| extension == "graphql")).collect();
                inside.sort();
                found.extend(inside);
            }
            Err(_) => found.push(path.clone()),
        }
    }
    found
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
    scope.query.as_ref().map(|query| query.operation.as_str()).unwrap_or_default()
}

fn root_path(scope: &ScopeDecl) -> Option<&str> {
    scope.response.as_ref().and_then(|read| read.from.as_ref()).map(|found| found.item.as_str())
}

impl<'a> Run<'a> {
    fn fail(&mut self, file: usize, pos: Pos, message: String) {
        self.found.push(FileError { file: self.files[file].clone(), error: ParseError::new(pos.line, pos.column, message) });
    }

    fn check_model(&mut self, node: &'a ModelDecl) {
        let mut seen = HashSet::new();
        for remote in node.scopes.iter().filter(|scope| scope.is_remote()) {
            self.check_text_types(node, remote);
        }
        for remote in node.scopes.iter().filter(|scope| scope.is_remote()) {
            self.edge = None;
            let Some(element) = self.resolve(node, remote) else { continue };
            if let Some(edge) = self.edge.take() {
                self.check_edge_fields(node, remote, &edge);
            }
            if !seen.insert(element.clone()) {
                continue;
            }
            let keys = node.associations.iter().map(|association| crate::build::association_key(association, &node.name)).collect();
            let fields: Vec<&AttributeDecl> = node.attributes.iter().collect();
            let mut covered = BTreeSet::new();
            let selects: Vec<&'a Select> = node.scopes.iter().filter(|scope| scope.is_remote()).filter_map(|scope| self.element_select(scope)).collect();
            self.check_fields(node, &fields, &element, &Scope { keys, top: true, ..Scope::inside(node.file, selects.clone()) }, &mut covered, &mut Vec::new());
            self.check_associations(node, &element, selects);
            self.list_unmodeled(node, remote, &element, &covered);
        }
        for scope in node.scopes.iter().filter(|scope| scope.is_remote()) {
            self.check_media(node, scope);
            self.check_subscription_args(node, scope);
        }
        self.check_mutation_args(node);
        self.check_response_paths(node);
    }

    fn check_subscription_args(&mut self, node: &ModelDecl, scope: &ScopeDecl) {
        for subscription in &scope.subscriptions {
            let Some(operation) = self.operations.named.get(&subscription.name) else { continue };
            let name = &subscription.name;
            for (variable, declared) in &operation.variables {
                let parameter = scope.parameters.iter().find(|parameter| {
                    let renamed = scope.variables.iter().flatten().find(|binding| matches!(&binding.value, VariableValue::Parameter(source) if *source == parameter.name));
                    renamed.map_or(parameter.name.as_str(), |binding| binding.name.as_str()) == variable
                });
                let Some(parameter) = parameter else {
                    if !declared.optional() {
                        self.fail(node.file, subscription.pos, format!("scope `{}` subscription `{name}`: argument `${variable}` has no scope parameter, declare `parameter :{variable}` or make the argument optional", scope.name));
                    }
                    continue;
                };
                let accepted: &[&str] = match &parameter.ty.base {
                    ParamBase::Bool => &["Boolean"],
                    ParamBase::Int => &["Int"],
                    ParamBase::String | ParamBase::Id => &["String", "ID"],
                    ParamBase::Named(kind) => &[kind.as_str()],
                    _ => continue,
                };
                if declared.is_list() != parameter.ty.list || !accepted.contains(&declared.name()) {
                    self.fail(node.file, subscription.pos, format!("scope `{}` subscription `{name}`: argument `${variable}` is `{}` but scope parameter `{}` is {}", scope.name, declared.describe(), parameter.name, accepted.join(" or ")));
                }
            }
        }
    }

    fn check_edge_fields(&mut self, node: &'a ModelDecl, remote: &'a ScopeDecl, edge: &str) {
        let select = self.edge_select(remote);
        let documented = self.root_select(remote).is_some();
        for field in &remote.edges {
            let name = field.name.as_str();
            if documented && select.is_none() {
                self.fail(node.file, field.pos, format!("attribute `{name}`: the document does not select `edges`"));
                continue;
            }
            let key = field.from.as_ref().map_or(name, |found| found.item.as_str());
            let segments: Vec<&str> = key.split('.').collect();
            let selected = select.map(|select| self.operations.fields_in(select, segments[0], &[]));
            let real = selected.as_ref().and_then(|found| found.first().map(|field| field.name.clone())).unwrap_or_else(|| segments[0].to_string());
            if let Err(message) = self.schema.field(edge, &real) {
                self.fail(node.file, field.pos, format!("attribute `{name}`: {message}"));
                continue;
            }
            if selected.is_some_and(|found| found.is_empty()) {
                self.fail(node.file, field.pos, format!("attribute `{name}`: the document does not select `{}` on the edges", segments[0]));
                continue;
            }
            let selects: Vec<&'a Select> = select.into_iter().collect();
            let Some(reached) = self.walk(node.file, field.pos, edge, &segments, &Scope::inside(node.file, selects), name) else { continue };
            let word = fit_word(field).0;
            if let Some(expected) = exact_word(reached.ty.name()).filter(|expected| *expected != word && !reached.ty.is_list()) {
                self.fail(node.file, field.pos, format!("attribute `{name}` is declared `{word}` but the edge field is `{}`, declare `{expected}`", reached.ty.describe()));
                continue;
            }
            self.check_kind(field, name, &reached.ty, reached.nullable_hop, node.file);
        }
    }

    fn edge_select(&self, remote: &ScopeDecl) -> Option<&'a Select> {
        let select = self.root_select(remote)?;
        Some(&select.fields.iter().find(|field| field.name == "edges")?.select)
    }

    fn check_text_defaults(&mut self, node: &ModelDecl, remote: &ScopeDecl) {
        for parameter in remote.parameters.iter().filter(|parameter| parameter.default.is_some()) {
            let Some(default) = &parameter.default else { continue };
            let scope = &remote.name;
            let ParamBase::Named(kind) = &parameter.ty.base else { continue };
            let (Some(TypeKind::Enum(values)), Some(value)) = (self.schema.types.get(kind), default.item.as_str()) else { continue };
            if !values.iter().any(|candidate| candidate == value) {
                self.fail(node.file, default.pos, format!("scope `{scope}` default `{value}` is not a value of `{kind}`"));
            }
        }
    }

    fn check_text_types(&mut self, node: &ModelDecl, remote: &ScopeDecl) {
        self.check_text_defaults(node, remote);
        let document = query_of(remote);
        let linked: Vec<&str> = remote.where_.iter().flat_map(|clause| clause.pairs.iter().map(|(_, parameter)| parameter.item.as_str())).collect();
        let mapped = |param: &str| {
            let renamed = remote.variables.iter().flatten().find(|binding| matches!(&binding.value, VariableValue::Parameter(source) if source == param));
            renamed.map_or(param.to_string(), |binding| binding.name.clone())
        };
        let Some(operation) = self.operations.named.get(document) else { return };
        for parameter in remote.parameters.iter().filter(|parameter| !linked.contains(&parameter.name.as_str()) && !parameter.ty.list) {
            let kind = match &parameter.ty.base {
                ParamBase::Bool => "Boolean".to_string(),
                ParamBase::Int => "Int".to_string(),
                ParamBase::Named(kind) => {
                    if !matches!(self.schema.types.get(kind), Some(TypeKind::Enum(_))) {
                        self.fail(node.file, parameter.pos, format!("scope `{}` param `{}` names `{kind}`, which is not an enum of the schema", remote.name, parameter.name));
                        continue;
                    }
                    kind.clone()
                }
                _ => continue,
            };
            let variable = mapped(&parameter.name);
            match operation.variables.get(&variable) {
                None => self.fail(node.file, parameter.pos, format!("scope `{}` param `{}` is `{kind}` but operation `{document}` has no variable `${variable}`", remote.name, parameter.name)),
                Some(declared) if declared.is_list() || declared.name() != kind => self.fail(node.file, parameter.pos, format!("scope `{}` param `{}` is `{kind}` but the variable `${variable}` of `{document}` is `{}`", remote.name, parameter.name, declared.describe())),
                Some(_) => {}
            }
        }
    }

    fn check_mutation_args(&mut self, node: &'a ModelDecl) {
        for operation in &node.operations {
            let Some(name) = operation.mutation.as_ref().map(|found| found.item.as_str()) else { continue };
            let input: Vec<(String, String)> = match (&operation.variables, operation.upload.is_some()) {
                (Some(variables), _) => variables.iter().filter_map(|binding| match &binding.value {
                    VariableValue::Parameter(source) => Some((binding.name.clone(), source.clone())),
                    VariableValue::Constant(_) => None,
                }).collect(),
                (None, true) => operation.parameters.iter().map(|parameter| (parameter.name.clone(), parameter.name.clone())).collect(),
                (None, false) => Vec::new(),
            };
            self.check_args_of(node.file, operation.pos, name, operation.wrap.as_deref(), &operation.parameters, &input);
            self.check_optimistic_maps(node, operation);
            if let Some(batch) = &operation.batch {
                self.check_upload_batch(node, operation, batch, name);
            }
        }
    }

    fn check_optimistic_maps(&mut self, node: &ModelDecl, operation: &OperationDecl) {
        let Some((OptimisticDecl::Fields(fields), _)) = &operation.optimistic else { return };
        for (attribute, value, pos) in fields {
            let OptimisticValue::Mapped { from, map } = value else { continue };
            let Some(ParamBase::Named(kind)) = operation.parameters.iter().find(|parameter| parameter.name == *from).map(|parameter| &parameter.ty.base) else { continue };
            let Some(TypeKind::Enum(values)) = self.schema.types.get(kind) else {
                self.fail(node.file, *pos, format!("operation `{}` optimistic `{attribute}`: `{kind}` is not an enum of the schema", operation.name));
                continue;
            };
            let missing: Vec<&str> = values.iter().filter(|value| !map.iter().any(|(key, _)| key == *value)).map(String::as_str).collect();
            let unknown: Vec<&str> = map.iter().map(|(key, _)| key.as_str()).filter(|key| !values.iter().any(|value| value == key)).collect();
            if !missing.is_empty() {
                self.fail(node.file, *pos, format!("operation `{}` optimistic `{attribute}`: the map does not cover {} of `{kind}`", operation.name, missing.join(", ")));
            }
            if !unknown.is_empty() {
                self.fail(node.file, *pos, format!("operation `{}` optimistic `{attribute}`: {} is not a value of `{kind}`", operation.name, unknown.join(", ")));
            }
        }
    }

    fn check_args_of(&mut self, file: usize, pos: Pos, mutation: &str, wrap: Option<&str>, args: &[ParameterDecl], input: &[(String, String)]) {
        if args.is_empty() || self.operations.named.is_empty() {
            return;
        }
        let Some(operation) = self.operations.named.get(mutation).filter(|operation| operation.kind == "mutation") else {
            self.fail(file, pos, format!("operation `{mutation}` is not in the operation documents"));
            return;
        };
        let variables = &operation.variables;
        let (owner, accepted) = match wrap {
            None => (format!("operation `{mutation}`"), variables.clone()),
            Some(key) => {
                let Some(wrapper) = variables.get(key) else {
                    self.fail(file, pos, format!("operation `{mutation}` has no variable `${key}`"));
                    return;
                };
                let Some(fields) = self.schema.input_fields(wrapper.name()) else {
                    self.fail(file, pos, format!("variable `${key}` of `{mutation}` is `{}`, not an input type", wrapper.describe()));
                    return;
                };
                (format!("`{}`", wrapper.name()), fields)
            }
        };
        for arg in args {
            let kind = arg.ty.kind_text(arg.null);
            let Some(variable) = input.iter().find(|(_, source)| *source == arg.name).map(|(name, _)| name.as_str()) else { continue };
            let Some(declared) = accepted.get(variable) else {
                self.fail(file, pos, format!("`{variable}` is not an input of {owner}"));
                continue;
            };
            let base = kind.trim_end_matches('?');
            if !self.arg_fits(base, declared) {
                self.fail(file, pos, format!("parameter `{}` of kind `{}` does not fit `{variable}: {}`{}", arg.name, crate::operation::display_kind(&kind), declared.describe(), self.unmapped_hint(declared.name())));
            } else if kind.ends_with('?') && !declared.optional() {
                self.fail(file, pos, format!("parameter `{}` may be nil but `{variable}: {}` is required", arg.name, declared.describe()));
            }
        }
    }

    fn check_upload_batch(&mut self, node: &ModelDecl, call: &OperationDecl, batch: &crate::decl::BatchDecl, mutation: &str) {
        let wrap = call.wrap.as_deref();
        let fail = |run: &mut Self, message| run.fail(node.file, call.pos, message);
        let Some(operation) = self.operations.named.get(mutation) else {
            fail(self, "upload batch mutation is missing from the operation documents".into());
            return;
        };
        let accepted = match wrap {
            Some(wrap) => operation.variables.get(wrap).and_then(|ty| self.schema.input_fields(ty.name())).unwrap_or_default(),
            None => operation.variables.clone(),
        };
        let items = batch.items.as_str();
        let Some(ty) = accepted.get(items).filter(|ty| ty.is_list()) else {
            fail(self, format!("upload batch `{items}` must be a list of input objects"));
            return;
        };
        let Some(fields) = self.schema.input_fields(ty.name()) else {
            fail(self, format!("upload batch `{items}` must contain input objects"));
            return;
        };
        if fields.get("signedId").is_none_or(|ty| !self.arg_fits("string", ty)) {
            fail(self, "upload batch items require a string signedId".into());
        }
        for (target, source) in &batch.input {
            let kind = node.attributes.iter().find(|field| &field.name == source).map(|field| fit_word(field).0);
            if fields.get(target).is_none_or(|ty| kind.is_none_or(|kind| !self.arg_fits(kind, ty))) {
                fail(self, format!("upload item `{target}` does not accept attribute `{source}`"));
            }
        }
        let single_operation = node.operations.iter().find(|candidate| candidate.name == batch.single);
        let name = single_operation.and_then(|single| single.mutation.as_ref()).map(|found| found.item.as_str());
        let Some(operation) = name.and_then(|name| self.operations.named.get(name)) else {
            fail(self, "single upload mutation is missing from the operation documents".into());
            return;
        };
        let single_fields = match single_operation.and_then(|single| single.wrap.as_deref()) {
            Some(wrap) => operation.variables.get(wrap).and_then(|ty| self.schema.input_fields(ty.name())).unwrap_or_default(),
            None => operation.variables.clone(),
        };
        for key in std::iter::once("signedId").chain(batch.input.iter().map(|(target, _)| target.as_str())) {
            if single_fields.get(key).is_none_or(|ty| fields.get(key).is_none_or(|item| item != ty)) {
                fail(self, format!("single upload variable `{key}` must match the batch item field"));
            }
        }
    }

    fn custom_fits(&self, named: &str, kind: &str) -> bool {
        matches!(self.schema.types.get(named), Some(TypeKind::Scalar)) && self.scalars.get(named).is_some_and(|mapped| scalars::fits(mapped, kind))
    }

    fn unmapped_hint(&self, named: &str) -> String {
        if matches!(self.schema.types.get(named), Some(TypeKind::Scalar)) && !Schema::is_builtin_scalar(named) && !self.scalars.contains_key(named) { format!(": {}", scalars::unmapped(named)) } else { String::new() }
    }

    fn arg_fits(&self, kind: &str, declared: &Ty) -> bool {
        let named = declared.name();
        match kind {
            _ if kind.starts_with('[') => {
                let TyKind::List(item) = &declared.kind else { return false };
                let inner = kind.trim_start_matches('[').trim_end_matches(']');
                (!inner.ends_with('?') || item.optional()) && !item.is_list() && self.arg_fits(inner.trim_end_matches('?'), item)
            }
            "ids" => declared.is_list() && (matches!(named, "ID" | "String" | "Int") || self.custom_fits(named, "id")),
            _ if declared.is_list() => false,
            "string" => matches!(named, "String" | "ID") || self.custom_fits(named, "string"),
            "id" => matches!(named, "ID" | "String" | "Int") || self.custom_fits(named, "id"),
            "int" => named == "Int" || self.custom_fits(named, "int"),
            "long" => self.custom_fits(named, "long"),
            "num" => matches!(named, "Float" | "Int") || self.custom_fits(named, "num"),
            "bool" => named == "Boolean" || self.custom_fits(named, "bool"),
            _ => named == kind && matches!(self.schema.types.get(named), Some(TypeKind::Input(_) | TypeKind::Enum(_))),
        }
    }

    fn resolve(&mut self, node: &ModelDecl, remote: &ScopeDecl) -> Option<String> {
        let query = remote.query.as_ref()?;
        let name = query.operation.clone();
        let root = if self.operations.named.is_empty() {
            graphql::Root { key: name.clone(), field: name.clone() }
        } else if let Some(root) = self.operations.named.get(&name).filter(|operation| operation.kind == "query").and_then(|operation| operation.root.as_ref()) {
            root.clone()
        } else {
            self.fail(node.file, query.pos, format!("operation `{name}` is not in the operation documents"));
            return None;
        };
        let path = root_path(remote).unwrap_or(&root.key).to_string();
        let (reached, miss) = self.follow_schema_path(node.file, query.pos, "response from", &name, &root, &path)?;
        if let Some(miss) = miss {
            let at = remote.response.as_ref().and_then(|read| read.from.as_ref()).map_or(query.pos, |found| found.pos);
            self.report_miss(node.file, at, "response from", &name, &path, &[], &path.split('.').collect::<Vec<_>>(), miss);
            return None;
        }
        let paginates = remote.paginate.is_some();
        let lists = remote.node == Some(NodeRead::Many) || remote.response.as_ref().is_some_and(|read| read.shape == Some(crate::decl::ResponseShape::List));
        let pos = remote.paginate.as_ref().map(|paginate| paginate.pos).or_else(|| remote.response.as_ref().map(|read| read.pos)).unwrap_or(query.pos);
        if paginates {
            let element = self.connection_node(node, pos, &reached, remote)?;
            self.check_total(node, remote, &name, &root)?;
            return Some(element);
        }
        if lists != reached.is_list() {
            let expected = if lists { "a list" } else { "one object" };
            self.fail(node.file, pos, format!("scope `{}` expects {expected} but `{path}` returns `{}`", remote.name, reached.describe()));
            return None;
        }
        Some(reached.name().to_string())
    }

    fn check_total(&mut self, node: &ModelDecl, remote: &ScopeDecl, operation: &str, root: &graphql::Root) -> Option<()> {
        let Some(total) = &remote.total else { return Some(()) };
        let path = total.item.clone();
        let (reached, miss) = self.follow_schema_path(node.file, total.pos, "total", operation, root, &path)?;
        if reached.name() != "Int" || reached.is_list() || !path.contains('.') {
            self.fail(node.file, total.pos, format!("total `{path}` must be an `Int` but is `{}`", reached.describe()));
            return None;
        }
        if let Some(miss) = miss {
            self.report_miss(node.file, total.pos, "total", operation, &path, &[], &path.split('.').collect::<Vec<_>>(), miss);
            return None;
        }
        Some(())
    }

    fn follow_schema_path(&mut self, file: usize, pos: Pos, label: &str, operation: &str, root: &graphql::Root, path: &str) -> Option<(Ty, Option<graphql::Miss>)> {
        let keys: Vec<&str> = path.split('.').collect();
        if keys[0] != root.key {
            self.fail(file, pos, format!("{label} `{path}` must start with the first field `{}` of operation `{operation}`", root.key));
            return None;
        }
        let mut reached = match self.schema.field(&self.schema.query, &root.field) {
            Ok(reached) => reached,
            Err(message) => {
                self.fail(file, pos, message);
                return None;
            }
        };
        if self.schema.is_interface(reached.name()) {
            reached = self.enter_implementation(file, pos, operation, &root.key, reached)?;
        }
        let followed = paths::segments_of(path).and_then(|_| self.follow_quiet(operation, None, &keys));
        let names: Vec<&str> = match &followed {
            Some(Ok(found)) => found.names.clone(),
            _ => keys.clone(),
        };
        for segment in names.into_iter().skip(1) {
            reached = match self.schema.field(reached.name(), segment) {
                Ok(next) if !reached.is_list() => next,
                Ok(_) => {
                    self.fail(file, pos, format!("{label} `{path}` crosses the list `{}`", reached.describe()));
                    return None;
                }
                Err(message) => {
                    self.fail(file, pos, message);
                    return None;
                }
            };
        }
        Some((reached, followed.and_then(Result::err)))
    }

    fn enter_implementation(&mut self, file: usize, pos: Pos, operation: &str, key: &str, reached: Ty) -> Option<Ty> {
        let interface = reached.name().to_string();
        let Some(select) = self.operations.named.get(operation).and_then(|found| found.select.fields.iter().find(|field| field.key == key)).map(|field| &field.select) else {
            self.fail(file, pos, format!("`{interface}` root needs the operation documents of `{operation}`"));
            return None;
        };
        let mut types: Vec<&str> = Vec::new();
        let mut bare: Vec<&str> = Vec::new();
        self.operations.each_select(select, None, &mut |inner, on| match on {
            Some(on) if on != interface => types.extend((!types.contains(&on)).then_some(on)),
            _ => bare.extend(inner.fields.iter().map(|field| field.key.as_str())),
        }, &mut Vec::new());
        let problems: Vec<String> = bare.iter().map(|field| format!("field `{field}` of `{key}` is selected outside `... on`, a type that does not match would still give a row")).collect();
        let implementation = match types.as_slice() {
            [one] if self.schema.implementers.get(&interface).is_some_and(|all| all.iter().any(|name| name == one)) => Some(one.to_string()),
            [one] => {
                self.fail(file, pos, format!("`... on {one}` is not an implementation of `{interface}`"));
                None
            }
            [] => {
                self.fail(file, pos, format!("`{key}` of `{operation}` is `{interface}`: select its fields under `... on <Type>`"));
                None
            }
            _ => {
                self.fail(file, pos, format!("`{key}` of `{operation}` selects more than one `... on` type: {}", types.join(", ")));
                None
            }
        };
        for problem in problems {
            self.fail(file, pos, problem);
        }
        let named = |name: String| Ty { kind: TyKind::Named(name), nullable: true, has_default: false };
        implementation.map(|name| Ty { nullable: reached.nullable, ..if reached.is_list() { named(name).into_list() } else { named(name) } })
    }

    fn connection_node(&mut self, node: &ModelDecl, pos: Pos, connection: &Ty, remote: &ScopeDecl) -> Option<String> {
        let nodes = self.schema.field(connection.name(), "nodes").ok().filter(Ty::is_list);
        let edges = self.schema.field(connection.name(), "edges").ok();
        self.edge = edges.as_ref().map(|edges| edges.name().to_string());
        let by_edges = nodes.is_none() || (self.edge_select(remote).is_some() && !self.selects_nodes(remote));
        let found = match (edges, nodes) {
            (Some(edges), _) if by_edges => self.schema.field(edges.name(), "node").ok().map(|edge| edge.name().to_string()),
            (_, Some(nodes)) => Some(nodes.name().to_string()),
            _ => None,
        };
        if found.is_none() {
            self.fail(node.file, pos, format!("`paginate` needs `nodes` or `edges {{ node }}` on `{}`", connection.name()));
        }
        found
    }

    fn body_of(&self, attribute: &'a AttributeDecl, file: usize) -> Option<(usize, Body<'a>)> {
        match &attribute.block {
            AttributeBlock::Fields(inner) => Some((file, Body::Fields(inner))),
            AttributeBlock::Union { shared, variants } => Some((file, Body::Union(shared.as_ref().map(|shared| shared.item.as_slice()), variants))),
            AttributeBlock::None => match &attribute.ty {
                TypeRef::Named(name) | TypeRef::NamedList(name) => self.declared.get(name.as_str()).copied(),
                _ => None,
            },
        }
    }

    fn check_fields(&mut self, node: &'a ModelDecl, attributes: &[&'a AttributeDecl], owner: &str, scope: &Scope<'a>, covered: &mut BTreeSet<String>, stack: &mut Vec<(String, String)>) {
        for attribute in attributes {
            let name = attribute.name.as_str();
            if attribute.ty == TypeRef::File || attribute.local || name.is_empty() {
                continue;
            }
            let Some(path) = self.path_of(node, attribute, name) else { continue };
            let segments: Vec<&str> = path.iter().map(String::as_str).collect();
            let Some(reached) = self.walk(scope.file, attribute.pos, owner, &segments, scope, name) else { continue };
            covered.insert(reached.first.clone());
            self.check_kind(attribute, name, &reached.ty, reached.nullable_hop, scope.file);
            let Some((file, body)) = self.body_of(attribute, scope.file) else { continue };
            match body {
                Body::Union(shared, variants) => {
                    for select in &reached.selects {
                        self.check_union_selection(scope.file, attribute, reached.ty.name(), variants, select);
                    }
                    self.check_union(node, attribute, reached.ty.name(), shared, variants, file, &reached.selects, stack);
                }
                Body::Fields(children) => {
                    let type_name = match &attribute.ty {
                        TypeRef::Named(type_name) | TypeRef::NamedList(type_name) => type_name.as_str(),
                        _ => name,
                    };
                    let marker = (type_name.to_string(), reached.ty.name().to_string());
                    if !matches!(self.schema.types.get(reached.ty.name()), Some(TypeKind::Fields(_))) || stack.contains(&marker) {
                        continue;
                    }
                    stack.push(marker);
                    let inner: Vec<&AttributeDecl> = children.iter().collect();
                    self.check_fields(node, &inner, reached.ty.name(), &Scope::inside(file, reached.selects), &mut BTreeSet::new(), stack);
                    stack.pop();
                }
            }
        }
    }

    fn element_select(&self, remote: &ScopeDecl) -> Option<&'a Select> {
        let mut select = self.root_select(remote)?;
        if remote.paginate.is_some() {
            select = match select.fields.iter().find(|field| field.name == "nodes") {
                Some(nodes) => &nodes.select,
                None => &select.fields.iter().find(|field| field.name == "edges")?.select.fields.iter().find(|field| field.name == "node")?.select,
            };
        }
        Some(select)
    }

    fn selects_nodes(&self, remote: &ScopeDecl) -> bool {
        self.root_select(remote).is_some_and(|select| select.fields.iter().any(|field| field.name == "nodes"))
    }

    fn root_select(&self, remote: &ScopeDecl) -> Option<&'a Select> {
        let operations: &'a Operations = self.operations;
        let operation = operations.named.get(query_of(remote))?;
        let root = operation.root.as_ref()?;
        let path = root_path(remote).unwrap_or(&root.key);
        let keys: Vec<&str> = path.split('.').collect();
        operations.follow(&[&operation.select], &keys).ok()?.selects.first().copied()
    }

    fn check_union_selection(&mut self, file: usize, attribute: &AttributeDecl, union: &str, variants: &[VariantDecl], select: &Select) {
        let noun = if self.schema.is_interface(union) { "interface" } else { "union" };
        let mut found: (bool, Vec<&str>) = (false, Vec::new());
        self.operations.each_select(select, None, &mut |inner, on| {
            found.0 |= inner.typename;
            found.1.extend(on.filter(|on| *on != union));
        }, &mut Vec::new());
        let name = attribute.name.as_str();
        if !found.0 {
            self.fail(file, attribute.pos, format!("attribute `{name}`: the selection of `{}` must select `__typename`", attribute.from.as_ref().map_or(name, |found| found.item.as_str())));
        }
        for on in found.1.iter().filter(|on| !variants.iter().any(|variant| variant.type_name == **on)) {
            self.fail(file, attribute.pos, format!("attribute `{name}`: `... on {on}` is not a declared variant of {noun} `{union}`"));
        }
    }

    fn check_union(&mut self, node: &'a ModelDecl, attribute: &'a AttributeDecl, union: &str, shared: Option<&'a [AttributeDecl]>, variants: &'a [VariantDecl], file: usize, selects: &[&'a Select], stack: &mut Vec<(String, String)>) {
        let interface = self.schema.is_interface(union);
        let members: Vec<String> = match self.schema.types.get(union) {
            Some(TypeKind::Union(members)) => members.clone(),
            Some(TypeKind::Fields(_)) if interface => self.schema.implementers.get(union).cloned().unwrap_or_default(),
            _ => return,
        };
        let declared: Vec<&str> = variants.iter().map(|variant| variant.type_name.as_str()).collect();
        for variant in variants {
            if !members.iter().any(|member| *member == variant.type_name) {
                let message = if interface { format!("variant `{}` is not an implementation of interface `{union}`", variant.type_name) } else { format!("variant `{}` is not a member of union `{union}`", variant.type_name) };
                self.fail(file, variant.pos, message);
            }
        }
        for member in members.iter().filter(|member| !declared.contains(&member.as_str())) {
            let pos = variants.first().map_or(attribute.pos, |variant| variant.pos);
            let message = if interface { format!("implementation `{member}` of interface `{union}` is not declared") } else { format!("variant `{member}` of union `{union}` is not declared") };
            self.fail(file, pos, message);
        }
        if let Some(shared) = shared {
            self.check_shared(node, shared, attribute.pos, union, &members, interface, file, selects, stack);
        }
        for variant in variants.iter().filter(|variant| members.iter().any(|member| *member == variant.type_name)) {
            let Some(block) = &variant.fields else { continue };
            let marker = (variant.type_name.clone(), union.to_string());
            if stack.contains(&marker) {
                continue;
            }
            stack.push(marker);
            let inner: Vec<&AttributeDecl> = block.iter().collect();
            let apart = declared.iter().filter(|other| **other != variant.type_name).map(|other| other.to_string()).collect();
            self.check_fields(node, &inner, &variant.type_name, &Scope { apart, ..Scope::inside(file, selects.to_vec()) }, &mut BTreeSet::new(), stack);
            stack.pop();
        }
    }

    fn check_shared(&mut self, node: &'a ModelDecl, shared: &'a [AttributeDecl], pos: Pos, union: &str, members: &[String], interface: bool, file: usize, selects: &[&'a Select], stack: &mut Vec<(String, String)>) {
        if !interface {
            self.fail(file, pos, format!("`shared` needs an interface: `{union}` is a union"));
            return;
        }
        let fields: Vec<&AttributeDecl> = shared.iter().collect();
        self.check_fields(node, &fields, union, &Scope::inside(file, selects.to_vec()), &mut BTreeSet::new(), stack);
        for field in &fields {
            let key = field.from.as_ref().map_or(field.name.as_str(), |found| found.item.as_str()).split('.').next().unwrap_or_default();
            for member in members {
                let apart: Vec<String> = members.iter().filter(|other| *other != member).cloned().collect();
                let name = selects.iter().find_map(|select| self.operations.fields_in(select, key, &apart).first().map(|found| found.name.as_str())).unwrap_or(key);
                if self.schema.field(member, name).is_err() {
                    self.fail(file, field.pos, format!("implementation `{member}` of interface `{union}` has no field `{name}`"));
                }
            }
        }
    }

    fn path_of(&mut self, node: &ModelDecl, attribute: &AttributeDecl, name: &str) -> Option<Vec<String>> {
        if let Some(parent) = &attribute.id_from {
            let association = node.associations.iter().find(|candidate| matches!(candidate.kind, AssociationKind::BelongsTo | AssociationKind::HasOne) && candidate.name == *parent);
            let base = association.and_then(|association| association.from.as_deref()).unwrap_or(parent);
            return Some(base.split('.').map(str::to_string).chain(["id".to_string()]).collect());
        }
        Some(attribute.from.as_ref().map_or(name, |found| found.item.as_str()).split('.').map(str::to_string).collect())
    }

    fn walk(&mut self, file: usize, pos: Pos, owner: &str, segments: &[&str], scope: &Scope<'a>, name: &str) -> Option<Walked<'a>> {
        let operations: &'a Operations = self.operations;
        let mut selects = scope.selects.clone();
        let mut first = String::new();
        let mut current = owner.to_string();
        let mut nullable_hop = false;
        let mut crossed = false;
        for (index, key) in segments.iter().enumerate() {
            let selected: Vec<&'a graphql::SelectedField> = selects.iter().flat_map(|inside| operations.fields_in(inside, key, &scope.apart)).collect();
            let segment = selected.first().map_or(*key, |field| field.name.as_str());
            selects = selected.iter().map(|field| &field.select).collect();
            if index == 0 {
                first = segment.to_string();
            }
            let reached = match self.schema.field(&current, segment) {
                Ok(reached) => reached,
                Err(_) if scope.top && scope.keys.contains(name) => return None,
                Err(message) => {
                    self.fail(file, pos, format!("attribute `{name}`: {message}"));
                    return None;
                }
            };
            if index + 1 == segments.len() {
                return Some(Walked { ty: if crossed && !reached.is_list() { reached.into_list() } else { reached }, nullable_hop, first, selects });
            }
            if reached.is_list() && !scope.lists {
                self.fail(file, pos, format!("attribute `{name}`: path `{}` crosses the list `{segment}`", segments.join(".")));
                return None;
            }
            crossed |= reached.is_list();
            nullable_hop |= reached.nullable;
            current = reached.name().to_string();
        }
        None
    }

    fn check_kind(&mut self, attribute: &AttributeDecl, name: &str, reached: &Ty, nullable_hop: bool, file: usize) {
        let (word, _) = fit_word(attribute);
        if word == "raw" {
            return;
        }
        let mut problems = Vec::new();
        if name != "id" && (reached.nullable || nullable_hop) && !(attribute.null || attribute.optional || attribute.default.is_some()) {
            problems.push(format!("is nullable in the schema (`{}`), declare `null: true`", reached.describe()));
        }
        let wants_list = matches!(word, "list" | "array");
        if wants_list != reached.is_list() {
            let schema_shape = if reached.is_list() { "a list" } else { "not a list" };
            problems.push(format!("`{word}` does not fit `{}`, which is {schema_shape}", reached.describe()));
        } else if !self.named_fits(attribute, reached.name()) {
            problems.push(format!("`{word}` does not fit the schema type `{}`{}", reached.describe(), self.unmapped_hint(reached.name())));
        }
        if matches!(attribute.ty, TypeRef::Enum | TypeRef::List(ListElement::Enum)) {
            problems.extend(self.enum_problems(attribute, reached.name()));
        }
        if attribute.is_union() && !matches!(self.schema.types.get(reached.name()), Some(TypeKind::Union(_))) && !self.schema.is_interface(reached.name()) {
            problems.push(format!("is not a union: schema type `{}` is not one", reached.name()));
        }
        for problem in problems {
            self.fail(file, attribute.pos, format!("attribute `{name}` {problem}"));
        }
    }

    fn named_fits(&self, attribute: &AttributeDecl, named: &str) -> bool {
        let kind = self.schema.types.get(named);
        let builtin = Schema::is_builtin_scalar(named);
        let structured = matches!(kind, Some(TypeKind::Fields(_) | TypeKind::Union(_))) || self.custom_fits(named, "object");
        let (word, of) = fit_word(attribute);
        let word = if word == "list" { of.unwrap_or("string") } else { word };
        match word {
            "string" => matches!(named, "String" | "ID") || self.custom_fits(named, "string"),
            "id" => matches!(named, "ID" | "String" | "Int") || self.custom_fits(named, "id"),
            "int" => named == "Int" || self.custom_fits(named, "int"),
            "num" => matches!(named, "Float" | "Int") || self.custom_fits(named, "num"),
            "bool" => named == "Boolean" || self.custom_fits(named, "bool"),
            "date" => named == "String" || self.custom_fits(named, "date"),
            "enum" => named == "String" || matches!(kind, Some(TypeKind::Enum(_))),
            "object" => structured,
            "union" => true,
            "array" => structured || builtin,
            _ => true,
        }
    }

    fn enum_problems(&self, attribute: &AttributeDecl, named: &str) -> Vec<String> {
        let Some(TypeKind::Enum(known)) = self.schema.types.get(named) else { return Vec::new() };
        let Some(values) = &attribute.values else { return Vec::new() };
        let missing: Vec<&str> = values.iter().map(String::as_str).filter(|value| !known.iter().any(|candidate| candidate == value)).collect();
        if missing.is_empty() { Vec::new() } else { vec![format!("lists values {} that enum `{named}` does not have", missing.join(", "))] }
    }

    fn check_associations(&mut self, node: &ModelDecl, element: &str, selects: Vec<&'a Select>) {
        for association in &node.associations {
            let Some(path) = &association.from else { continue };
            let segments: Vec<&str> = path.split('.').collect();
            let Some(Walked { ty: reached, .. }) = self.walk(node.file, association.pos, element, &segments, &Scope { lists: true, ..Scope::inside(node.file, selects.clone()) }, &association.name) else { continue };
            let one = matches!(association.kind, AssociationKind::BelongsTo | AssociationKind::HasOne);
            if one == reached.is_list() {
                let expected = if one { "one object" } else { "a list" };
                self.fail(node.file, association.pos, format!("`{}` `{}` expects {expected} but `{path}` is `{}`", association.kind.word(), association.name, reached.describe()));
            }
        }
    }

    fn list_unmodeled(&mut self, node: &ModelDecl, remote: &ScopeDecl, element: &str, covered: &BTreeSet<String>) {
        let Some(fields) = self.schema.fields(element) else { return };
        let missing: BTreeSet<&str> = fields.keys().map(String::as_str).filter(|name| !covered.contains(*name) && !name.starts_with("__")).collect();
        if !missing.is_empty() {
            self.notes.push(format!("{}: model `{}` scope `{}` does not use {} fields of `{element}`: {}", self.files[node.file], node.name, remote.name, missing.len(), missing.into_iter().collect::<Vec<_>>().join(", ")));
        }
    }
}

#[cfg(test)]
mod schema_check_tests;

#[cfg(test)]
mod node_tests;
#[cfg(test)]
mod members_tests;
#[cfg(test)]
mod total_tests;

#[cfg(test)]
mod union_tests;

#[cfg(test)]
mod interface_tests;

#[cfg(test)]
mod alias_tests;

#[cfg(test)]
mod edge_tests;

#[cfg(test)]
mod paths_tests;

#[cfg(test)]
mod total_alias_tests;

#[cfg(test)]
mod edge_nodes_tests;
