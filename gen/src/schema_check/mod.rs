use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::parser::{Call, ModelNode, Node, OperationNode, Pair, Value};
use crate::scalars::{self, Scalars};
use crate::{FileError, ParseError};

mod graphql;
mod inputs;
mod paths;
mod standalone;

pub use inputs::{InputField, InputObject, SchemaTypes, Ty, TyKind};
pub(crate) use inputs::load_schema_types;

use graphql::{Operations, Select};
pub(crate) use graphql::{Schema, TypeKind};

type Declared<'a> = HashMap<&'a str, (usize, &'a [Call])>;

pub struct Outcome {
    pub notes: Vec<String>,
}

pub fn check_schema(schema: &Schema, scalars: &Scalars, operations: &[PathBuf], files: &[String], nodes: &[ModelNode], standalone: &[OperationNode]) -> Result<Outcome, Vec<FileError>> {
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
        for schema_block in node.body.iter().filter(|call| call.word == "schema") {
            collect_declared(schema_block.block.as_deref().unwrap_or_default(), node.file, &mut declared);
        }
    }
    let mut run = Run { schema, scalars, operations: &documents, files, declared, found: Vec::new(), notes: Vec::new(), edge: None };
    for node in nodes.iter().filter(|node| !is_local(node)) {
        run.check_model(node);
    }
    for node in standalone { run.check_standalone(node); }
    let mut found = run.found;
    found.sort_by(|left, right| (&left.file, left.error.line, &left.error.message).cmp(&(&right.file, right.error.line, &right.error.message)));
    found.dedup();
    if found.is_empty() { Ok(Outcome { notes: run.notes }) } else { Err(found) }
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

fn collect_declared<'a>(calls: &'a [Call], file: usize, declared: &mut Declared<'a>) {
    for call in calls {
        let declared_name = if call.word == "variant" { Some(name_of(call)) } else { option_text(call, "as") };
        if let (Some(block), Some(name)) = (&call.block, declared_name) {
            declared.entry(name).or_insert((file, block.as_slice()));
        }
        collect_declared(call.block.as_deref().unwrap_or_default(), file, declared);
    }
}

fn is_local(node: &ModelNode) -> bool {
    node.options.iter().any(|pair| pair.key == "storage" && matches!(&pair.value.item, Value::Symbol(name) if name == "local"))
}

fn text(value: &Value) -> Option<&str> {
    match value {
        Value::Symbol(text) | Value::Str(text) | Value::Const(text) => Some(text),
        _ => None,
    }
}

fn option<'a>(call: &'a Call, key: &str) -> Option<&'a Pair> {
    call.options.iter().find(|pair| pair.key == key)
}

fn option_text<'a>(call: &'a Call, key: &str) -> Option<&'a str> {
    option(call, key).and_then(|pair| text(&pair.value.item))
}

fn flag(call: &Call, key: &str) -> bool {
    option(call, key).is_some_and(|pair| pair.value.item == Value::Bool(true))
}

fn exact_word(named: &str) -> Option<&'static str> {
    match named {
        "Int" => Some("int"),
        "Float" => Some("num"),
        "Boolean" => Some("bool"),
        _ => None,
    }
}

fn name_of(call: &Call) -> &str {
    call.args.first().and_then(|arg| text(&arg.item)).unwrap_or_default()
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

impl<'a> Run<'a> {
    fn fail(&mut self, file: usize, line: usize, column: usize, message: String) {
        self.found.push(FileError { file: self.files[file].clone(), error: ParseError::new(line, column, message) });
    }

    fn check_model(&mut self, node: &'a ModelNode) {
        let mut seen = HashSet::new();
        for remote in node.body.iter().filter(|call| call.word == "remote") {
            self.check_text_types(node, remote);
        }
        for remote in node.body.iter().filter(|call| call.word == "remote") {
            self.edge = None;
            let Some(element) = self.resolve(node, remote) else { continue };
            if let Some(edge) = self.edge.take() {
                self.check_edge_fields(node, remote, &edge);
            }
            if !seen.insert(element.clone()) {
                continue;
            }
            let keys = node.body.iter().filter(|call| matches!(call.word.as_str(), "belongs_to" | "has_one" | "has_many" | "references")).map(|call| crate::build::association_key(call, &node.name)).collect();
            let fields: Vec<&Call> = node.body.iter().filter(|call| call.word == "schema").flat_map(|call| call.block.iter().flatten()).collect();
            let mut covered = BTreeSet::new();
            let selects: Vec<&'a Select> = node.body.iter().filter(|call| call.word == "remote").filter_map(|call| self.element_select(call)).collect();
            self.check_fields(node, &fields, &element, &Scope { keys, top: true, ..Scope::inside(node.file, selects.clone()) }, &mut covered, &mut Vec::new());
            self.check_associations(node, &element, selects);
            self.list_unmodeled(node, remote, &element, &covered);
        }
        self.check_mutation_args(node);
        self.check_response_paths(node);
    }

    fn check_edge_fields(&mut self, node: &'a ModelNode, remote: &'a Call, edge: &str) {
        let select = self.edge_select(remote);
        for field in remote.block.iter().flatten().filter(|call| call.word == "edge").flat_map(|call| call.block.iter().flatten()) {
            let name = name_of(field);
            let key = option_text(field, "from").unwrap_or(name);
            let segments: Vec<&str> = key.split('.').collect();
            let selected = select.map(|select| self.operations.fields_in(select, segments[0], &[]));
            let real = selected.as_ref().and_then(|found| found.first().map(|field| field.name.clone())).unwrap_or_else(|| segments[0].to_string());
            if let Err(message) = self.schema.field(edge, &real) {
                self.fail(node.file, field.line, field.column, format!("field `{name}`: {message}"));
                continue;
            }
            if selected.is_some_and(|found| found.is_empty()) {
                self.fail(node.file, field.line, field.column, format!("field `{name}`: the document does not select `{}` on the edges", segments[0]));
                continue;
            }
            let selects: Vec<&'a Select> = select.into_iter().collect();
            let Some(reached) = self.walk(node.file, field, edge, &segments, &Scope::inside(node.file, selects), name) else { continue };
            if let Some(expected) = exact_word(reached.ty.name()).filter(|expected| *expected != field.word && !reached.ty.is_list()) {
                self.fail(node.file, field.line, field.column, format!("field `{name}` is declared `{}` but the edge field is `{}`, declare `{expected}`", field.word, reached.ty.describe()));
                continue;
            }
            self.check_kind(node, field, name, &reached.ty, reached.nullable_hop, node.file);
        }
    }

    fn edge_select(&self, remote: &Call) -> Option<&'a Select> {
        let select = self.root_select(remote)?;
        Some(&select.fields.iter().find(|field| field.name == "edges")?.select)
    }

    fn check_text_defaults(&mut self, node: &ModelNode, remote: &Call, items: &[Node<Value>]) {
        let Some(Pair { value: Node { item: Value::Hash(defaults), .. }, .. }) = option(remote, "defaults") else { return };
        let typed: Vec<&Pair> = items.iter().filter_map(|item| match &item.item { Value::Hash(pairs) => Some(pairs.iter()), _ => None }).flatten().collect();
        for default in defaults {
            let (line, column) = (default.value.line, default.value.column);
            let scope = name_of(remote);
            let declared = items.iter().any(|item| matches!(&item.item, Value::Symbol(name) | Value::Str(name) if *name == default.key));
            let Some(param) = typed.iter().find(|pair| pair.key == default.key) else {
                let message = if declared { format!("scope `{scope}` default for `{}` needs an enum param", default.key) } else { format!("scope `{scope}` default for `{}` names no param", default.key) };
                self.fail(node.file, line, column, message);
                continue;
            };
            let (Some(TypeKind::Enum(values)), Some(value)) = (text(&param.value.item).and_then(|kind| self.schema.types.get(kind)), text(&default.value.item)) else { continue };
            if !values.iter().any(|candidate| candidate == value) {
                let kind = text(&param.value.item).unwrap_or_default();
                self.fail(node.file, line, column, format!("scope `{scope}` default `{value}` is not a value of `{kind}`"));
            }
        }
    }

    fn check_text_types(&mut self, node: &ModelNode, remote: &Call) {
        let items: &[Node<Value>] = match option(remote, "text") {
            Some(Pair { value: Node { item: Value::Array(items), .. }, .. }) => items,
            _ => &[],
        };
        self.check_text_defaults(node, remote, items);
        let lines = remote.block.as_deref().unwrap_or_default();
        let document = lines.iter().find(|call| call.word == "query").and_then(|call| call.args.first()).and_then(|arg| text(&arg.item));
        let mapped = |param: &str| {
            let renamed = lines.iter().filter(|call| call.word == "variables").flat_map(|call| &call.options).find(|pair| matches!(&pair.value.item, Value::Symbol(source) if source == param));
            renamed.map_or(param.to_string(), |pair| pair.key.clone())
        };
        for item in items {
            let Value::Hash(pairs) = &item.item else { continue };
            for pair in pairs {
                let Some(kind) = text(&pair.value.item) else { continue };
                if !matches!(self.schema.types.get(kind), Some(TypeKind::Enum(_))) {
                    self.fail(node.file, pair.value.line, pair.value.column, format!("scope `{}` param `{}` names `{kind}`, which is not an enum of the schema", name_of(remote), pair.key));
                    continue;
                }
                let variable = mapped(&pair.key);
                let Some(operation) = document.and_then(|name| self.operations.named.get(name)) else { continue };
                match operation.variables.get(&variable) {
                    None => self.fail(node.file, pair.value.line, pair.value.column, format!("scope `{}` param `{}` is `{kind}` but operation `{}` has no variable `${variable}`", name_of(remote), pair.key, document.unwrap_or_default())),
                    Some(declared) if declared.is_list() || declared.name() != kind => self.fail(node.file, pair.value.line, pair.value.column, format!("scope `{}` param `{}` is `{kind}` but the variable `${variable}` of `{}` is `{}`", name_of(remote), pair.key, document.unwrap_or_default(), declared.describe())),
                    Some(_) => {}
                }
            }
        }
    }

    fn check_mutation_args(&mut self, node: &'a ModelNode) {
        for call in &node.body {
            match call.word.as_str() {
                "create" | "update" => {
                    let pairs = |key: &str| match option(call, key) {
                        Some(Pair { value: Node { item: Value::Hash(pairs), .. }, .. }) => pairs.as_slice(),
                        _ => &[],
                    };
                    self.check_args_of(node.file, call, option_text(call, "with"), option_text(call, "wrap"), pairs("args"), pairs("input"));
                }
                "action" => {
                    let lines = call.block.as_deref().unwrap_or_default();
                    let line = |word: &str| lines.iter().find(|candidate| candidate.word == word);
                    let name = line("mutation").map(name_of).or_else(|| option_text(call, "mutation"));
                    let wrap = line("wrap").map(name_of);
                    let pairs = |word: &str| line(word).map_or(&[][..], |found| found.options.as_slice());
                    let same_names: Vec<Pair> = pairs("args").iter().map(|arg| Pair { value: Node { item: Value::Symbol(arg.key.clone()), ..arg.value.clone() }, ..arg.clone() }).collect();
                    let input = if name_of(call) == "upload" && pairs("input").is_empty() { same_names.as_slice() } else { pairs("input") };
                    self.check_args_of(node.file, call, name, wrap, pairs("args"), input);
                }
                _ => {}
            }
        }
    }

    fn check_args_of(&mut self, file: usize, call: &Call, mutation: Option<&str>, wrap: Option<&str>, args: &[Pair], input: &[Pair]) {
        let Some(mutation) = mutation.filter(|_| !args.is_empty()) else { return };
        if self.operations.named.is_empty() {
            return;
        }
        let Some(operation) = self.operations.named.get(mutation).filter(|operation| operation.kind == "mutation") else {
            self.fail(file, call.line, call.column, format!("operation `{mutation}` is not in the operation documents"));
            return;
        };
        let variables = &operation.variables;
        let (owner, accepted) = match wrap {
            None => (format!("operation `{mutation}`"), variables.clone()),
            Some(key) => {
                let Some(wrapper) = variables.get(key) else {
                    self.fail(file, call.line, call.column, format!("operation `{mutation}` has no variable `${key}`"));
                    return;
                };
                let Some(fields) = self.schema.input_fields(wrapper.name()) else {
                    self.fail(file, call.line, call.column, format!("variable `${key}` of `{mutation}` is `{}`, not an input type", wrapper.describe()));
                    return;
                };
                (format!("`{}`", wrapper.name().to_string()), fields)
            }
        };
        for arg in args {
            let Some(kind) = &crate::operation::arg_kind_text(&arg.value.item) else { continue };
            let Some(variable) = input.iter().find(|pair| matches!(&pair.value.item, Value::Symbol(source) if *source == arg.key)).map(|pair| pair.key.as_str()) else { continue };
            let Some(declared) = accepted.get(variable) else {
                self.fail(file, call.line, call.column, format!("`{variable}` is not an input of {owner}"));
                continue;
            };
            let base = kind.trim_end_matches('?');
            if !self.arg_fits(base, declared) {
                self.fail(file, call.line, call.column, format!("arg `{}` of kind `{}` does not fit `{variable}: {}`{}", arg.key, crate::operation::display_kind(kind), declared.describe(), self.unmapped_hint(declared.name())));
            } else if kind.ends_with('?') && !declared.optional() {
                self.fail(file, call.line, call.column, format!("arg `{}` may be nil but `{variable}: {}` is required", arg.key, declared.describe()));
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

    fn resolve(&mut self, node: &ModelNode, remote: &Call) -> Option<String> {
        let lines = remote.block.as_deref().unwrap_or_default();
        let query = lines.iter().find(|call| call.word == "query")?;
        let name = query.args.first().and_then(|arg| text(&arg.item))?.to_string();
        let root = if self.operations.named.is_empty() {
            graphql::Root { key: name.clone(), field: name.clone() }
        } else if let Some(root) = self.operations.named.get(&name).filter(|operation| operation.kind == "query").and_then(|operation| operation.root.as_ref()) {
            root.clone()
        } else {
            self.fail(node.file, query.line, query.column, format!("operation `{name}` is not in the operation documents"));
            return None;
        };
        let path = option_text(query, "root").unwrap_or(&root.key).to_string();
        let mut segments = path.split('.');
        if segments.next() != Some(root.key.as_str()) {
            self.fail(node.file, query.line, query.column, format!("root `{path}` must start with the first field `{}` of operation `{name}`", root.key));
            return None;
        }
        let mut reached = match self.schema.field(&self.schema.query, &root.field) {
            Ok(reached) => reached,
            Err(message) => {
                self.fail(node.file, query.line, query.column, message);
                return None;
            }
        };
        if self.schema.is_interface(reached.name()) {
            reached = self.enter_implementation(node.file, query, &name, &root.key, reached)?;
        }
        let keys: Vec<&str> = path.split('.').collect();
        let followed = paths::segments_of(&path).and_then(|_| self.follow_quiet(&name, None, &keys));
        let names: Vec<&str> = match &followed {
            Some(Ok(found)) => found.names.clone(),
            _ => keys.clone(),
        };
        for segment in names.into_iter().skip(1) {
            reached = match self.schema.field(reached.name(), segment) {
                Ok(next) if !reached.is_list() => next,
                Ok(_) => {
                    self.fail(node.file, query.line, query.column, format!("root `{path}` crosses the list `{}`", reached.describe()));
                    return None;
                }
                Err(message) => {
                    self.fail(node.file, query.line, query.column, message);
                    return None;
                }
            };
        }
        if let Some(Err(miss)) = followed {
            let at = option(query, "root").map_or((query.line, query.column), |pair| (pair.value.line, pair.value.column));
            self.report_miss(node.file, at, "root", &name, &path, &[], &keys, miss);
            return None;
        }
        let paginates = lines.iter().any(|call| call.word == "paginate");
        let lists = lines.iter().any(|call| call.word == "list");
        let line = lines.iter().find(|call| matches!(call.word.as_str(), "paginate" | "list")).unwrap_or(query);
        if paginates {
            let element = self.connection_node(node, line, &reached)?;
            self.check_total(node, lines, &name, &root)?;
            return Some(element);
        }
        if lists != reached.is_list() {
            let expected = if lists { "a list" } else { "one object" };
            self.fail(node.file, line.line, line.column, format!("scope `{}` expects {expected} but `{path}` returns `{}`", name_of(remote), reached.describe()));
            return None;
        }
        Some(reached.name().to_string())
    }

    fn check_total(&mut self, node: &ModelNode, lines: &[Call], operation: &str, root: &graphql::Root) -> Option<()> {
        let Some(total) = lines.iter().find(|call| call.word == "total") else { return Some(()) };
        let path = option_text(total, "from")?.to_string();
        let mut segments = path.split('.');
        if segments.next() != Some(root.key.as_str()) {
            self.fail(node.file, total.line, total.column, format!("total `{path}` must start with the first field `{}` of operation `{operation}`", root.key));
            return None;
        }
        let segments: Vec<&str> = segments.collect();
        let mut reached = match self.schema.field(&self.schema.query, &root.field) {
            Ok(reached) => reached,
            Err(message) => {
                self.fail(node.file, total.line, total.column, message);
                return None;
            }
        };
        if self.schema.is_interface(reached.name()) {
            reached = self.enter_implementation(node.file, total, operation, &root.key, reached)?;
        }
        for segment in &segments {
            reached = match self.schema.field(reached.name(), segment) {
                Ok(next) if !reached.is_list() => next,
                Ok(_) => {
                    self.fail(node.file, total.line, total.column, format!("total `{path}` crosses the list `{}`", reached.describe()));
                    return None;
                }
                Err(message) => {
                    self.fail(node.file, total.line, total.column, message);
                    return None;
                }
            };
        }
        if reached.name() != "Int" || reached.is_list() || segments.is_empty() {
            self.fail(node.file, total.line, total.column, format!("total `{path}` must be an `Int` but is `{}`", reached.describe()));
            return None;
        }
        self.follow_operation(node.file, (total.line, total.column), "total", operation, &path)?;
        Some(())
    }

    fn enter_implementation(&mut self, file: usize, query: &Call, operation: &str, key: &str, reached: Ty) -> Option<Ty> {
        let interface = reached.name().to_string();
        let Some(select) = self.operations.named.get(operation).and_then(|found| found.select.fields.iter().find(|field| field.key == key)).map(|field| &field.select) else {
            self.fail(file, query.line, query.column, format!("`{interface}` root needs the operation documents of `{operation}`"));
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
                self.fail(file, query.line, query.column, format!("`... on {one}` is not an implementation of `{interface}`"));
                None
            }
            [] => {
                self.fail(file, query.line, query.column, format!("`{key}` of `{operation}` is `{interface}`: select its fields under `... on <Type>`"));
                None
            }
            _ => {
                self.fail(file, query.line, query.column, format!("`{key}` of `{operation}` selects more than one `... on` type: {}", types.join(", ")));
                None
            }
        };
        for problem in problems {
            self.fail(file, query.line, query.column, problem);
        }
        let named = |name: String| Ty { kind: TyKind::Named(name), nullable: true, has_default: false };
        implementation.map(|name| Ty { nullable: reached.nullable, ..if reached.is_list() { named(name).into_list() } else { named(name) } })
    }

    fn connection_node(&mut self, node: &ModelNode, line: &Call, connection: &Ty) -> Option<String> {
        let found = match self.schema.field(connection.name(), "nodes") {
            Ok(nodes) if nodes.is_list() => Some(nodes.name().to_string()),
            _ => self.schema.field(connection.name(), "edges").and_then(|edges| {
                self.edge = Some(edges.name().to_string());
                self.schema.field(edges.name(), "node")
            }).ok().map(|edge| edge.name().to_string()),
        };
        if found.is_none() {
            self.fail(node.file, line.line, line.column, format!("`paginate` needs `nodes` or `edges {{ node }}` on `{}`", connection.name().to_string()));
        }
        found
    }

    fn check_fields(&mut self, node: &'a ModelNode, calls: &[&'a Call], owner: &str, scope: &Scope<'a>, covered: &mut BTreeSet<String>, stack: &mut Vec<(String, String)>) {
        for call in calls {
            let name = name_of(call);
            if matches!(call.word.as_str(), "index" | "ephemeral" | "file") || flag(call, "local") || name.is_empty() {
                continue;
            }
            let Some(path) = self.path_of(node, call, name, scope) else { continue };
            let segments: Vec<&str> = path.iter().map(String::as_str).collect();
            let Some(reached) = self.walk(scope.file, call, owner, &segments, scope, name) else { continue };
            covered.insert(reached.first.clone());
            self.check_kind(node, call, name, &reached.ty, reached.nullable_hop, scope.file);
            let block: Option<(usize, &'a [Call])> = match (&call.block, option_text(call, "as")) {
                (Some(block), _) => Some((scope.file, block.as_slice())),
                (None, Some(declared)) => self.declared.get(declared).copied(),
                _ => None,
            };
            let Some((file, children)) = block else { continue };
            if children.iter().any(|child| child.word == "variant") {
                for select in &reached.selects {
                    self.check_union_selection(scope.file, call, reached.ty.name(), children, select);
                }
                self.check_union(node, call, reached.ty.name(), children, file, &reached.selects, stack);
                continue;
            }
            let marker = (option_text(call, "as").unwrap_or(name).to_string(), reached.ty.name().to_string());
            if !matches!(self.schema.types.get(reached.ty.name()), Some(TypeKind::Fields(_))) || stack.contains(&marker) {
                continue;
            }
            stack.push(marker);
            let inner: Vec<&Call> = children.iter().collect();
            self.check_fields(node, &inner, reached.ty.name(), &Scope::inside(file, reached.selects), &mut BTreeSet::new(), stack);
            stack.pop();
        }
    }

    fn element_select(&self, remote: &Call) -> Option<&'a Select> {
        let lines = remote.block.as_deref().unwrap_or_default();
        let mut select = self.root_select(remote)?;
        if lines.iter().any(|call| call.word == "paginate") {
            select = match select.fields.iter().find(|field| field.name == "nodes") {
                Some(nodes) => &nodes.select,
                None => &select.fields.iter().find(|field| field.name == "edges")?.select.fields.iter().find(|field| field.name == "node")?.select,
            };
        }
        Some(select)
    }

    fn root_select(&self, remote: &Call) -> Option<&'a Select> {
        let operations: &'a Operations = self.operations;
        let lines = remote.block.as_deref().unwrap_or_default();
        let query = lines.iter().find(|call| call.word == "query")?;
        let operation = operations.named.get(name_of(query))?;
        let root = operation.root.as_ref()?;
        let path = option_text(query, "root").unwrap_or(&root.key);
        let keys: Vec<&str> = path.split('.').collect();
        operations.follow(&[&operation.select], &keys).ok()?.selects.first().copied()
    }

    fn check_union_selection(&mut self, file: usize, call: &Call, union: &str, children: &[Call], select: &Select) {
        let noun = if self.schema.is_interface(union) { "interface" } else { "union" };
        let mut found: (bool, Vec<&str>) = (false, Vec::new());
        self.operations.each_select(select, None, &mut |inner, on| {
            found.0 |= inner.typename;
            found.1.extend(on.filter(|on| *on != union));
        }, &mut Vec::new());
        if !found.0 {
            self.fail(file, call.line, call.column, format!("field `{}`: the selection of `{}` must select `__typename`", name_of(call), option_text(call, "from").unwrap_or(name_of(call))));
        }
        for on in found.1.iter().filter(|on| !children.iter().any(|variant| variant.word == "variant" && name_of(variant) == **on)) {
            self.fail(file, call.line, call.column, format!("field `{}`: `... on {on}` is not a declared variant of {noun} `{union}`", name_of(call)));
        }
    }

    fn check_union(&mut self, node: &'a ModelNode, call: &Call, union: &str, children: &'a [Call], file: usize, selects: &[&'a Select], stack: &mut Vec<(String, String)>) {
        let interface = self.schema.is_interface(union);
        let members: Vec<String> = match self.schema.types.get(union) {
            Some(TypeKind::Union(members)) => members.clone(),
            Some(TypeKind::Fields(_)) if interface => self.schema.implementers.get(union).cloned().unwrap_or_default(),
            _ => return,
        };
        let variants: Vec<&Call> = children.iter().filter(|child| child.word == "variant").collect();
        let declared: Vec<&str> = variants.iter().map(|variant| name_of(variant)).collect();
        for variant in &variants {
            if !members.iter().any(|member| member == name_of(variant)) {
                let message = if interface { format!("variant `{}` is not an implementation of interface `{union}`", name_of(variant)) } else { format!("variant `{}` is not a member of union `{union}`", name_of(variant)) };
                self.fail(file, variant.line, variant.column, message);
            }
        }
        for member in members.iter().filter(|member| !declared.contains(&member.as_str())) {
            let at = variants.first().copied().unwrap_or(call);
            let message = if interface { format!("implementation `{member}` of interface `{union}` is not declared") } else { format!("variant `{member}` of union `{union}` is not declared") };
            self.fail(file, at.line, at.column, message);
        }
        if let Some(shared) = children.iter().find(|child| child.word == "shared") {
            self.check_shared(node, shared, union, &members, interface, file, selects, stack);
        }
        for variant in variants.iter().filter(|variant| members.iter().any(|member| member == name_of(variant))) {
            let Some(block) = &variant.block else { continue };
            let marker = (name_of(variant).to_string(), union.to_string());
            if stack.contains(&marker) {
                continue;
            }
            stack.push(marker);
            let inner: Vec<&Call> = block.iter().collect();
            let apart = declared.iter().filter(|other| **other != name_of(variant)).map(|other| other.to_string()).collect();
            self.check_fields(node, &inner, name_of(variant), &Scope { apart, ..Scope::inside(file, selects.to_vec()) }, &mut BTreeSet::new(), stack);
            stack.pop();
        }
    }

    fn check_shared(&mut self, node: &'a ModelNode, shared: &'a Call, union: &str, members: &[String], interface: bool, file: usize, selects: &[&'a Select], stack: &mut Vec<(String, String)>) {
        if !interface {
            self.fail(file, shared.line, shared.column, format!("`shared` needs an interface: `{union}` is a union"));
            return;
        }
        let fields: Vec<&Call> = shared.block.iter().flatten().collect();
        self.check_fields(node, &fields, union, &Scope::inside(file, selects.to_vec()), &mut BTreeSet::new(), stack);
        for field in &fields {
            let key = option_text(field, "from").unwrap_or(name_of(field)).split('.').next().unwrap_or_default();
            for member in members {
                let apart: Vec<String> = members.iter().filter(|other| *other != member).cloned().collect();
                let name = selects.iter().find_map(|select| self.operations.fields_in(select, key, &apart).first().map(|found| found.name.as_str())).unwrap_or(key);
                if self.schema.field(member, name).is_err() {
                    self.fail(file, field.line, field.column, format!("implementation `{member}` of interface `{union}` has no field `{name}`"));
                }
            }
        }
    }

    fn path_of(&mut self, node: &ModelNode, call: &Call, name: &str, scope: &Scope) -> Option<Vec<String>> {
        if let Some(parent) = option_text(call, "id_from") {
            let association = node.body.iter().find(|candidate| matches!(candidate.word.as_str(), "belongs_to" | "has_one") && name_of(candidate) == parent);
            let base = association.and_then(|association| option_text(association, "from")).unwrap_or(parent);
            return Some(base.split('.').map(str::to_string).chain(["id".to_string()]).collect());
        }
        let _ = scope;
        Some(option_text(call, "from").unwrap_or(name).split('.').map(str::to_string).collect())
    }

    fn walk(&mut self, file: usize, call: &Call, owner: &str, segments: &[&str], scope: &Scope<'a>, name: &str) -> Option<Walked<'a>> {
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
                    self.fail(file, call.line, call.column, format!("field `{name}`: {message}"));
                    return None;
                }
            };
            if index + 1 == segments.len() {
                return Some(Walked { ty: if crossed && !reached.is_list() { reached.into_list() } else { reached }, nullable_hop, first, selects });
            }
            if reached.is_list() && !scope.lists {
                self.fail(file, call.line, call.column, format!("field `{name}`: path `{}` crosses the list `{segment}`", segments.join(".")));
                return None;
            }
            crossed |= reached.is_list();
            nullable_hop |= reached.nullable;
            current = reached.name().to_string();
        }
        None
    }

    fn check_kind(&mut self, _node: &ModelNode, call: &Call, name: &str, reached: &Ty, nullable_hop: bool, file: usize) {
        let word = call.word.as_str();
        if word == "raw" {
            return;
        }
        let mut problems = Vec::new();
        if name != "id" && (reached.nullable || nullable_hop) && !(flag(call, "null") || flag(call, "optional") || option(call, "default").is_some()) {
            problems.push(format!("is nullable in the schema (`{}`), declare `null: true`", reached.describe()));
        }
        let wants_list = matches!(word, "list" | "array");
        if wants_list != reached.is_list() {
            let schema_shape = if reached.is_list() { "a list" } else { "not a list" };
            problems.push(format!("`{word}` does not fit `{}`, which is {schema_shape}", reached.describe()));
        } else if !self.named_fits(call, reached.name()) {
            problems.push(format!("`{word}` does not fit the schema type `{}`{}", reached.describe(), self.unmapped_hint(reached.name())));
        }
        if word == "enum" || (word == "list" && option_text(call, "of") == Some("enum")) {
            problems.extend(self.enum_problems(call, reached.name()));
        }
        if crate::unions::is_union_call(call) && !matches!(self.schema.types.get(reached.name()), Some(TypeKind::Union(_))) && !self.schema.is_interface(reached.name()) {
            problems.push(format!("is not a union: schema type `{}` is not one", reached.name()));
        }
        for problem in problems {
            self.fail(file, call.line, call.column, format!("field `{name}` {problem}"));
        }
    }

    fn named_fits(&self, call: &Call, named: &str) -> bool {
        let kind = self.schema.types.get(named);
        let builtin = Schema::is_builtin_scalar(named);
        let structured = matches!(kind, Some(TypeKind::Fields(_) | TypeKind::Union(_))) || self.custom_fits(named, "object");
        let word = match call.word.as_str() {
            "list" => option_text(call, "of").unwrap_or("string"),
            other => other,
        };
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

    fn enum_problems(&self, call: &Call, named: &str) -> Vec<String> {
        let Some(TypeKind::Enum(known)) = self.schema.types.get(named) else { return Vec::new() };
        let Some(Pair { value, .. }) = option(call, "values") else { return Vec::new() };
        let Value::Array(items) = &value.item else { return Vec::new() };
        let missing: Vec<&str> = items.iter().filter_map(|item| text(&item.item)).filter(|value| !known.iter().any(|candidate| candidate == value)).collect();
        if missing.is_empty() { Vec::new() } else { vec![format!("lists values {} that enum `{named}` does not have", missing.join(", "))] }
    }

    fn check_associations(&mut self, node: &ModelNode, element: &str, selects: Vec<&'a Select>) {
        for call in node.body.iter().filter(|call| matches!(call.word.as_str(), "belongs_to" | "has_one" | "has_many" | "references")) {
            let Some(path) = option_text(call, "from") else { continue };
            let segments: Vec<&str> = path.split('.').collect();
            let Some(Walked { ty: reached, .. }) = self.walk(node.file, call, element, &segments, &Scope { lists: true, ..Scope::inside(node.file, selects.clone()) }, name_of(call)) else { continue };
            let one = matches!(call.word.as_str(), "belongs_to" | "has_one");
            if one == reached.is_list() {
                let expected = if one { "one object" } else { "a list" };
                self.fail(node.file, call.line, call.column, format!("`{}` `{}` expects {expected} but `{path}` is `{}`", call.word, name_of(call), reached.describe()));
            }
        }
    }

    fn list_unmodeled(&mut self, node: &ModelNode, remote: &Call, element: &str, covered: &BTreeSet<String>) {
        let Some(fields) = self.schema.fields(element) else { return };
        let missing: BTreeSet<&str> = fields.keys().map(String::as_str).filter(|name| !covered.contains(*name) && !name.starts_with("__")).collect();
        if !missing.is_empty() {
            self.notes.push(format!("{}: model `{}` scope `{}` does not use {} fields of `{element}`: {}", self.files[node.file], node.name, name_of(remote), missing.len(), missing.into_iter().collect::<Vec<_>>().join(", ")));
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
