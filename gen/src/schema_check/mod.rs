use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::parser::{Call, ModelNode, Node, OperationNode, Pair, Value};
use crate::{FileError, ParseError};

mod graphql;
mod inputs;
mod standalone;

pub use inputs::{InputField, InputObject, Ty, TyKind};
pub(crate) use inputs::load_input_objects;

use graphql::Operations;
pub(crate) use graphql::{Schema, TypeKind};

type Declared<'a> = HashMap<&'a str, (usize, &'a [Call])>;

pub struct Outcome {
    pub notes: Vec<String>,
}

pub fn check_schema(schema: &Schema, operations: &[PathBuf], files: &[String], nodes: &[ModelNode], standalone: &[OperationNode]) -> Result<Outcome, Vec<FileError>> {
    let failed = |file: &Path, message: String| vec![FileError { file: file.display().to_string(), error: ParseError::new(0, 0, message) }];
    let mut documents = Operations::default();
    for path in operation_files(operations) {
        documents.load(&path).map_err(|message| failed(&path, message))?;
    }
    let mut declared = Declared::new();
    for node in nodes {
        for schema_block in node.body.iter().filter(|call| call.word == "schema") {
            collect_declared(schema_block.block.as_deref().unwrap_or_default(), node.file, &mut declared);
        }
    }
    let mut run = Run { schema, operations: &documents, files, declared, found: Vec::new(), notes: Vec::new() };
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
        if let (Some(block), Some(name)) = (&call.block, option_text(call, "as")) {
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

fn name_of(call: &Call) -> &str {
    call.args.first().and_then(|arg| text(&arg.item)).unwrap_or_default()
}

struct Run<'a> {
    schema: &'a Schema,
    operations: &'a Operations,
    files: &'a [String],
    declared: Declared<'a>,
    found: Vec<FileError>,
    notes: Vec<String>,
}

struct Scope {
    file: usize,
    keys: HashSet<String>,
    top: bool,
    lists: bool,
}

impl<'a> Run<'a> {
    fn fail(&mut self, file: usize, line: usize, column: usize, message: String) {
        self.found.push(FileError { file: self.files[file].clone(), error: ParseError::new(line, column, message) });
    }

    fn check_model(&mut self, node: &'a ModelNode) {
        let mut seen = HashSet::new();
        for remote in node.body.iter().filter(|call| call.word == "remote") {
            let Some(element) = self.resolve(node, remote) else { continue };
            if !seen.insert(element.clone()) {
                continue;
            }
            let keys = node.body.iter().filter(|call| matches!(call.word.as_str(), "belongs_to" | "has_one" | "has_many" | "references")).map(|call| crate::build::association_key(call, &node.name)).collect();
            let fields: Vec<&Call> = node.body.iter().filter(|call| call.word == "schema").flat_map(|call| call.block.iter().flatten()).collect();
            let mut covered = BTreeSet::new();
            self.check_fields(node, &fields, &element, &Scope { file: node.file, keys, top: true, lists: false }, &mut covered, &mut Vec::new());
            self.check_associations(node, &element);
            self.list_unmodeled(node, remote, &element, &covered);
        }
        self.check_mutation_args(node);
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
                    self.check_args_of(node.file, call, name, wrap, pairs("args"), pairs("input"));
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
            let Value::Symbol(kind) = &arg.value.item else { continue };
            let Some(variable) = input.iter().find(|pair| matches!(&pair.value.item, Value::Symbol(source) if *source == arg.key)).map(|pair| pair.key.as_str()) else { continue };
            let Some(declared) = accepted.get(variable) else {
                self.fail(file, call.line, call.column, format!("`{variable}` is not an input of {owner}"));
                continue;
            };
            let base = kind.trim_end_matches('?');
            if !self.arg_fits(base, declared) {
                self.fail(file, call.line, call.column, format!("arg `{}` of kind `:{base}` does not fit `{variable}: {}`", arg.key, declared.describe()));
            } else if kind.ends_with('?') && !declared.optional() {
                self.fail(file, call.line, call.column, format!("arg `{}` may be nil but `{variable}: {}` is required", arg.key, declared.describe()));
            }
        }
    }

    fn arg_fits(&self, kind: &str, declared: &Ty) -> bool {
        let named = declared.name();
        let custom = matches!(self.schema.types.get(named), Some(TypeKind::Scalar));
        let enumerated = matches!(self.schema.types.get(named), Some(TypeKind::Enum(_)));
        match kind {
            "ids" => declared.is_list() && (matches!(named, "ID" | "String" | "Int") || custom),
            _ if declared.is_list() => false,
            "string" => matches!(named, "String" | "ID") || custom || enumerated,
            "id" => matches!(named, "ID" | "String" | "Int") || custom,
            "int" => named == "Int" || custom,
            "num" => matches!(named, "Float" | "Int") || custom,
            "bool" => named == "Boolean",
            _ => named == kind && matches!(self.schema.types.get(named), Some(TypeKind::Input(_))),
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
        for segment in segments {
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
        let paginates = lines.iter().any(|call| call.word == "paginate");
        let lists = lines.iter().any(|call| call.word == "list");
        let line = lines.iter().find(|call| matches!(call.word.as_str(), "paginate" | "list")).unwrap_or(query);
        if paginates {
            return self.connection_node(node, line, &reached);
        }
        if lists != reached.is_list() {
            let expected = if lists { "a list" } else { "one object" };
            self.fail(node.file, line.line, line.column, format!("scope `{}` expects {expected} but `{path}` returns `{}`", name_of(remote), reached.describe()));
            return None;
        }
        Some(reached.name().to_string())
    }

    fn connection_node(&mut self, node: &ModelNode, line: &Call, connection: &Ty) -> Option<String> {
        let found = match self.schema.field(connection.name(), "nodes") {
            Ok(nodes) if nodes.is_list() => Some(nodes.name().to_string()),
            _ => self.schema.field(connection.name(), "edges").and_then(|edges| self.schema.field(edges.name(), "node")).ok().map(|edge| edge.name().to_string()),
        };
        if found.is_none() {
            self.fail(node.file, line.line, line.column, format!("`paginate` needs `nodes` or `edges {{ node }}` on `{}`", connection.name().to_string()));
        }
        found
    }

    fn check_fields(&mut self, node: &'a ModelNode, calls: &[&'a Call], owner: &str, scope: &Scope, covered: &mut BTreeSet<String>, stack: &mut Vec<(String, String)>) {
        for call in calls {
            let name = name_of(call);
            if matches!(call.word.as_str(), "index" | "ephemeral" | "file") || flag(call, "local") || name.is_empty() {
                continue;
            }
            let Some(path) = self.path_of(node, call, name, scope) else { continue };
            let segments: Vec<&str> = path.iter().map(String::as_str).collect();
            let Some(reached) = self.walk(scope.file, call, owner, &segments, scope, name) else { continue };
            covered.insert(segments[0].to_string());
            self.check_kind(node, call, name, &reached.0, reached.1, scope.file);
            let block: Option<(usize, &'a [Call])> = match (&call.block, option_text(call, "as")) {
                (Some(block), _) => Some((scope.file, block.as_slice())),
                (None, Some(declared)) => self.declared.get(declared).copied(),
                _ => None,
            };
            let Some((file, children)) = block else { continue };
            let marker = (option_text(call, "as").unwrap_or(name).to_string(), reached.0.name().to_string());
            if !matches!(self.schema.types.get(reached.0.name()), Some(TypeKind::Fields(_))) || stack.contains(&marker) {
                continue;
            }
            stack.push(marker);
            let inner: Vec<&Call> = children.iter().collect();
            self.check_fields(node, &inner, reached.0.name(), &Scope { file, keys: HashSet::new(), top: false, lists: false }, &mut BTreeSet::new(), stack);
            stack.pop();
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

    fn walk(&mut self, file: usize, call: &Call, owner: &str, segments: &[&str], scope: &Scope, name: &str) -> Option<(Ty, bool)> {
        let mut current = owner.to_string();
        let mut nullable_hop = false;
        let mut crossed = false;
        for (index, segment) in segments.iter().enumerate() {
            let reached = match self.schema.field(&current, segment) {
                Ok(reached) => reached,
                Err(_) if scope.top && scope.keys.contains(name) => return None,
                Err(message) => {
                    self.fail(file, call.line, call.column, format!("field `{name}`: {message}"));
                    return None;
                }
            };
            if index + 1 == segments.len() {
                return Some((if crossed && !reached.is_list() { reached.into_list() } else { reached }, nullable_hop));
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
            problems.push(format!("`{word}` does not fit the schema type `{}`", reached.describe()));
        }
        if word == "enum" {
            problems.extend(self.enum_problems(call, reached.name()));
        }
        for problem in problems {
            self.fail(file, call.line, call.column, format!("field `{name}` {problem}"));
        }
    }

    fn named_fits(&self, call: &Call, named: &str) -> bool {
        let kind = self.schema.types.get(named);
        let custom = matches!(kind, Some(TypeKind::Scalar));
        let builtin = Schema::is_builtin_scalar(named);
        let structured = matches!(kind, Some(TypeKind::Fields(_) | TypeKind::Union)) || custom;
        let word = match call.word.as_str() {
            "list" => option_text(call, "of").unwrap_or("string"),
            other => other,
        };
        match word {
            "string" => matches!(named, "String" | "ID") || custom || matches!(kind, Some(TypeKind::Enum(_))),
            "id" => matches!(named, "ID" | "String" | "Int") || custom,
            "int" => named == "Int" || custom,
            "num" => matches!(named, "Float" | "Int") || custom,
            "bool" => named == "Boolean",
            "date" => named == "String" || custom,
            "enum" => named == "String" || matches!(kind, Some(TypeKind::Enum(_))),
            "object" => structured,
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

    fn check_associations(&mut self, node: &ModelNode, element: &str) {
        for call in node.body.iter().filter(|call| matches!(call.word.as_str(), "belongs_to" | "has_one" | "has_many" | "references")) {
            let Some(path) = option_text(call, "from") else { continue };
            let segments: Vec<&str> = path.split('.').collect();
            let keys = HashSet::new();
            let Some((reached, _)) = self.walk(node.file, call, element, &segments, &Scope { file: node.file, keys, top: false, lists: true }, name_of(call)) else { continue };
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
