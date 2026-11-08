
use dblayer::schema::{MergeRule, NodeRead, OnFailure, Storage};
use dblayer::value::{ScalarKind, SortDirection};
use serde_json::{Map, Number, Value as Json};

use super::*;
use crate::parser::{Call, ModelNode, Node, OperationNode, Pair, UploadsNode, Value};
use crate::schema_check::{Schema, Ty, TyKind, TypeKind};
use crate::ParseError;

mod fields;
mod operations;
mod scopes;

use fields::*;
use operations::*;
use scopes::*;

type Found = (usize, ParseError);

struct Cx<'a> {
    file: usize,
    errors: &'a mut Vec<Found>,
    schema: Option<&'a Schema>,
    memory: bool,
}

impl Cx<'_> {
    fn fail(&mut self, pos: Pos, message: impl Into<String>) {
        self.errors.push((self.file, ParseError::new(pos.line, pos.column, message)));
    }
}

fn at(node: &Node<Value>) -> Pos {
    Pos { line: node.line, column: node.column }
}

struct Line<'a> {
    word: &'a str,
    pos: Pos,
    args: &'a [Node<Value>],
    options: &'a [Pair],
    used: Vec<bool>,
}

impl<'a> Line<'a> {
    fn of(call: &'a Call) -> Self {
        Self { word: &call.word, pos: Pos::of(call), args: &call.args, options: &call.options, used: vec![false; call.options.len()] }
    }

    fn opt(&mut self, cx: &mut Cx, key: &str) -> Option<&'a Pair> {
        let mut found: Option<&'a Pair> = None;
        for (index, pair) in self.options.iter().enumerate().filter(|(_, pair)| pair.key == key) {
            self.used[index] = true;
            if found.is_some() {
                cx.fail(Pos { line: pair.line, column: pair.column }, format!("`{key}` is given twice for `{}`", self.word));
            } else {
                found = Some(pair);
            }
        }
        found
    }

    fn finish(&mut self, cx: &mut Cx) {
        for (pair, used) in self.options.iter().zip(&self.used) {
            if !used {
                cx.fail(Pos { line: pair.line, column: pair.column }, format!("unknown key `{}` for `{}`", pair.key, self.word));
            }
        }
    }

    fn no_args(&self, cx: &mut Cx) {
        if !self.args.is_empty() {
            cx.fail(self.pos, format!("`{}` takes no arguments", self.word));
        }
    }

    fn no_options(&mut self, cx: &mut Cx) {
        self.used.iter_mut().for_each(|used| *used = true);
        if let Some(pair) = self.options.first() {
            cx.fail(Pos { line: pair.line, column: pair.column }, format!("unknown key `{}` for `{}`", pair.key, self.word));
        }
    }
}

fn pair_pos(pair: &Pair) -> Pos {
    Pos { line: pair.line, column: pair.column }
}

fn symbol(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<String> {
    match &node.item {
        Value::Symbol(text) => Some(text.clone()),
        _ => {
            cx.fail(at(node), format!("{what} must be a symbol like `:name`"));
            None
        }
    }
}

fn lowercase_symbol(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<String> {
    let name = symbol(cx, node, what)?;
    if name.chars().next().is_some_and(char::is_lowercase) {
        return Some(name);
    }
    cx.fail(at(node), format!("{what} `{name}` must start with a lowercase letter: generated code uses capitalised names for types"));
    None
}

fn string(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<String> {
    match &node.item {
        Value::Str(text) => Some(text.clone()),
        _ => {
            cx.fail(at(node), format!("{what} must be a string like \"name\""));
            None
        }
    }
}

fn konst(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<String> {
    match &node.item {
        Value::Const(text) => Some(text.clone()),
        _ => {
            cx.fail(at(node), format!("{what} must be a type name like `Record`"));
            None
        }
    }
}

fn boolean(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<bool> {
    match &node.item {
        Value::Bool(flag) => Some(*flag),
        _ => {
            cx.fail(at(node), format!("{what} must be true or false"));
            None
        }
    }
}

fn duration(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<String> {
    match &node.item {
        Value::Duration { amount, unit } => {
            let text = format!("{amount}.{unit}");
            if dblayer::runtime::parse_duration(&text).is_none() {
                cx.fail(at(node), format!("{what} duration exceeds supported milliseconds"));
                return None;
            }
            Some(text)
        }
        _ => {
            cx.fail(at(node), format!("{what} must be a duration like `5.minutes`"));
            None
        }
    }
}

fn path(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<Spanned<String>> {
    let text = string(cx, node, what)?;
    if text.split('.').any(str::is_empty) {
        cx.fail(at(node), format!("{what} `{text}` has an empty segment"));
        return None;
    }
    Some(Spanned::new(text, at(node)))
}

fn json_of(value: &Value) -> Json {
    match value {
        Value::Int(number) => Json::from(*number),
        Value::Float(number) => Json::Number(Number::from_f64(*number).expect("the lexer accepts finite numbers only")),
        Value::Bool(flag) => Json::Bool(*flag),
        Value::Nil => Json::Null,
        Value::Array(items) => Json::Array(items.iter().map(|item| json_of(&item.item)).collect()),
        Value::Hash(pairs) => Json::Object(pairs.iter().map(|pair| (pair.key.clone(), json_of(&pair.value.item))).collect::<Map<_, _>>()),
        Value::Symbol(text) | Value::Str(text) | Value::Const(text) => Json::String(text.clone()),
        Value::Duration { amount, unit } => Json::String(format!("{amount}.{unit}")),
        Value::OptionalArray(_) => Json::Null,
    }
}

fn require<T>(cx: &mut Cx, line: &Line, key: &str, found: Option<T>) -> Option<T> {
    if found.is_none() {
        cx.fail(line.pos, format!("`{}` needs `{key}:`", line.word));
    }
    found
}

fn exactly_one_arg<'a>(cx: &mut Cx, line: &Line<'a>, what: &str) -> Option<&'a Node<Value>> {
    match line.args {
        [one] => Some(one),
        _ => {
            cx.fail(line.pos, format!("`{}` needs {what}", line.word));
            None
        }
    }
}

fn hash_pairs<'a>(cx: &mut Cx, node: &'a Node<Value>, what: &str) -> Option<&'a [Pair]> {
    match &node.item {
        Value::Hash(pairs) => Some(pairs),
        _ => {
            cx.fail(at(node), format!("{what} must be a hash like `{{ key: value }}`"));
            None
        }
    }
}

fn block<'a>(cx: &mut Cx, call: &'a Call) -> Option<&'a [Call]> {
    if call.block.is_none() {
        cx.fail(Pos::of(call), format!("`{}` needs a `do ... end` block", call.word));
    }
    call.block.as_deref()
}

fn no_block(cx: &mut Cx, call: &Call) {
    if call.block.is_some() {
        cx.fail(Pos::of(call), format!("`{}` takes no block", call.word));
    }
}

fn sort_direction(cx: &mut Cx, node: &Node<Value>) -> Option<SortDirection> {
    let name = symbol(cx, node, "order direction")?;
    let found = SortDirection::parse(&name);
    if found.is_none() {
        cx.fail(at(node), "order direction must be `:asc`, `:desc`, `:asc_nulls_last` or `:desc_nulls_first`");
    }
    found
}


fn model(cx: &mut Cx, node: &ModelNode) -> ModelDecl {
    let mut options = Line { word: "model", pos: Pos { line: node.line, column: node.column }, args: &[], options: &node.options, used: vec![false; node.options.len()] };
    let storage = options.opt(cx, "storage").map_or(Storage::Disk, |pair| match &pair.value.item {
        Value::Symbol(name) if name == "memory" => Storage::Memory,
        Value::Symbol(name) if name == "disk" => Storage::Disk,
        Value::Symbol(name) if name == "local" => Storage::Local,
        _ => {
            cx.fail(at(&pair.value), "`storage` must be `:memory`, `:disk` or `:local`");
            Storage::Disk
        }
    });
    let expires_in = options.opt(cx, "expires_in").and_then(|pair| duration(cx, &pair.value, "`expires_in`").map(|text| Spanned::new(text, at(&pair.value))));
    options.finish(cx);
    cx.memory = storage == Storage::Memory;
    let owner = Owner(cx.schema.map(|_| node.name.clone()));
    let mut decl = ModelDecl {
        name: node.name.clone(), pos: Pos { line: node.line, column: node.column }, file: node.file, storage, expires_in, attributes: Vec::new(), ephemeral: Vec::new(),
        indexes: Vec::new(), search: Vec::new(), associations: Vec::new(), scopes: Vec::new(), subscriptions: Vec::new(), changes: Vec::new(), changed_by: Vec::new(), removed_by: Vec::new(), server_revision: None, operations: Vec::new(),
    };
    for call in &node.body {
        let mut line = Line::of(call);
        match call.word.as_str() {
            "attribute" => decl.attributes.extend(attribute(cx, call, Site::Model, &owner)),
            "ephemeral" => {
                line.no_args(cx);
                line.no_options(cx);
                if let Some(children) = block(cx, call) {
                    let attributes = children.iter().filter_map(|child| attribute(cx, child, Site::Ephemeral, &Owner(None))).collect();
                    decl.ephemeral.push(EphemeralDecl { pos: line.pos, attributes });
                }
            }
            "index" => {
                no_block(cx, call);
                line.no_options(cx);
                let columns: Vec<String> = call.args.iter().filter_map(|arg| symbol(cx, arg, "index column")).collect();
                if call.args.is_empty() {
                    cx.fail(line.pos, "`index` needs 1 or more attribute names like `index :chatId, :createdAt`");
                }
                decl.indexes.push(IndexDecl { pos: line.pos, columns });
            }
            "search" => {
                no_block(cx, call);
                line.no_options(cx);
                if call.args.is_empty() {
                    cx.fail(line.pos, "`search` needs 1 or more attribute names");
                }
                decl.search.extend(call.args.iter().filter_map(|arg| symbol(cx, arg, "search name").map(|name| Spanned::new(name, at(arg)))));
            }
            "belongs_to" => decl.associations.extend(association(cx, call, AssociationKind::BelongsTo)),
            "has_many" => decl.associations.extend(association(cx, call, AssociationKind::HasMany)),
            "has_one" => decl.associations.extend(association(cx, call, AssociationKind::HasOne)),
            "references" => decl.associations.extend(association(cx, call, AssociationKind::References)),
            "scope" => decl.scopes.extend(scope(cx, call)),
            "subscribe" => decl.subscriptions.extend(subscribe(cx, call)),
            "changes" => {
                no_block(cx, call);
                let arg = exactly_one_arg(cx, &line, "a subscription name like `changes \"RecordChanges\"`");
                let name = arg.and_then(|arg| string(cx, arg, "the subscription name"));
                let from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`"));
                let area = line.opt(cx, "area").and_then(|pair| path(cx, &pair.value, "`area`"));
                let kind = line.opt(cx, "kind").and_then(|pair| path(cx, &pair.value, "`kind`"));
                let id = line.opt(cx, "id").and_then(|pair| path(cx, &pair.value, "`id`"));
                line.finish(cx);
                let from = require(cx, &line, "from", from);
                let area = require(cx, &line, "area", area);
                let kind = require(cx, &line, "kind", kind);
                let id = require(cx, &line, "id", id);
                if let (Some(subscription), Some(from), Some(area), Some(kind), Some(id)) = (name, from, area, kind, id) {
                    decl.changes.push(ChangesDecl { pos: line.pos, subscription, from, area, kind, id });
                }
            }
            "server_revision" => {
                no_block(cx, call);
                let arg = exactly_one_arg(cx, &line, "an integer attribute");
                let field = arg.and_then(|arg| symbol(cx, arg, "server revision"));
                if decl.server_revision.is_some() { cx.fail(line.pos, "duplicate server_revision"); }
                decl.server_revision = field;
                line.finish(cx);
            }
            "changed_by" => {
                no_block(cx, call);
                let arg = exactly_one_arg(cx, &line, "a server area like `changed_by \"CHAT\"`");
                let area = arg.and_then(|arg| string(cx, arg, "the area"));
                let via = line.opt(cx, "via").and_then(|pair| symbol(cx, &pair.value, "`via`"));
                let parameter = line.opt(cx, "parameter").and_then(|pair| symbol(cx, &pair.value, "`parameter`"));
                let removes_on = line.opt(cx, "removes_on").and_then(|pair| string_list(cx, &pair.value, "`removes_on`"));
                let payload = line.opt(cx, "payload").and_then(|pair| string(cx, &pair.value, "`payload`"));
                let id_of = line.opt(cx, "id_of").and_then(|pair| match &pair.value.item {
                    Value::Hash(parts) if parts.len() == 2 && parts.iter().any(|part| part.key == "of") && parts.iter().any(|part| part.key == "field") => {
                        let model = parts.iter().find(|part| part.key == "of").and_then(|part| konst(cx, &part.value, "`of:`"));
                        let field = parts.iter().find(|part| part.key == "field").and_then(|part| symbol(cx, &part.value, "`field:`"));
                        model.zip(field)
                    }
                    _ => {
                        cx.fail(at(&pair.value), "`id_of` is `{ of: Model, field: :name }`");
                        None
                    }
                });
                line.finish(cx);
                let via = require(cx, &line, "via", via);
                if let (Some(area), Some(via)) = (area, via) {
                    decl.changed_by.push(ChangedByDecl { pos: line.pos, area, via, parameter, removes_on, id_of, payload });
                }
            }
            "removed_by" => {
                no_block(cx, call);
                let model = exactly_one_arg(cx, &line, "a model like `removed_by Message, match: { chatId: :chatId }`").and_then(|arg| konst(cx, arg, "the model"));
                let matches = line.opt(cx, "match").and_then(|pair| match &pair.value.item {
                    Value::Hash(parts) if !parts.is_empty() => parts.iter().map(|part| symbol(cx, &part.value, &format!("`{}:`", part.key)).map(|theirs| (part.key.clone(), theirs))).collect::<Option<Vec<_>>>(),
                    _ => {
                        cx.fail(at(&pair.value), "`match` is `{ field: :model_field }`");
                        None
                    }
                });
                line.finish(cx);
                let matches = require(cx, &line, "match", matches);
                if let (Some(model), Some(matches)) = (model, matches) {
                    decl.removed_by.push(RemovedByDecl { pos: line.pos, model, matches });
                }
            }
            "operation" => decl.operations.extend(operation(cx, call)),
            other => cx.fail(line.pos, format!("unknown model declaration `{other}`")),
        }
    }
    decl
}

pub(crate) fn lower_models(nodes: &[ModelNode], schema: Option<&Schema>) -> Result<Vec<ModelDecl>, Vec<Found>> {
    let mut errors = Vec::new();
    let mut models = Vec::new();
    for node in nodes {
        let mut cx = Cx { file: node.file, errors: &mut errors, schema, memory: false };
        models.push(model(&mut cx, node));
    }
    if errors.is_empty() { Ok(models) } else { Err(errors) }
}

pub(crate) fn lower_calls(nodes: &[OperationNode], errors: &mut Vec<Found>) -> Vec<CallDecl> {
    let mut found = Vec::new();
    for node in nodes {
        let mut cx = Cx { file: node.file, errors, schema: None, memory: false };
        let pos = Pos { line: node.line, column: node.column };
        let mut decl = CallDecl { name: node.name.clone(), pos, file: node.file, kind: None, wrap: None, parameters: Vec::new(), returns: None, returns_pos: pos, shape: None };
        let mut seen: Vec<&str> = Vec::new();
        let mut named = 0;
        for call in &node.body {
            let mut line = Line::of(call);
            let word = call.word.as_str();
            if !matches!(word, "parameter") && seen.contains(&word) {
                cx.fail(line.pos, format!("`{word}` is declared twice"));
                continue;
            }
            seen.push(word);
            match word {
                "query" | "mutation" => {
                    named += 1;
                    no_block(&mut cx, call);
                    line.no_options(&mut cx);
                    if let Some(arg) = exactly_one_arg(&mut cx, &line, "an operation name like `mutation \"QrCodeCreate\"`") {
                        if let Some(text) = string(&mut cx, arg, "the operation name") {
                            decl.kind = Some((if word == "query" { CallKind::Query } else { CallKind::Mutation }, text, line.pos));
                        }
                    }
                }
                "wrap" => {
                    no_block(&mut cx, call);
                    line.no_options(&mut cx);
                    if let Some(arg) = exactly_one_arg(&mut cx, &line, "a variable name like `wrap \"input\"`") {
                        decl.wrap = string(&mut cx, arg, "the variable path");
                    }
                }
                "parameter" => decl.parameters.extend(parameter(&mut cx, call)),
                "returns" => {
                    decl.returns_pos = line.pos;
                    decl.returns = returns(&mut cx, call, false);
                }
                "response" => match response(&mut cx, call) {
                    Some(ResponseDecl::Read(ResponseRead { from: None, shape: Some(shape), pos })) => decl.shape = Some((shape, pos)),
                    Some(_) => cx.fail(line.pos, "an operation without a model has `response shape: :list` only"),
                    None => {}
                },
                other => cx.fail(line.pos, format!("unknown line `{other}` in an operation, expected one of mutation, query, wrap, parameter, returns, response")),
            }
        }
        if named != 1 {
            cx.fail(pos, format!("operation `{}` needs exactly one `mutation` or `query` line", node.name));
        }
        found.push(decl);
    }
    found
}

/// The one `uploads do` block of the project: the steps of file uploads. A project with upload operations needs it, a project without them refuses it.
pub(crate) fn lower_uploads(nodes: &[UploadsNode], models: &[ModelDecl], errors: &mut Vec<Found>) -> Option<dblayer::owners::write::upload::UploadOperations> {
    use dblayer::owners::write::upload::{MultipartOperations, UploadOperation, UploadOperations};
    let uses = models.iter().flat_map(|model| model.operations.iter().flat_map(move |operation| operation.upload.map(|pos| (model.file, pos, operation.name.clone()))));
    let Some(first) = nodes.first() else {
        if let Some((file, pos, name)) = uses.into_iter().next() {
            errors.push((file, ParseError::new(pos.line, pos.column, format!("operation `{name}` has `upload: true`: declare the steps of the upload once in `uploads do ... end`"))));
        }
        return None;
    };
    if let Some(second) = nodes.get(1) {
        errors.push((second.file, ParseError::new(second.line, second.column, "`uploads do ... end` is declared once per project")));
        return None;
    }
    let mut cx = Cx { file: first.file, errors, schema: None, memory: false };
    let mut steps = UploadOperations { create_blob: None, multipart: None };
    for call in &first.body {
        let mut line = Line::of(call);
        no_block(&mut cx, call);
        match call.word.as_str() {
            "create_blob" => {
                let name = exactly_one_arg(&mut cx, &line, "an operation like `create_blob :CreateBlob, root: \"createBlob\"`").and_then(|arg| symbol(&mut cx, arg, "the operation"));
                let root = line.opt(&mut cx, "root").and_then(|pair| string(&mut cx, &pair.value, "`root`"));
                line.finish(&mut cx);
                match (name, steps.create_blob.is_some()) {
                    (Some(_), true) => cx.fail(line.pos, "`create_blob` is declared twice"),
                    (Some(name), false) => steps.create_blob = Some(UploadOperation { name, root_path: root }),
                    (None, _) => {}
                }
            }
            "multipart" => {
                line.no_args(&mut cx);
                let mut part = |key: &str, cx: &mut Cx| match line.opt(cx, key) {
                    Some(pair) => symbol(cx, &pair.value, &format!("`{key}:`")).map(|name| UploadOperation { name, root_path: None }),
                    None => {
                        cx.fail(call_pos(call), format!("`multipart` needs `{key}:`"));
                        None
                    }
                };
                let parts = (part("initiate", &mut cx), part("presign", &mut cx), part("complete", &mut cx), part("abort", &mut cx));
                line.finish(&mut cx);
                if let (Some(initiate), Some(presign), Some(complete), Some(abort)) = parts {
                    if steps.multipart.replace(MultipartOperations { initiate, presign, complete, abort }).is_some() {
                        cx.fail(line.pos, "`multipart` is declared twice");
                    }
                }
            }
            other => cx.fail(line.pos, format!("`uploads` knows `create_blob` and `multipart`, not `{other}`")),
        }
    }
    if steps.create_blob.is_none() && steps.multipart.is_none() {
        cx.fail(Pos { line: first.line, column: first.column }, "`uploads` declares neither `create_blob` nor `multipart`");
        return None;
    }
    if uses.into_iter().next().is_none() {
        cx.fail(Pos { line: first.line, column: first.column }, "`uploads` is declared but no operation has `upload: true`");
        return None;
    }
    Some(steps)
}

fn call_pos(call: &Call) -> Pos { Pos::of(call) }
