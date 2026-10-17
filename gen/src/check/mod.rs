use std::collections::HashSet;

use dblayer::schema::{validate_models, ModelSpec};
use dblayer::value::SortDirection;

use crate::build::build;
use crate::operation::{is_arg_kind, ARG_KIND_MESSAGE};
use crate::parser::{Call, ModelNode, Node, Pair, Value};
use crate::ParseError;

mod nested;
mod enums;

type Found = (usize, ParseError);

#[cfg(test)]
pub(crate) fn check_source(source: &str) -> Result<Vec<ModelSpec>, Vec<ParseError>> {
    crate::parse_tree(&[source]).and_then(|parsed| check(&parsed.models)).map_err(|errors| errors.into_iter().map(|(_, error)| error).collect())
}

const SCALAR_WORDS: [&str; 6] = ["string", "num", "int", "bool", "date", "id"];
const FIELD_WORDS: [&str; 13] =
    ["string", "num", "int", "bool", "date", "id", "enum", "object", "array", "raw", "list", "file", "union"];
const NESTED_WORDS: [&str; 12] = ["string", "num", "int", "bool", "date", "id", "enum", "object", "array", "raw", "list", "union"];
const EPHEMERAL_WORDS: [&str; 11] = ["string", "num", "int", "bool", "date", "id", "enum", "object", "array", "raw", "list"];
const ASSOCIATION_WORDS: [&str; 4] = ["belongs_to", "has_many", "has_one", "references"];

#[derive(Clone, Copy, PartialEq)]
enum Context {
    Model,
    Schema,
    Ephemeral,
    Nested,
    Union,
    Remote,
    Action,
}

#[derive(Clone, Copy)]
enum Shape {
    Name,
    Names,
    Const,
    Int,
    Duration,
    Symbols,
    Paths,
    Nothing,
}

#[derive(Clone, Copy, PartialEq)]
enum Want {
    Bool,
    Symbol,
    Const,
    Operation,
    TypeName,
    Symbols,
    TextParams,
    Hash,
    Duration,
    Policy,
    Merge,
    Any,
}

struct Key {
    name: &'static str,
    want: Want,
    required: bool,
}

struct Word {
    shape: Shape,
    keys: Vec<Key>,
    block: bool,
    free_keys: bool,
}

fn key(name: &'static str, want: Want, required: bool) -> Key {
    Key { name, want, required }
}

fn field_keys(word: &str) -> Vec<Key> {
    let mut keys = vec![
        key("null", Want::Bool, false),
        key("optional", Want::Bool, false),
        key("default", Want::Any, false),
        key("id_from", Want::Symbol, false),
        key("from", Want::Any, false),
        key("merge", Want::Merge, false),
        key("local", Want::Bool, false),
    ];
    match word {
        "file" => keys.retain(|candidate| !matches!(candidate.name, "id_from" | "from" | "merge" | "local")),
        "enum" => keys.extend([key("values", Want::Symbols, true), key("as", Want::TypeName, false)]),
        "list" => keys.extend([key("of", Want::Symbol, true), key("values", Want::Symbols, false), key("as", Want::TypeName, false)]),
        "object" | "array" => keys.push(key("as", Want::TypeName, false)),
        "union" => {
            keys.retain(|candidate| !matches!(candidate.name, "default" | "id_from"));
            keys.push(key("as", Want::TypeName, false));
        }
        _ => {}
    }
    if word != "file" && word != "union" {
        keys.push(key("index", Want::Bool, false));
    }
    if matches!(word, "string" | "list") {
        keys.push(key("searchable", Want::Bool, false));
    }
    keys
}

fn nested_keys(word: &str) -> Vec<Key> {
    let mut keys = vec![key("null", Want::Bool, false), key("optional", Want::Bool, false), key("default", Want::Any, false), key("from", Want::Any, false)];
    match word {
        "enum" => keys.extend([key("values", Want::Symbols, true), key("as", Want::TypeName, false)]),
        "list" => keys.extend([key("of", Want::Symbol, true), key("values", Want::Symbols, false), key("as", Want::TypeName, false)]),
        "object" | "array" => keys.push(key("as", Want::TypeName, false)),
        "union" => {
            keys.retain(|candidate| candidate.name != "default");
            keys.push(key("as", Want::TypeName, false));
        }
        _ => {}
    }
    keys
}

fn ephemeral_keys(word: &str) -> Vec<Key> {
    let mut keys = field_keys(word);
    keys.retain(|candidate| !matches!(candidate.name, "optional" | "id_from" | "from" | "as" | "merge" | "local" | "searchable"));
    keys.push(key("reset_after", Want::Duration, false));
    keys
}

fn word_for(context: Context, word: &str) -> Option<Word> {
    let plain = |shape, keys| Some(Word { shape, keys, block: false, free_keys: false });
    let free = || Some(Word { shape: Shape::Nothing, keys: vec![], block: false, free_keys: true });
    match (context, word) {
        (Context::Model, "schema") => Some(Word { shape: Shape::Nothing, keys: vec![], block: true, free_keys: false }),
        (Context::Model, "remote") => Some(Word { shape: Shape::Name, keys: vec![key("params", Want::Symbols, false), key("text", Want::TextParams, false), key("order", Want::Hash, false), key("joins", Want::Symbol, false), key("node", Want::Symbol, false)], block: true, free_keys: false }),
        (Context::Model, "action") => Some(Word { shape: Shape::Name, keys: vec![key("mutation", Want::Operation, false), key("optimistic", Want::Hash, false)], block: true, free_keys: false }),
        (Context::Model, "create" | "update") => {
            plain(
                Shape::Nothing,
                vec![key("with", Want::Operation, true), key("input", Want::Hash, false), key("on_failure", Want::Policy, false), key("wrap", Want::Symbol, false), key("root", Want::Any, false), key("response", Want::Symbol, false), key("refetch", Want::Symbols, false), key("local", Want::Symbols, false), key("correlate", Want::Symbols, false), key("args", Want::Hash, false)],
            )
        }
        (Context::Model, "destroy") => {
            plain(Shape::Nothing, vec![key("with", Want::Operation, true), key("input", Want::Hash, false), key("wrap", Want::Symbol, false), key("root", Want::Any, false)])
        }
        (Context::Action, "mutation") => plain(Shape::Const, vec![]),
        (Context::Action, "local_effect") => plain(Shape::Name, vec![key("reason", Want::Any, false)]),
        (Context::Action, "on_failure") => plain(Shape::Name, vec![]),
        (Context::Action, "wrap" | "response") => plain(Shape::Name, vec![]),
        (Context::Action, "root") => plain(Shape::Const, vec![]),
        (Context::Action, "lands") => plain(Shape::Const, vec![key("root", Want::Any, true)]),
        (Context::Action, "input" | "optimistic" | "args" | "result") => free(),
        (Context::Remote, "variables") => free(),
        (Context::Action, "refetch_of") => plain(Shape::Const, vec![key("relation", Want::Symbol, true)]),
        (Context::Action, "refetch") => plain(Shape::Symbols, vec![]),
        (Context::Action, "correlate") => plain(Shape::Paths, vec![]),
        (Context::Model, "listen") => {
            plain(Shape::Const, vec![key("root", Want::Any, false), key("id", Want::Any, false), key("deleted_by", Want::Symbol, false), key("set", Want::Hash, false), key("removes", Want::Bool, false), key("debounce", Want::Duration, false)])
        }
        (Context::Remote, "updates") => {
            plain(Shape::Const, vec![key("root", Want::Any, false), key("id", Want::Any, false), key("deleted_by", Want::Symbol, false), key("set", Want::Hash, false), key("removes", Want::Bool, false), key("debounce", Want::Duration, false)])
        }
        (Context::Model, "scope") => plain(
            Shape::Name,
            vec![key("params", Want::Symbols, false), key("where", Want::Hash, false), key("order", Want::Hash, false)],
        ),
        (Context::Model, "belongs_to") => plain(
            Shape::Name,
            vec![key("model", Want::Const, false), key("key", Want::Symbol, false), key("touch", Want::Bool, false), key("sets", Want::Hash, false), key("newer", Want::Hash, false), key("from", Want::Any, false)],
        ),
        (Context::Model, "has_many" | "has_one") => plain(
            Shape::Name,
            vec![key("model", Want::Const, false), key("key", Want::Symbol, false), key("dependent", Want::Symbol, false), key("from", Want::Any, false)],
        ),
        (Context::Model, "references") => {
            plain(Shape::Name, vec![key("model", Want::Const, false), key("key", Want::Symbol, false), key("from", Want::Any, false)])
        }
        (Context::Schema, "index") => plain(Shape::Names, vec![]),
        (Context::Schema, "ephemeral") => Some(Word { shape: Shape::Nothing, keys: vec![], block: true, free_keys: false }),
        (Context::Schema, field) if FIELD_WORDS.contains(&field) => plain(Shape::Name, field_keys(field)),
        (Context::Nested, field) if NESTED_WORDS.contains(&field) => plain(Shape::Name, nested_keys(field)),
        (Context::Ephemeral, field) if EPHEMERAL_WORDS.contains(&field) => plain(Shape::Name, ephemeral_keys(field)),
        (Context::Union, "variant") => plain(Shape::Const, vec![key("case", Want::Symbol, false)]),
        (Context::Union, "shared") => Some(Word { shape: Shape::Nothing, keys: vec![], block: true, free_keys: false }),
        (Context::Remote, "query") => plain(Shape::Const, vec![key("root", Want::Any, false)]),
        (Context::Remote, "paginate") => plain(Shape::Int, vec![key("backward", Want::Bool, false), key("cursor", Want::Symbol, false), key("from", Want::Any, false), key("cursor_type", Want::Symbol, false)]),
        (Context::Remote, "edge") => Some(Word { shape: Shape::Nothing, keys: vec![], block: true, free_keys: false }),
        (Context::Remote, "list") => plain(Shape::Nothing, vec![]),
        (Context::Remote, "fresh_for") => plain(Shape::Duration, vec![key("empty", Want::Duration, false)]),
        (Context::Remote, "max_pages") => plain(Shape::Int, vec![]),
        (Context::Remote, "total") => plain(Shape::Nothing, vec![key("from", Want::Any, true)]),
        _ => None,
    }
}

pub fn check(nodes: &[ModelNode]) -> Result<Vec<ModelSpec>, Vec<Found>> {
    let mut errors = Vec::new();
    let mut seen_models = HashSet::new();
    for node in nodes {
        if !seen_models.insert(node.name.as_str()) {
            errors.push(at(node.file, node.line, node.column, format!("model `{}` is declared twice", node.name)));
        }
        check_model(node, nodes, &mut errors);
    }
    check_types(nodes, &mut errors);
    enums::check_enums(nodes, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    let models = build(nodes);
    nested::check_defaults(nodes, &models, &mut errors);
    if !errors.is_empty() { return Err(errors); }
    let mut resolved = models.clone();
    dblayer::schema::resolve_types(&mut resolved);
    let faults = validate_models(&resolved, &[]);
    if faults.is_empty() {
        return Ok(models);
    }
    let found = faults
        .iter()
        .filter_map(|fault| {
            let node = nodes.iter().find(|node| node.name == fault.model.as_str())?;
            let hint = if matches!(fault.error, dblayer::schema::DeclarationError::TooManyColumns { .. }) { ". A union stores its variants in the columns of one table: declare fewer variants or fields, or split the model" } else { "" };
            let message = format!("model {}: {}{hint}", node.name, fault.error);
            Some(at(node.file, node.line, node.column, message))
        })
        .collect();
    Err(found)
}

struct TypeUse<'a> {
    name: String,
    call: &'a Call,
    file: usize,
    declares: bool,
}

fn collect_types<'a>(calls: &'a [Call], owner: &str, file: usize, out: &mut Vec<TypeUse<'a>>) {
    for call in calls.iter().filter(|call| matches!(call.word.as_str(), "object" | "array" | "union")) {
        let named = call.options.iter().find(|pair| pair.key == "as").and_then(|pair| match &pair.value.item {
            Value::Const(name) => Some(name.clone()),
            _ => None,
        });
        let field = symbol_name(call.args.first()).unwrap_or_default();
        let name = named.clone().unwrap_or_else(|| format!("{owner}{}", crate::build::camel(field)));
        if crate::unions::is_union_call(call) {
            out.push(TypeUse { name: name.clone(), call, file, declares: call.block.is_some() });
            for variant in call.block.iter().flatten() {
                if variant.word == "shared" {
                    collect_types(variant.block.as_deref().unwrap_or_default(), &name, file, out);
                    continue;
                }
                let Some(Node { item: Value::Const(typename), .. }) = variant.args.first() else { continue };
                out.push(TypeUse { name: typename.clone(), call: variant, file, declares: variant.block.is_some() });
                collect_types(variant.block.as_deref().unwrap_or_default(), typename, file, out);
            }
        } else if let Some(block) = &call.block {
            out.push(TypeUse { name: name.clone(), call, file, declares: true });
            collect_types(block, &name, file, out);
        } else if named.is_some() {
            out.push(TypeUse { name, call, file, declares: false });
        }
    }
}

fn check_types(nodes: &[ModelNode], errors: &mut Vec<Found>) {
    let mut uses = Vec::new();
    for node in nodes {
        for schema in node.body.iter().filter(|call| call.word == "schema") {
            collect_types(schema.block.as_deref().unwrap_or_default(), &node.name, node.file, &mut uses);
        }
    }
    let mut declared = HashSet::new();
    for item in uses.iter().filter(|item| item.declares) {
        if !declared.insert(item.name.as_str()) {
            errors.push(at(item.file, item.call.line, item.call.column, format!("type `{}` is declared twice", item.name)));
        }
        if nodes.iter().any(|node| node.name == item.name) {
            errors.push(at(item.file, item.call.line, item.call.column, format!("type `{}` has the name of a model", item.name)));
        }
    }
    for item in uses.iter().filter(|item| !item.declares && !declared.contains(item.name.as_str())) {
        errors.push(at(item.file, item.call.line, item.call.column, format!("type `{}` is not declared by any `do ... end` block", item.name)));
    }
}

fn at(file: usize, line: usize, column: usize, message: String) -> Found {
    (file, ParseError::new(line, column, message))
}

fn check_model(node: &ModelNode, all: &[ModelNode], errors: &mut Vec<Found>) {
    let file = node.file;
    let mut report = |line: usize, column: usize, message: String| errors.push(at(file, line, column, message));
    check_model_options(node, &mut report);
    if node.options.iter().any(|pair| pair.key == "storage" && matches!(&pair.value.item, Value::Symbol(name) if name == "local")) {
        for call in node.body.iter().filter(|call| matches!(call.word.as_str(), "remote" | "listen" | "action" | "create" | "update" | "destroy")) {
            report(call.line, call.column, format!("local model has no server: `{}` is not allowed with `storage: :local`", call.word));
        }
    }
    let mut schemas = 0;
    let mut member_names = HashSet::new();
    let mut standard_words = HashSet::new();
    let fields: HashSet<&str> = node
        .body
        .iter()
        .filter(|call| call.word == "schema")
        .flat_map(|schema| schema.block.iter().flatten())
        .filter_map(|call| symbol_name(call.args.first()))
        .collect();
    let remotes: HashSet<&str> =
        node.body.iter().filter(|call| call.word == "remote").filter_map(|call| symbol_name(call.args.first())).collect();
    for call in &node.body {
        if call.word == "schema" {
            schemas += 1;
            if schemas > 1 {
                report(call.line, call.column, "`schema` is declared twice".to_string());
            }
        }
        if call.word == "ephemeral" {
            report(call.line, call.column, "`ephemeral` must be inside a `schema` block".to_string());
            continue;
        }
        if call.word != "schema" && FIELD_WORDS.contains(&call.word.as_str()) || call.word == "index" {
            report(call.line, call.column, format!("field `{}` must be inside a `schema` block", call.word));
            continue;
        }
        check_call(call, Context::Model, &mut report);
        if let Some(name) = symbol_name(call.args.first()).filter(|_| call.word != "schema") {
            if !member_names.insert(name.to_string()) {
                report(call.line, call.column, format!("name `{name}` is declared twice"));
            }
        }
        if ASSOCIATION_WORDS.contains(&call.word.as_str()) {
            check_target(call, all, &mut report);
        }
        if call.word == "remote" {
            check_remote(call, &mut report);
        }
        if call.word == "scope" {
            check_scope(call, &mut report);
        }
        if matches!(call.word.as_str(), "create" | "update" | "destroy") {
            if !standard_words.insert(call.word.as_str()) {
                report(call.line, call.column, format!("`{}` is declared twice", call.word));
            }
            check_standard_input(call, &fields, &mut report);
        }
        if call.word == "action" {
            check_action(call, &fields, &remotes, &mut report);
        }
    }
    for schema in node.body.iter().filter(|call| call.word == "schema") {
        check_schema(schema, &mut report);
    }
}

fn check_model_options(node: &ModelNode, report: &mut impl FnMut(usize, usize, String)) {
    for pair in &node.options {
        match pair.key.as_str() {
            "storage" if !matches!(&pair.value.item, Value::Symbol(name) if name == "memory" || name == "disk" || name == "local") => {
                report(pair.value.line, pair.value.column, "`storage` must be `:memory`, `:disk` or `:local`".to_string())
            }
            "storage" => {}
            "expire_after" => check_value(pair, Want::Duration, report),
            other => report(pair.line, pair.column, format!("unknown key `{other}` for `model`")),
        }
    }
}

fn check_standard_input(call: &Call, fields: &HashSet<&str>, report: &mut impl FnMut(usize, usize, String)) {
    let hash = |key: &str| match call.options.iter().find(|pair| pair.key == key) {
        Some(Pair { value: Node { item: Value::Hash(pairs), .. }, .. }) => Some(pairs),
        _ => None,
    };
    let args = hash("args").map(Vec::as_slice).unwrap_or_default();
    for pair in args {
        if !is_arg_kind(&pair.value.item) {
            report(pair.value.line, pair.value.column, ARG_KIND_MESSAGE.to_string());
        }
        if fields.contains(pair.key.as_str()) {
            report(call.line, call.column, format!("arg `{}` has the name of a field", pair.key));
        }
    }
    let Some(pairs) = hash("input") else { return };
    for pair in pairs {
        match &pair.value.item {
            Value::Symbol(field) if field == "id" || fields.contains(field.as_str()) || args.iter().any(|arg| arg.key == *field) => {}
            Value::Symbol(field) => report(call.line, call.column, format!("input names unknown field `{field}`")),
            _ => report(call.line, call.column, "input value must be a field symbol".to_string()),
        }
    }
    for arg in args.iter().filter(|arg| !pairs.iter().any(|pair| matches!(&pair.value.item, Value::Symbol(name) if *name == arg.key))) {
        report(call.line, call.column, format!("arg `{}` is not named by `input`", arg.key));
    }
}

fn check_action(action: &Call, fields: &HashSet<&str>, remotes: &HashSet<&str>, report: &mut impl FnMut(usize, usize, String)) {
    let lines: Vec<&Call> = action.block.iter().flatten().collect();
    let mut seen = HashSet::new();
    for call in &lines {
        check_call(call, Context::Action, report);
        if !seen.insert(call.word.as_str()) && !matches!(call.word.as_str(), "lands" | "refetch_of") {
            report(call.line, call.column, format!("`{}` is declared twice", call.word));
        }
        check_action_line(call, fields, remotes, report);
    }
    if action.block.is_some() && !seen.contains("mutation") {
        report(action.line, action.column, "`action` needs a `mutation` line".to_string());
    }
    check_local_effect(action, &seen, report);
}

fn check_local_effect(action: &Call, seen: &HashSet<&str>, report: &mut impl FnMut(usize, usize, String)) {
    let Some(block) = &action.block else { return };
    let name = symbol_name(action.args.first()).unwrap_or_default();
    if let Some(effect) = block.iter().find(|call| call.word == "local_effect") {
        let reason = effect.options.iter().find(|pair| pair.key == "reason").map(|pair| &pair.value.item);
        if symbol_name(effect.args.first()) != Some("none") {
            report(effect.line, effect.column, "`local_effect` must be `:none`".to_string());
        }
        if !matches!(reason, Some(Value::Str(text)) if !text.trim().is_empty()) {
            report(effect.line, effect.column, "`local_effect :none` needs a non-empty `reason:` string".to_string());
        }
    } else if !seen.contains("optimistic") && !action.options.iter().any(|pair| pair.key == "optimistic") {
        report(action.line, action.column, format!("`action :{name}` must declare `optimistic` or `local_effect :none, reason: \"...\"`"));
    }
}

fn check_action_line(call: &Call, fields: &HashSet<&str>, remotes: &HashSet<&str>, report: &mut impl FnMut(usize, usize, String)) {
    match call.word.as_str() {
        "input" => {
            for pair in call.options.iter().filter(|pair| !matches!(pair.value.item, Value::Symbol(_))) {
                report(pair.value.line, pair.value.column, "input value must be a field symbol".to_string());
            }
        }
        "optimistic" => {
            for pair in &call.options {
                if !fields.contains(pair.key.as_str()) {
                    report(pair.line, pair.column, format!("`optimistic` names unknown field `{}`", pair.key));
                }
                match &pair.value.item {
                    Value::Array(_) => report(pair.value.line, pair.value.column, "optimistic value must be a literal".to_string()),
                    Value::Hash(parts) => report_keyed_optimistic(parts, pair, report),
                    _ => {}
                }
            }
        }
        "args" => {
            for pair in call.options.iter().filter(|pair| !is_arg_kind(&pair.value.item)) {
                report(pair.value.line, pair.value.column, ARG_KIND_MESSAGE.to_string());
            }
        }
        "result" => {
            for pair in call.options.iter().filter(|pair| !matches!(&pair.value.item, Value::Symbol(kind) if ["bool", "int", "num", "string", "id"].contains(&kind.as_str()))) {
                report(pair.value.line, pair.value.column, "result type must be :bool, :int, :num, :string or :id".to_string());
            }
        }
        "on_failure" => {
            if !symbol_name(call.args.first()).is_some_and(is_policy) {
                report(call.line, call.column, "`on_failure` must be `:keep` or `:rollback`".to_string());
            }
        }
        "refetch" => {
            for name in call.args.iter().filter_map(|arg| symbol_name(Some(arg))).filter(|name| !remotes.contains(name)) {
                report(call.line, call.column, format!("`refetch` names `{name}`, which is not a `remote`"));
            }
        }
        _ => {}
    }
}

fn check_schema(schema: &Call, report: &mut impl FnMut(usize, usize, String)) {
    let mut names = HashSet::new();
    for call in schema.block.iter().flatten() {
        check_call(call, Context::Schema, report);
        if call.word == "index" {
            continue;
        }
        if let Some(name) = symbol_name(call.args.first()) {
            if !names.insert(name.to_string()) {
                report(call.line, call.column, format!("field `{name}` is declared twice"));
            }
        }
        if call.word == "list" {
            check_list_element(call, report);
        }
        check_local(call, report);
        if call.block.is_some() && matches!(call.word.as_str(), "object" | "array" | "union") {
            check_nested_block(call, report);
        }
    }
    for ephemeral in schema.block.iter().flatten().filter(|call| call.word == "ephemeral") {
        check_ephemeral(ephemeral, &mut names, report);
    }
}

fn check_nested_block(owner: &Call, report: &mut impl FnMut(usize, usize, String)) {
    let name = symbol_name(owner.args.first()).unwrap_or_default();
    let calls: &[Call] = owner.block.as_deref().unwrap_or_default();
    if crate::unions::is_union_call(owner) {
        return check_union_block(owner, name, report);
    }
    check_field_block(owner, name, calls, report);
}

fn check_union_block(owner: &Call, name: &str, report: &mut impl FnMut(usize, usize, String)) {
    let calls: &[Call] = owner.block.as_deref().unwrap_or_default();
    let mut typenames: Vec<&str> = Vec::new();
    let mut repeated = false;
    let shared_calls: Vec<&Call> = calls.iter().filter(|call| call.word == "shared").collect();
    let shared_names: Vec<&str> = shared_calls.iter().flat_map(|call| call.block.iter().flatten()).filter_map(|field| symbol_name(field.args.first())).collect();
    for call in calls {
        check_call(call, Context::Union, report);
        if call.word == "shared" {
            if !std::ptr::eq(*shared_calls.first().unwrap_or(&call), call) {
                report(call.line, call.column, "`shared` is declared twice".to_string());
                continue;
            }
            let block: &[Call] = call.block.as_deref().unwrap_or_default();
            check_field_block(call, "shared", block, report);
            for field in block.iter().filter(|field| symbol_name(field.args.first()) == Some("kind")) {
                report(field.line, field.column, "`kind` is reserved: a union with `shared` fields already has a `kind` property".to_string());
            }
            continue;
        }
        let Some(Node { item: Value::Const(typename), .. }) = call.args.first() else { continue };
        if typenames.contains(&typename.as_str()) {
            repeated = true;
            report(call.line, call.column, format!("variant `{typename}` is declared twice"));
            continue;
        }
        typenames.push(typename);
        if let Some(block) = &call.block {
            if block.is_empty() && !shared_names.is_empty() {
                continue;
            }
            check_field_block(call, typename, block, report);
            for field in block.iter().filter(|field| symbol_name(field.args.first()).is_some_and(|field| shared_names.contains(&field))) {
                report(field.line, field.column, format!("field `{}` is declared twice: it is also a `shared` field", symbol_name(field.args.first()).unwrap_or_default()));
            }
        }
    }
    if repeated {
        return;
    }
    let union = option_const(owner, "as").unwrap_or(name);
    if let Err(message) = crate::unions::check_cases(union, &crate::unions::cases_of(calls)) {
        report(owner.line, owner.column, message);
    }
}

fn option_const<'a>(call: &'a Call, key: &str) -> Option<&'a str> {
    call.options.iter().find(|pair| pair.key == key).and_then(|pair| match &pair.value.item {
        Value::Const(name) => Some(name.as_str()),
        _ => None,
    })
}

fn check_field_block(owner: &Call, name: &str, calls: &[Call], report: &mut impl FnMut(usize, usize, String)) {
    if calls.is_empty() {
        report(owner.line, owner.column, format!("`{name}` block needs at least 1 field"));
    }
    let mut names = HashSet::new();
    for call in calls {
        check_call(call, Context::Nested, report);
        let field = symbol_name(call.args.first()).unwrap_or_default();
        if !names.insert(field.to_string()) {
            report(call.line, call.column, format!("field `{field}` is declared twice"));
        }
        if let Some(pair) = call.options.iter().find(|pair| pair.key == "from") {
            if !matches!(&pair.value.item, Value::Str(key) | Value::Symbol(key) if !key.is_empty()) {
                report(pair.value.line, pair.value.column, "nested `from:` must be a non-empty JSON key".to_string());
            }
        }
        if call.word == "list" {
            check_list_element(call, report);
        }
        if call.block.is_some() {
            check_nested_block(call, report);
        }
    }
}

fn check_local(call: &Call, report: &mut impl FnMut(usize, usize, String)) {
    let has = |key: &str| call.options.iter().any(|pair| pair.key == key);
    if !call.options.iter().any(|pair| pair.key == "local" && pair.value.item == Value::Bool(true)) {
        return;
    }
    let name = symbol_name(call.args.first()).unwrap_or_default();
    if !has("optional") && !has("null") {
        report(call.line, call.column, format!("local field `{name}` needs `optional: true` or `null: true`"));
    } else if has("merge") || has("index") || has("default") || has("from") || has("id_from") {
        report(call.line, call.column, format!("local field `{name}` cannot set `merge`, `index`, `default`, `from` or `id_from`"));
    }
}

fn check_ephemeral(block: &Call, names: &mut HashSet<String>, report: &mut impl FnMut(usize, usize, String)) {
    for call in block.block.iter().flatten() {
        let name = symbol_name(call.args.first()).unwrap_or_default();
        if call.word == "file" {
            report(call.line, call.column, format!("ephemeral field `{name}` cannot be a file"));
            continue;
        }
        check_call(call, Context::Ephemeral, report);
        if !name.is_empty() && !names.insert(name.to_string()) {
            report(call.line, call.column, format!("field `{name}` is declared twice"));
        }
        let has = |key: &str| call.options.iter().any(|pair| pair.key == key);
        let nullable = call.options.iter().any(|pair| pair.key == "null" && pair.value.item == Value::Bool(true));
        if has("index") {
            report(call.line, call.column, format!("ephemeral field `{name}` cannot be indexed"));
        } else if has("reset_after") && !has("default") {
            report(call.line, call.column, "`reset_after` needs `default:`".to_string());
        } else if !has("default") && !nullable && EPHEMERAL_WORDS.contains(&call.word.as_str()) {
            report(call.line, call.column, format!("ephemeral field `{name}` needs `default:` or `null: true`"));
        }
        if call.word == "list" {
            check_list_element(call, report);
        }
    }
}

fn check_list_element(call: &Call, report: &mut impl FnMut(usize, usize, String)) {
    let Some(pair) = call.options.iter().find(|pair| pair.key == "of") else { return };
    let valid = matches!(&pair.value.item, Value::Symbol(name) if SCALAR_WORDS.contains(&name.as_str()) || name == "enum");
    if !valid {
        let message = format!("`of` must be one of {}, enum", SCALAR_WORDS.join(", "));
        report(pair.value.line, pair.value.column, message);
        return;
    }
    let enumeration = matches!(&pair.value.item, Value::Symbol(name) if name == "enum");
    let has = |key: &str| call.options.iter().any(|candidate| candidate.key == key);
    if enumeration && !has("values") {
        report(call.line, call.column, "a list of `:enum` needs `values:`".to_string());
    }
    if !enumeration && (has("values") || has("as")) {
        report(call.line, call.column, "`values:` and `as:` belong to a list of `:enum`".to_string());
    }
}

fn check_target(call: &Call, all: &[ModelNode], report: &mut impl FnMut(usize, usize, String)) {
    let target = crate::build::association_target(call);
    if !all.iter().any(|node| node.name == target) {
        let name = symbol_name(call.args.first()).unwrap_or_default();
        report(call.line, call.column, format!("association `{name}` targets unknown model `{target}`"));
    }
    let dependent = call.options.iter().find(|pair| pair.key == "dependent");
    if let Some(pair) = dependent.filter(|pair| pair.value.item != Value::Symbol("destroy".to_string())) {
        report(pair.line, pair.column, "`dependent` must be `:destroy`".to_string());
    }
}

fn check_scope(call: &Call, report: &mut impl FnMut(usize, usize, String)) {
    let option = |name: &str| call.options.iter().find(|pair| pair.key == name);
    let params: Vec<&str> = match option("params").map(|pair| &pair.value.item) {
        Some(Value::Array(items)) => items.iter().filter_map(|item| symbol_name(Some(item))).collect(),
        _ => Vec::new(),
    };
    if let Some(Value::Hash(pairs)) = option("where").map(|pair| &pair.value.item) {
        for pair in pairs {
            match &pair.value.item {
                Value::Symbol(name) if params.contains(&name.as_str()) => {}
                Value::Symbol(name) => {
                    report(call.line, call.column, format!("where value `:{name}` is not listed in `params`"))
                }
                _ => report(call.line, call.column, "where value must be a parameter symbol".to_string()),
            }
        }
    }
    let used: Vec<&str> = match option("where").map(|pair| &pair.value.item) {
        Some(Value::Hash(pairs)) => pairs.iter().filter_map(|pair| symbol_name(Some(&pair.value))).collect(),
        _ => Vec::new(),
    };
    for name in params.iter().filter(|name| !used.contains(name)) {
        report(call.line, call.column, format!("param `:{name}` is not used in `where`"));
    }
    if let Some(Value::Hash(pairs)) = option("order").map(|pair| &pair.value.item) {
        let valid = |pair: &Pair| matches!(&pair.value.item, Value::Symbol(name) if SortDirection::parse(name).is_some());
        if pairs.iter().any(|pair| !valid(pair)) {
            report(call.line, call.column, "order direction must be `:asc`, `:desc`, `:asc_nulls_last` or `:desc_nulls_first`".to_string());
        }
    }
}

fn check_remote(remote: &Call, report: &mut impl FnMut(usize, usize, String)) {
    let lines: Vec<&Call> = remote.block.iter().flatten().collect();
    for call in &lines {
        check_call(call, Context::Remote, report);
    }
    let has = |word: &str| lines.iter().any(|call| call.word == word);
    if has("paginate") && has("list") {
        report(remote.line, remote.column, "`paginate` and `list` cannot be used together".to_string());
    }
    if let Some(pair) = remote.options.iter().find(|pair| pair.key == "joins" && matches!(&pair.value.item, Value::Symbol(kind) if kind == "events")).filter(|_| remote.options.iter().any(|pair| pair.key == "text")) {
        report(pair.line, pair.column, format!("scope `{}`: `joins: :events` joins rows by their `where` columns and a scope with `text:` params has none, remove `joins:` and the server answer decides the members", symbol_name(remote.args.first()).unwrap_or_default()));
    }
    if let Some(call) = lines.iter().find(|call| call.word == "max_pages").filter(|_| !has("paginate")) {
        report(call.line, call.column, "`max_pages` needs `paginate`".to_string());
    }
    check_edges(&lines, has("paginate"), report);
    if let Some(call) = lines.iter().find(|call| call.word == "total").filter(|_| !has("paginate")) {
        report(call.line, call.column, "`total` needs `paginate`".to_string());
    }
    let mut seen = HashSet::new();
    for call in &lines {
        if !matches!(call.word.as_str(), "updates" | "edge") && !seen.insert(call.word.as_str()) {
            report(call.line, call.column, format!("`{}` is declared twice", call.word));
        }
    }
}

fn check_edges(lines: &[&Call], paginated: bool, report: &mut impl FnMut(usize, usize, String)) {
    let mut names = HashSet::new();
    for edge in lines.iter().filter(|call| call.word == "edge") {
        let Some(field) = edge.block.iter().flatten().next() else { continue };
        check_call(field, Context::Schema, report);
        let name = symbol_name(field.args.first()).unwrap_or_default();
        let has = |key: &str| field.options.iter().any(|pair| pair.key == key);
        if !names.insert(name.to_string()) {
            report(field.line, field.column, format!("edge `{name}` is declared twice"));
        } else if !paginated {
            report(field.line, field.column, format!("edge `{name}` needs `paginate` in its scope"));
        } else if !has("null") && !has("optional") {
            report(field.line, field.column, format!("edge `{name}` needs `null: true` or `optional: true`"));
        } else if ["merge", "local", "index", "default"].iter().any(|key| has(key)) {
            report(field.line, field.column, format!("edge `{name}` cannot set `merge`, `local`, `index` or `default`"));
        }
    }
}

fn check_call(call: &Call, context: Context, report: &mut impl FnMut(usize, usize, String)) {
    let Some(word) = word_for(context, &call.word) else {
        report(call.line, call.column, format!("unknown word `{}`", call.word));
        return;
    };
    check_shape(call, word.shape, report);
    match (&call.block, word.block) {
        (None, true) if call.word == "action" && call.options.iter().any(|pair| pair.key == "mutation") => {}
        (None, true) => report(call.line, call.column, format!("`{}` needs a `do ... end` block", call.word)),
        (Some(_), false) if matches!(context, Context::Schema | Context::Nested) && matches!(call.word.as_str(), "object" | "array" | "union") => {}
        (Some(_), false) if context == Context::Union => {}
        (Some(_), false) => report(call.line, call.column, format!("`{}` takes no block", call.word)),
        _ => {}
    }
    for pair in &call.options {
        match word.keys.iter().find(|candidate| candidate.name == pair.key) {
            None if word.free_keys => {}
            None => report(pair.line, pair.column, format!("unknown key `{}` for `{}`", pair.key, call.word)),
            Some(expected) => check_value(pair, expected.want, report),
        }
    }
    for expected in word.keys.iter().filter(|candidate| candidate.required) {
        if !call.options.iter().any(|pair| pair.key == expected.name) {
            report(call.line, call.column, format!("`{}` needs `{}:`", call.word, expected.name));
        }
    }
    check_response_paths(call, context, report);
}

fn check_response_paths(call: &Call, context: Context, report: &mut impl FnMut(usize, usize, String)) {
    let option_paths: &[(&str, &str)] = match (context, call.word.as_str()) {
        (Context::Model, "create" | "update" | "destroy") => &[("root", "root")],
        (Context::Model, "listen") => &[("root", "listen root"), ("id", "listen id")],
        (Context::Remote, "updates") => &[("root", "updates root"), ("id", "updates id")],
        (Context::Remote, "query") => &[("root", "root")],
        (Context::Remote, "total") => &[("from", "total")],
        (Context::Remote, "paginate") => &[("from", "paginate from")],
        (Context::Action, "lands") => &[("root", "lands")],
        _ => &[],
    };
    let mut paths: Vec<(&str, &Node<Value>)> = option_paths.iter().filter_map(|(key, label)| call.options.iter().find(|pair| pair.key == *key).map(|pair| (*label, &pair.value))).collect();
    if (context, call.word.as_str()) == (Context::Action, "root") {
        paths.extend(call.args.first().map(|arg| ("root", arg)));
    }
    for (label, node) in paths {
        if let Value::Symbol(path) | Value::Str(path) | Value::Const(path) = &node.item {
            if path.split('.').any(str::is_empty) {
                report(node.line, node.column, format!("{label} `{path}` has an empty segment"));
            }
        }
    }
}

fn check_shape(call: &Call, shape: Shape, report: &mut impl FnMut(usize, usize, String)) {
    let word = &call.word;
    let args = call.args.as_slice();
    let message = match shape {
        Shape::Name if !matches!(args, [Node { item: Value::Symbol(_), .. }]) => format!("`{word}` needs a name like `:name`"),
        Shape::Names if args.is_empty() || args.iter().any(|arg| !matches!(arg.item, Value::Symbol(_))) => {
            format!("`{word}` needs field names like `:a` or `:a, :b`")
        }
        Shape::Const if !matches!(args, [Node { item: Value::Const(_) | Value::Str(_), .. }]) => {
            format!("`{word}` needs an operation name like `MomentsFeed` or \"feed\"")
        }
        Shape::Int if !matches!(args, [Node { item: Value::Int(_), .. }]) => format!("`{word}` needs a number"),
        Shape::Duration if !matches!(args, [Node { item: Value::Duration { .. }, .. }]) => {
            format!("`{word}` needs a duration like `5.minutes`")
        }
        Shape::Symbols if args.is_empty() || args.iter().any(|arg| !matches!(arg.item, Value::Symbol(_))) => {
            format!("`{word}` needs 1 or more names like `:a, :b`")
        }
        Shape::Paths if args.is_empty() || args.iter().any(|arg| !matches!(arg.item, Value::Symbol(_) | Value::Str(_))) => {
            format!("`{word}` needs 1 or more names like `:a, \"media.blobId\"`")
        }
        Shape::Nothing if !args.is_empty() => format!("`{word}` takes no arguments"),
        _ => return,
    };
    report(call.line, call.column, message);
}

fn check_value(pair: &Pair, want: Want, report: &mut impl FnMut(usize, usize, String)) {
    let value = &pair.value.item;
    let key = &pair.key;
    let message = match want {
        Want::Bool if !matches!(value, Value::Bool(_)) => format!("`{key}` must be true or false"),
        Want::Symbol if !matches!(value, Value::Symbol(_)) => format!("`{key}` must be a symbol like `:name`"),
        Want::TypeName if !matches!(value, Value::Const(_)) => format!("`{key}` must be a type name like `MomentMedia`"),
        Want::Const if !matches!(value, Value::Const(_)) => format!("`{key}` must be a model name like `User`"),
        Want::Operation if !matches!(value, Value::Const(_) | Value::Str(_)) => {
            format!("`{key}` must be an operation name like `CreateMoment` or \"likeMoment\"")
        }
        Want::Symbols if !is_symbol_list(value) => format!("`{key}` must be a list of symbols"),
        Want::TextParams if !is_text_params(value) => format!("`{key}` must be a list of names like `[:query]` or typed names like `[list: :ChatList]`"),
        Want::Hash if !matches!(value, Value::Hash(_)) => format!("`{key}` must be a hash like `{{ field: :param }}`"),
        Want::Duration if !matches!(value, Value::Duration { .. }) => format!("`{key}` must be a duration like `5.minutes`"),
        Want::Policy if !matches!(value, Value::Symbol(name) if is_policy(name)) => format!("`{key}` must be `:keep` or `:rollback`"),
        Want::Merge if !is_merge(value) => format!("`{key}` must be `:present`, `:non_empty`, `:positive`, `:monotonic`, a list of tiers like `[[:a, :b], [:c]]` or a hash of those per key (a key may also be `:local`)"),
        _ => return,
    };
    report(pair.value.line, pair.value.column, message);
}

fn is_merge(value: &Value) -> bool {
    match value {
        Value::Symbol(name) => matches!(name.as_str(), "present" | "non_empty" | "positive" | "monotonic"),
        Value::Hash(pairs) => !pairs.is_empty() && pairs.iter().all(|pair| matches!(&pair.value.item, Value::Symbol(name) if name == "local") || is_merge(&pair.value.item) && !matches!(pair.value.item, Value::Hash(_))),
        Value::Array(tiers) => !tiers.is_empty() && tiers.iter().all(|tier| matches!(&tier.item, Value::Array(names) if !names.is_empty() && names.iter().all(|name| matches!(name.item, Value::Symbol(_) | Value::Str(_))))),
        _ => false,
    }
}

fn is_policy(name: &str) -> bool {
    matches!(name, "keep" | "rollback")
}

fn is_text_params(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().all(|item| match &item.item {
            Value::Symbol(_) | Value::Str(_) => true,
            Value::Hash(pairs) => matches!(pairs.as_slice(), [Pair { value: Node { item: Value::Symbol(kind), .. }, .. }] if kind.starts_with(char::is_uppercase)),
            _ => false,
        }),
        _ => false,
    }
}

fn is_symbol_list(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().all(|item| matches!(item.item, Value::Symbol(_) | Value::Str(_))),
        _ => false,
    }
}

fn symbol_name(node: Option<&Node<Value>>) -> Option<&str> {
    match node.map(|node| &node.item) {
        Some(Value::Symbol(name)) => Some(name),
        _ => None,
    }
}

#[cfg(test)]
mod check_tests;
#[cfg(test)]
mod option_tests;
#[cfg(test)]
mod edge_tests;

fn report_keyed_optimistic(parts: &[Pair], pair: &Pair, report: &mut impl FnMut(usize, usize, String)) {
    let by = parts.iter().find(|part| part.key == "by").and_then(|part| match &part.value.item {
        Value::Symbol(name) => Some(name.as_str()),
        _ => None,
    });
    let element = parts.iter().find(|part| part.key == "element").and_then(|part| match &part.value.item {
        Value::Hash(fields) => Some(fields),
        _ => None,
    });
    let keyed = parts.len() == 2 && by.is_some_and(|by| element.is_some_and(|fields| fields.iter().any(|field| field.key == by)));
    if !keyed {
        report(pair.value.line, pair.value.column, "optimistic hash must be `{ by: :key, element: { key: :arg, ... } }` with the key among the element fields".to_string());
    }
}
