use std::collections::HashSet;

use dblayer::schema::{validate_models, ModelSpec};

use crate::build::build;
use crate::parser::{Call, ModelNode, Node, Pair, Value};
use crate::ParseError;

type Found = (usize, ParseError);

const SCALAR_WORDS: [&str; 6] = ["string", "num", "int", "bool", "date", "id"];
const FIELD_WORDS: [&str; 12] =
    ["string", "num", "int", "bool", "date", "id", "enum", "object", "array", "raw", "list", "file"];
const EPHEMERAL_WORDS: [&str; 11] = ["string", "num", "int", "bool", "date", "id", "enum", "object", "array", "raw", "list"];
const ASSOCIATION_WORDS: [&str; 4] = ["belongs_to", "has_many", "has_one", "references"];

#[derive(Clone, Copy, PartialEq)]
enum Context {
    Model,
    Schema,
    Ephemeral,
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
    Nothing,
}

#[derive(Clone, Copy, PartialEq)]
enum Want {
    Bool,
    Symbol,
    Const,
    Operation,
    Symbols,
    Hash,
    Duration,
    Policy,
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
    ];
    match word {
        "file" => keys.retain(|candidate| !matches!(candidate.name, "id_from" | "from")),
        "enum" => keys.push(key("values", Want::Symbols, true)),
        "list" => keys.push(key("of", Want::Symbol, true)),
        "object" | "array" | "raw" => keys.push(key("as", Want::Any, false)),
        _ => {}
    }
    if word != "file" {
        keys.push(key("index", Want::Bool, false));
    }
    keys
}

fn ephemeral_keys(word: &str) -> Vec<Key> {
    let mut keys = field_keys(word);
    keys.retain(|candidate| !matches!(candidate.name, "optional" | "id_from" | "from" | "as"));
    keys.push(key("reset_after", Want::Duration, false));
    keys
}

fn word_for(context: Context, word: &str) -> Option<Word> {
    let plain = |shape, keys| Some(Word { shape, keys, block: false, free_keys: false });
    let free = || Some(Word { shape: Shape::Nothing, keys: vec![], block: false, free_keys: true });
    match (context, word) {
        (Context::Model, "schema") => Some(Word { shape: Shape::Nothing, keys: vec![], block: true, free_keys: false }),
        (Context::Model, "remote") => Some(Word { shape: Shape::Name, keys: vec![key("params", Want::Symbols, false), key("text", Want::Symbols, false), key("order", Want::Hash, false)], block: true, free_keys: false }),
        (Context::Model, "action") => Some(Word { shape: Shape::Name, keys: vec![key("mutation", Want::Operation, false), key("optimistic", Want::Hash, false)], block: true, free_keys: false }),
        (Context::Model, "create" | "update") => {
            plain(
                Shape::Nothing,
                vec![key("with", Want::Operation, true), key("input", Want::Hash, false), key("on_failure", Want::Policy, false), key("wrap", Want::Symbol, false), key("root", Want::Any, false), key("response", Want::Symbol, false), key("refetch", Want::Symbols, false), key("local", Want::Symbols, false)],
            )
        }
        (Context::Model, "destroy") => {
            plain(Shape::Nothing, vec![key("with", Want::Operation, true), key("input", Want::Hash, false), key("wrap", Want::Symbol, false), key("root", Want::Any, false)])
        }
        (Context::Action, "mutation") => plain(Shape::Const, vec![]),
        (Context::Action, "on_failure") => plain(Shape::Name, vec![]),
        (Context::Action, "wrap" | "response") => plain(Shape::Name, vec![]),
        (Context::Action, "root") => plain(Shape::Const, vec![]),
        (Context::Action, "input" | "optimistic" | "args") => free(),
        (Context::Remote, "variables") => free(),
        (Context::Action, "refetch") => plain(Shape::Symbols, vec![]),
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
            vec![key("model", Want::Const, false), key("key", Want::Symbol, false), key("touch", Want::Bool, false), key("from", Want::Any, false)],
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
        (Context::Ephemeral, field) if EPHEMERAL_WORDS.contains(&field) => plain(Shape::Name, ephemeral_keys(field)),
        (Context::Remote, "query") => plain(Shape::Const, vec![key("root", Want::Any, false)]),
        (Context::Remote, "paginate") => plain(Shape::Int, vec![key("backward", Want::Bool, false), key("cursor", Want::Symbol, false), key("from", Want::Any, false), key("cursor_type", Want::Symbol, false)]),
        (Context::Remote, "list") => plain(Shape::Nothing, vec![]),
        (Context::Remote, "fresh_for") => plain(Shape::Duration, vec![key("empty", Want::Duration, false)]),
        (Context::Remote, "max_pages") => plain(Shape::Int, vec![]),
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
    if !errors.is_empty() {
        return Err(errors);
    }
    let models = build(nodes);
    let faults = validate_models(&models, &[]);
    if faults.is_empty() {
        return Ok(models);
    }
    let found = faults
        .iter()
        .filter_map(|fault| {
            let node = nodes.iter().find(|node| node.name == fault.model.as_str())?;
            let message = format!("model {}: {}", node.name, fault.error);
            Some(at(node.file, node.line, node.column, message))
        })
        .collect();
    Err(found)
}

fn at(file: usize, line: usize, column: usize, message: String) -> Found {
    (file, ParseError::new(line, column, message))
}

fn check_model(node: &ModelNode, all: &[ModelNode], errors: &mut Vec<Found>) {
    let file = node.file;
    let mut report = |line: usize, column: usize, message: String| errors.push(at(file, line, column, message));
    check_model_options(node, &mut report);
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
            "storage" if !matches!(&pair.value.item, Value::Symbol(name) if name == "memory" || name == "disk") => {
                report(pair.value.line, pair.value.column, "`storage` must be `:memory` or `:disk`".to_string())
            }
            "storage" => {}
            "expire_after" => check_value(pair, Want::Duration, report),
            other => report(pair.line, pair.column, format!("unknown key `{other}` for `model`")),
        }
    }
}

fn check_standard_input(call: &Call, fields: &HashSet<&str>, report: &mut impl FnMut(usize, usize, String)) {
    let Some(Pair { value: Node { item: Value::Hash(pairs), .. }, .. }) = call.options.iter().find(|pair| pair.key == "input")
    else {
        return;
    };
    for pair in pairs {
        match &pair.value.item {
            Value::Symbol(field) if field == "id" || fields.contains(field.as_str()) => {}
            Value::Symbol(field) => report(call.line, call.column, format!("input names unknown field `{field}`")),
            _ => report(call.line, call.column, "input value must be a field symbol".to_string()),
        }
    }
}

fn check_action(action: &Call, fields: &HashSet<&str>, remotes: &HashSet<&str>, report: &mut impl FnMut(usize, usize, String)) {
    let lines: Vec<&Call> = action.block.iter().flatten().collect();
    let mut seen = HashSet::new();
    for call in &lines {
        check_call(call, Context::Action, report);
        if !seen.insert(call.word.as_str()) {
            report(call.line, call.column, format!("`{}` is declared twice", call.word));
        }
        check_action_line(call, fields, remotes, report);
    }
    if action.block.is_some() && !seen.contains("mutation") {
        report(action.line, action.column, "`action` needs a `mutation` line".to_string());
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
                if matches!(pair.value.item, Value::Hash(_) | Value::Array(_)) {
                    report(pair.value.line, pair.value.column, "optimistic value must be a literal".to_string());
                }
            }
        }
        "args" => {
            for pair in call.options.iter().filter(|pair| !matches!(&pair.value.item, Value::Symbol(kind) if ["bool", "int", "num", "string", "id", "ids"].contains(&kind.as_str()))) {
                report(pair.value.line, pair.value.column, "arg type must be :bool, :int, :num, :string, :id or :ids".to_string());
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
    }
    for ephemeral in schema.block.iter().flatten().filter(|call| call.word == "ephemeral") {
        check_ephemeral(ephemeral, &mut names, report);
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
    let valid = matches!(&pair.value.item, Value::Symbol(name) if SCALAR_WORDS.contains(&name.as_str()));
    if !valid {
        let message = format!("`of` must be one of {}", SCALAR_WORDS.join(", "));
        report(pair.value.line, pair.value.column, message);
    }
}

fn check_target(call: &Call, all: &[ModelNode], report: &mut impl FnMut(usize, usize, String)) {
    let Some(Pair { value: Node { item: Value::Const(target), .. }, .. }) = call.options.iter().find(|pair| pair.key == "model")
    else {
        return;
    };
    if !all.iter().any(|node| &node.name == target) {
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
        let valid = |pair: &Pair| matches!(&pair.value.item, Value::Symbol(name) if name == "asc" || name == "desc");
        if pairs.iter().any(|pair| !valid(pair)) {
            report(call.line, call.column, "order direction must be `:asc` or `:desc`".to_string());
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
    if let Some(call) = lines.iter().find(|call| call.word == "max_pages").filter(|_| !has("paginate")) {
        report(call.line, call.column, "`max_pages` needs `paginate`".to_string());
    }
    let mut seen = HashSet::new();
    for call in &lines {
        if call.word != "updates" && !seen.insert(call.word.as_str()) {
            report(call.line, call.column, format!("`{}` is declared twice", call.word));
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
}

fn check_shape(call: &Call, shape: Shape, report: &mut impl FnMut(usize, usize, String)) {
    let word = &call.word;
    let args = call.args.as_slice();
    let message = match shape {
        Shape::Name if !matches!(args, [Node { item: Value::Symbol(_), .. }]) => format!("`{word}` needs a name like `:name`"),
        Shape::Names if args.len() < 2 || args.iter().any(|arg| !matches!(arg.item, Value::Symbol(_))) => {
            format!("`{word}` needs 2 or more field names like `:a, :b`")
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
        Want::Const if !matches!(value, Value::Const(_)) => format!("`{key}` must be a model name like `User`"),
        Want::Operation if !matches!(value, Value::Const(_) | Value::Str(_)) => {
            format!("`{key}` must be an operation name like `CreateMoment` or \"likeMoment\"")
        }
        Want::Symbols if !is_symbol_list(value) => format!("`{key}` must be a list of symbols"),
        Want::Hash if !matches!(value, Value::Hash(_)) => format!("`{key}` must be a hash like `{{ field: :param }}`"),
        Want::Duration if !matches!(value, Value::Duration { .. }) => format!("`{key}` must be a duration like `5.minutes`"),
        Want::Policy if !matches!(value, Value::Symbol(name) if is_policy(name)) => format!("`{key}` must be `:keep` or `:rollback`"),
        _ => return,
    };
    report(pair.value.line, pair.value.column, message);
}

fn is_policy(name: &str) -> bool {
    matches!(name, "keep" | "rollback")
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
