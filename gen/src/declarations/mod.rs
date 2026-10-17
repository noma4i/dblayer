use std::collections::HashSet;

use crate::parser::{Call, ModelNode, Node, Pair, Value};
use crate::ParseError;
use dblayer::schema::NODE_PARAM;
use crate::schema_check::{Schema, Ty, TyKind, TypeKind};

fn inferred_type(schema: &Schema, ty: &Ty) -> Result<Value, String> {
    match &ty.kind {
        TyKind::List(item) => {
            if item.is_list() || item.nullable {
                return Err(format!("model field type `{}` needs an explicit storage type", ty.describe()));
            }
            Ok(Value::Array(vec![Node { item: inferred_type(schema, item)?, line: 0, column: 0 }]))
        }
        TyKind::Named(name) => Ok(match name.as_str() {
            "String" => Value::Symbol("string".into()),
            "ID" => Value::Symbol("id".into()),
            "Int" => Value::Symbol("int".into()),
            "Float" => Value::Symbol("num".into()),
            "Boolean" => Value::Symbol("bool".into()),
            _ => match schema.types.get(name) {
                Some(TypeKind::Fields(_) | TypeKind::Union(_)) => Value::Const(name.clone()),
                _ => return Err(format!("GraphQL type `{name}` needs an explicit storage type")),
            },
        }),
    }
}

fn needs_inference(call: &Call) -> bool {
    let inside = |children: &Vec<Call>| children.iter().any(|child| needs_inference(child) || (child.word == "on" && child.block.as_ref().is_some_and(|block| block.iter().any(needs_inference))));
    call.word == "field" && (!call.options.iter().any(|pair| pair.key == "type") || call.block.as_ref().is_some_and(inside))
}

fn infer_fields(calls: &mut [Call], owner: &str, schema: &Schema) -> Result<(), ParseError> {
    for call in calls.iter_mut().filter(|call| call.word == "field") {
        let explicit = call.options.iter().any(|pair| pair.key == "type");
        if !needs_inference(call) { continue; }
        if call.options.iter().any(|pair| pair.key == "local" && pair.value.item == Value::Bool(true)) {
            continue;
        }
        let source = call.options.iter().find(|pair| pair.key == "from").map(|pair| &pair.value)
            .or_else(|| call.args.first());
        let Some(Node { item: Value::Symbol(name) | Value::Str(name), .. }) = source else {
            return Err(fail(call, "field needs a name"));
        };
        let mut ty = Ty { kind: TyKind::Named(owner.into()), nullable: false, has_default: false };
        for part in name.split('.') {
            ty = schema.field(ty.name(), part).map_err(|message| fail(call, message))?;
        }
        if !explicit {
            let kind = inferred_type(schema, &ty).map_err(|message| fail(call, message))?;
            call.options.push(pair(call, "type", kind));
            if ty.nullable && !call.options.iter().any(|pair| pair.key == "null") {
                call.options.push(pair(call, "null", Value::Bool(true)));
            }
        }
        if let Some(children) = &mut call.block {
            if children.iter().any(|child| child.word == "on") {
                for variant in children.iter_mut().filter(|child| child.word == "on") {
                    let Some(Node { item: Value::Const(name), .. }) = variant.args.first().cloned() else { continue };
                    if let Some(block) = &mut variant.block {
                        infer_fields(block, &name, schema)?;
                    }
                }
            } else {
                infer_fields(children, ty.name(), schema)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn infer(nodes: &mut [ModelNode], schema: &Schema) -> Result<(), Vec<(usize, ParseError)>> {
    let mut errors = Vec::new();
    for node in nodes {
        if let Err(error) = infer_fields(&mut node.body, &node.name, schema) {
            errors.push((node.file, error));
        }
    }
    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

fn pair(call: &Call, key: &str, item: Value) -> Pair {
    Pair { key: key.into(), value: Node { item, line: call.line, column: call.column }, line: call.line, column: call.column }
}

fn fail(call: &Call, message: impl Into<String>) -> ParseError {
    ParseError::new(call.line, call.column, message)
}

fn named_values(call: &Call) -> Result<(), ParseError> {
    if !call.args.is_empty() {
        return Err(fail(call, format!("`{}` needs named values", call.word)));
    }
    if call.block.is_some() {
        return Err(fail(call, format!("`{}` takes no block", call.word)));
    }
    Ok(())
}

fn positional_values(call: &Call, count: std::ops::RangeInclusive<usize>) -> Result<(), ParseError> {
    if !count.contains(&call.args.len()) || !call.options.is_empty() || call.block.is_some() {
        return Err(fail(call, format!("invalid arguments or block for `{}`", call.word)));
    }
    Ok(())
}

fn field(mut call: Call) -> Result<Call, ParseError> {
    if call.word != "field" { return Err(fail(&call, "nested fields must use `field`")); }
    if call.options.iter().any(|pair| matches!(pair.key.as_str(), "index" | "searchable")) {
        return Err(fail(&call, "declare indexes with `index` and full-text fields with `search`"));
    }
    let enumeration = call.options.iter().any(|pair| pair.key == "type" && matches!(&pair.value.item, Value::Symbol(kind) if kind == "enum"));
    if call.options.iter().any(|pair| pair.key == "of" || (pair.key == "as" && !enumeration)) {
        return Err(fail(&call, "declare named objects and lists with `type:`"));
    }
    let Some(at) = call.options.iter().position(|pair| pair.key == "type") else {
        return Err(fail(&call, "field needs `type:` when no GraphQL schema supplies its type"));
    };
    let ty = call.options.remove(at).value.item;
    let variants = call.block.as_ref().is_some_and(|children| children.iter().any(|child| child.word == "on"));
    if variants && !matches!(ty, Value::Const(_)) {
        return Err(fail(&call, "`on` is allowed only inside a union field with a type name"));
    }
    match ty {
        Value::Symbol(kind) => call.word = kind,
        Value::Const(name) => {
            call.word = if variants { "union" } else { "object" }.into();
            call.options.push(pair(&call, "as", Value::Const(name)));
        }
        Value::Array(items) if items.len() == 1 => match &items[0].item {
            Value::Symbol(kind) => {
                call.word = "list".into();
                call.options.push(pair(&call, "of", Value::Symbol(kind.clone())));
            }
            Value::Const(name) => {
                call.word = "array".into();
                call.options.push(pair(&call, "as", Value::Const(name.clone())));
            }
            _ => return Err(fail(&call, "list type needs a scalar or a declared type name")),
        },
        _ => return Err(fail(&call, "field type needs a scalar, named value type or list")),
    }
    if let Some(children) = call.block.take() {
        let convert = if variants { variant } else { field };
        call.block = Some(children.into_iter().map(convert).collect::<Result<_, _>>()?);
    }
    Ok(call)
}

fn variant(mut call: Call) -> Result<Call, ParseError> {
    if call.word != "on" { return Err(fail(&call, "a union holds only `on` variants")); }
    if !matches!(call.args.as_slice(), [Node { item: Value::Const(_), .. }]) {
        return Err(fail(&call, "`on` needs a type name like `GiftAttachment`"));
    }
    call.word = "variant".into();
    if let Some(children) = call.block.take() {
        call.block = Some(children.into_iter().map(field).collect::<Result<_, _>>()?);
    }
    Ok(call)
}

fn scope(mut call: Call) -> Result<Call, ParseError> {
    if call.options.iter().any(|pair| matches!(pair.key.as_str(), "params" | "where" | "order")) {
        return Err(fail(&call, "declare `where` and `order` inside the scope block"));
    }
    let Some(lines) = call.block.take() else { return Err(fail(&call, "scope needs a `do ... end` block")) };
    let remote = lines.iter().any(|line| line.word == "fetch");
    let mut seen = HashSet::new();
    let mut body = Vec::new();
    for mut line in lines {
        if line.word != "updates" && !seen.insert(line.word.clone()) {
            return Err(fail(&line, format!("`{}` is declared twice", line.word)));
        }
        match line.word.as_str() {
            "where" => {
                positional_values(&line, 1..=usize::MAX)?;
                let mut columns = Vec::new();
                for argument in &line.args {
                    let Value::Symbol(name) = &argument.item else {
                        return Err(fail(&line, "where needs column names as symbols"));
                    };
                    columns.push(Pair { key: name.clone(), value: argument.clone(), line: argument.line, column: argument.column });
                }
                call.options.push(pair(&line, "params", Value::Array(line.args.clone())));
                if !remote { call.options.push(pair(&line, "where", Value::Hash(columns))); }
            }
            "order" => {
                named_values(&line)?;
                call.options.push(pair(&line, "order", Value::Hash(line.options.clone())));
            }
            "fetch" => { line.word = "query".into(); body.push(line); }
            _ if remote => body.push(line),
            _ => return Err(fail(&line, format!("`{}` needs a scope with `fetch`", line.word))),
        }
    }
    call.word = if remote { "remote" } else { "scope" }.into();
    call.block = remote.then_some(body);
    Ok(call)
}

fn node_read(call: Call, many: bool) -> Result<Call, ParseError> {
    let word = if many { "nodes" } else { "node" };
    if !matches!(call.args.as_slice(), [Node { item: Value::Const(_), .. }]) || call.block.is_some() {
        return Err(fail(&call, format!("`{word}` needs an operation name like `ChatNode`")));
    }
    let allowed: &[&str] = if many { &["as"] } else { &["as", "variable", "fresh_for", "empty"] };
    if let Some(unknown) = call.options.iter().find(|pair| !allowed.contains(&pair.key.as_str())) {
        return Err(ParseError::new(unknown.line, unknown.column, format!("unknown key `{}` for `{word}`", unknown.key)));
    }
    let option = |key: &str| call.options.iter().find(|pair| pair.key == key);
    let name = match option("as").map(|pair| &pair.value.item) {
        Some(Value::Symbol(name)) => name.clone(),
        Some(_) => return Err(fail(&call, "`as` needs a symbol name")),
        None => word.to_string(),
    };
    let line = |word: &str, args: Vec<Node<Value>>, options: Vec<Pair>| Call { word: word.into(), line: call.line, column: call.column, args, options, block: None };
    let at = |item| Node { item, line: call.line, column: call.column };
    let mut body = vec![line("query", call.args.clone(), Vec::new())];
    let mut options = vec![pair(&call, "node", Value::Symbol(if many { "many" } else { "one" }.into()))];
    if many {
        body.push(line("list", Vec::new(), Vec::new()));
    } else {
        options.push(pair(&call, "text", Value::Array(vec![at(Value::Symbol(NODE_PARAM.into()))])));
        let variable = match option("variable").map(|pair| &pair.value.item) {
            Some(Value::Symbol(name)) => name.clone(),
            Some(_) => return Err(fail(&call, "`variable` needs a symbol name")),
            None => "id".to_string(),
        };
        body.push(line("variables", Vec::new(), vec![pair(&call, &variable, Value::Symbol(NODE_PARAM.into()))]));
        if let Some(fresh) = option("fresh_for") {
            body.push(line("fresh_for", vec![fresh.value.clone()], option("empty").cloned().into_iter().collect()));
        }
    }
    Ok(Call { word: "remote".into(), args: vec![at(Value::Symbol(name))], options, block: Some(body), ..call })
}

fn operation(mut call: Call) -> Result<Call, ParseError> {
    if !call.options.is_empty() {
        return Err(fail(&call, "declare operation settings inside the `do ... end` block"));
    }
    let name = match call.args.as_slice() {
        [Node { item: Value::Symbol(name), .. }] => name.clone(),
        _ => return Err(fail(&call, "operation needs a name")),
    };
    if !matches!(name.as_str(), "create" | "update" | "destroy") {
        call.word = "action".into();
        return Ok(call);
    }
    let Some(lines) = call.block.take() else { return Err(fail(&call, "operation needs a `do ... end` block")) };
    let mut seen = HashSet::new();
    for line in lines {
        if !seen.insert(line.word.clone()) { return Err(fail(&line, format!("`{}` is declared twice", line.word))); }
        let (key, value) = match line.word.as_str() {
            "mutation" => {
                positional_values(&line, 1..=1)?;
                ("with", line.args.first().map(|arg| arg.item.clone()))
            }
            "input" | "args" => {
                named_values(&line)?;
                (line.word.as_str(), Some(Value::Hash(line.options.clone())))
            }
            "local" | "refetch" | "correlate" => {
                positional_values(&line, 0..=usize::MAX)?;
                (line.word.as_str(), Some(Value::Array(line.args.clone())))
            }
            "on_failure" | "wrap" | "root" | "response" => {
                positional_values(&line, 1..=1)?;
                (line.word.as_str(), line.args.first().map(|arg| arg.item.clone()))
            }
            _ => return Err(fail(&line, format!("unknown line `{}` for `{name}`", line.word))),
        };
        let Some(value) = value else { return Err(fail(&line, "declaration needs a value")) };
        call.options.push(pair(&line, key, value));
    }
    call.word = name;
    call.args.clear();
    Ok(call)
}

pub(crate) fn prepare(nodes: &mut [ModelNode]) -> Result<(), Vec<(usize, ParseError)>> {
    let mut errors = Vec::new();
    for node in nodes {
        let mut body = Vec::new();
        let mut fields = Vec::new();
        let mut search = Vec::new();
        for call in std::mem::take(&mut node.body) {
            let result = match call.word.as_str() {
                "field" => { match field(call) { Ok(field) => fields.push(field), Err(error) => errors.push((node.file, error)) }; continue; }
                "index" => { fields.push(call); continue; }
                "ephemeral" => {
                    let mut call = call;
                    if let Some(children) = call.block.take() {
                        let mut converted = Vec::new();
                        for child in children {
                            match field(child) {
                                Ok(child) => converted.push(child),
                                Err(error) => errors.push((node.file, error)),
                            }
                        }
                        call.block = Some(converted);
                    }
                    fields.push(call);
                    continue;
                }
                "search" => {
                    match positional_values(&call, 1..=usize::MAX) {
                        Ok(()) => search.extend(call.args),
                        Err(error) => errors.push((node.file, error)),
                    }
                    continue;
                }
                "scope" => scope(call),
                "node" => node_read(call, false),
                "nodes" => node_read(call, true),
                "operation" => operation(call),
                "schema" | "query" | "remote" | "action" | "create" | "update" | "destroy" => Err(fail(&call, format!("unknown model declaration `{}`", call.word))),
                _ => Ok(call),
            };
            match result { Ok(call) => body.push(call), Err(error) => errors.push((node.file, error)) }
        }
        for name in search {
            if let Some(field) = fields.iter_mut().find(|field| field.args.first().is_some_and(|arg| arg.item == name.item)) {
                field.options.push(pair(field, "searchable", Value::Bool(true)));
            } else {
                errors.push((node.file, ParseError::new(name.line, name.column, "search needs a declared field")));
            }
        }
        if !fields.is_empty() {
            body.insert(0, Call { word: "schema".into(), line: node.line, column: node.column, args: Vec::new(), options: Vec::new(), block: Some(fields) });
        }
        node.body = body;
    }
    if errors.is_empty() { Ok(()) } else { Err(errors) }
}
