use super::*;

pub(super) fn optimistic(cx: &mut Cx, call: &Call) -> Option<OptimisticDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    if let [arg] = call.args.as_slice() {
        line.used.iter_mut().for_each(|used| *used = true);
        return match (&arg.item, call.options.is_empty()) {
            (Value::Bool(false), true) => Some(OptimisticDecl::Off),
            _ => {
                cx.fail(line.pos, "`optimistic` is `optimistic false` or `optimistic status: \"SENT\", flag: :flag`");
                None
            }
        };
    }
    line.no_args(cx);
    line.used.iter_mut().for_each(|used| *used = true);
    if call.options.is_empty() {
        cx.fail(line.pos, "`optimistic` needs `false` or attribute values");
        return None;
    }
    let mut fields = Vec::new();
    for pair in &call.options {
        let value = match &pair.value.item {
            Value::Symbol(name) => OptimisticValue::Parameter(name.clone()),
            Value::Hash(parts) => match if parts.iter().any(|part| part.key == "from") { mapped(cx, parts, &pair.value) } else if parts.iter().any(|part| matches!(part.key.as_str(), "include" | "at_least")) { flagged(cx, parts, &pair.value) } else { keyed(cx, parts, &pair.value) } {
                Some(value) => value,
                None => continue,
            },
            Value::Array(_) | Value::OptionalArray(_) | Value::Const(_) | Value::Duration { .. } => {
                cx.fail(at(&pair.value), "optimistic value must be a literal or a parameter symbol");
                continue;
            }
            other => OptimisticValue::Literal(json_of(other)),
        };
        fields.push((pair.key.clone(), value, pair_pos(pair)));
    }
    Some(OptimisticDecl::Fields(fields))
}

pub(super) fn copy_set(cx: &mut Cx, pair: &Pair) -> Vec<(String, CopySetDecl, Pos)> {
    let Some(pairs) = hash_pairs(cx, &pair.value, "`set`") else { return Vec::new() };
    let mut set = Vec::new();
    for entry in pairs {
        let pos = pair_pos(entry);
        let value = match &entry.value.item {
            Value::Hash(parts) => source_hash(cx, parts, &entry.value).map(CopySetDecl::Source),
            Value::Array(_) | Value::OptionalArray(_) | Value::Duration { .. } => {
                cx.fail(at(&entry.value), "`set` takes a literal, `{ owner: :field }` or `{ of: Model, field: :name }`");
                None
            }
            other => Some(CopySetDecl::Literal(json_of(other))),
        };
        if let Some(value) = value { set.push((entry.key.clone(), value, pos)); }
    }
    set
}

pub(super) fn source_hash(cx: &mut Cx, parts: &[Pair], node: &Node<Value>) -> Option<SourceDecl> {
    let find = |key: &str| parts.iter().find(|part| part.key == key);
    let name_of = |cx: &mut Cx, key: &str| find(key).and_then(|part| symbol(cx, &part.value, &format!("`{key}:`")));
    let single = |key: &str| parts.len() == 1 && find(key).is_some();
    if single("owner") {
        return name_of(cx, "owner").map(SourceDecl::Owner);
    }
    let row_shape = parts.iter().all(|part| matches!(part.key.as_str(), "of" | "row" | "field")) && find("of").is_some() && find("field").is_some();
    if !row_shape {
        cx.fail(at(node), "a source hash is `{ owner: :field }`, `{ of: Model, field: :name }` or `{ of: Model, row: :param, field: :name }`");
        return None;
    }
    let model = find("of").and_then(|part| konst(cx, &part.value, "`of:`"));
    let row = find("row").and_then(|part| symbol(cx, &part.value, "`row:`"));
    let field = name_of(cx, "field");
    Some(SourceDecl::Row { model: model?, row, field: field? })
}

pub(super) fn mapped(cx: &mut Cx, parts: &[Pair], node: &Node<Value>) -> Option<OptimisticValue> {
    let find = |key: &str| parts.iter().find(|part| part.key == key);
    let from = find("from").and_then(|part| match &part.value.item {
        Value::Symbol(name) => Some(name.clone()),
        _ => None,
    });
    let map = find("map").and_then(|part| match &part.value.item {
        Value::Hash(entries) => Some(entries),
        _ => None,
    });
    let (Some(from), Some(map), 2) = (from, map, parts.len()) else {
        cx.fail(at(node), "optimistic hash must be `{ from: :parameter, map: { VALUE: \"literal\", ... } }`");
        return None;
    };
    let mut entries = Vec::new();
    for entry in map {
        match &entry.value.item {
            Value::Array(_) | Value::Hash(_) | Value::Symbol(_) | Value::OptionalArray(_) | Value::Const(_) | Value::Duration { .. } => {
                cx.fail(at(&entry.value), "an optimistic map value must be a literal");
                return None;
            }
            other => entries.push((entry.key.clone(), json_of(other))),
        }
    }
    Some(OptimisticValue::Mapped { from, map: entries })
}

pub(super) fn flagged(cx: &mut Cx, parts: &[Pair], node: &Node<Value>) -> Option<OptimisticValue> {
    let find = |key: &str| parts.iter().find(|part| part.key == key);
    let when = find("when").and_then(|part| match &part.value.item {
        Value::Symbol(name) => Some(name.clone()),
        _ => None,
    });
    let (Some(when), 2) = (when, parts.len()) else {
        cx.fail(at(node), "optimistic hash is `{ include: { of: Model, field: :name }, when: :flag }` or `{ at_least: 1, when: :flag }`");
        return None;
    };
    match (find("include"), find("at_least")) {
        (Some(part), None) => match &part.value.item {
            Value::Hash(source) => match source_hash(cx, source, &part.value)? {
                source @ SourceDecl::Row { .. } => Some(OptimisticValue::Include { source, when }),
                _ => {
                    cx.fail(at(&part.value), "`include` takes `{ of: Model, field: :name }`");
                    None
                }
            },
            _ => {
                cx.fail(at(&part.value), "`include` takes `{ of: Model, field: :name }`");
                None
            }
        },
        (None, Some(part)) => match part.value.item {
            Value::Int(min) => Some(OptimisticValue::AtLeast { min, when }),
            _ => {
                cx.fail(at(&part.value), "`at_least` takes a whole number");
                None
            }
        },
        _ => {
            cx.fail(at(node), "optimistic hash is `{ include: { of: Model, field: :name }, when: :flag }` or `{ at_least: 1, when: :flag }`");
            None
        }
    }
}

pub(super) fn keyed(cx: &mut Cx, parts: &[Pair], node: &Node<Value>) -> Option<OptimisticValue> {
    let find = |key: &str| parts.iter().find(|part| part.key == key);
    let by = find("by").and_then(|part| match &part.value.item {
        Value::Symbol(name) => Some(name.clone()),
        _ => None,
    });
    let element = find("element").and_then(|part| match &part.value.item {
        Value::Hash(fields) => Some(fields),
        _ => None,
    });
    let valid = parts.len() == 2 && by.as_ref().is_some_and(|by| element.is_some_and(|fields| fields.iter().any(|field| &field.key == by)));
    if !valid {
        cx.fail(at(node), "optimistic hash must be `{ by: :key, element: { key: :parameter, ... } }` with the key among the element fields");
        return None;
    }
    let element = element?.iter().filter_map(|field| match &field.value.item {
        Value::Symbol(name) => Some((field.key.clone(), OptimisticElement::Parameter(name.clone()))),
        Value::Array(_) | Value::Hash(_) | Value::OptionalArray(_) | Value::Const(_) | Value::Duration { .. } => {
            cx.fail(at(&field.value), "an optimistic element value must be a literal or a parameter symbol");
            None
        }
        other => Some((field.key.clone(), OptimisticElement::Literal(json_of(other)))),
    }).collect();
    Some(OptimisticValue::Keyed { by: by?, element })
}

pub(super) fn batch(cx: &mut Cx, call: &Call) -> Option<BatchDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    line.no_args(cx);
    let items = line.opt(cx, "items").and_then(|pair| string(cx, &pair.value, "`items`"));
    let single = line.opt(cx, "single").and_then(|pair| symbol(cx, &pair.value, "`single`"));
    let input = line.opt(cx, "input").map(|pair| symbol_pairs(cx, Some(pair), "`input`")).unwrap_or_default();
    let renumber = line.opt(cx, "renumber").and_then(|pair| renumber(cx, pair));
    line.finish(cx);
    let items = require(cx, &line, "items", items);
    let single = require(cx, &line, "single", single);
    Some(BatchDecl { pos: line.pos, items: items?, single: single?, input, renumber })
}

pub(super) fn renumber(cx: &mut Cx, pair: &Pair) -> Option<RenumberDecl> {
    let pairs = hash_pairs(cx, &pair.value, "`renumber`")?;
    for unknown in pairs.iter().filter(|candidate| !matches!(candidate.key.as_str(), "position" | "size" | "start")) {
        cx.fail(pair_pos(unknown), format!("unknown key `{}` for `renumber`", unknown.key));
    }
    let find = |key: &str| pairs.iter().find(|candidate| candidate.key == key);
    let missing: Vec<&str> = ["position", "size", "start"].into_iter().filter(|key| find(key).is_none()).collect();
    if !missing.is_empty() {
        cx.fail(at(&pair.value), format!("`renumber` needs `position:`, `size:` and `start:`: missing {}", missing.join(", ")));
        return None;
    }
    let position = path(cx, &find("position")?.value, "renumber `position`");
    let size = path(cx, &find("size")?.value, "renumber `size`");
    let start = match find("start")?.value.item {
        Value::Int(start @ (0 | 1)) => Some(start as usize),
        _ => {
            cx.fail(at(&find("start")?.value), "renumber `start` must be 0 or 1");
            None
        }
    };
    Some(RenumberDecl { position: position?, size: size?, start: start?, start_pos: at(&find("start")?.value) })
}

pub(super) fn validates(cx: &mut Cx, call: &Call) -> Option<LengthDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let attribute = match call.args.as_slice() {
        [node] => symbol(cx, node, "validated name")?,
        _ => {
            cx.fail(line.pos, "`validates` needs a name and `length:` like `validates :items, length: { minimum: 2, maximum: 10 }`");
            return None;
        }
    };
    let hash = line.opt(cx, "length").and_then(|pair| hash_pairs(cx, &pair.value, "`length`").map(|pairs| (pairs, pair)));
    line.finish(cx);
    let (pairs, pair) = require(cx, &line, "length", hash)?;
    for unknown in pairs.iter().filter(|candidate| !matches!(candidate.key.as_str(), "minimum" | "maximum")) {
        cx.fail(pair_pos(unknown), format!("unknown key `{}` for `length`", unknown.key));
    }
    let number = |cx: &mut Cx, key: &str| match pairs.iter().find(|candidate| candidate.key == key).map(|found| &found.value) {
        Some(Node { item: Value::Int(count), .. }) if *count >= 0 => Some(*count as usize),
        Some(node) => {
            cx.fail(at(node), format!("`{key}` must be a number"));
            None
        }
        None => {
            cx.fail(at(&pair.value), format!("`length` needs `{key}:`"));
            None
        }
    };
    let minimum = number(cx, "minimum");
    let maximum = number(cx, "maximum");
    Some(LengthDecl { pos: line.pos, attribute, minimum: minimum?, maximum: maximum? })
}

pub(super) fn returns(cx: &mut Cx, call: &Call) -> Option<Pos> {
    let mut line = Line::of(call);
    line.no_args(cx);
    line.no_options(cx);
    if call.block.is_some() {
        cx.fail(line.pos, "`returns` takes no block: the answer type comes from the document");
        return None;
    }
    Some(line.pos)
}

pub(super) fn operation(cx: &mut Cx, call: &Call) -> Option<OperationDecl> {
    let mut line = Line::of(call);
    let name = match call.args.as_slice() {
        [node] => lowercase_symbol(cx, node, "operation name")?,
        _ => {
            cx.fail(line.pos, "operation needs a name like `operation :publish do`");
            return None;
        }
    };
    let kind = match name.as_str() {
        "create" => OperationKind::Create,
        "update" => OperationKind::Update,
        "destroy" => OperationKind::Destroy,
        _ => OperationKind::Custom,
    };
    let upload = line.opt(cx, "upload").and_then(|pair| match boolean(cx, &pair.value, "`upload`") {
        Some(true) => Some(pair_pos(pair)),
        _ => {
            cx.fail(at(&pair.value), "`upload` is only `true`: omit it for the default");
            None
        }
    });
    if upload.is_some() && kind != OperationKind::Custom {
        cx.fail(line.pos, format!("`{name}` is a standard operation: `upload: true` belongs to a named operation"));
    }
    let expires_after = line.opt(cx, "expires_after").and_then(|pair| {
        let text = duration(cx, &pair.value, "`expires_after`")?;
        let seconds = dblayer::runtime::parse_duration(&text)?.as_secs();
        if seconds == 0 {
            cx.fail(pair_pos(pair), "`expires_after` is a duration of 1 second or more");
        }
        Some(seconds)
    });
    line.finish(cx);
    let lines = block(cx, call)?;
    let mut decl = OperationDecl {
        pos: line.pos, name: name.clone(), kind, upload, expires_after, dimensions: None, mutation: None, parameters: Vec::new(), variables: None, local: None, local_from: Vec::new(), local_sites: Vec::new(), wrap: None, response: None, match_by: None,
        on_failure: None, optimistic: None, upserts: Vec::new(), refetch: Vec::new(), returns: None, answer: None, batch: None, length: None,
        removes_receiver: None, copies_receiver_to: None, scope_effects: Vec::new(),
    };
    let mut seen: Vec<&str> = Vec::new();
    for item in lines {
        let word = item.word.as_str();
        let allowed = matches!(
            (kind, word),
            (_, "mutation" | "variables" | "response")
                | (OperationKind::Create | OperationKind::Update | OperationKind::Custom, "parameter")
                | (OperationKind::Create | OperationKind::Update, "local" | "refetch" | "match_by" | "on_failure")
                | (OperationKind::Custom, "match_by" | "on_failure" | "optimistic" | "upsert" | "refetch" | "returns" | "batch" | "validates")
                | (OperationKind::Custom, "dimensions" | "removes_receiver" | "copies_receiver_to" | "joins_scope" | "leaves_scope")
        );
        if !allowed && !OPERATION_WORDS.contains(&word) {
            cx.fail(Pos::of(item), format!("unknown word `{word}` in an operation"));
            continue;
        }
        if !allowed {
            cx.fail(Pos::of(item), format!("`{word}` does not apply to operation `{name}`"));
            continue;
        }
        if !matches!(word, "parameter" | "local" | "upsert" | "refetch" | "joins_scope" | "leaves_scope") && seen.contains(&word) {
            cx.fail(Pos::of(item), format!("`{word}` is declared twice"));
            continue;
        }
        seen.push(word);
        operation_line(cx, item, &mut decl);
    }
    if decl.kind == OperationKind::Destroy {
        if let Some(ResponseDecl::Ignore) = &decl.response {
            cx.fail(decl.pos, "`destroy` reads its response with `response from:`");
        }
    }
    Some(decl)
}

const OPERATION_WORDS: [&str; 18] = [
    "mutation", "variables", "response", "parameter", "local", "refetch", "match_by", "on_failure", "optimistic", "upsert", "returns", "batch", "validates",
    "dimensions", "removes_receiver", "copies_receiver_to", "joins_scope", "leaves_scope",
];

pub(super) fn operation_line(cx: &mut Cx, call: &Call, decl: &mut OperationDecl) {
    let mut line = Line::of(call);
    match call.word.as_str() {
        "mutation" => {
            no_block(cx, call);
            line.no_options(cx);
            if let Some(arg) = exactly_one_arg(cx, &line, "an operation name like `mutation \"RecordCreate\"`") {
                decl.mutation = string(cx, arg, "the operation name").map(|text| Spanned::new(text, line.pos));
            }
        }
        "parameter" => decl.parameters.extend(parameter(cx, call)),
        "variables" => {
            decl.variables = variables(cx, call, false);
            for binding in decl.variables.iter().flatten() {
                if matches!(&binding.value, VariableValue::Parameter(source) if *source == binding.name) {
                    cx.fail(binding.pos, format!("variables `{0}: :{0}` repeats the name; omit it", binding.name));
                }
            }
        }
        "local" => {
            no_block(cx, call);
            let from = line.opt(cx, "from").and_then(|pair| match &pair.value.item {
                Value::Hash(parts) => match source_hash(cx, parts, &pair.value) {
                    Some(source @ SourceDecl::Row { row: None, .. }) => Some(source),
                    Some(_) => {
                        cx.fail(at(&pair.value), "`local` takes `from: { of: Model, field: :name }` of a singleton model");
                        None
                    }
                    None => None,
                },
                _ => {
                    cx.fail(at(&pair.value), "`local` takes `from: { of: Model, field: :name }` of a singleton model");
                    None
                }
            });
            line.finish(cx);
            let names: Vec<String> = call.args.iter().filter_map(|arg| symbol(cx, arg, "`local` name")).collect();
            if call.args.is_empty() {
                cx.fail(line.pos, "`local` needs 1 or more attribute names");
            }
            if let Some(source) = from {
                if decl.kind == OperationKind::Update {
                    cx.fail(line.pos, "`local from:` is supported only in `create`");
                }
                decl.local_from.extend(names.iter().map(|name| (name.clone(), source.clone(), line.pos)));
            }
            for name in &names {
                if decl.local.as_ref().is_some_and(|local| local.contains(name)) || names.iter().filter(|other| *other == name).count() > 1 {
                    let first = decl.local_sites.iter().find(|(field, _)| field == name).map_or(line.pos, |(_, pos)| *pos);
                    cx.fail(line.pos, format!("local attribute `{name}` is declared twice, first declaration at {}:{}", first.line, first.column));
                }
            }
            decl.local_sites.extend(names.iter().map(|name| (name.clone(), line.pos)));
            decl.local.get_or_insert_with(Vec::new).extend(names);
        }
        "dimensions" => {
            no_block(cx, call);
            line.no_args(cx);
            let width = line.opt(cx, "width").and_then(|pair| symbol(cx, &pair.value, "`width`"));
            let height = line.opt(cx, "height").and_then(|pair| symbol(cx, &pair.value, "`height`"));
            line.finish(cx);
            match (width, height) {
                (Some(width), Some(height)) => decl.dimensions = Some(DimensionsDecl { pos: line.pos, width, height }),
                _ => cx.fail(line.pos, "`dimensions` needs `width: :attribute, height: :attribute`"),
            }
        }
        "response" => decl.response = response(cx, call),
        "match_by" => {
            no_block(cx, call);
            line.no_options(cx);
            if call.args.is_empty() {
                cx.fail(line.pos, "`match_by` needs 1 or more paths like `match_by \"clientKey\"`");
            }
            decl.match_by = Some(call.args.iter().filter_map(|arg| path(cx, arg, "a `match_by` path")).collect());
        }
        "on_failure" => {
            no_block(cx, call);
            line.no_options(cx);
            if let Some(arg) = exactly_one_arg(cx, &line, "`:keep` or `:rollback`") {
                decl.on_failure = match &arg.item {
                    Value::Symbol(policy) if policy == "keep" => Some(Spanned::new(OnFailure::Keep, line.pos)),
                    Value::Symbol(policy) if policy == "rollback" => Some(Spanned::new(OnFailure::Rollback, line.pos)),
                    _ => {
                        cx.fail(line.pos, "`on_failure` must be `:keep` or `:rollback`");
                        None
                    }
                };
            }
        }
        "optimistic" => decl.optimistic = optimistic(cx, call).map(|found| (found, line.pos)),
        "upsert" => decl.upserts.extend(upsert(cx, call)),
        "refetch" => decl.refetch.extend(refetch(cx, call)),
        "removes_receiver" => {
            no_block(cx, call);
            line.no_args(cx);
            line.no_options(cx);
            decl.removes_receiver = Some(line.pos);
        }
        "copies_receiver_to" => {
            no_block(cx, call);
            let arg = match call.args.as_slice() {
                [node] => symbol(cx, node, "the argument that names the target"),
                _ => {
                    cx.fail(line.pos, "`copies_receiver_to` needs an argument like `copies_receiver_to :targetChatId, field: :chatId`");
                    None
                }
            };
            let field = line.opt(cx, "field").and_then(|pair| symbol(cx, &pair.value, "`field`"));
            let set = line.opt(cx, "set").map(|pair| copy_set(cx, pair)).unwrap_or_default();
            line.finish(cx);
            let field = require(cx, &line, "field", field);
            if let (Some(arg), Some(field)) = (arg, field) {
                decl.copies_receiver_to = Some(ReceiverCopyDecl { pos: line.pos, arg, field, set });
            }
        }
        "joins_scope" | "leaves_scope" => {
            no_block(cx, call);
            let scope = match call.args.as_slice() {
                [node] => symbol(cx, node, "the scope"),
                _ => {
                    cx.fail(line.pos, format!("`{}` needs a scope like `{} :group_members, model: User, rows: :userIds, with: {{ groupId: :id }}, order: :order, orders: [\"JOINED\", \"OWNER_FIRST\"]`", call.word, call.word));
                    None
                }
            };
            let model = line.opt(cx, "model").and_then(|pair| konst(cx, &pair.value, "`model`"));
            let rows = line.opt(cx, "rows").and_then(|pair| symbol(cx, &pair.value, "`rows`"));
            let with_pair = line.opt(cx, "with");
            let with = symbol_pairs(cx, with_pair, "`with`");
            let order = line.opt(cx, "order").and_then(|pair| symbol(cx, &pair.value, "`order`"));
            let orders: Vec<String> = match line.opt(cx, "orders") {
                Some(pair) => match &pair.value.item {
                    Value::Array(items) => items.iter().filter_map(|item| string(cx, item, "an order")).collect(),
                    _ => {
                        cx.fail(at(&pair.value), "`orders` must be a list like `[\"JOINED\"]`");
                        Vec::new()
                    }
                },
                None => Vec::new(),
            };
            line.finish(cx);
            let (model, rows, order) = (require(cx, &line, "model", model), require(cx, &line, "rows", rows), require(cx, &line, "order", order));
            if orders.is_empty() {
                cx.fail(line.pos, format!("`{}` needs `orders:`", call.word));
            }
            if let (Some(scope), Some(model), Some(rows), Some(order)) = (scope, model, rows, order) {
                decl.scope_effects.push(ScopeEffectDecl { pos: line.pos, joins: call.word == "joins_scope", scope, model, rows, with, order, orders });
            }
        }
        "returns" => decl.returns = returns(cx, call),
        "batch" => decl.batch = batch(cx, call),
        "validates" => decl.length = validates(cx, call),
        _ => {}
    }
}


/// `upsert Model, from: "path"`: the rows of the answer under the path land in the table of the model (a model operation and a standalone operation read it the same way).
pub(super) fn refetch(cx: &mut Cx, call: &Call) -> Option<RefetchDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let scope = match call.args.as_slice() {
        [node] => symbol(cx, node, "the refetched scope"),
        _ => {
            cx.fail(line.pos, "`refetch` needs a scope like `refetch :thread`");
            None
        }
    };
    let model = line.opt(cx, "model").and_then(|pair| konst(cx, &pair.value, "`model`"));
    line.finish(cx);
    scope.map(|scope| RefetchDecl { pos: line.pos, scope, model })
}

pub(super) fn upsert(cx: &mut Cx, call: &Call) -> Option<UpsertDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let model = match call.args.as_slice() {
        [node] => konst(cx, node, "the upserted model"),
        _ => {
            cx.fail(line.pos, "`upsert` needs a model like `upsert Container, from: \"path\"`");
            None
        }
    };
    let from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`"));
    line.finish(cx);
    let from = require(cx, &line, "from", from);
    Some(UpsertDecl { pos: line.pos, model: model?, from: from? })
}
