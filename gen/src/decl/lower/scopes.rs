use super::*;

pub(super) fn association(cx: &mut Cx, call: &Call, kind: AssociationKind) -> Option<AssociationDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let name = match call.args.as_slice() {
        [node] => lowercase_symbol(cx, node, "association name")?,
        _ => {
            cx.fail(line.pos, format!("`{}` needs a name like `:owner`", call.word));
            return None;
        }
    };
    let mut decl = AssociationDecl { pos: line.pos, kind, name, class_name: None, foreign_key: None, touch: false, sets: Vec::new(), newer: Vec::new(), newer_pos: None, from: None, dependent: false };
    decl.class_name = line.opt(cx, "class_name").and_then(|pair| konst(cx, &pair.value, "`class_name`"));
    decl.foreign_key = line.opt(cx, "foreign_key").and_then(|pair| symbol(cx, &pair.value, "`foreign_key`"));
    decl.from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`")).map(|found| found.item);
    if kind == AssociationKind::BelongsTo {
        decl.touch = line.opt(cx, "touch").and_then(|pair| boolean(cx, &pair.value, "`touch`")).unwrap_or(false);
        let sets = line.opt(cx, "sets");
        decl.sets = symbol_pairs(cx, sets, "`sets`");
        if let Some(pair) = line.opt(cx, "newer") {
            decl.newer_pos = Some(pair_pos(pair));
            decl.newer = symbol_list(cx, &pair.value, "`newer`");
        }
    }
    if matches!(kind, AssociationKind::HasMany | AssociationKind::HasOne) {
        if let Some(pair) = line.opt(cx, "dependent") {
            if matches!(&pair.value.item, Value::Symbol(name) if name == "destroy") {
                decl.dependent = true;
            } else {
                cx.fail(at(&pair.value), "`dependent` must be `:destroy`");
            }
        }
    }
    line.finish(cx);
    Some(decl)
}

fn symbol_list(cx: &mut Cx, node: &Node<Value>, what: &str) -> Vec<String> {
    match &node.item {
        Value::Array(items) => items.iter().filter_map(|item| symbol(cx, item, what)).collect(),
        _ => {
            cx.fail(at(node), format!("{what} must be a list of child fields like `[:createdAt]`"));
            Vec::new()
        }
    }
}

pub(super) fn symbol_pairs(cx: &mut Cx, pair: Option<&Pair>, what: &str) -> Vec<(String, String)> {
    let Some(pair) = pair else { return Vec::new() };
    let Some(pairs) = hash_pairs(cx, &pair.value, what) else { return Vec::new() };
    pairs.iter().filter_map(|inner| symbol(cx, &inner.value, what).map(|value| (inner.key.clone(), value))).collect()
}


pub(super) fn parameter(cx: &mut Cx, call: &Call) -> Option<ParameterDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let (name, ty) = match call.args.as_slice() {
        [name, ty] => (lowercase_symbol(cx, name, "parameter name")?, ty),
        _ => {
            cx.fail(line.pos, "`parameter` needs a name and a type like `parameter :limit, :int`");
            return None;
        }
    };
    let Some(mut parsed) = ParamType::from_value(&ty.item) else {
        cx.fail(at(ty), params::PARAM_TYPES);
        return None;
    };
    let null = line.opt(cx, "null").and_then(|pair| boolean(cx, &pair.value, "`null`")).unwrap_or(false);
    if let Some(pair) = line.opt(cx, "null_items") {
        match boolean(cx, &pair.value, "`null_items`") {
            Some(true) if parsed.list && matches!(parsed.base, ParamBase::Named(_)) => parsed.null_items = true,
            Some(true) => cx.fail(pair_pos(pair), "`null_items` applies to a list of schema inputs or enums like [WidgetInput]"),
            _ => {}
        }
    }
    let default = line.opt(cx, "default").map(|pair| Spanned::new(json_of(&pair.value.item), at(&pair.value)));
    line.finish(cx);
    Some(ParameterDecl { pos: line.pos, name, ty: parsed, null, default })
}

pub(super) fn variables(cx: &mut Cx, call: &Call, constants: bool) -> Option<Vec<VariableBinding>> {
    let mut line = Line::of(call);
    no_block(cx, call);
    line.no_args(cx);
    line.used.iter_mut().for_each(|used| *used = true);
    if call.options.is_empty() {
        cx.fail(line.pos, "`variables` needs bindings like `variables id: :id`");
        return None;
    }
    let mut out: Vec<VariableBinding> = Vec::new();
    for pair in &call.options {
        if out.iter().any(|binding| binding.name == pair.key) {
            cx.fail(pair_pos(pair), format!("variable `{}` is bound twice", pair.key));
            continue;
        }
        let value = match &pair.value.item {
            Value::Symbol(name) => VariableValue::Parameter(name.clone()),
            Value::Const(_) | Value::Duration { .. } | Value::OptionalArray(_) => {
                cx.fail(at(&pair.value), "a variable is bound to a parameter symbol like `:id`" .to_string() + if constants { " or a literal" } else { "" });
                continue;
            }
            _ if !constants => {
                cx.fail(at(&pair.value), "a variable is bound to an attribute or parameter symbol like `:id`");
                continue;
            }
            other => VariableValue::Constant(json_of(other)),
        };
        out.push(VariableBinding { pos: pair_pos(pair), name: pair.key.clone(), value });
    }
    Some(out)
}

pub(super) fn response(cx: &mut Cx, call: &Call) -> Option<ResponseDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    if let [arg] = call.args.as_slice() {
        line.used.iter_mut().for_each(|used| *used = true);
        if matches!(&arg.item, Value::Symbol(name) if name == "ignore") && call.options.is_empty() {
            return Some(ResponseDecl::Ignore);
        }
        cx.fail(line.pos, "`response` is `response :ignore` or `response from: \"path\", shape: :list`");
        return None;
    }
    line.no_args(cx);
    let from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`"));
    let shape = line.opt(cx, "shape").and_then(|pair| match &pair.value.item {
        Value::Symbol(name) if name == "one" => Some(ResponseShape::One),
        Value::Symbol(name) if name == "list" => Some(ResponseShape::List),
        _ => {
            cx.fail(at(&pair.value), "`shape` must be `:one` or `:list`");
            None
        }
    });
    line.finish(cx);
    if call.options.is_empty() {
        cx.fail(line.pos, "`response` needs `from:` or `shape:`");
    }
    Some(ResponseDecl::Read(ResponseRead { pos: line.pos, from, shape }))
}


pub(super) fn insert_on(cx: &mut Cx, pair: &Pair) -> Option<Spanned<Vec<InsertOn>>> {
    let Value::Array(items) = &pair.value.item else {
        cx.fail(at(&pair.value), "`insert_on` must be a list like `[:event, :response]`");
        return None;
    };
    let mut found: Vec<InsertOn> = Vec::new();
    for item in items {
        let source = match &item.item {
            Value::Symbol(name) if name == "create" => InsertOn::Create,
            Value::Symbol(name) if name == "event" => InsertOn::Event,
            Value::Symbol(name) if name == "response" => InsertOn::Response,
            Value::Symbol(name) if name == "node" => InsertOn::Node,
            _ => {
                cx.fail(at(item), "`insert_on` takes `:create`, `:event` and `:response`");
                continue;
            }
        };
        if found.contains(&source) {
            cx.fail(at(item), "`insert_on` names a source twice");
        } else {
            found.push(source);
        }
    }
    if found.is_empty() {
        cx.fail(at(&pair.value), "`insert_on` needs at least 1 source");
        return None;
    }
    Some(Spanned::new(found, at(&pair.value)))
}

pub(super) fn scope(cx: &mut Cx, call: &Call) -> Option<ScopeDecl> {
    let mut line = Line::of(call);
    let name = match call.args.as_slice() {
        [node] => lowercase_symbol(cx, node, "scope name")?,
        _ => {
            cx.fail(line.pos, "`scope` needs a name like `scope :recent do`");
            return None;
        }
    };
    let node = line.opt(cx, "node").and_then(|pair| match &pair.value.item {
        Value::Symbol(kind) if kind == "one" => Some(NodeRead::One),
        Value::Symbol(kind) if kind == "many" => Some(NodeRead::Many),
        _ => {
            cx.fail(at(&pair.value), "`node` must be `:one` or `:many`");
            None
        }
    });
    let insert = line.opt(cx, "insert_on").and_then(|pair| insert_on(cx, pair));
    let stable = line.opt(cx, "stable_order").and_then(|pair| boolean(cx, &pair.value, "`stable_order`").filter(|flag| *flag).map(|_| pair_pos(pair)));
    let foreground = line.opt(cx, "refresh_on_foreground").and_then(|pair| boolean(cx, &pair.value, "`refresh_on_foreground`").filter(|flag| *flag).map(|_| pair_pos(pair)));
    let keep_on_gap = line.opt(cx, "keep_on_gap").and_then(|pair| boolean(cx, &pair.value, "`keep_on_gap`").filter(|flag| *flag).map(|_| pair_pos(pair)));
    let members_complete = line.opt(cx, "members_complete").and_then(|pair| boolean(cx, &pair.value, "`members_complete`")).unwrap_or(false);
    line.finish(cx);
    let lines = block(cx, call)?;
    let mut decl = ScopeDecl {
        pos: line.pos, name, node, insert_on: insert, stable_order: stable, refresh_on_foreground: foreground, keep_on_gap, members_complete, parameters: Vec::new(), where_: None, membership: Vec::new(), row_rules: Vec::new(), order: None, query: None, variables: None,
        response: None, paginate: None, freshness: None, preload: None, marker: None, tail: None, total: None, edges: Vec::new(), subscriptions: Vec::new(), has_block: true,
    };
    let remote = lines.iter().any(|candidate| candidate.word == "query");
    let mut seen: Vec<&str> = Vec::new();
    for item in lines {
        let once = !matches!(item.word.as_str(), "parameter" | "subscribe");
        if once && seen.contains(&item.word.as_str()) {
            cx.fail(Pos::of(item), format!("`{}` is declared twice", item.word));
            continue;
        }
        seen.push(&item.word);
        if !remote && !matches!(item.word.as_str(), "parameter" | "where" | "order" | "subscribe") {
            cx.fail(Pos::of(item), format!("`{}` needs a scope with `query`", item.word));
            continue;
        }
        scope_line(cx, item, &mut decl);
    }
    if !remote && !decl.subscriptions.is_empty() && !cx.memory {
        cx.fail(decl.subscriptions[0].pos, "`subscribe` in a scope without `query` requires memory storage");
    }
    Some(decl)
}

pub(super) fn scope_line(cx: &mut Cx, call: &Call, decl: &mut ScopeDecl) {
    let mut line = Line::of(call);
    match call.word.as_str() {
        "parameter" => decl.parameters.extend(parameter(cx, call)),
        "membership" => {
            no_block(cx, call);
            line.no_args(cx);
            line.used.iter_mut().for_each(|used| *used = true);
            if call.options.is_empty() { cx.fail(line.pos, "`membership` needs parameter rules"); }
            for pair in &call.options {
                let Some(rules) = hash_pairs(cx, &pair.value, "a membership rule") else { continue };
                if rules.len() != 1 || !matches!(rules[0].key.as_str(), "contains" | "equals") {
                    cx.fail(pair_pos(pair), "membership needs exactly one `contains` or `equals` field");
                    continue;
                }
                let Some(field) = symbol(cx, &rules[0].value, "a membership field") else { continue };
                let rule = if rules[0].key == "contains" { dblayer::schema::Membership::Contains(field) } else { dblayer::schema::Membership::Equals(field) };
                decl.membership.push((pair.key.clone(), rule, pair_pos(pair)));
            }
        }
        "matches" => {
            no_block(cx, call);
            line.no_args(cx);
            line.used.iter_mut().for_each(|used| *used = true);
            if call.options.is_empty() { cx.fail(line.pos, "`matches` needs row rules like `matches kind: { equals: \"GIFT\" }`"); }
            for pair in &call.options {
                let Some(rules) = hash_pairs(cx, &pair.value, "a row rule") else { continue };
                if rules.len() != 1 || !matches!(rules[0].key.as_str(), "in" | "equals" | "greater_than") {
                    cx.fail(pair_pos(pair), "`matches` needs exactly one `in`, `equals` or `greater_than` test per field");
                    continue;
                }
                let value = &rules[0].value;
                let test = match (rules[0].key.as_str(), &value.item) {
                    ("greater_than", _) => symbol(cx, value, "a `greater_than` parameter").map(|parameter| dblayer::schema::RowTest::GreaterThan { parameter }),
                    ("in", Value::Array(items)) if !items.is_empty() && items.iter().all(|item| matches!(item.item, Value::Str(_) | Value::Int(_) | Value::Bool(_))) => {
                        Some(dblayer::schema::RowTest::In { values: items.iter().map(|item| json_of(&item.item)).collect() })
                    }
                    ("equals", Value::Str(_) | Value::Int(_) | Value::Bool(_)) => Some(dblayer::schema::RowTest::Equals { value: json_of(&value.item) }),
                    ("in", _) => {
                        cx.fail(at(value), "`in` needs a non-empty array of strings, numbers or booleans");
                        None
                    }
                    _ => {
                        cx.fail(at(value), "`equals` needs a string, a number or a boolean");
                        None
                    }
                };
                if let Some(test) = test {
                    decl.row_rules.push((dblayer::schema::RowRule { field: pair.key.clone(), test }, pair_pos(pair)));
                }
            }
        }
        "where" => {
            no_block(cx, call);
            line.no_args(cx);
            line.used.iter_mut().for_each(|used| *used = true);
            if call.options.is_empty() {
                cx.fail(line.pos, "`where` needs pairs like `where chatId: :chatId`");
            }
            let mut pairs = Vec::new();
            for pair in &call.options {
                if let Some(parameter) = symbol(cx, &pair.value, "`where` value") {
                    pairs.push((Spanned::new(pair.key.clone(), pair_pos(pair)), Spanned::new(parameter, at(&pair.value))));
                }
            }
            decl.where_ = Some(WhereDecl { pos: line.pos, pairs });
        }
        "order" => {
            no_block(cx, call);
            line.no_args(cx);
            line.used.iter_mut().for_each(|used| *used = true);
            if call.options.is_empty() {
                cx.fail(line.pos, "`order` needs pairs like `order createdAt: :asc`");
            }
            let fields = call.options.iter().filter_map(|pair| sort_direction(cx, &pair.value).map(|direction| (pair.key.clone(), direction, pair_pos(pair)))).collect();
            decl.order = Some(OrderDecl { pos: line.pos, fields });
        }
        "query" => {
            no_block(cx, call);
            line.no_options(cx);
            if let Some(arg) = exactly_one_arg(cx, &line, "an operation name like `query \"Records\"`") {
                if let Some(operation) = string(cx, arg, "the operation name") {
                    decl.query = Some(QueryDecl { pos: line.pos, operation });
                }
            }
        }
        "variables" => decl.variables = variables(cx, call, true),
        "response" => match response(cx, call) {
            Some(ResponseDecl::Read(read)) => decl.response = Some(read),
            Some(ResponseDecl::Ignore) => cx.fail(line.pos, "a scope reads its response: `response from: \"path\", shape: :list`"),
            None => {}
        },
        "paginate" => decl.paginate = paginate(cx, call),
        "freshness" => {
            no_block(cx, call);
            line.no_args(cx);
            let expires = line.opt(cx, "expires_in").and_then(|pair| duration(cx, &pair.value, "`expires_in`").map(|text| Spanned::new(text, at(&pair.value))));
            let empty = line.opt(cx, "empty_in").and_then(|pair| duration(cx, &pair.value, "`empty_in`").map(|text| Spanned::new(text, at(&pair.value))));
            line.finish(cx);
            if let Some(expires_in) = require(cx, &line, "expires_in", expires) {
                decl.freshness = Some(FreshnessDecl { pos: line.pos, expires_in, empty_in: empty });
            }
        }
        "marker" => {
            no_block(cx, call);
            let operation = exactly_one_arg(cx, &line, "an operation name").and_then(|arg| string(cx, arg, "the marker operation"));
            let from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`"));
            let sequence = line.opt(cx, "sequence").and_then(|pair| symbol(cx, &pair.value, "the sequence field").map(|value| Spanned::new(value, at(&pair.value))));
            let mut fields = Vec::new();
            if let Some(pair) = line.opt(cx, "fields") {
                if let Value::Array(values) = &pair.value.item {
                    for value in values {
                        if let Some(name) = symbol(cx, value, "a marker field") { fields.push(Spanned::new(name, at(value))); }
                    }
                } else { cx.fail(at(&pair.value), "marker fields must be an array of symbols"); }
            }
            if fields.is_empty() { cx.fail(line.pos, "marker needs numeric fields including revision"); }
            let from = require(cx, &line, "from", from);
            let sequence = require(cx, &line, "sequence", sequence);
            line.finish(cx);
            if let (Some(operation), Some(from), Some(sequence)) = (operation, from, sequence) { decl.marker = Some(MarkerDecl { pos: line.pos, operation, from, fields, sequence }); }
        }
        "tail" => {
            no_block(cx, call);
            let operation = exactly_one_arg(cx, &line, "an operation name").and_then(|arg| string(cx, arg, "the tail operation"));
            let after = line.opt(cx, "after").and_then(|pair| symbol(cx, &pair.value, "the tail variable").map(|value| Spanned::new(value, at(&pair.value))));
            let after = require(cx, &line, "after", after);
            line.finish(cx);
            if let (Some(operation), Some(after)) = (operation, after) { decl.tail = Some(TailDecl { pos: line.pos, operation, after }); }
        }
        "preload" => {
            no_block(cx, call);
            line.no_args(cx);
            let limit = line.opt(cx, "limit").and_then(|pair| match &pair.value.item {
                Value::Int(number) if *number > 0 && usize::try_from(*number).is_ok() => Some(*number as usize),
                _ => { cx.fail(at(&pair.value), "`preload limit` must be a positive integer"); None }
            });
            line.used.iter_mut().for_each(|used| *used = true);
            let values = call.options.iter().filter(|pair| pair.key != "limit").map(|pair| {
                let choice = match &pair.value.item {
                    Value::Array(nodes) => PreloadChoice::Listed(nodes.iter().filter_map(|node| match &node.item {
                        Value::Str(text) => Some(Spanned::new(Json::String(text.clone()), at(node))),
                        Value::Int(number) => Some(Spanned::new(Json::from(*number), at(node))),
                        Value::Bool(flag) => Some(Spanned::new(Json::Bool(*flag), at(node))),
                        _ => {
                            cx.fail(at(node), "a `preload` value is a string, a number or true or false");
                            None
                        }
                    }).collect()),
                    Value::Hash(parts) => match parts.as_slice() {
                        [part] if part.key == "from" => match &part.value.item {
                            Value::Str(text) if text.contains('.') => PreloadChoice::RowsOf(Spanned::new(text.clone(), at(&part.value))),
                            Value::Str(_) => {
                                cx.fail(at(&part.value), "`preload rows of Model` names its source scope like `\"Vibe.list\"`");
                                PreloadChoice::Listed(Vec::new())
                            }
                            _ => {
                                cx.fail(at(&part.value), "`preload ...: { from: }` takes a string like `\"Vibe.list\"`");
                                PreloadChoice::Listed(Vec::new())
                            }
                        },
                        _ => {
                            cx.fail(at(&pair.value), "`preload` takes a list of values or `{ from: \"Model.scope\" }`");
                            PreloadChoice::Listed(Vec::new())
                        }
                    },
                    _ => {
                        cx.fail(at(&pair.value), "`preload` takes a list of values like `preload list: [\"primary\", \"secondary\"]` or `{ from: \"Model.scope\" }`");
                        PreloadChoice::Listed(Vec::new())
                    }
                };
                (Spanned::new(pair.key.clone(), pair_pos(pair)), choice)
            }).collect();
            decl.preload = Some(PreloadDecl { pos: line.pos, limit, values });
        }
        "total" => {
            no_block(cx, call);
            line.no_args(cx);
            let from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`"));
            line.finish(cx);
            decl.total = require(cx, &line, "from", from);
        }
        "edge" => {
            line.no_args(cx);
            line.no_options(cx);
            if let Some(children) = block(cx, call) {
                if children.is_empty() {
                    cx.fail(line.pos, "`edge` block needs at least 1 attribute");
                }
                let owner = Owner(None);
                decl.edges = children.iter().filter_map(|child| attribute(cx, child, Site::Edge, &owner)).collect();
            }
        }
        "subscribe" => decl.subscriptions.extend(subscribe(cx, call)),
        other => cx.fail(line.pos, format!("unknown word `{other}` in a scope")),
    }
}

pub(super) fn paginate(cx: &mut Cx, call: &Call) -> Option<PaginateDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    line.no_args(cx);
    let size = line.opt(cx, "size").and_then(|pair| match pair.value.item {
        Value::Int(size) if size > 0 && u32::try_from(size).is_ok() => Some(size as u32),
        _ => {
            cx.fail(at(&pair.value), "`size` must be a positive number");
            None
        }
    });
    let direction = line.opt(cx, "direction").and_then(|pair| match &pair.value.item {
        Value::Symbol(name) if name == "forward" => Some(PageDirectionDecl::Forward),
        Value::Symbol(name) if name == "backward" => Some(PageDirectionDecl::Backward),
        _ => {
            cx.fail(at(&pair.value), "`direction` must be `:forward` or `:backward`");
            None
        }
    });
    let max_pages = line.opt(cx, "max_pages").and_then(|pair| match pair.value.item {
        Value::Int(count) if count > 0 && u32::try_from(count).is_ok() => Some(Spanned::new(count as u32, at(&pair.value))),
        _ => {
            cx.fail(at(&pair.value), "`max_pages` must be a positive number");
            None
        }
    });
    let cursor = line.opt(cx, "cursor").and_then(|pair| cursor(cx, pair));
    line.finish(cx);
    let size = require(cx, &line, "size", size)?;
    Some(PaginateDecl { pos: line.pos, size, direction: direction.unwrap_or(PageDirectionDecl::Forward), max_pages, cursor })
}

pub(super) fn cursor(cx: &mut Cx, pair: &Pair) -> Option<CursorDecl> {
    let pairs = hash_pairs(cx, &pair.value, "`cursor`")?;
    let find = |key: &str| pairs.iter().find(|candidate| candidate.key == key);
    for unknown in pairs.iter().filter(|candidate| !matches!(candidate.key.as_str(), "parameter" | "from" | "type")) {
        cx.fail(pair_pos(unknown), format!("unknown key `{}` for `cursor`", unknown.key));
    }
    let missing: Vec<&str> = ["parameter", "from", "type"].into_iter().filter(|key| find(key).is_none()).collect();
    if !missing.is_empty() {
        cx.fail(at(&pair.value), format!("`cursor` needs `parameter:`, `from:` and `type:`: missing {}", missing.join(", ")));
        return None;
    }
    let parameter = string(cx, &find("parameter")?.value, "cursor `parameter`");
    let from = path(cx, &find("from")?.value, "cursor `from`");
    let ty = match &find("type")?.value.item {
        Value::Symbol(kind) if kind == "string" => Some(CursorType::String),
        Value::Symbol(kind) if kind == "int" => Some(CursorType::Int),
        _ => {
            cx.fail(at(&find("type")?.value), "cursor `type` must be `:string` or `:int`");
            None
        }
    };
    Some(CursorDecl { parameter: parameter?, from: from?, ty: ty? })
}


pub(super) fn subscribe(cx: &mut Cx, call: &Call) -> Option<SubscribeDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let arg = exactly_one_arg(cx, &line, "a subscription name like `subscribe \"RecordChanged\"`")?;
    let name = string(cx, arg, "the subscription name")?;
    let from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`"));
    let id = line.opt(cx, "id").map_or(Vec::new(), |pair| match &pair.value.item {
        Value::Array(items) if !items.is_empty() => items.iter().filter_map(|item| path(cx, item, "`id`")).collect(),
        _ => path(cx, &pair.value, "`id`").into_iter().collect(),
    });
    let debounce = line.opt(cx, "debounce").and_then(|pair| duration(cx, &pair.value, "`debounce`"));
    let apply = match line.opt(cx, "apply") {
        Some(pair) => apply(cx, pair)?,
        None => ApplyDecl { pos: line.pos, effect: Effect::Upsert, fields: Vec::new(), values: Vec::new(), matches: Vec::new(), via: None, delete_if: None, delete_unless: None },
    };
    line.finish(cx);
    Some(SubscribeDecl { pos: line.pos, name, from, id, apply, debounce })
}

pub(super) fn apply(cx: &mut Cx, pair: &Pair) -> Option<ApplyDecl> {
    let pairs = hash_pairs(cx, &pair.value, "`apply`")?;
    let mut decl = ApplyDecl { pos: at(&pair.value), effect: Effect::Upsert, fields: Vec::new(), values: Vec::new(), matches: Vec::new(), via: None, delete_if: None, delete_unless: None };
    let mut seen_effect = false;
    for entry in pairs {
        match entry.key.as_str() {
            "effect" => {
                seen_effect = true;
                decl.effect = match &entry.value.item {
                    Value::Symbol(name) if name == "upsert" => Effect::Upsert,
                    Value::Symbol(name) if name == "delete" => Effect::Delete,
                    Value::Symbol(name) if name == "ephemeral" => Effect::Ephemeral,
                    Value::Symbol(name) if name == "reread" => Effect::Reread,
                    Value::Symbol(name) if name == "ignore" => Effect::Ignore,
                    _ => {
                        cx.fail(at(&entry.value), "`effect` must be `:upsert`, `:delete`, `:ephemeral`, `:reread` or `:ignore`");
                        continue;
                    }
                };
            }
            "fields" => {
                if let Some(inner) = hash_pairs(cx, &entry.value, "`fields`") {
                    for field in inner {
                        if let Some(found) = path(cx, &field.value, "a `fields` path") {
                            decl.fields.push((field.key.clone(), found.item, pair_pos(field)));
                        }
                    }
                }
            }
            "values" => {
                if let Some(inner) = hash_pairs(cx, &entry.value, "`values`") {
                    decl.values = inner.iter().map(|value| (value.key.clone(), json_of(&value.value.item), pair_pos(value))).collect();
                }
            }
            "matches" => {
                if let Some(inner) = hash_pairs(cx, &entry.value, "`matches`") {
                    for matched in inner {
                        if matched.key.split('.').any(str::is_empty) {
                            cx.fail(pair_pos(matched), "apply matches requires nonempty paths");
                        } else if decl.matches.iter().any(|(path, _, _)| path == &matched.key) {
                            cx.fail(pair_pos(matched), format!("apply matches path `{}` is declared twice", matched.key));
                        } else {
                            let (value, except) = match &matched.value.item {
                                Value::Hash(parts) if parts.len() == 1 && parts[0].key == "not" => (&parts[0].value, true),
                                _ => (&matched.value, false),
                            };
                            let Value::Array(items) = &value.item else {
                                cx.fail(at(value), format!("matches `{}` needs a nonempty string list or `not` list", matched.key));
                                continue;
                            };
                            if items.is_empty() {
                                cx.fail(at(value), format!("matches `{}` needs a nonempty string list", matched.key));
                                continue;
                            }
                            let values = items.iter().filter_map(|item| string(cx, item, "a `matches` list item")).collect();
                            let predicate = if except { dblayer::schema::MatchValues::Except { not: values } } else { dblayer::schema::MatchValues::OneOf(values) };
                            decl.matches.push((matched.key.clone(), predicate, pair_pos(matched)));
                        }
                    }
                }
            }
            "via" => decl.via = symbol(cx, &entry.value, "`via`"),
            "delete_if" => decl.delete_if = path(cx, &entry.value, "`delete_if`"),
            "delete_unless" => decl.delete_unless = path(cx, &entry.value, "`delete_unless`"),
            other => cx.fail(pair_pos(entry), format!("unknown key `{other}` for `apply`")),
        }
    }
    if !seen_effect && pairs.iter().any(|entry| matches!(entry.key.as_str(), "fields" | "via")) {
        cx.fail(decl.pos, "`apply` needs `effect:`");
    }
    Some(decl)
}


