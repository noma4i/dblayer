use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Site {
    Model,
    Nested,
    Ephemeral,
    Edge,
    Returns,
}

const SCALARS: [(&str, ScalarKind); 6] = [
    ("string", ScalarKind::Str),
    ("num", ScalarKind::Num),
    ("int", ScalarKind::Int),
    ("bool", ScalarKind::Bool),
    ("date", ScalarKind::Date),
    ("id", ScalarKind::Id),
];

const TYPE_MESSAGE: &str = "an attribute type is :string, :num, :int, :bool, :date, :id, :enum, :json, :file, a type name like `Media`, or a list like `[:string]`, `[:enum]` or `[Media]`";

pub(super) fn type_ref(value: &Value) -> Option<TypeRef> {
    match value {
        Value::Symbol(name) => match name.as_str() {
            "enum" => Some(TypeRef::Enum),
            "json" => Some(TypeRef::Json),
            "file" => Some(TypeRef::File),
            other => SCALARS.iter().find(|(word, _)| *word == other).map(|(_, kind)| TypeRef::Scalar(*kind)),
        },
        Value::Const(name) => Some(TypeRef::Named(name.clone())),
        Value::Array(items) => match items.as_slice() {
            [Node { item: Value::Symbol(name), .. }] if name == "enum" => Some(TypeRef::List(ListElement::Enum)),
            [Node { item: Value::Symbol(name), .. }] => SCALARS.iter().find(|(word, _)| word == name).map(|(_, kind)| TypeRef::List(ListElement::Scalar(*kind))),
            [Node { item: Value::Const(name), .. }] => Some(TypeRef::NamedList(name.clone())),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn inferred_type(schema: &Schema, ty: &Ty) -> Result<TypeRef, String> {
    match &ty.kind {
        TyKind::List(item) => {
            if item.is_list() || item.nullable {
                return Err(format!("model attribute type `{}` needs an explicit storage type", ty.describe()));
            }
            Ok(match inferred_type(schema, item)? {
                TypeRef::Scalar(kind) => TypeRef::List(ListElement::Scalar(kind)),
                TypeRef::Named(name) => TypeRef::NamedList(name),
                _ => return Err(format!("model attribute type `{}` needs an explicit storage type", ty.describe())),
            })
        }
        TyKind::Named(name) => Ok(match name.as_str() {
            "String" => TypeRef::Scalar(ScalarKind::Str),
            "ID" => TypeRef::Scalar(ScalarKind::Id),
            "Int" => TypeRef::Scalar(ScalarKind::Int),
            "Float" => TypeRef::Scalar(ScalarKind::Num),
            "Boolean" => TypeRef::Scalar(ScalarKind::Bool),
            _ => match schema.types.get(name) {
                Some(TypeKind::Fields(_) | TypeKind::Union(_)) => TypeRef::Named(name.clone()),
                _ => return Err(format!("GraphQL type `{name}` needs an explicit storage type")),
            },
        }),
    }
}

#[derive(Clone)]
pub(super) struct Owner(pub(super) Option<String>);

fn schema_field(schema: &Schema, owner: &str, source: &str) -> Option<Ty> {
    let mut reached = Ty { kind: TyKind::Named(owner.to_string()), nullable: false, has_default: false };
    for part in source.split('.') {
        match schema.field(reached.name(), part) {
            Ok(next) => reached = next,
            Err(_) => return None,
        }
    }
    Some(reached)
}

pub(super) fn needs_inference(call: &Call) -> bool {
    let typed = call.args.len() >= 2;
    let inside = |children: &[Call]| children.iter().any(|child| match child.word.as_str() {
        "on" | "shared" => child.block.as_ref().is_some_and(|block| block.iter().any(needs_inference)),
        _ => needs_inference(child),
    });
    call.word == "attribute" && (!typed || call.block.as_ref().is_some_and(|children| inside(children)))
}

pub(super) fn attribute(cx: &mut Cx, call: &Call, site: Site, owner: &Owner) -> Option<AttributeDecl> {
    let mut line = Line::of(call);
    if call.word != "attribute" {
        cx.fail(line.pos, if site == Site::Model { format!("unknown model declaration `{}`", call.word) } else { "a block holds only `attribute` declarations".to_string() });
        return None;
    }
    let name = match call.args.first() {
        Some(node) => lowercase_symbol(cx, node, "attribute name")?,
        None => {
            cx.fail(line.pos, "attribute needs a name like `attribute :title, :string`");
            return None;
        }
    };
    if call.args.len() > 2 {
        cx.fail(line.pos, format!("attribute `{name}` takes a name and a type"));
    }
    let local = line.opt(cx, "local").and_then(|pair| boolean(cx, &pair.value, "`local`")).unwrap_or(false);
    let derived = line.opt(cx, "derived").and_then(|pair| boolean(cx, &pair.value, "`derived`")).unwrap_or(false);
    let explicit = call.args.get(1).map(|node| (type_ref(&node.item), node));
    let mut ty_owner = owner.clone();
    let mut resolved: Option<Ty> = None;
    let source_name = match line.opt(cx, "from").map(|pair| &pair.value).or_else(|| call.args.first()).map(|node| &node.item) {
        Some(Value::Str(text) | Value::Symbol(text)) => text.clone(),
        _ => name.clone(),
    };
    let schema_field = match (cx.schema, &owner.0, local || derived) {
        (Some(schema), Some(type_name), false) => schema_field(schema, type_name, &source_name),
        _ => None,
    };
    if let Some(reached) = &schema_field {
        ty_owner = Owner(Some(reached.name().to_string()));
    }
    let schema_null = site != Site::Returns && schema_field.as_ref().is_some_and(|reached| reached.nullable);
    if let (Some(schema), Some(type_name), true) = (cx.schema, &owner.0, !local && !derived && needs_inference(call)) {
        let mut ty = Ty { kind: TyKind::Named(type_name.clone()), nullable: false, has_default: false };
        let mut failed = None;
        for part in source_name.split('.') {
            match schema.field(ty.name(), part) {
                Ok(next) => ty = next,
                Err(message) => {
                    failed = Some(message);
                    break;
                }
            }
        }
        match failed {
            Some(message) => {
                cx.fail(line.pos, message);
                return None;
            }
            None => {
                ty_owner = Owner(Some(ty.name().to_string()));
                resolved = Some(ty);
            }
        }
    }
    let ty = match explicit {
        Some((Some(ty), _)) => ty,
        Some((None, node)) => {
            cx.fail(at(node), TYPE_MESSAGE);
            return None;
        }
        None => match (&resolved, cx.schema) {
            (Some(reached), Some(schema)) => match inferred_type(schema, reached) {
                Ok(ty) => ty,
                Err(message) => {
                    cx.fail(line.pos, message);
                    return None;
                }
            },
            _ => {
                cx.fail(line.pos, format!("attribute `{name}` needs a type: no GraphQL schema supplies it"));
                return None;
            }
        },
    };
    if site == Site::Ephemeral && ty == TypeRef::File {
        cx.fail(line.pos, format!("ephemeral attribute `{name}` cannot be a file"));
    }
    if site == Site::Nested && ty == TypeRef::File {
        cx.fail(line.pos, format!("nested attribute `{name}` cannot be a file"));
    }
    let mut decl = AttributeDecl {
        pos: line.pos,
        name: name.clone(),
        ty,
        null: false,
        optional: false,
        default: None,
        id_from: None,
        link: None,
        from: None,
        merge: None,
        local,
        derived,
        values: None,
        as_name: None,
        parse_json: false,
        reset_after: None,
        block: AttributeBlock::None,
    };
    let null_given = line.opt(cx, "null");
    decl.null = null_given.map_or(schema_null, |pair| boolean(cx, &pair.value, "`null`").unwrap_or(false));
    let optional_given = line.opt(cx, "optional").and_then(|pair| boolean(cx, &pair.value, "`optional`").map(|value| (value, pair_pos(pair))));
    if let (true, Some((true, at_optional))) = (local, optional_given) {
        cx.fail(at_optional, format!("attribute `{}`: local implies optional, omit `optional: true`", decl.name));
    }
    decl.optional = local || optional_given.is_some_and(|(value, _)| value);
    if let Some(pair) = line.opt(cx, "default") {
        decl.default = Some(Spanned::new(json_of(&pair.value.item), at(&pair.value)));
    }
    let is_file = decl.ty == TypeRef::File;
    let is_named = matches!(decl.ty, TypeRef::Named(_) | TypeRef::NamedList(_));
    if let Some(pair) = line.opt(cx, "from") {
        if is_file || site == Site::Ephemeral {
            cx.fail(pair_pos(pair), "`from` does not apply to this attribute".to_string());
        } else if let Some(found) = path(cx, &pair.value, "`from`") {
            decl.from = Some(found);
        }
    }
    if let Some(pair) = line.opt(cx, "id_from") {
        if site != Site::Model || is_file || is_named && decl_is_union_block(call) {
            cx.fail(pair_pos(pair), "`id_from` applies only to a top-level attribute that is not a file or a union");
        } else {
            decl.id_from = symbol(cx, &pair.value, "`id_from`");
        }
    }
    let link_model = line.opt(cx, "ref");
    let link_id = line.opt(cx, "id");
    match (link_model, link_id) {
        (None, None) => {}
        (Some(model), Some(id)) => {
            if site != Site::Nested || !matches!(decl.ty, TypeRef::Named(_)) || decl_is_union_block(call) {
                cx.fail(pair_pos(model), "`ref` and `id` apply only to a nested object attribute that is not a union");
            } else if let (Some(model), Some(id)) = (konst(cx, &model.value, "`ref`"), symbol(cx, &id.value, "`id`")) {
                decl.link = Some((model, id));
            }
        }
        (Some(pair), None) | (None, Some(pair)) => cx.fail(pair_pos(pair), "`ref` and `id` are given together: `ref: Model, id: :attribute`"),
    }
    if let Some(pair) = line.opt(cx, "merge") {
        if site != Site::Model || is_file {
            cx.fail(pair_pos(pair), "`merge` applies only to a top-level attribute that is not a file");
        } else if let Some(rule) = merge_rule(&pair.value.item) {
            decl.merge = Some(Spanned::new(rule, at(&pair.value)));
        } else {
            cx.fail(at(&pair.value), "`merge` must be `:present`, `:non_empty`, `:positive`, `:monotonic`, a list of tiers like `[[:a, :b], [:c]]` or a hash of those per key (a key may also be `:local`)");
        }
    }
    if derived {
        for pair in call.options.iter().filter(|pair| matches!(pair.key.as_str(), "null" | "optional" | "default" | "from" | "local" | "merge" | "id_from")) {
            cx.fail(pair_pos(pair), format!("derived attribute `{name}` cannot set `{}`: a derived field is nullable and written only by `sets:` of a child", pair.key));
        }
        if site != Site::Model || is_file {
            cx.fail(line.pos, format!("derived attribute `{name}` must be a top-level attribute that is not a file"));
        }
        decl.null = true;
    }
    if local && !matches!(site, Site::Model) {
        cx.fail(line.pos, "`local` applies only to a top-level attribute");
    }
    if let Some(pair) = line.opt(cx, "values") {
        if !matches!(decl.ty, TypeRef::Enum | TypeRef::List(ListElement::Enum)) {
            cx.fail(pair_pos(pair), "`values:` belongs to `:enum` and `[:enum]`");
        } else {
            decl.values = string_list(cx, &pair.value, "`values`");
        }
    }
    if let Some(pair) = line.opt(cx, "as") {
        if !matches!(decl.ty, TypeRef::Enum | TypeRef::List(ListElement::Enum)) || site == Site::Ephemeral {
            cx.fail(pair_pos(pair), "`as:` names the generated enum of `:enum` and `[:enum]`");
        } else {
            decl.as_name = konst(cx, &pair.value, "`as`");
        }
    }
    if let Some(pair) = line.opt(cx, "parse") {
        if decl.ty != TypeRef::Json || !matches!(pair.value.item, Value::Symbol(ref kind) if kind == "json") {
            cx.fail(pair_pos(pair), "`parse:` accepts only `:json` on a `:json` attribute");
        } else {
            decl.parse_json = true;
        }
    }
    if let Some(pair) = line.opt(cx, "reset_after") {
        if site != Site::Ephemeral {
            cx.fail(pair_pos(pair), "`reset_after` applies only inside `ephemeral`");
        } else {
            decl.reset_after = duration(cx, &pair.value, "`reset_after`");
        }
    }
    line.finish(cx);
    if let (true, Some(default)) = (decl.parse_json, &decl.default) {
        if !matches!(default.item, Json::Object(_) | Json::Null) {
            cx.fail(default.pos, "json `parse: :json` default must be an object or null");
        }
    }
    if matches!(decl.ty, TypeRef::Enum | TypeRef::List(ListElement::Enum)) && decl.values.is_none() && site != Site::Returns {
        cx.fail(line.pos, format!("enum attribute `{name}` needs `values:`"));
    }
    attribute_block(cx, call, &mut decl, site, &ty_owner);
    check_site(cx, &decl, site);
    Some(decl)
}

pub(super) fn decl_is_union_block(call: &Call) -> bool {
    call.block.as_ref().is_some_and(|block| block.iter().any(|child| matches!(child.word.as_str(), "on" | "shared")))
}

pub(super) fn check_site(cx: &mut Cx, decl: &AttributeDecl, site: Site) {
    if site == Site::Returns && (decl.null || decl.optional || decl.default.is_some() || decl.from.is_some() || decl.local) {
        cx.fail(decl.pos, format!("returns attribute `{}`: every returned attribute is nullable and read by name, so `null`, `optional`, `default`, `from` and `local` do not apply", decl.name));
    }
}

pub(super) fn string_list(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<Vec<String>> {
    let Value::Array(items) = &node.item else {
        cx.fail(at(node), format!("{what} must be a list of strings"));
        return None;
    };
    items.iter().map(|item| match &item.item {
        Value::Str(text) => Some(text.clone()),
        _ => {
            cx.fail(at(item), format!("{what} must be a list of strings like [\"PENDING\", \"SENT\"]"));
            None
        }
    }).collect()
}

pub(super) fn attribute_block(cx: &mut Cx, call: &Call, decl: &mut AttributeDecl, site: Site, owner: &Owner) {
    let Some(children) = &call.block else {
        if decl.is_union() {
            return;
        }
        return;
    };
    if !matches!(decl.ty, TypeRef::Named(_) | TypeRef::NamedList(_)) {
        cx.fail(Pos::of(call), format!("`attribute :{}` of this type takes no block", decl.name));
        return;
    }
    let type_label = match &decl.ty {
        TypeRef::Named(name) | TypeRef::NamedList(name) => name.clone(),
        _ => String::new(),
    };
    let nested_site = if site == Site::Returns { Site::Returns } else { Site::Nested };
    if decl_is_union_block(call) {
        if site == Site::Returns || site == Site::Edge || site == Site::Ephemeral {
            cx.fail(Pos::of(call), "a union is declared only on a model attribute");
        }
        if let Some(default) = &decl.default {
            cx.fail(default.pos, "unknown key `default` for a union attribute");
        }
        decl.block = union_block(cx, children, &type_label, owner);
        return;
    }
    if children.is_empty() {
        cx.fail(Pos::of(call), format!("`{type_label}` block needs at least 1 attribute"));
    }
    decl.block = AttributeBlock::Fields(children.iter().filter_map(|child| attribute(cx, child, nested_site, owner)).collect());
}

pub(super) fn union_block(cx: &mut Cx, children: &[Call], name: &str, owner: &Owner) -> AttributeBlock {
    let mut shared: Option<Spanned<Vec<AttributeDecl>>> = None;
    let mut variants = Vec::new();
    for child in children {
        let line = Line::of(child);
        match child.word.as_str() {
            "shared" => {
                let mut line = line;
                line.no_args(cx);
                line.no_options(cx);
                if shared.is_some() {
                    cx.fail(line.pos, "`shared` is declared twice");
                    continue;
                }
                let Some(block) = block(cx, child) else { continue };
                let fields: Vec<AttributeDecl> = block.iter().filter_map(|inner| attribute(cx, inner, Site::Nested, owner)).collect();
                if block.is_empty() {
                    cx.fail(line.pos, "`shared` block needs at least 1 attribute");
                }
                shared = Some(Spanned::new(fields, line.pos));
            }
            "on" => {
                let mut line = line;
                let Some(type_name) = (match child.args.as_slice() {
                    [node @ Node { item: Value::Const(_), .. }] => konst(cx, node, "variant"),
                    _ => {
                        cx.fail(line.pos, "`on` needs a type name like `ImageAttachment`");
                        None
                    }
                }) else { continue };
                let case = line.opt(cx, "case").and_then(|pair| symbol(cx, &pair.value, "`case`"));
                line.finish(cx);
                let fields = child.block.as_ref().map(|block| block.iter().filter_map(|inner| attribute(cx, inner, Site::Nested, &Owner(owner.0.as_ref().map(|_| type_name.clone())))).collect());
                variants.push(VariantDecl { pos: line.pos, type_name, case, fields });
            }
            _ => cx.fail(line.pos, format!("a union `{name}` holds only `on` variants and one `shared` block")),
        }
    }
    AttributeBlock::Union { shared, variants }
}

pub(super) fn merge_rule(value: &Value) -> Option<MergeRule> {
    match value {
        Value::Symbol(name) => match name.as_str() {
            "present" => Some(MergeRule::Present),
            "non_empty" => Some(MergeRule::NonEmpty),
            "positive" => Some(MergeRule::Positive),
            "monotonic" => Some(MergeRule::Monotonic),
            "stable_list" => Some(MergeRule::StableList),
            _ => None,
        },
        Value::Array(tiers) if !tiers.is_empty() => {
            let mut out = Vec::new();
            for tier in tiers {
                let Value::Array(names) = &tier.item else { return None };
                if names.is_empty() {
                    return None;
                }
                out.push(names.iter().map(|name| match &name.item { Value::Symbol(text) | Value::Str(text) => Some(text.clone()), _ => None }).collect::<Option<Vec<_>>>()?);
            }
            Some(MergeRule::Ladder { tiers: out })
        }
        Value::Hash(pairs) if !pairs.is_empty() => {
            let mut keys = Vec::new();
            for pair in pairs {
                let rule = match &pair.value.item {
                    Value::Symbol(name) if name == "local" => MergeRule::Local,
                    Value::Hash(_) => return None,
                    other => merge_rule(other)?,
                };
                keys.push((pair.key.clone(), rule));
            }
            Some(MergeRule::Keys { keys: keys.into_iter().collect() })
        }
        _ => None,
    }
}


