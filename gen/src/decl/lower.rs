
use dblayer::schema::{MergeRule, NodeRead, OnFailure, Storage};
use dblayer::value::{ScalarKind, SortDirection};
use serde_json::{Map, Number, Value as Json};

use super::*;
use crate::parser::{Call, ModelNode, Node, OperationNode, Pair, Value};
use crate::schema_check::{Schema, Ty, TyKind, TypeKind};
use crate::ParseError;

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
            if dblayer::pacing::parse_duration(&text).is_none() {
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
        Value::Float(number) => Number::from_f64(*number).map_or(Json::Null, Json::Number),
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


#[derive(Clone, Copy, PartialEq, Eq)]
enum Site {
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

fn type_ref(value: &Value) -> Option<TypeRef> {
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

fn inferred_type(schema: &Schema, ty: &Ty) -> Result<TypeRef, String> {
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
struct Owner(Option<String>);

fn needs_inference(call: &Call) -> bool {
    let typed = call.args.len() >= 2;
    let inside = |children: &[Call]| children.iter().any(|child| match child.word.as_str() {
        "on" | "shared" => child.block.as_ref().is_some_and(|block| block.iter().any(needs_inference)),
        _ => needs_inference(child),
    });
    call.word == "attribute" && (!typed || call.block.as_ref().is_some_and(|children| inside(children)))
}

fn attribute(cx: &mut Cx, call: &Call, site: Site, owner: &Owner) -> Option<AttributeDecl> {
    let mut line = Line::of(call);
    if call.word != "attribute" {
        cx.fail(line.pos, if site == Site::Model { format!("unknown model declaration `{}`", call.word) } else { "a block holds only `attribute` declarations".to_string() });
        return None;
    }
    let name = match call.args.first() {
        Some(node) => symbol(cx, node, "attribute name")?,
        None => {
            cx.fail(line.pos, "attribute needs a name like `attribute :title, :string`");
            return None;
        }
    };
    if call.args.len() > 2 {
        cx.fail(line.pos, format!("attribute `{name}` takes a name and a type"));
    }
    let local = line.opt(cx, "local").and_then(|pair| boolean(cx, &pair.value, "`local`")).unwrap_or(false);
    let explicit = call.args.get(1).map(|node| (type_ref(&node.item), node));
    let mut inferred_null = false;
    let mut ty_owner = owner.clone();
    let mut resolved: Option<Ty> = None;
    let walk = !local && needs_inference(call) && cx.schema.is_some() && owner.0.is_some();
    if walk {
        let schema = cx.schema.expect("walk needs a schema");
        let source = line.opt(cx, "from").map(|pair| &pair.value).or_else(|| call.args.first());
        let source_name = match source.map(|node| &node.item) {
            Some(Value::Str(text) | Value::Symbol(text)) => text.clone(),
            _ => name.clone(),
        };
        let mut ty = Ty { kind: TyKind::Named(owner.0.clone().unwrap_or_default()), nullable: false, has_default: false };
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
                Ok(ty) => {
                    inferred_null = reached.nullable;
                    ty
                }
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
        values: None,
        as_name: None,
        parse_json: false,
        reset_after: None,
        block: AttributeBlock::None,
    };
    let null_given = line.opt(cx, "null");
    decl.null = null_given.and_then(|pair| boolean(cx, &pair.value, "`null`")).unwrap_or(inferred_null && null_given.is_none());
    decl.optional = line.opt(cx, "optional").and_then(|pair| boolean(cx, &pair.value, "`optional`")).unwrap_or(false);
    if let Some(pair) = line.opt(cx, "default") {
        decl.default = Some(Spanned::new(json_of(&pair.value.item), at(&pair.value)));
    }
    let is_file = decl.ty == TypeRef::File;
    let is_named = matches!(decl.ty, TypeRef::Named(_) | TypeRef::NamedList(_));
    if let Some(pair) = line.opt(cx, "from") {
        if is_file || site == Site::Ephemeral {
            cx.fail(pair_pos(pair), format!("`from` does not apply to this attribute"));
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

fn decl_is_union_block(call: &Call) -> bool {
    call.block.as_ref().is_some_and(|block| block.iter().any(|child| matches!(child.word.as_str(), "on" | "shared")))
}

fn check_site(cx: &mut Cx, decl: &AttributeDecl, site: Site) {
    if site == Site::Returns {
        if decl.null || decl.optional || decl.default.is_some() || decl.from.is_some() || decl.local {
            cx.fail(decl.pos, format!("returns attribute `{}`: every returned attribute is nullable and read by name, so `null`, `optional`, `default`, `from` and `local` do not apply", decl.name));
        }
    }
}

fn string_list(cx: &mut Cx, node: &Node<Value>, what: &str) -> Option<Vec<String>> {
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

fn attribute_block(cx: &mut Cx, call: &Call, decl: &mut AttributeDecl, site: Site, owner: &Owner) {
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

fn union_block(cx: &mut Cx, children: &[Call], name: &str, owner: &Owner) -> AttributeBlock {
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

fn merge_rule(value: &Value) -> Option<MergeRule> {
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


fn association(cx: &mut Cx, call: &Call, kind: AssociationKind) -> Option<AssociationDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let name = match call.args.as_slice() {
        [node] => symbol(cx, node, "association name")?,
        _ => {
            cx.fail(line.pos, format!("`{}` needs a name like `:owner`", call.word));
            return None;
        }
    };
    let mut decl = AssociationDecl { pos: line.pos, kind, name, class_name: None, foreign_key: None, touch: false, sets: Vec::new(), newer: Vec::new(), from: None, dependent: false };
    decl.class_name = line.opt(cx, "class_name").and_then(|pair| konst(cx, &pair.value, "`class_name`"));
    decl.foreign_key = line.opt(cx, "foreign_key").and_then(|pair| symbol(cx, &pair.value, "`foreign_key`"));
    decl.from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`")).map(|found| found.item);
    if kind == AssociationKind::BelongsTo {
        decl.touch = line.opt(cx, "touch").and_then(|pair| boolean(cx, &pair.value, "`touch`")).unwrap_or(false);
        let sets = line.opt(cx, "sets");
        decl.sets = symbol_pairs(cx, sets, "`sets`");
        let newer = line.opt(cx, "newer");
        decl.newer = symbol_pairs(cx, newer, "`newer`");
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

fn symbol_pairs(cx: &mut Cx, pair: Option<&Pair>, what: &str) -> Vec<(String, String)> {
    let Some(pair) = pair else { return Vec::new() };
    let Some(pairs) = hash_pairs(cx, &pair.value, what) else { return Vec::new() };
    pairs.iter().filter_map(|inner| symbol(cx, &inner.value, what).map(|value| (inner.key.clone(), value))).collect()
}


fn parameter(cx: &mut Cx, call: &Call) -> Option<ParameterDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let (name, ty) = match call.args.as_slice() {
        [name, ty] => (symbol(cx, name, "parameter name")?, ty),
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

fn variables(cx: &mut Cx, call: &Call, constants: bool) -> Option<Vec<VariableBinding>> {
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

fn response(cx: &mut Cx, call: &Call) -> Option<ResponseDecl> {
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


fn insert_on(cx: &mut Cx, pair: &Pair) -> Option<Spanned<Vec<InsertOn>>> {
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

fn scope(cx: &mut Cx, call: &Call) -> Option<ScopeDecl> {
    let mut line = Line::of(call);
    let name = match call.args.as_slice() {
        [node] => symbol(cx, node, "scope name")?,
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
    let members_complete = line.opt(cx, "members_complete").and_then(|pair| boolean(cx, &pair.value, "`members_complete`")).unwrap_or(false);
    line.finish(cx);
    let lines = block(cx, call)?;
    let mut decl = ScopeDecl {
        pos: line.pos, name, node, insert_on: insert, stable_order: stable, refresh_on_foreground: foreground, members_complete, parameters: Vec::new(), where_: None, membership: Vec::new(), order: None, query: None, variables: None,
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

fn scope_line(cx: &mut Cx, call: &Call, decl: &mut ScopeDecl) {
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
                Value::Int(number) if *number > 0 => usize::try_from(*number).ok(),
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
                            Value::Str(text) => PreloadChoice::RowsOf(Spanned::new(text.clone(), at(&part.value))),
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

fn paginate(cx: &mut Cx, call: &Call) -> Option<PaginateDecl> {
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

fn cursor(cx: &mut Cx, pair: &Pair) -> Option<CursorDecl> {
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


fn subscribe(cx: &mut Cx, call: &Call) -> Option<SubscribeDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let arg = exactly_one_arg(cx, &line, "a subscription name like `subscribe \"RecordChanged\"`")?;
    let name = string(cx, arg, "the subscription name")?;
    let from = line.opt(cx, "from").and_then(|pair| path(cx, &pair.value, "`from`"));
    let id = line.opt(cx, "id").map(|pair| match &pair.value.item {
        Value::Array(items) if !items.is_empty() => items.iter().filter_map(|item| path(cx, item, "`id`")).collect(),
        _ => path(cx, &pair.value, "`id`").into_iter().collect(),
    }).unwrap_or_default();
    let debounce = line.opt(cx, "debounce").and_then(|pair| duration(cx, &pair.value, "`debounce`"));
    let apply = match line.opt(cx, "apply") {
        Some(pair) => apply(cx, pair)?,
        None => ApplyDecl { pos: line.pos, effect: Effect::Upsert, fields: Vec::new(), values: Vec::new(), matches: Vec::new(), via: None, delete_if: None, delete_unless: None },
    };
    line.finish(cx);
    Some(SubscribeDecl { pos: line.pos, name, from, id, apply, debounce })
}

fn apply(cx: &mut Cx, pair: &Pair) -> Option<ApplyDecl> {
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


fn optimistic(cx: &mut Cx, call: &Call) -> Option<OptimisticDecl> {
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

fn source(cx: &mut Cx, node: &Node<Value>) -> Option<SourceDecl> {
    match &node.item {
        Value::Symbol(name) if name == "id" => Some(SourceDecl::Owner("id".to_string())),
        Value::Symbol(name) => Some(SourceDecl::Parameter(name.clone())),
        Value::Hash(parts) => source_hash(cx, parts, node),
        Value::Array(_) | Value::OptionalArray(_) | Value::Const(_) | Value::Duration { .. } => {
            cx.fail(at(node), "an effect value must be a literal, a parameter symbol or a hash");
            None
        }
        other => Some(SourceDecl::Literal(json_of(other))),
    }
}

fn source_hash(cx: &mut Cx, parts: &[Pair], node: &Node<Value>) -> Option<SourceDecl> {
    let find = |key: &str| parts.iter().find(|part| part.key == key);
    let name_of = |cx: &mut Cx, key: &str| find(key).and_then(|part| symbol(cx, &part.value, &format!("`{key}:`")));
    let single = |key: &str| parts.len() == 1 && find(key).is_some();
    if single("owner") {
        return name_of(cx, "owner").map(SourceDecl::Owner);
    }
    if single("plus") {
        return name_of(cx, "plus").map(SourceDecl::Plus);
    }
    if single("minus") {
        return name_of(cx, "minus").map(SourceDecl::Minus);
    }
    let row_shape = parts.iter().all(|part| matches!(part.key.as_str(), "of" | "row" | "field")) && find("of").is_some() && find("field").is_some();
    if !row_shape {
        cx.fail(at(node), "an effect hash is `{ owner: :field }`, `{ of: Model, field: :name }`, `{ of: Model, row: :param, field: :name }`, `{ plus: :param }` or `{ minus: :param }`");
        return None;
    }
    let model = find("of").and_then(|part| konst(cx, &part.value, "`of:`"));
    let row = find("row").and_then(|part| symbol(cx, &part.value, "`row:`"));
    let field = name_of(cx, "field");
    Some(SourceDecl::Row { model: model?, row, field: field? })
}

fn source_pairs(cx: &mut Cx, pairs: &[Pair]) -> Vec<SourcePair> {
    pairs.iter().filter_map(|pair| Some(SourcePair { name: pair.key.clone(), source: source(cx, &pair.value)?, pos: pair_pos(pair) })).collect()
}

fn select(cx: &mut Cx, call: &Call, adds: bool) -> Option<EffectDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let (scope, model) = match call.args.as_slice() {
        [scope] => (symbol(cx, scope, "the selection")?, None),
        [scope, model] => (symbol(cx, scope, "the selection")?, Some(konst(cx, model, "the selection model")?)),
        _ => {
            cx.fail(line.pos, format!("`{}` needs a selection like `{} :blocked` or `{} :members, User, rows: :userIds`", line.word, line.word, line.word));
            return None;
        }
    };
    let rows = line.opt(cx, "rows").and_then(|pair| symbol(cx, &pair.value, "`rows:`").map(|name| Spanned::new(name, pair_pos(pair))));
    let with = line.opt(cx, "with").and_then(|pair| hash_pairs(cx, &pair.value, "`with:`")).map(|pairs| source_pairs(cx, pairs)).unwrap_or_default();
    line.finish(cx);
    Some(EffectDecl::Select { pos: line.pos, adds, scope, model, rows, with })
}

fn create_row(cx: &mut Cx, call: &Call) -> Option<EffectDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let model = match call.args.as_slice() {
        [model] => konst(cx, model, "the created model")?,
        _ => {
            cx.fail(line.pos, "`create_row` needs a model like `create_row Message, body: :body`");
            return None;
        }
    };
    line.used.iter_mut().for_each(|used| *used = true);
    Some(EffectDecl::Create { pos: line.pos, model, values: source_pairs(cx, &call.options) })
}

fn change_row(cx: &mut Cx, call: &Call) -> Option<EffectDecl> {
    let mut line = Line::of(call);
    no_block(cx, call);
    let model = match call.args.as_slice() {
        [model] => konst(cx, model, "the changed model")?,
        _ => {
            cx.fail(line.pos, "`change_row` needs a model like `change_row CurrentUser, balance: { minus: :price }`");
            return None;
        }
    };
    let row = line.opt(cx, "row").and_then(|pair| symbol(cx, &pair.value, "`row:`").map(|name| Spanned::new(name, pair_pos(pair))));
    let fields: Vec<Pair> = call.options.iter().filter(|pair| pair.key != "row").cloned().collect();
    line.used.iter_mut().for_each(|used| *used = true);
    Some(EffectDecl::Change { pos: line.pos, model, row, values: source_pairs(cx, &fields) })
}

fn mapped(cx: &mut Cx, parts: &[Pair], node: &Node<Value>) -> Option<OptimisticValue> {
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

fn flagged(cx: &mut Cx, parts: &[Pair], node: &Node<Value>) -> Option<OptimisticValue> {
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

fn keyed(cx: &mut Cx, parts: &[Pair], node: &Node<Value>) -> Option<OptimisticValue> {
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

fn batch(cx: &mut Cx, call: &Call) -> Option<BatchDecl> {
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

fn renumber(cx: &mut Cx, pair: &Pair) -> Option<RenumberDecl> {
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

fn validates(cx: &mut Cx, call: &Call) -> Option<LengthDecl> {
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

fn returns(cx: &mut Cx, call: &Call, scalars_only: bool) -> Option<Vec<AttributeDecl>> {
    let mut line = Line::of(call);
    line.no_args(cx);
    line.no_options(cx);
    let children = block(cx, call)?;
    if children.is_empty() {
        cx.fail(line.pos, "`returns` block needs at least 1 attribute");
    }
    let owner = Owner(None);
    let found: Vec<AttributeDecl> = children.iter().filter_map(|child| attribute(cx, child, Site::Returns, &owner)).collect();
    if scalars_only {
        for decl in found.iter().filter(|decl| !matches!(decl.ty, TypeRef::Scalar(kind) if kind != ScalarKind::Date)) {
            cx.fail(decl.pos, "a model operation returns :bool, :int, :num, :string or :id attributes");
        }
    }
    Some(found)
}

fn operation(cx: &mut Cx, call: &Call) -> Option<OperationDecl> {
    let mut line = Line::of(call);
    let name = match call.args.as_slice() {
        [node] => symbol(cx, node, "operation name")?,
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
        Some(false) => {
            cx.fail(pair_pos(pair), "`upload: false` is the default: remove it");
            None
        }
        None => None,
    });
    if upload.is_some() && kind != OperationKind::Custom {
        cx.fail(line.pos, format!("`{name}` is a standard operation: `upload: true` belongs to a named operation"));
    }
    line.finish(cx);
    let lines = block(cx, call)?;
    let mut decl = OperationDecl {
        pos: line.pos, name: name.clone(), kind, upload, mutation: None, parameters: Vec::new(), variables: None, local: None, local_from: Vec::new(), local_sites: Vec::new(), wrap: None, response: None, match_by: None,
        on_failure: None, optimistic: None, effects: Vec::new(), upserts: Vec::new(), refetch: Vec::new(), returns: None, batch: None, length: None,
    };
    let mut seen: Vec<&str> = Vec::new();
    for item in lines {
        let word = item.word.as_str();
        let allowed = match (kind, word) {
            (_, "mutation" | "variables" | "wrap" | "response") => true,
            (OperationKind::Create | OperationKind::Update | OperationKind::Custom, "parameter") => true,
            (OperationKind::Create | OperationKind::Update, "local" | "refetch" | "match_by" | "on_failure") => true,
            (OperationKind::Custom, "match_by" | "on_failure" | "optimistic" | "upsert" | "refetch" | "returns" | "batch" | "validates" | "add_to" | "remove_from" | "create_row" | "change_row") => true,
            _ => false,
        };
        if !allowed {
            cx.fail(Pos::of(item), format!("`{word}` does not apply to operation `{name}`"));
            continue;
        }
        if !matches!(word, "parameter" | "local" | "upsert" | "refetch" | "add_to" | "remove_from" | "create_row" | "change_row") && seen.contains(&word) {
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

fn operation_line(cx: &mut Cx, call: &Call, decl: &mut OperationDecl) {
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
        "variables" => decl.variables = variables(cx, call, false),
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
        "wrap" => {
            no_block(cx, call);
            line.no_options(cx);
            if let Some(arg) = exactly_one_arg(cx, &line, "a variable name like `wrap \"input\"`") {
                decl.wrap = string(cx, arg, "the variable path");
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
        "add_to" => decl.effects.extend(select(cx, call, true)),
        "remove_from" => decl.effects.extend(select(cx, call, false)),
        "create_row" => decl.effects.extend(create_row(cx, call)),
        "change_row" => decl.effects.extend(change_row(cx, call)),
        "upsert" => {
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
            if let (Some(model), Some(from)) = (model, from) {
                decl.upserts.push(UpsertDecl { pos: line.pos, model, from });
            }
        }
        "refetch" => {
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
            if let Some(scope) = scope {
                decl.refetch.push(RefetchDecl { pos: line.pos, scope, model });
            }
        }
        "returns" => decl.returns = returns(cx, call, true),
        "batch" => decl.batch = batch(cx, call),
        "validates" => decl.length = validates(cx, call),
        _ => {}
    }
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
        name: node.name.clone(), pos: Pos { line: node.line, column: node.column }, file: node.file, storage, expires_in, attributes: Vec::new(), ephemeral: Vec::new(), cloud: None,
        indexes: Vec::new(), search: Vec::new(), associations: Vec::new(), scopes: Vec::new(), subscriptions: Vec::new(), changes: Vec::new(), changed_by: Vec::new(), server_revision: None, operations: Vec::new(),
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
            "cloud" => {
                no_block(cx, call);
                line.no_options(cx);
                let names = call.args.iter().filter_map(|arg| symbol(cx, arg, "cloud attribute name").map(|name| Spanned::new(name, at(arg)))).collect();
                if decl.cloud.is_some() {
                    cx.fail(line.pos, "`cloud` is declared twice");
                } else {
                    decl.cloud = Some(CloudDecl { pos: line.pos, names });
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
