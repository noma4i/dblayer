use dblayer::schema::{FieldKind, MergeRule, Newer, CursorSpec, VariableSource, 
    ActionDeclaration, ActionLanding, AssociationKind, AssociationSpec, Coverage, Dependent, EphemeralSpec, FieldSpec, ModelSpec,
    NodeRead, OnFailure, PageDirection, RelationSpec, RemoteShape, RemoteSpec, RootShape, Sort, SortField, Storage, UpdatesSpec,
};
use dblayer::value::{ScalarKind, SortDirection};
use serde_json::{Map, Number, Value as Json};

use crate::parser::{Call, ModelNode, Node, Pair, Value};

const DEFAULT_FRESHNESS: &str = "5.minutes";

pub fn build(nodes: &[ModelNode]) -> Vec<ModelSpec> {
    let mut models: Vec<_> = nodes.iter().map(build_model).collect();
    let indexes: Vec<_> = models.iter().flat_map(|model| model.associations.iter().filter(|association| association.kind != AssociationKind::References).map(move |association| {
        let owner = if association.kind == AssociationKind::BelongsTo { model.key.to_string() } else { association.target.to_string() };
        (owner, association.foreign_key.clone())
    })).collect();
    for model in &mut models {
        for field in model.fields.iter_mut().filter(|field| field.name != "id") {
            if indexes.iter().any(|(owner, key)| owner == model.key.as_str() && key == &field.name)
                || model.relations.iter().any(|relation| relation.by_columns.contains(&field.name)
                    || matches!(&relation.sort, Sort::Fields(fields) if fields.iter().any(|sort| sort.column == field.name))) {
                field.indexed = true;
            }
        }
    }
    let unions = union_names(&models);
    models.iter_mut().flat_map(|model| model.fields.iter_mut()).for_each(|field| retag_references(field, &unions));
    models
}

fn union_names(models: &[ModelSpec]) -> Vec<String> {
    fn walk(field: &FieldSpec, out: &mut Vec<String>) {
        if let (FieldKind::Union | FieldKind::UnionArray, Some(name)) = (&field.kind, &field.app_type) {
            if !field.fields.is_empty() { out.push(name.clone()); }
        }
        field.fields.iter().chain(field.shared.iter()).for_each(|inner| walk(inner, out));
    }
    let mut found = Vec::new();
    models.iter().flat_map(|model| model.fields.iter()).for_each(|field| walk(field, &mut found));
    found
}

fn retag_references(field: &mut FieldSpec, unions: &[String]) {
    if field.fields.is_empty() && field.app_type.as_ref().is_some_and(|name| unions.contains(name)) {
        match field.kind {
            FieldKind::Object => field.kind = FieldKind::Union,
            FieldKind::Array => field.kind = FieldKind::UnionArray,
            _ => {}
        }
    }
    field.fields.iter_mut().chain(field.shared.iter_mut()).for_each(|inner| retag_references(inner, unions));
}

fn build_model(node: &ModelNode) -> ModelSpec {
    let mut model = ModelSpec::new(&node.name, vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    if let Some(Value::Symbol(storage)) = node.options.iter().find(|pair| pair.key == "storage").map(|pair| &pair.value.item) {
        model.storage = match storage.as_str() {
            "memory" => Storage::Memory,
            "local" => Storage::Local,
            _ => Storage::Disk,
        };
    }
    model.expire_after = node.options.iter().find(|pair| pair.key == "expire_after").and_then(|pair| text_of(&pair.value.item));
    for call in &node.body {
        match call.word.as_str() {
            "schema" => build_schema(call, &mut model),
            "belongs_to" => model.associations.push(build_association(call, AssociationKind::BelongsTo, &node.name)),
            "has_many" => model.associations.push(build_association(call, AssociationKind::HasMany, &node.name)),
            "has_one" => model.associations.push(build_association(call, AssociationKind::HasOne, &node.name)),
            "references" => model.associations.push(build_association(call, AssociationKind::References, &node.name)),
            "scope" => model.relations.push(build_scope(call)),
            "remote" => model.relations.push(build_remote(call, &node.name)),
            "listen" => model.listens.push(updates_spec(call)),
            "changes" => model.change_feed = Some(dblayer::schema::ChangeFeed {
                subscription: first_name(call),
                root: option(call, "root").and_then(|pair| text_of(&pair.value.item)).unwrap_or_default(),
                area: option(call, "area").and_then(|pair| text_of(&pair.value.item)).unwrap_or_default(),
                kind: option(call, "kind").and_then(|pair| text_of(&pair.value.item)).unwrap_or_default(),
                id: option(call, "id").and_then(|pair| text_of(&pair.value.item)).unwrap_or_default(),
            }),
            "changed_by" => model.changes.push(dblayer::schema::ChangeRead {
                area: first_name(call).to_ascii_uppercase(),
                relation: option(call, "via").and_then(|pair| text_of(&pair.value.item)).unwrap_or_default(),
                parameter: option(call, "id").and_then(|pair| text_of(&pair.value.item)),
                removes_on: list_of(call, "removes_on").into_iter().map(|kind| kind.to_ascii_uppercase()).collect(),
            }),
            "create" | "update" | "destroy" => model.actions.push(build_standard_action(call)),
            "action" => model.actions.push(build_action(call, &node.name)),
            _ => {}
        }
    }
    if model.field("id").is_none() {
        model.fields.push(FieldSpec::scalar("id", ScalarKind::Id));
    }
    model
}

fn build_schema(schema: &Call, model: &mut ModelSpec) {
    for call in schema.block.iter().flatten() {
        match call.word.as_str() {
            "index" => model.indexes.push(call.args.iter().filter_map(|arg| text_of(&arg.item)).collect()),
            "ephemeral" => model.ephemeral.extend(call.block.iter().flatten().map(|call| build_ephemeral(call, model.key.as_str()))),
            _ => {
                let field = build_field(call, model.key.as_str());
                if field.name == "id" { model.fields.retain(|field| field.name != "id"); }
                model.fields.push(field);
            }
        }
    }
}

fn build_field(call: &Call, owner: &str) -> FieldSpec {
    let name = first_name(call);
    let mut field = match call.word.as_str() {
        "string" => FieldSpec::scalar(&name, ScalarKind::Str),
        "num" => FieldSpec::scalar(&name, ScalarKind::Num),
        "int" => FieldSpec::scalar(&name, ScalarKind::Int),
        "bool" => FieldSpec::scalar(&name, ScalarKind::Bool),
        "date" => FieldSpec::scalar(&name, ScalarKind::Date),
        "id" => FieldSpec::scalar(&name, ScalarKind::Id),
        "object" => FieldSpec::object(&name),
        "union" => FieldSpec::union(&name, "", Vec::new()),
        "array" if crate::unions::is_union_call(call) => FieldSpec::union_array(&name, "", Vec::new()),
        "array" => FieldSpec::array(&name),
        "raw" => FieldSpec::raw(&name),
        "file" => FieldSpec::file(&name),
        "enum" => {
            let values = list_of(call, "values");
            FieldSpec::enumeration(&name, &values.iter().map(String::as_str).collect::<Vec<_>>())
        }
        _ => {
            let element = symbol_of(call, "of");
            let mut list = FieldSpec::list(&name, scalar_kind(&element));
            if element == "enum" {
                list.values = list_of(call, "values");
            }
            list
        }
    };
    field.nullable = flag(call, "null");
    field.optional = flag(call, "optional");
    field.indexed = flag(call, "index");
    field.searchable = flag(call, "searchable");
    field.id_from_parent = option(call, "id_from").and_then(|pair| text_of(&pair.value.item));
    field.app_type = option(call, "as").and_then(|pair| text_of(&pair.value.item));
    if (matches!(field.kind, FieldKind::Enum { .. }) || !field.values.is_empty()) && field.app_type.is_none() {
        field.app_type = Some(format!("{owner}{}", camel(&name)));
    }
    if let Some(block) = &call.block {
        let type_name = field.app_type.clone().unwrap_or_else(|| format!("{owner}{}", camel(&name)));
        field.fields = match field.kind {
            FieldKind::Union | FieldKind::UnionArray => block.iter().filter(|call| call.word == "variant").zip(crate::unions::cases_of(block)).map(|(variant, case)| build_variant(variant, &case)).collect(),
            _ => block.iter().map(|child| build_field(child, &type_name)).collect(),
        };
        if matches!(field.kind, FieldKind::Union | FieldKind::UnionArray) {
            field.shared = block.iter().filter(|call| call.word == "shared").flat_map(|call| call.block.iter().flatten()).map(|child| build_field(child, &type_name)).collect();
        }
        field.app_type = Some(type_name);
    }
    field.from = option(call, "from").and_then(|pair| text_of(&pair.value.item));
    field.default = option(call, "default").map(|pair| json_of(&pair.value.item));
    field.merge = option(call, "merge").and_then(|pair| merge_of(&pair.value.item));
    if flag(call, "local") {
        field.merge = Some(MergeRule::Local);
    }
    field
}

fn build_variant(call: &Call, case: &str) -> FieldSpec {
    let type_name = text_of(&call.args[0].item).unwrap_or_default();
    let fields = call.block.iter().flatten().map(|child| build_field(child, &type_name)).collect();
    FieldSpec::variant(case, &type_name, fields)
}

fn merge_of(value: &Value) -> Option<MergeRule> {
    match value {
        Value::Symbol(name) => match name.as_str() {
            "present" => Some(MergeRule::Present),
            "non_empty" => Some(MergeRule::NonEmpty),
            "positive" => Some(MergeRule::Positive),
            "monotonic" => Some(MergeRule::Monotonic),
            "local" => Some(MergeRule::Local),
            _ => None,
        },
        Value::Array(tiers) => Some(MergeRule::Ladder {
            tiers: tiers.iter().filter_map(|tier| match &tier.item { Value::Array(names) => Some(names.iter().filter_map(|name| text_of(&name.item)).collect()), _ => None }).collect(),
        }),
        Value::Hash(pairs) => Some(MergeRule::Keys { keys: pairs.iter().filter_map(|pair| merge_of(&pair.value.item).map(|rule| (pair.key.clone(), rule))).collect() }),
        _ => None,
    }
}

fn build_ephemeral(call: &Call, owner: &str) -> EphemeralSpec {
    EphemeralSpec {
        field: build_field(call, owner),
        reset_after: option(call, "reset_after").and_then(|pair| text_of(&pair.value.item)),
    }
}

fn scalar_kind(name: &str) -> ScalarKind {
    match name {
        "num" => ScalarKind::Num,
        "int" => ScalarKind::Int,
        "bool" => ScalarKind::Bool,
        "date" => ScalarKind::Date,
        "id" => ScalarKind::Id,
        _ => ScalarKind::Str,
    }
}

pub(crate) fn camel(name: &str) -> String {
    name.split('_').filter(|part| !part.is_empty()).map(|part| {
        let mut chars = part.chars();
        chars.next().map(|first| first.to_uppercase().collect::<String>() + chars.as_str()).unwrap_or_default()
    }).collect()
}

fn lower(name: &str) -> String {
    let mut chars = name.chars();
    chars.next().map(|first| first.to_lowercase().collect::<String>() + chars.as_str()).unwrap_or_default()
}

pub(crate) fn association_key(call: &Call, owner: &str) -> String {
    option(call, "key").and_then(|pair| text_of(&pair.value.item)).unwrap_or_else(|| match call.word.as_str() {
        "belongs_to" => format!("{}Id", first_name(call)),
        "references" => first_name(call),
        _ => format!("{}Id", lower(owner)),
    })
}

pub(crate) fn association_target(call: &Call) -> String {
    let name = first_name(call);
    option(call, "model").and_then(|pair| text_of(&pair.value.item)).unwrap_or_else(|| {
        let singular = match call.word.as_str() {
            "has_many" | "references" => name.strip_suffix('s').unwrap_or(&name),
            _ => &name,
        };
        camel(singular)
    })
}

fn build_association(call: &Call, kind: AssociationKind, owner: &str) -> AssociationSpec {
    let name = first_name(call);
    let target = association_target(call);
    let key = association_key(call, owner);
    let mut association = AssociationSpec::new(&name, kind, &target, &key);
    association.touches_parent = flag(call, "touch");
    association.sets = hash_texts(call, "sets").into_iter().collect();
    association.newer = hash_texts(call, "newer").into_iter().map(|(child, parent)| Newer { child, parent }).collect();
    association.from = option(call, "from").and_then(|pair| text_of(&pair.value.item));
    association.dependent = option(call, "dependent").map(|_| Dependent::Destroy);
    association
}

fn build_scope(call: &Call) -> RelationSpec {
    let columns: Vec<String> = match option(call, "where").map(|pair| &pair.value.item) {
        Some(Value::Hash(pairs)) => pairs.iter().map(|pair| pair.key.clone()).collect(),
        _ => Vec::new(),
    };
    let mut relation = RelationSpec::new(&first_name(call), &columns.iter().map(String::as_str).collect::<Vec<_>>());
    relation.is_global = columns.is_empty();
    relation.updates = call.block.iter().flatten().filter(|candidate| candidate.word == "updates").map(updates_spec).collect();
    if let Some(Value::Hash(pairs)) = option(call, "order").map(|pair| &pair.value.item) {
        relation.sort = Sort::Fields(pairs.iter().map(sort_field).collect());
    }
    relation
}

fn sort_field(pair: &Pair) -> SortField {
    let direction = match &pair.value.item {
        Value::Symbol(name) => SortDirection::parse(name).unwrap_or(SortDirection::Asc),
        _ => SortDirection::Asc,
    };
    SortField { column: pair.key.clone(), direction }
}

fn variables_of(call: &Call) -> std::collections::BTreeMap<String, VariableSource> {
    call.options.iter().map(|pair| {
        let source = match &pair.value.item {
            Value::Symbol(name) => VariableSource::Param { name: name.clone() },
            other => VariableSource::Constant { value: json_of(other) },
        };
        (pair.key.clone(), source)
    }).collect()
}

fn cursor_of(paginate: &Call) -> Option<CursorSpec> {
    let variable = option(paginate, "cursor").and_then(|pair| text_of(&pair.value.item))?;
    let from = option(paginate, "from").and_then(|pair| text_of(&pair.value.item))?;
    let numeric = option(paginate, "cursor_type").and_then(|pair| text_of(&pair.value.item)).is_some_and(|kind| kind == "int");
    Some(CursorSpec { variable, from, numeric })
}

fn build_remote(call: &Call, owner: &str) -> RelationSpec {
    let lines = call.block.as_deref().unwrap_or_default();
    let line = |word: &str| lines.iter().find(|candidate| candidate.word == word);
    let shape = match (line("paginate"), line("list")) {
        (Some(paginate), _) => RemoteShape::Connection {
            direction: if flag(paginate, "backward") { PageDirection::Backward } else { PageDirection::Forward },
            coverage: Coverage::Page,
            cursor: cursor_of(paginate),
            max_pages: line("max_pages").and_then(|pages| pages.args.first()).and_then(|arg| match arg.item {
                Value::Int(count) => u32::try_from(count).ok(),
                _ => None,
            }),
        },
        (None, Some(_)) => RemoteShape::List { coverage: Coverage::Complete },
        (None, None) => RemoteShape::Single,
    };
    let fresh = line("fresh_for");
    let page_size = line("paginate").and_then(|paginate| paginate.args.first()).and_then(|arg| match arg.item {
        Value::Int(size) => u32::try_from(size).ok(),
        _ => None,
    });
    let remote = RemoteSpec {
        operation: line("query").and_then(|query| query.args.first()).and_then(|arg| text_of(&arg.item)).unwrap_or_else(|| format!("{owner}s{}", camel(&first_name(call)))),
        selects_page_info: matches!(shape, RemoteShape::Connection { .. }),
        shape,
        stale_time: fresh.and_then(|fresh| fresh.args.first()).and_then(|arg| text_of(&arg.item)).unwrap_or(DEFAULT_FRESHNESS.to_string()),
        empty_stale_time: fresh.and_then(|fresh| option(fresh, "empty")).and_then(|pair| text_of(&pair.value.item)),
        page_size,
        variables: line("variables").map(variables_of).unwrap_or_default(),
        root: line("query").and_then(|query| option(query, "root")).and_then(|pair| text_of(&pair.value.item)),
        total_from: line("total").and_then(|total| option(total, "from")).and_then(|pair| text_of(&pair.value.item)),
    };
    let columns: Vec<String> = match option(call, "params").map(|pair| &pair.value.item) {
        Some(Value::Array(items)) => items.iter().filter_map(|item| text_of(&item.item)).collect(),
        Some(single) => text_of(single).into_iter().collect(),
        None => Vec::new(),
    };
    let mut relation = RelationSpec::new(&first_name(call), &columns.iter().map(String::as_str).collect::<Vec<_>>());
    if let Some(Value::Array(items)) = option(call, "text").map(|pair| &pair.value.item) {
        for item in items {
            match &item.item {
                Value::Hash(pairs) => for pair in pairs {
                    relation.text_params.push(pair.key.clone());
                    if let Some(kind) = text_of(&pair.value.item) { relation.text_types.insert(pair.key.clone(), kind); }
                },
                other => relation.text_params.extend(text_of(other)),
            }
        }
    }
    if let Some(Value::Hash(pairs)) = option(call, "defaults").map(|pair| &pair.value.item) {
        for pair in pairs {
            if let Some(value) = text_of(&pair.value.item) { relation.text_defaults.insert(pair.key.clone(), value); }
        }
    }
    relation.is_global = columns.is_empty() && relation.text_params.is_empty();
    relation.node = match option(call, "node").and_then(|pair| text_of(&pair.value.item)).as_deref() {
        Some("one") => Some(NodeRead::One),
        Some("many") => Some(NodeRead::Many),
        _ => None,
    };
    if let Some(Value::Hash(pairs)) = option(call, "order").map(|pair| &pair.value.item) {
        relation.sort = Sort::Fields(pairs.iter().map(sort_field).collect());
    }
    let joins = option(call, "joins").and_then(|pair| text_of(&pair.value.item));
    relation.joins_events = joins.as_deref() == Some("events");
    relation.joins_created = joins.as_deref() == Some("created");
    relation.order_stable = flag(call, "order_stable");
    relation.remote = Some(remote);
    relation.updates = lines.iter().filter(|candidate| candidate.word == "updates").map(updates_spec).collect();
    relation.edge_fields = lines.iter().filter(|candidate| candidate.word == "edge").flat_map(|edge| edge.block.iter().flatten()).map(|field| build_field(field, owner)).collect();
    relation
}

fn updates_spec(updates: &Call) -> UpdatesSpec {
    UpdatesSpec {
        subscription: first_name(updates),
        deleted_by: option(updates, "deleted_by").and_then(|pair| text_of(&pair.value.item)),
        set: match option(updates, "set").map(|pair| &pair.value.item) {
            Some(Value::Hash(pairs)) => pairs.iter().map(|pair| (pair.key.clone(), json_of(&pair.value.item))).collect(),
            _ => Map::new(),
        },
        removes: flag(updates, "removes"),
        debounce: option(updates, "debounce").and_then(|pair| text_of(&pair.value.item)),
        root: option(updates, "root").and_then(|pair| text_of(&pair.value.item)),
        id: option(updates, "id").and_then(|pair| text_of(&pair.value.item)),
    }
}

fn build_standard_action(call: &Call) -> ActionDeclaration {
    let mut action = match call.word.as_str() {
        "create" => ActionDeclaration::new("create", RootShape::Upsert),
        "update" => ActionDeclaration::new("update", RootShape::Upsert),
        _ => ActionDeclaration::new("destroy", RootShape::Destroy),
    };
    action.has_optimistic = call.word == "create";
    action.has_temp_row_retention = call.word == "create";
    action.mutation = Some(symbol_of(call, "with"));
    action.on_failure = on_failure_of(&symbol_of(call, "on_failure"));
    action.ignore_response = option(call, "response").and_then(|pair| text_of(&pair.value.item)).as_deref() == Some("ignore");
    action.refetch = list_of(call, "refetch");
    action.local = list_of(call, "local");
    action.correlate = list_of(call, "correlate");
    action.wrap = option(call, "wrap").and_then(|pair| text_of(&pair.value.item));
    action.root = option(call, "root").and_then(|pair| text_of(&pair.value.item));
    if let Some(Value::Hash(pairs)) = option(call, "input").map(|pair| &pair.value.item) {
        action.variables = pairs.iter().filter_map(variable_binding).collect();
    }
    if let Some(Value::Hash(pairs)) = option(call, "args").map(|pair| &pair.value.item) {
        action.args = pairs.iter().filter_map(variable_binding).collect();
    }
    action
}

fn build_action(call: &Call, owner: &str) -> ActionDeclaration {
    let mut action = ActionDeclaration::new(&first_name(call), RootShape::Upsert);
    action.mutation = option(call, "mutation").and_then(|pair| text_of(&pair.value.item));
    if let Some(Value::Hash(pairs)) = option(call, "optimistic").map(|pair| &pair.value.item) {
        action.optimistic = pairs.iter().map(|pair| (pair.key.clone(), json_of(&pair.value.item))).collect();
    }
    for line in call.block.iter().flatten() {
        match line.word.as_str() {
            "mutation" => action.mutation = Some(first_name(line)),
            "upload" => { action.mutation = Some(first_name(line)); action.upload = true; }
            "input" => action.variables = line.options.iter().filter_map(variable_binding).collect(),
            "optimistic" => action.optimistic = line.options.iter().map(|pair| (pair.key.clone(), json_of(&pair.value.item))).collect(),
            "on_failure" => action.on_failure = on_failure_of(&first_name(line)),
            "args" => action.args = line.options.iter().filter_map(variable_binding).collect(),
            "result" => action.result = line.options.iter().filter_map(variable_binding).collect(),
            "response" => action.ignore_response = first_name(line) == "ignore",
            "wrap" => action.wrap = Some(first_name(line)),
            "root" => action.root = Some(first_name(line)),
            "refetch_of" => action.refetch.push(format!("{}.{}", first_name(line), option(line, "relation").and_then(|pair| text_of(&pair.value.item)).unwrap_or_default())),
            "lands" => action.lands.push(ActionLanding { model: first_name(line), root: option(line, "root").and_then(|pair| text_of(&pair.value.item)).unwrap_or_default() }),
            "refetch" => action.refetch = line.args.iter().filter_map(|arg| text_of(&arg.item)).collect(),
            "correlate" => action.correlate = line.args.iter().filter_map(|arg| text_of(&arg.item)).collect(),
            _ => {}
        }
    }
    for value in action.optimistic.values_mut() {
        if let Json::String(name) = value {
            if action.args.contains_key(name.as_str()) { *value = serde_json::json!({ "$arg": name.clone() }); }
        } else if let Json::Object(op) = value {
            if let Some(Json::Object(element)) = op.get_mut("element") {
                for part in element.values_mut() {
                    if let Json::String(name) = part {
                        if action.args.contains_key(name.as_str()) { *part = serde_json::json!({ "$arg": name.clone() }); }
                    }
                }
            }
            *value = serde_json::json!({ "$keyed": value.clone() });
        }
    }
    if action.variables.is_empty() { action.variables.insert(format!("{}Id", lower(owner)), "id".into()); }
    action
}

fn on_failure_of(policy: &str) -> OnFailure {
    if policy == "keep" {
        OnFailure::Keep
    } else {
        OnFailure::Rollback
    }
}

fn variable_binding(pair: &Pair) -> Option<(String, String)> {
    let kind = match &pair.value.item {
        Value::Array(_) | Value::OptionalArray(_) => crate::operation::arg_kind_text(&pair.value.item),
        other => text_of(other),
    };
    kind.map(|field| (pair.key.clone(), field))
}

fn option<'a>(call: &'a Call, name: &str) -> Option<&'a Pair> {
    call.options.iter().find(|pair| pair.key == name)
}

fn hash_texts(call: &Call, name: &str) -> Vec<(String, String)> {
    match option(call, name).map(|pair| &pair.value.item) {
        Some(Value::Hash(pairs)) => pairs.iter().filter_map(|pair| text_of(&pair.value.item).map(|text| (pair.key.clone(), text))).collect(),
        _ => Vec::new(),
    }
}

fn flag(call: &Call, name: &str) -> bool {
    option(call, name).is_some_and(|pair| pair.value.item == Value::Bool(true))
}

fn first_name(call: &Call) -> String {
    call.args.first().and_then(|arg| text_of(&arg.item)).unwrap_or_default()
}

fn symbol_of(call: &Call, name: &str) -> String {
    option(call, name).and_then(|pair| text_of(&pair.value.item)).unwrap_or_default()
}

fn list_of(call: &Call, name: &str) -> Vec<String> {
    match option(call, name).map(|pair| &pair.value.item) {
        Some(Value::Array(items)) => items.iter().filter_map(|item| text_of(&item.item)).collect(),
        _ => Vec::new(),
    }
}

fn text_of(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(text) | Value::Str(text) | Value::Const(text) => Some(text.clone()),
        Value::Duration { amount, unit } => Some(format!("{amount}.{unit}")),
        _ => None,
    }
}

fn json_of(value: &Value) -> Json {
    match value {
        Value::Int(number) => Json::from(*number),
        Value::Float(number) => Number::from_f64(*number).map_or(Json::Null, Json::Number),
        Value::Bool(flag) => Json::Bool(*flag),
        Value::Nil => Json::Null,
        Value::Array(items) => Json::Array(items.iter().map(|item: &Node<Value>| json_of(&item.item)).collect()),
        Value::Hash(pairs) => Json::Object(pairs.iter().map(|pair| (pair.key.clone(), json_of(&pair.value.item))).collect::<Map<_, _>>()),
        other => text_of(other).map_or(Json::Null, Json::String),
    }
}

#[cfg(test)]
mod build_tests;
