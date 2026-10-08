use dblayer::schema::{
    AssociationKind, AssociationSpec, Coverage, Dependent, FieldSpec, ModelSpec, PageDirection, RelationSpec,
    RemoteShape, RemoteSpec, Sort, SortField,
};
use dblayer::value::{ScalarKind, SortDirection};
use serde_json::{Map, Number, Value as Json};

use crate::parser::{Call, ModelNode, Node, Pair, Value};

const DEFAULT_FRESHNESS: &str = "5.minutes";

pub fn build(nodes: &[ModelNode]) -> Vec<ModelSpec> {
    nodes.iter().map(build_model).collect()
}

fn build_model(node: &ModelNode) -> ModelSpec {
    let mut model = ModelSpec::new(&node.name, Vec::new());
    for call in &node.body {
        match call.word.as_str() {
            "schema" => build_schema(call, &mut model),
            "belongs_to" => model.associations.push(build_association(call, AssociationKind::BelongsTo)),
            "has_many" => model.associations.push(build_association(call, AssociationKind::HasMany)),
            "has_one" => model.associations.push(build_association(call, AssociationKind::HasOne)),
            "references" => model.associations.push(build_association(call, AssociationKind::References)),
            "scope" => model.relations.push(build_scope(call)),
            "remote" => model.relations.push(build_remote(call)),
            _ => {}
        }
    }
    model
}

fn build_schema(schema: &Call, model: &mut ModelSpec) {
    for call in schema.block.iter().flatten() {
        if call.word == "index" {
            model.composite_indexes.push(call.args.iter().filter_map(|arg| text_of(&arg.item)).collect());
        } else {
            model.fields.push(build_field(call));
        }
    }
}

fn build_field(call: &Call) -> FieldSpec {
    let name = first_name(call);
    let mut field = match call.word.as_str() {
        "string" => FieldSpec::scalar(&name, ScalarKind::Str),
        "num" => FieldSpec::scalar(&name, ScalarKind::Num),
        "int" => FieldSpec::scalar(&name, ScalarKind::Int),
        "bool" => FieldSpec::scalar(&name, ScalarKind::Bool),
        "date" => FieldSpec::scalar(&name, ScalarKind::Date),
        "id" => FieldSpec::scalar(&name, ScalarKind::Id),
        "object" => FieldSpec::object(&name),
        "array" => FieldSpec::array(&name),
        "raw" => FieldSpec::raw(&name),
        "file" => FieldSpec::file(&name),
        "enum" => {
            let values = list_of(call, "values");
            FieldSpec::enumeration(&name, &values.iter().map(String::as_str).collect::<Vec<_>>())
        }
        _ => FieldSpec::list(&name, scalar_kind(&symbol_of(call, "of"))),
    };
    field.nullable = flag(call, "null");
    field.optional = flag(call, "optional");
    field.indexed = flag(call, "index");
    field.id_from_parent = option(call, "id_from").and_then(|pair| text_of(&pair.value.item));
    field.default = option(call, "default").map(|pair| json_of(&pair.value.item));
    field
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

fn build_association(call: &Call, kind: AssociationKind) -> AssociationSpec {
    let mut association = AssociationSpec::new(&first_name(call), kind, &symbol_of(call, "model"), &symbol_of(call, "key"));
    association.touches_parent = flag(call, "touch");
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
    if let Some(Value::Hash(pairs)) = option(call, "order").map(|pair| &pair.value.item) {
        relation.sort = Sort::Fields(pairs.iter().map(sort_field).collect());
    }
    relation
}

fn sort_field(pair: &Pair) -> SortField {
    let descending = pair.value.item == Value::Symbol("desc".to_string());
    SortField { column: pair.key.clone(), direction: if descending { SortDirection::Desc } else { SortDirection::Asc } }
}

fn build_remote(call: &Call) -> RelationSpec {
    let lines = call.block.as_deref().unwrap_or_default();
    let line = |word: &str| lines.iter().find(|candidate| candidate.word == word);
    let shape = match (line("paginate"), line("list")) {
        (Some(paginate), _) => RemoteShape::Connection {
            direction: if flag(paginate, "backward") { PageDirection::Backward } else { PageDirection::Forward },
            coverage: Coverage::Page,
            max_pages: line("max_pages").and_then(|pages| pages.args.first()).and_then(|arg| match arg.item {
                Value::Int(count) => u32::try_from(count).ok(),
                _ => None,
            }),
        },
        (None, Some(_)) => RemoteShape::List { coverage: Coverage::Complete },
        (None, None) => RemoteShape::Single,
    };
    let fresh = line("fresh_for");
    let remote = RemoteSpec {
        operation: line("query").and_then(|query| query.args.first()).and_then(|arg| text_of(&arg.item)).unwrap_or_default(),
        selects_page_info: matches!(shape, RemoteShape::Connection { .. }),
        shape,
        stale_time: fresh.and_then(|fresh| fresh.args.first()).and_then(|arg| text_of(&arg.item)).unwrap_or(DEFAULT_FRESHNESS.to_string()),
        empty_stale_time: fresh.and_then(|fresh| option(fresh, "empty")).and_then(|pair| text_of(&pair.value.item)),
    };
    let mut relation = RelationSpec::global(&first_name(call));
    relation.remote = Some(remote);
    relation
}

fn option<'a>(call: &'a Call, name: &str) -> Option<&'a Pair> {
    call.options.iter().find(|pair| pair.key == name)
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
