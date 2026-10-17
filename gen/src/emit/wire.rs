use super::nested::literal;
use super::*;

enum Shape<'a> {
    Scalar(&'static str),
    Enum(&'a str),
    List(&'static str),
    EnumList(&'a str),
    Object(&'a str),
    Array(&'a str),
    Json,
}

fn converter(kind: ScalarKind) -> &'static str {
    match kind {
        ScalarKind::Bool => "asBool",
        ScalarKind::Int => "asInt",
        ScalarKind::Num => "asDouble",
        ScalarKind::Id => "asId",
        _ => "asText",
    }
}

fn shape(field: &FieldSpec) -> Shape<'_> {
    match (&field.kind, field.app_type.as_deref()) {
        (FieldKind::Scalar { kind }, _) => Shape::Scalar(converter(*kind)),
        (FieldKind::Enum { .. }, Some(name)) => Shape::Enum(name),
        (FieldKind::List { .. }, Some(name)) if !field.values.is_empty() => Shape::EnumList(name),
        (FieldKind::List { element }, _) => Shape::List(converter(*element)),
        (FieldKind::Object | FieldKind::Union, Some(name)) => Shape::Object(name),
        (FieldKind::Array | FieldKind::UnionArray, Some(name)) => Shape::Array(name),
        _ => Shape::Json,
    }
}

pub(super) fn read_swift(field: &FieldSpec, at: usize, types: &[&FieldSpec]) -> String {
    let optional = is_nullable(field) || field.default.is_some();
    let (method, argument) = match shape(field) {
        Shape::Enum(name) => {
            let read = if optional { format!("r.opt({at}, \\.asText).map({name}.init(rawValue:))") } else { format!("{name}(rawValue: r.req({at}, \\.asText))") };
            let fallback = field.default.as_ref().filter(|value| !value.is_null()).map(|value| format!(" ?? {}", literal(field, value, true, types))).unwrap_or_default();
            return format!("try {read}{fallback}");
        }
        Shape::EnumList(name) => {
            let read = if optional { format!("r.listOpt({at}, \\.asText)?.map({name}.init(rawValue:))") } else { format!("r.list({at}, \\.asText).map({name}.init(rawValue:))") };
            let fallback = field.default.as_ref().filter(|value| !value.is_null()).map(|value| format!(" ?? {}", literal(field, value, true, types))).unwrap_or_default();
            return format!("try {read}{fallback}");
        }
        Shape::Scalar(convert) => ("", format!("\\.{convert}")),
        Shape::List(convert) => ("list", format!("\\.{convert}")),
        Shape::Object(name) => ("obj", format!("{name}.init(cells:)")),
        Shape::Array(name) => ("arr", format!("{name}.init(cells:)")),
        Shape::Json => ("json", String::new()),
    };
    let call = match (method, optional) {
        ("", false) => format!("req({at}, {argument})"),
        ("", true) => format!("opt({at}, {argument})"),
        ("json", false) => format!("json({at})"),
        ("json", true) => format!("jsonOpt({at})"),
        (other, false) => format!("{other}({at}, {argument})"),
        (other, true) => format!("{other}Opt({at}, {argument})"),
    };
    let fallback = field.default.as_ref().filter(|value| !value.is_null()).map(|value| format!(" ?? {}", literal(field, value, true, types))).unwrap_or_default();
    format!("try r.{call}{fallback}")
}

pub(super) fn read_kotlin(field: &FieldSpec, at: usize, types: &[&FieldSpec]) -> String {
    let optional = is_nullable(field) || field.default.is_some();
    let call = match (shape(field), optional) {
        (Shape::Enum(name), false) => format!("req({at}, Cell::asText).let {{ {name}.of(it) }}"),
        (Shape::Enum(name), true) => format!("opt({at}, Cell::asText)?.let {{ {name}.of(it) }}"),
        (Shape::EnumList(name), false) => format!("list({at}, Cell::asText).map {{ {name}.of(it) }}"),
        (Shape::EnumList(name), true) => format!("listOpt({at}, Cell::asText)?.map {{ {name}.of(it) }}"),
        (Shape::Scalar(convert), false) => format!("req({at}, Cell::{convert})"),
        (Shape::Scalar(convert), true) => format!("opt({at}, Cell::{convert})"),
        (Shape::List(convert), false) => format!("list({at}, Cell::{convert})"),
        (Shape::List(convert), true) => format!("listOpt({at}, Cell::{convert})"),
        (Shape::Object(name), false) => format!("obj({at}) {{ {name}.fromCells(it) }}"),
        (Shape::Object(name), true) => format!("objOpt({at}) {{ {name}.fromCells(it) }}"),
        (Shape::Array(name), false) => format!("arr({at}) {{ {name}.fromCells(it) }}"),
        (Shape::Array(name), true) => format!("arrOpt({at}) {{ {name}.fromCells(it) }}"),
        (Shape::Json, false) => format!("json({at})"),
        (Shape::Json, true) => format!("jsonOpt({at})"),
    };
    let fallback = field.default.as_ref().filter(|value| !value.is_null()).map(|value| format!(" ?: {}", literal(field, value, false, types))).unwrap_or_default();
    format!("r.{call}{fallback}")
}

pub(super) fn wired<'a>(model: &'a ModelSpec) -> Vec<&'a FieldSpec> {
    dblayer::ffi::wire::wire_fields(model)
}

pub(super) fn layout_of(models: &[ModelSpec], model: &ModelSpec) -> String {
    let mut resolved = models.to_vec();
    dblayer::schema::resolve_types(&mut resolved);
    resolved.iter().find(|candidate| candidate.key == model.key).map(dblayer::ffi::wire::wire_layout).unwrap_or_default()
}
