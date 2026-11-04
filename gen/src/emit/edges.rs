use std::fmt::Write;

use dblayer::schema::{FieldKind, FieldSpec, ModelSpec, RelationSpec};
use dblayer::value::ScalarKind;

use super::{field_type, identifier, name, text_param};
use crate::EnumDecl;

fn pascal(text: &str) -> String {
    let mut chars = text.chars();
    chars.next().map(|first| first.to_uppercase().collect::<String>() + chars.as_str()).unwrap_or_default()
}

fn read(field: &FieldSpec, swift: bool) -> String {
    let key = &field.name;
    let enum_type = field.app_type.as_deref().unwrap_or_default();
    match (&field.kind, swift) {
        (FieldKind::Scalar { kind: ScalarKind::Int }, true) => format!("values[\"{key}\"]?.intValue"),
        (FieldKind::Scalar { kind: ScalarKind::Num }, true) => format!("values[\"{key}\"]?.doubleValue"),
        (FieldKind::Scalar { kind: ScalarKind::Bool }, true) => format!("values[\"{key}\"]?.boolValue"),
        (FieldKind::Enum { .. }, true) => format!("values[\"{key}\"]?.stringValue.map({enum_type}.init(rawValue:))"),
        (_, true) => format!("values[\"{key}\"]?.stringValue"),
        (FieldKind::Scalar { kind: ScalarKind::Int }, false) => format!("(values[\"{key}\"] as? JsonPrimitive)?.longOrNull"),
        (FieldKind::Scalar { kind: ScalarKind::Num }, false) => format!("(values[\"{key}\"] as? JsonPrimitive)?.doubleOrNull"),
        (FieldKind::Scalar { kind: ScalarKind::Bool }, false) => format!("(values[\"{key}\"] as? JsonPrimitive)?.booleanOrNull"),
        (FieldKind::Enum { .. }, false) => format!("(values[\"{key}\"] as? JsonPrimitive)?.contentOrNull?.let {{ {enum_type}.of(it) }}"),
        (_, false) => format!("(values[\"{key}\"] as? JsonPrimitive)?.contentOrNull"),
    }
}

fn type_name(model: &ModelSpec, relation: &RelationSpec) -> String {
    format!("{}{}Edge", model.key.as_str(), pascal(&name(&relation.name)))
}

fn parameters(model: &ModelSpec, relation: &RelationSpec, decls: &[EnumDecl], swift: bool) -> (String, String) {
    let columns = relation.by_columns.iter().map(|key| (key.as_str(), model.field(key).map(|field| field_type(field, swift)).unwrap_or("String".into())));
    let all: Vec<(String, String)> = columns.map(|(key, kind)| {
        let key = identifier(key, swift);
        (if swift { format!("_ {key}: {kind}") } else { format!("{key}: {kind}") }, key)
    }).chain(relation.text_params.iter().map(|key| {
        let printed = identifier(key, swift);
        (text_param(relation, key, decls, swift), if swift && (relation.text_types.contains_key(key) || relation.scalar_types.contains_key(key)) { format!("{key}: {printed}") } else { printed })
    })).collect();
    let declared = all.iter().map(|(parameter, _)| format!("{parameter}, ")).collect::<String>();
    let passed = all.iter().map(|(_, argument)| argument.as_str()).collect::<Vec<_>>().join(", ");
    (declared, passed)
}

pub(super) fn swift_accessors(out: &mut String, types: &mut String, model: &ModelSpec, relation: &RelationSpec, decls: &[EnumDecl]) {
    if relation.edge_fields.is_empty() {
        return;
    }
    let (method, class) = (name(&relation.name), type_name(model, relation));
    let (declared, passed) = parameters(model, relation, decls, true);
    let printed = identifier(&method, true);
    let query = if passed.is_empty() { format!("Self.{printed}") } else { format!("Self.{printed}({passed})") };
    writeln!(out, "    @concurrent public static func {method}Edge({declared}of id: String, in scope_: AccountScope? = nil) async throws -> {class}? {{ try await {query}.edgeValues(of: id, in: scope_).map({class}.init) }}").unwrap();
    writeln!(types, "public struct {class}: Sendable, Equatable {{").unwrap();
    for field in &relation.edge_fields {
        writeln!(types, "    public let {}: {}", identifier(&field.name, true), field_type(field, true)).unwrap();
    }
    writeln!(types, "    init(_ values: [String: JSONValue]) {{").unwrap();
    for field in &relation.edge_fields {
        writeln!(types, "        self.{} = {}", identifier(&field.name, true), read(field, true)).unwrap();
    }
    types.push_str("    }\n}\n\n");
}

pub(super) fn kotlin_accessors(out: &mut String, types: &mut String, model: &ModelSpec, relation: &RelationSpec, decls: &[EnumDecl]) {
    if relation.edge_fields.is_empty() {
        return;
    }
    let (method, class) = (name(&relation.name), type_name(model, relation));
    let (declared, passed) = parameters(model, relation, decls, false);
    let printed = identifier(&method, false);
    let query = if passed.is_empty() { format!("{}.{printed}", model.key.as_str()) } else { format!("{}.{printed}({passed})", model.key.as_str()) };
    writeln!(out, "        suspend fun {method}Edge({declared}id: String, account_: AccountScope? = null): {class}? = {query}.edgeValues(id, account_)?.let {{ {class}(it) }}").unwrap();
    let fields: Vec<String> = relation.edge_fields.iter().map(|field| format!("    val {}: {} = null", identifier(&field.name, false), field_type(field, false))).collect();
    writeln!(types, "@Immutable\ndata class {class}(\n{}\n) {{", fields.join(",\n")).unwrap();
    let reads: Vec<String> = relation.edge_fields.iter().map(|field| read(field, false)).collect();
    writeln!(types, "    internal constructor(values: JsonObject) : this({})\n}}\n", reads.join(", ")).unwrap();
}
