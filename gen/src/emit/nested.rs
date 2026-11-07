use super::*;
use crate::enums::EnumDecl;
use serde_json::Value;

pub(super) fn string_literal(text: &str, swift: bool) -> String {
    if !swift { return serde_json::to_string(text).unwrap().replace('$', "${'$'}"); }
    let mut out = String::from("\"");
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if control.is_control() => write!(out, "\\u{{{:x}}}", control as u32).unwrap(),
            _ => out.push(character),
        }
    }
    out.push('"');
    out
}

fn json_literal(value: &Value, swift: bool) -> String {
    match value {
        Value::Null => if swift { ".null".into() } else { "JsonNull".into() },
        Value::Object(fields) => {
            let pairs = fields.iter().map(|(key, value)| format!("{}{}{}", string_literal(key, swift), if swift { ": " } else { " to " }, json_literal(value, swift))).collect::<Vec<_>>().join(", ");
            if swift { format!(".object([{}])", if pairs.is_empty() { ":" } else { &pairs }) } else { format!("JsonObject(mapOf({pairs}))") }
        }
        Value::Array(items) => {
            let items = items.iter().map(|item| json_literal(item, swift)).collect::<Vec<_>>().join(", ");
            if swift { format!(".array([{items}])") } else { format!("JsonArray(listOf({items}))") }
        }
        Value::String(text) => if swift { format!(".string({})", string_literal(text, true)) } else { format!("JsonPrimitive({})", string_literal(text, false)) },
        Value::Bool(flag) => if swift { format!(".bool({flag})") } else { format!("JsonPrimitive({flag})") },
        Value::Number(number) => if swift { format!(".{}({number})", if number.is_i64() { "integer" } else { "number" }) } else { format!("JsonPrimitive({number})") },
    }
}

fn enum_literal(field: &FieldSpec, values: &[String], text: &str, swift: bool) -> String {
    let name = field.app_type.as_deref().expect("an enum field names its type");
    let at = values.iter().position(|value| value == text).expect("an enum default is one of its values");
    format!("{name}.{}", EnumDecl::new(name, values).cases(swift)[at])
}

pub(super) fn literal(field: &FieldSpec, value: &Value, swift: bool, types: &[&FieldSpec]) -> String {
    if value.is_null() { return if swift { "nil".into() } else { "null".into() }; }
    match (&field.kind, value) {
        (FieldKind::Scalar { kind: ScalarKind::Num }, Value::Number(number)) => format!("{:?}", number.as_f64().unwrap()),
        (FieldKind::Scalar { kind: ScalarKind::Int }, Value::Number(number)) => format!("{}{}", number, if swift { "" } else { "L" }),
        (FieldKind::Scalar { kind: ScalarKind::Id }, _) => string_literal(value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string()).as_str(), swift),
        (FieldKind::Enum { values }, Value::String(text)) => enum_literal(field, values, text, swift),
        (FieldKind::Scalar { .. }, Value::String(text)) => string_literal(text, swift),
        (FieldKind::Scalar { .. }, Value::Bool(flag)) => flag.to_string(),
        (FieldKind::List { .. }, Value::Array(items)) if !field.values.is_empty() => {
            let element = FieldSpec { kind: FieldKind::Enum { values: field.values.clone() }, ..field.clone() };
            let items = items.iter().map(|item| literal(&element, item, swift, types)).collect::<Vec<_>>().join(", ");
            if swift { format!("[{items}]") } else { format!("listOf({items})") }
        }
        (FieldKind::List { element }, Value::Array(items)) => {
            let element = FieldSpec::scalar("", *element);
            let items = items.iter().map(|item| literal(&element, item, swift, types)).collect::<Vec<_>>().join(", ");
            if swift { format!("[{items}]") } else { format!("listOf({items})") }
        }
        (FieldKind::Array, Value::Array(items)) if field.app_type.is_some() => {
            let element = FieldSpec { kind: FieldKind::Object, ..field.clone() };
            let items = items.iter().map(|item| literal(&element, item, swift, types)).collect::<Vec<_>>().join(", ");
            if swift { format!("[{items}]") } else { format!("listOf({items})") }
        }
        (FieldKind::Object, Value::Object(values)) if field.app_type.is_some() => {
            let target = types.iter().find(|candidate| candidate.app_type == field.app_type).unwrap();
            let args = target.fields.iter().filter_map(|child| values.get(key_of(child)).map(|value| format!("{}{}{}", if swift { child.name.clone() } else { identifier(&child.name, false) }, if swift { ": " } else { " = " }, literal(child, value, swift, types)))).collect::<Vec<_>>().join(", ");
            format!("{}({args})", target.app_type.as_ref().unwrap())
        }
        _ => json_literal(value, swift),
    }
}

fn default_argument(field: &FieldSpec, swift: bool, types: &[&FieldSpec]) -> String {
    if let Some(value) = &field.default { return format!(" = {}", literal(field, value, swift, types)); }
    if is_nullable(field) { return if swift { " = nil".into() } else { " = null".into() }; }
    String::new()
}

pub(super) fn emit_swift_type(out: &mut String, declaration: &FieldSpec, types: &[&FieldSpec], cells: bool) {
    let name = declaration.app_type.as_deref().unwrap();
    let fields = &declaration.fields;
    writeln!(out, "public struct {name}: Codable, Sendable, Equatable {{").unwrap();
    for field in fields { writeln!(out, "    public let {}: {}", identifier(&field.name, true), field_type(field, true)).unwrap(); }
    let params = fields.iter().map(|field| format!("{}: {}{}", identifier(&field.name, true), field_type(field, true), default_argument(field, true, types))).collect::<Vec<_>>().join(", ");
    writeln!(out, "\n    public init({params}) {{").unwrap();
    for field in fields { writeln!(out, "        self.{0} = {0}", identifier(&field.name, true)).unwrap(); }
    out.push_str("    }\n\n    private enum CodingKeys: String, CodingKey {\n");
    for field in fields { writeln!(out, "        case {} = {}", identifier(&field.name, true), string_literal(key_of(field), true)).unwrap(); }
    out.push_str("    }\n");
    if cells {
        out.push_str("\n    public init(cells: [Cell]) throws {\n        let r = CellReader(cells)\n");
        for (at, field) in fields.iter().enumerate() { writeln!(out, "        self.{} = {}", identifier(&field.name, true), super::wire::read_swift(field, at)).unwrap(); }
        out.push_str("    }\n");
    }
    out.push_str("}\n\n");
}

pub(super) fn emit_kotlin_type(out: &mut String, declaration: &FieldSpec, types: &[&FieldSpec], cells: bool) {
    writeln!(out, "@Immutable\n@Serializable\ndata class {}(", declaration.app_type.as_ref().unwrap()).unwrap();
    for field in &declaration.fields {
        let mut notes = String::new();
        if key_of(field) != field.name { write!(notes, "@SerialName({}) ", string_literal(key_of(field), false)).unwrap(); }
        writeln!(out, "    {notes}val {}: {}{},", identifier(&field.name, false), field_type(field, false), default_argument(field, false, types)).unwrap();
    }
    if !cells {
        out.push_str(")\n\n");
        return;
    }
    out.push_str(") {\n    companion object {\n        fun fromCells(cells: List<Cell>): ");
    write!(out, "{} {{\n            val r = CellReader(cells)\n            return {}(\n", declaration.app_type.as_ref().unwrap(), declaration.app_type.as_ref().unwrap()).unwrap();
    for (at, field) in declaration.fields.iter().enumerate() { writeln!(out, "                {} = {},", identifier(&field.name, false), super::wire::read_kotlin(field, at)).unwrap(); }
    out.push_str("            )\n        }\n    }\n}\n\n");
}
