use std::fmt::Write;

use crate::schema_check::{InputField, InputObject};

fn element(field: &InputField, swift: bool) -> String {
    match (field.base.as_str(), swift) {
        ("string", _) => "String".to_string(),
        ("int", _) => "Int".to_string(),
        ("num", _) => "Double".to_string(),
        ("bool", true) => "Bool".to_string(),
        ("bool", false) => "Boolean".to_string(),
        ("json", true) => "JSONValue".to_string(),
        ("json", false) => "JsonElement".to_string(),
        (other, _) => other.to_string(),
    }
}

fn field_type(field: &InputField, swift: bool) -> String {
    let item = format!("{}{}", element(field, swift), if field.item_optional { "?" } else { "" });
    let base = match (field.list, swift) {
        (false, _) => item,
        (true, true) => format!("[{item}]"),
        (true, false) => format!("List<{item}>"),
    };
    if field.optional { format!("{base}?") } else { base }
}

pub(super) fn emit_swift_inputs(out: &mut String, inputs: &[InputObject]) {
    for input in inputs {
        writeln!(out, "public struct {}: Encodable, Sendable {{", input.name).unwrap();
        for field in &input.fields { writeln!(out, "    public let {}: {}", field.name, field_type(field, true)).unwrap(); }
        let params = input.fields.iter().map(|field| format!("{}: {}{}", field.name, field_type(field, true), if field.optional { " = nil" } else { "" })).collect::<Vec<_>>().join(", ");
        writeln!(out, "\n    public init({params}) {{").unwrap();
        for field in &input.fields { writeln!(out, "        self.{0} = {0}", field.name).unwrap(); }
        writeln!(out, "    }}\n}}\n").unwrap();
    }
}

pub(super) fn emit_kotlin_inputs(out: &mut String, inputs: &[InputObject]) {
    for input in inputs {
        let fields = input.fields.iter().map(|field| {
            if field.optional { format!("@EncodeDefault(EncodeDefault.Mode.NEVER) val {}: {} = null", field.name, field_type(field, false)) }
            else { format!("val {}: {}", field.name, field_type(field, false)) }
        }).collect::<Vec<_>>().join(", ");
        writeln!(out, "@OptIn(ExperimentalSerializationApi::class)\n@Serializable\ndata class {}({fields})\n", input.name).unwrap();
    }
}
