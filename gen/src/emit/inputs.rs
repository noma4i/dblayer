use std::fmt::Write;

use crate::schema_check::{InputField, InputObject, Ty, TyKind};

fn element(name: &str, swift: bool) -> String {
    match (name, swift) {
        ("string", _) => "String".to_string(),
        ("int", _) | ("long", true) => "Int".to_string(),
        ("long", false) => "Long".to_string(),
        ("num", _) => "Double".to_string(),
        ("bool", true) => "Bool".to_string(),
        ("bool", false) => "Boolean".to_string(),
        ("json", true) => "JSONValue".to_string(),
        ("json", false) => "JsonElement".to_string(),
        (other, _) => other.to_string(),
    }
}

fn field_type(field: &InputField, swift: bool) -> String {
    if field.ty.nullable {
        let mut ty = field.ty.clone();
        ty.nullable = false;
        ty.has_default = false;
        return format!("GraphQLNullable<{}>", type_name(&ty, swift));
    }
    type_name(&field.ty, swift)
}

fn type_name(ty: &Ty, swift: bool) -> String {
    let base = match &ty.kind {
        TyKind::Named(name) => element(name, swift),
        TyKind::List(item) if swift => format!("[{}]", type_name(item, swift)),
        TyKind::List(item) => format!("List<{}>", type_name(item, swift)),
    };
    if ty.optional() { format!("{base}?") } else { base }
}

pub(super) fn emit_swift_inputs(out: &mut String, inputs: &[InputObject]) {
    for input in inputs {
        writeln!(out, "public struct {}: Encodable, Sendable, Equatable {{", input.name).unwrap();
        for field in &input.fields { writeln!(out, "    public let {}: {}", field.name, field_type(field, true)).unwrap(); }
        let params = input.fields.iter().map(|field| format!("{}: {}{}", field.name, field_type(field, true), if field.ty.nullable { " = .omitted" } else if field.ty.has_default { " = nil" } else { "" })).collect::<Vec<_>>().join(", ");
        writeln!(out, "\n    public init({params}) {{").unwrap();
        for field in &input.fields { writeln!(out, "        self.{0} = {0}", field.name).unwrap(); }
        writeln!(out, "    }}\n\n    enum CodingKeys: String, CodingKey {{").unwrap();
        for field in &input.fields { writeln!(out, "        case {}", field.name).unwrap(); }
        writeln!(out, "    }}\n\n    public func encode(to encoder: any Encoder) throws {{\n        var container = encoder.container(keyedBy: CodingKeys.self)").unwrap();
        for field in &input.fields {
            let method = if !field.ty.nullable && field.ty.has_default { "encodeIfPresent" } else { "encode" };
            writeln!(out, "        try container.{method}({}, forKey: .{})", field.name, field.name).unwrap();
        }
        writeln!(out, "    }}\n}}\n").unwrap();
    }
}

pub(super) fn emit_kotlin_inputs(out: &mut String, inputs: &[InputObject]) {
    for input in inputs {
        let fields = input.fields.iter().map(|field| {
            if field.ty.optional() { format!("@EncodeDefault(EncodeDefault.Mode.NEVER) val {}: {} = {}", field.name, field_type(field, false), if field.ty.nullable { "GraphQLNullable.Omitted" } else { "null" }) }
            else { format!("val {}: {}", field.name, field_type(field, false)) }
        }).collect::<Vec<_>>().join(", ");
        writeln!(out, "@OptIn(ExperimentalSerializationApi::class)\n@Serializable\ndata class {}({fields})\n", input.name).unwrap();
    }
}
