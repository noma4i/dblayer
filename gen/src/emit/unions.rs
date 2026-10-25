use std::fmt::Write;

use dblayer::schema::FieldSpec;

use super::nested::string_literal;
use super::{field_type, identifier};
use crate::enums::EnumDecl;

fn cases(declaration: &FieldSpec, swift: bool) -> Vec<(String, String, &str)> {
    let names: Vec<String> = declaration.fields.iter().map(|variant| variant.name.clone()).collect();
    let rendered = EnumDecl::new(declaration.app_type.as_deref().unwrap_or_default(), &names).cases(swift);
    declaration.fields.iter().zip(rendered).map(|(variant, case)| (case, variant.app_type.clone().unwrap_or_default(), variant.name.as_str())).collect()
}

pub(super) fn emit_swift_union(out: &mut String, declaration: &FieldSpec) {
    let name = declaration.app_type.as_deref().unwrap();
    let cases = cases(declaration, true);
    writeln!(out, "public enum {name}: Codable, Sendable, Equatable {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "    case {case}({variant})").unwrap(); }
    writeln!(out, "    case unknown(typename: String)\n\n    public init(from decoder: Decoder) throws {{\n        let typename = try decoder.container(keyedBy: UnionTypenameKey.self).decode(String.self, forKey: .typename)\n        switch typename {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "        case {}: self = .{case}(try {variant}(from: decoder))", string_literal(variant, true)).unwrap(); }
    writeln!(out, "        default: self = .unknown(typename: typename)\n        }}\n    }}\n\n    public func encode(to encoder: Encoder) throws {{\n        switch self {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "        case let .{case}(value): try JSONValue.tagged({}, value).encode(to: encoder)", string_literal(variant, true)).unwrap(); }
    writeln!(out, "        case let .unknown(typename): try JSONValue.tagged(typename, [String: String]()).encode(to: encoder)\n        }}\n    }}\n\n    public init(cells: [Cell]) throws {{\n        let r = CellReader(cells)\n        let typename = try r.req(0, \\.asText)\n        switch typename {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "        case {}: self = .{case}(try r.obj(1, {variant}.init(cells:)))", string_literal(variant, true)).unwrap(); }
    writeln!(out, "        default: self = .unknown(typename: typename)\n        }}\n    }}\n").unwrap();
    for field in &declaration.shared {
        let optional = field_type(field, true);
        let optional = if optional.ends_with('?') { optional } else { format!("{optional}?") };
        writeln!(out, "    public var {}: {optional} {{\n        switch self {{", identifier(&field.name, true)).unwrap();
        for (case, _, _) in &cases { writeln!(out, "        case let .{case}(value): return value.{}", identifier(&field.name, true)).unwrap(); }
        writeln!(out, "        case .unknown: return nil\n        }}\n    }}\n").unwrap();
    }
    writeln!(out, "    public var kind: {name}Kind? {{\n        switch self {{").unwrap();
    for (case, _, _) in &cases { writeln!(out, "        case .{case}: return .{case}").unwrap(); }
    writeln!(out, "        case .unknown: return nil\n        }}\n    }}\n}}\n\npublic enum {name}Kind: String, Codable, Sendable {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "    case {case} = {}", string_literal(variant, true)).unwrap(); }
    out.push_str("}\n\n");
}

pub(super) fn emit_kotlin_union(out: &mut String, declaration: &FieldSpec) {
    let name = declaration.app_type.as_deref().unwrap();
    let cases = cases(declaration, false);
    let inside = |case: &str, variant: &str| if case == variant { format!("com.noma4i.dblayer.models.{variant}") } else { variant.to_string() };
    writeln!(out, "@Immutable\n@Serializable(with = {name}Serializer::class)\nsealed class {name} {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "    @Immutable\n    data class {case}(val value: {}) : {name}()", inside(case, variant)).unwrap(); }
    writeln!(out, "    @Immutable\n    data class Unknown(val typename: String) : {name}()\n\n    val kind: {name}Kind?\n        get() = when (this) {{").unwrap();
    for (case, _, _) in &cases { writeln!(out, "            is {case} -> {name}Kind.{case}").unwrap(); }
    writeln!(out, "            is Unknown -> null\n        }}\n").unwrap();
    for field in &declaration.shared {
        let optional = field_type(field, false);
        let optional = if optional.ends_with('?') { optional } else { format!("{optional}?") };
        writeln!(out, "    val {}: {optional}\n        get() = when (this) {{", identifier(&field.name, false)).unwrap();
        for (case, _, _) in &cases { writeln!(out, "            is {case} -> value.{}", identifier(&field.name, false)).unwrap(); }
        writeln!(out, "            is Unknown -> null\n        }}\n").unwrap();
    }
    writeln!(out, "    companion object {{\n        fun fromCells(cells: List<Cell>): {name} {{\n            val r = CellReader(cells)\n            return when (val typename = r.req(0, Cell::asText)) {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "                {} -> {case}(r.obj(1) {{ {}.fromCells(it) }})", string_literal(variant, false), inside(case, variant)).unwrap(); }
    writeln!(out, "                else -> Unknown(typename)\n            }}\n        }}\n    }}\n}}\n\nobject {name}Serializer : UnionSerializer<{name}>(\"{name}\") {{\n    override fun read(typename: String, body: JsonObject): {name} = when (typename) {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "        {} -> {name}.{case}(dbJson.decodeFromJsonElement({variant}.serializer(), body))", string_literal(variant, false)).unwrap(); }
    writeln!(out, "        else -> {name}.Unknown(typename)\n    }}\n\n    override fun write(value: {name}): Pair<String, JsonElement> = when (value) {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "        is {name}.{case} -> {} to dbJson.encodeToJsonElement({variant}.serializer(), value.value)", string_literal(variant, false)).unwrap(); }
    writeln!(out, "        is {name}.Unknown -> value.typename to JsonObject(emptyMap())\n    }}\n}}\n\n@Serializable\nenum class {name}Kind {{").unwrap();
    for (case, variant, _) in &cases { writeln!(out, "    @SerialName({}) {case},", string_literal(variant, false)).unwrap(); }
    out.push_str("}\n\n");
}
