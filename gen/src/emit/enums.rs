use std::fmt::Write;

use crate::enums::EnumDecl;
use super::nested::string_literal;

pub(super) fn emit_swift_enums(out: &mut String, enums: &[EnumDecl]) {
    for declared in enums {
        let cases = declared.cases(true);
        writeln!(out, "public enum {}: Codable, Sendable, Hashable {{", declared.name).unwrap();
        for case in &cases { writeln!(out, "    case {case}").unwrap(); }
        writeln!(out, "    case unknown(String)\n\n    public init(rawValue: String) {{\n        switch rawValue {{").unwrap();
        for (case, value) in cases.iter().zip(&declared.values) { writeln!(out, "        case {}: self = .{case}", string_literal(value, true)).unwrap(); }
        writeln!(out, "        default: self = .unknown(rawValue)\n        }}\n    }}\n\n    public var rawValue: String {{\n        switch self {{").unwrap();
        for (case, value) in cases.iter().zip(&declared.values) { writeln!(out, "        case .{case}: return {}", string_literal(value, true)).unwrap(); }
        writeln!(out, "        case .unknown(let raw): return raw\n        }}\n    }}\n\n    public init(from decoder: Decoder) throws {{\n        self.init(rawValue: try decoder.singleValueContainer().decode(String.self))\n    }}\n\n    public func encode(to encoder: Encoder) throws {{\n        var container = encoder.singleValueContainer()\n        try container.encode(rawValue)\n    }}\n}}\n").unwrap();
    }
}

pub(super) fn emit_kotlin_enums(out: &mut String, enums: &[EnumDecl]) {
    for declared in enums {
        let name = &declared.name;
        let cases = declared.cases(false);
        writeln!(out, "@Serializable(with = {name}Serializer::class)\nsealed class {name}(override val raw: String) : RawEnum {{").unwrap();
        for (case, value) in cases.iter().zip(&declared.values) { writeln!(out, "    data object {case} : {name}({})", string_literal(value, false)).unwrap(); }
        writeln!(out, "    data class Unknown(val value: String) : {name}(value)\n\n    companion object {{\n        fun of(raw: String): {name} = when (raw) {{").unwrap();
        for (case, value) in cases.iter().zip(&declared.values) { writeln!(out, "            {} -> {case}", string_literal(value, false)).unwrap(); }
        writeln!(out, "            else -> Unknown(raw)\n        }}\n    }}\n}}\n\nobject {name}Serializer : RawEnumSerializer<{name}>(\"{name}\", {{ {name}.of(it) }})\n").unwrap();
    }
}
