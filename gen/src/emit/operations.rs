use std::fmt::Write;

use crate::operation::OperationCall;

use super::{arg_type, name};

fn result_name(method: &str) -> String {
    let mut chars = method.chars();
    format!("{}{}Result", chars.next().map(|first| first.to_uppercase().collect::<String>()).unwrap_or_default(), chars.as_str())
}

pub(super) fn emit_swift_operations(out: &mut String, operations: &[OperationCall]) {
    if operations.is_empty() { return; }
    for operation in operations.iter().filter(|operation| !operation.result.is_empty()) {
        writeln!(out, "public struct {}: Codable, Sendable {{", result_name(&name(&operation.name))).unwrap();
        for (field, kind) in &operation.result { writeln!(out, "    public let {field}: {}?", arg_type(kind, true)).unwrap(); }
        writeln!(out, "}}\n").unwrap();
    }
    writeln!(out, "public enum Operations {{").unwrap();
    for operation in operations {
        let method = name(&operation.name);
        let params = operation.args.iter().map(|(arg, kind)| format!("{arg}: {}{}", arg_type(kind, true), if kind.ends_with('?') { "? = nil" } else { "" })).collect::<Vec<_>>().join(", ");
        let returns = if operation.result.is_empty() { String::new() } else { format!(" -> {}", result_name(&method)) };
        writeln!(out, "    public static func {method}({params}) async throws{returns} {{\n        var variables: [String: JSONValue] = [:]").unwrap();
        for (arg, kind) in &operation.args {
            if kind.ends_with('?') { writeln!(out, "        if let value = {arg} {{ variables[\"{arg}\"] = JSONValue.encode(value) }}").unwrap(); }
            else { writeln!(out, "        variables[\"{arg}\"] = JSONValue.encode({arg})").unwrap(); }
        }
        let sent = operation.wrap.as_ref().map_or("variables".to_string(), |wrap| format!("[\"{wrap}\": JSONValue.object(variables)]"));
        if operation.result.is_empty() { writeln!(out, "        try await DB.run(\"{}\", variables: {sent})\n    }}", operation.operation).unwrap(); }
        else { writeln!(out, "        return try await DB.run(\"{}\", variables: {sent}, as: {}.self)\n    }}", operation.operation, result_name(&method)).unwrap(); }
    }
    writeln!(out, "}}\n").unwrap();
}

pub(super) fn emit_kotlin_operations(out: &mut String, operations: &[OperationCall]) {
    if operations.is_empty() { return; }
    for operation in operations.iter().filter(|operation| !operation.result.is_empty()) {
        let fields = operation.result.iter().map(|(field, kind)| format!("val {field}: {}? = null", arg_type(kind, false))).collect::<Vec<_>>().join(", ");
        writeln!(out, "@Serializable\ndata class {}({fields})\n", result_name(&name(&operation.name))).unwrap();
    }
    writeln!(out, "object Operations {{").unwrap();
    for operation in operations {
        let method = name(&operation.name);
        let params = operation.args.iter().map(|(arg, kind)| format!("{arg}: {}{}", arg_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" })).collect::<Vec<_>>().join(", ");
        let returns = if operation.result.is_empty() { String::new() } else { format!(": {}", result_name(&method)) };
        writeln!(out, "    suspend fun {method}({params}){returns} {{\n        val variables = mutableMapOf<String, JsonElement>()").unwrap();
        for (arg, kind) in &operation.args {
            if kind.ends_with('?') { writeln!(out, "        if ({arg} != null) variables[\"{arg}\"] = jsonValue({arg})").unwrap(); }
            else { writeln!(out, "        variables[\"{arg}\"] = jsonValue({arg})").unwrap(); }
        }
        let sent = operation.wrap.as_ref().map_or("JsonObject(variables)".to_string(), |wrap| format!("JsonObject(mapOf(\"{wrap}\" to JsonObject(variables)))"));
        if operation.result.is_empty() { writeln!(out, "        DBlayer.run(\"{}\", {sent})\n    }}", operation.operation).unwrap(); }
        else { writeln!(out, "        return DBlayer.run(\"{}\", {sent}, {}.serializer())\n    }}", operation.operation, result_name(&method)).unwrap(); }
    }
    writeln!(out, "}}\n").unwrap();
}
