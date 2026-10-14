use std::fmt::Write;

use dblayer::schema::FieldSpec;

use crate::operation::OperationCall;
use super::nested::{emit_kotlin_type, emit_swift_type};

use super::{arg_type, name};
use crate::operation::is_input_object;

fn param_type(kind: &str, swift: bool) -> String {
    if is_input_object(kind) { kind.trim_end_matches('?').to_string() } else { arg_type(kind, swift).to_string() }
}

fn declared(root: &FieldSpec) -> Vec<&FieldSpec> {
    fn walk<'a>(field: &'a FieldSpec, out: &mut Vec<&'a FieldSpec>) {
        out.push(field);
        for inner in field.fields.iter().filter(|inner| !inner.fields.is_empty()) { walk(inner, out); }
    }
    let mut out = Vec::new();
    walk(root, &mut out);
    out
}

fn result_name(operation: &OperationCall) -> &str {
    operation.result.as_ref().and_then(|root| root.app_type.as_deref()).unwrap_or_default()
}

pub(super) fn emit_swift_operations(out: &mut String, operations: &[OperationCall]) {
    if operations.is_empty() { return; }
    for operation in operations {
        let Some(root) = &operation.result else { continue };
        let types = declared(root);
        for declaration in &types { emit_swift_type(out, declaration, &types, false); }
    }
    writeln!(out, "public enum Operations {{").unwrap();
    for operation in operations {
        let method = name(&operation.name);
        let params = operation.args.iter().map(|(arg, kind)| format!("{arg}: {}{}", param_type(kind, true), if kind.ends_with('?') { "? = nil" } else { "" })).collect::<Vec<_>>().join(", ");
        let returns = if operation.result.is_none() { String::new() } else { format!(" -> {}", result_name(operation)) };
        writeln!(out, "    public static func {method}({params}) async throws{returns} {{\n        var variables: [String: JSONValue] = [:]").unwrap();
        for (arg, kind) in &operation.args {
            if kind.ends_with('?') { writeln!(out, "        if let value = {arg} {{ variables[\"{arg}\"] = JSONValue.encode(value) }}").unwrap(); }
            else { writeln!(out, "        variables[\"{arg}\"] = JSONValue.encode({arg})").unwrap(); }
        }
        let sent = operation.wrap.as_ref().map_or("variables".to_string(), |wrap| format!("[\"{wrap}\": JSONValue.object(variables)]"));
        if operation.result.is_none() { writeln!(out, "        try await DB.run(\"{}\", variables: {sent})\n    }}", operation.operation).unwrap(); }
        else { writeln!(out, "        return try await DB.run(\"{}\", variables: {sent}, as: {}.self)\n    }}", operation.operation, result_name(operation)).unwrap(); }
    }
    writeln!(out, "}}\n").unwrap();
}

pub(super) fn emit_kotlin_operations(out: &mut String, operations: &[OperationCall]) {
    if operations.is_empty() { return; }
    for operation in operations {
        let Some(root) = &operation.result else { continue };
        let types = declared(root);
        for declaration in &types { emit_kotlin_type(out, declaration, &types, false); }
    }
    writeln!(out, "object Operations {{").unwrap();
    for operation in operations {
        let method = name(&operation.name);
        let params = operation.args.iter().map(|(arg, kind)| format!("{arg}: {}{}", param_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" })).collect::<Vec<_>>().join(", ");
        let returns = if operation.result.is_none() { String::new() } else { format!(": {}", result_name(operation)) };
        writeln!(out, "    suspend fun {method}({params}){returns} {{\n        val variables = mutableMapOf<String, JsonElement>()").unwrap();
        for (arg, kind) in &operation.args {
            if kind.ends_with('?') { writeln!(out, "        if ({arg} != null) variables[\"{arg}\"] = jsonValue({arg})").unwrap(); }
            else { writeln!(out, "        variables[\"{arg}\"] = jsonValue({arg})").unwrap(); }
        }
        let sent = operation.wrap.as_ref().map_or("JsonObject(variables)".to_string(), |wrap| format!("JsonObject(mapOf(\"{wrap}\" to JsonObject(variables)))"));
        if operation.result.is_none() { writeln!(out, "        DBlayer.run(\"{}\", {sent})\n    }}", operation.operation).unwrap(); }
        else { writeln!(out, "        return DBlayer.run(\"{}\", {sent}, {}.serializer())\n    }}", operation.operation, result_name(operation)).unwrap(); }
    }
    writeln!(out, "}}\n").unwrap();
}
