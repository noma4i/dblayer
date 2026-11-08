use std::fmt::Write;

use dblayer::schema::FieldSpec;

use crate::operation::OperationCall;
use super::nested::{emit_kotlin_type, emit_swift_type};

use super::{arg_type, identifier, name};
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
    operation.result.as_ref().and_then(|root| root.app_type.as_deref()).expect("a returning operation has a result type")
}

fn returned(operation: &OperationCall, swift: bool) -> String {
    match (operation.list, swift) {
        (true, true) => format!("[{}]", result_name(operation)),
        (true, false) => format!("List<{}>", result_name(operation)),
        (false, _) => result_name(operation).to_string(),
    }
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
        let method = identifier(&name(&operation.name), true);
        let params = operation.args.iter().map(|(arg, kind)| format!("{}: {}{}", identifier(arg, true), arg_type(kind, true), if kind.ends_with('?') { "? = nil" } else { "" })).collect::<Vec<_>>().join(", ");
        let scope = "scope_";
        let params = super::swift_scope(&params, scope);
        let returns = if operation.result.is_none() { String::new() } else { format!(" -> {}", returned(operation, true)) };
        let binding = if operation.args.is_empty() { "let" } else { "var" };
        writeln!(out, "    @concurrent public static func {method}({params}) async throws{returns} {{\n        {binding} variables_: [String: JSONValue] = [:]").unwrap();
        for (arg, kind) in &operation.args {
            if kind.ends_with('?') { writeln!(out, "        if let value = {printed} {{ variables_[\"{arg}\"] = try JSONValue.encode(value) }}", printed = identifier(arg, true)).unwrap(); }
            else { writeln!(out, "        variables_[\"{arg}\"] = try JSONValue.encode({printed})", printed = identifier(arg, true)).unwrap(); }
        }
        let sent = operation.wrap.as_ref().map_or("variables_".to_string(), |wrap| format!("[\"{wrap}\": JSONValue.object(variables_)]"));
        if operation.result.is_none() { writeln!(out, "        try await DB.call(\"{}\", variables: {sent}, in: {scope})\n    }}", operation.operation).unwrap(); }
        else { writeln!(out, "        return try await DB.call(\"{}\", variables: {sent}, as: {}.self, in: {scope})\n    }}", operation.operation, returned(operation, true)).unwrap(); }
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
        let method = identifier(&name(&operation.name), false);
        let params = operation.args.iter().map(|(arg, kind)| format!("{}: {}{}", identifier(arg, false), arg_type(kind, false), if kind.ends_with('?') { "? = null" } else { "" })).collect::<Vec<_>>().join(", ");
        let account = super::ACCOUNT;
        let params = super::kotlin_scope(&params, account);
        let returns = if operation.result.is_none() { String::new() } else { format!(": {}", returned(operation, false)) };
        writeln!(out, "    suspend fun {method}({params}){returns} {{\n        val variables_ = mutableMapOf<String, JsonElement>()").unwrap();
        for (arg, kind) in &operation.args {
            if kind.ends_with('?') { writeln!(out, "        if ({printed} != null) variables_[\"{arg}\"] = jsonValue({printed})", printed = identifier(arg, false)).unwrap(); }
            else { writeln!(out, "        variables_[\"{arg}\"] = jsonValue({printed})", printed = identifier(arg, false)).unwrap(); }
        }
        let sent = operation.wrap.as_ref().map_or("JsonObject(variables_)".to_string(), |wrap| format!("JsonObject(mapOf(\"{wrap}\" to JsonObject(variables_)))"));
        if operation.result.is_none() { writeln!(out, "        DBlayer.call(\"{}\", {sent}, {account})\n    }}", operation.operation).unwrap(); }
        else if operation.list { writeln!(out, "        return DBlayer.call(\"{}\", {sent}, ListSerializer({}.serializer()), {account})\n    }}", operation.operation, result_name(operation)).unwrap(); }
        else { writeln!(out, "        return DBlayer.call(\"{}\", {sent}, {}.serializer(), {account})\n    }}", operation.operation, result_name(operation)).unwrap(); }
    }
    writeln!(out, "}}\n").unwrap();
}
