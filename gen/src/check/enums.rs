use super::*;
use crate::enums::EnumDecl;

struct Declared<'a> {
    decl: EnumDecl,
    call: &'a Call,
    file: usize,
}

fn text_of(value: &Value) -> Option<&str> {
    match value {
        Value::Symbol(text) | Value::Const(text) | Value::Str(text) => Some(text),
        _ => None,
    }
}

fn collect<'a>(calls: &'a [Call], owner: &str, file: usize, out: &mut Vec<Declared<'a>>) {
    for call in calls {
        let option = |key: &str| call.options.iter().find(|pair| pair.key == key).map(|pair| &pair.value.item);
        match call.word.as_str() {
            "enum" => {
                let field = symbol_name(call.args.first()).unwrap_or_default();
                let name = option("as").and_then(text_of).map(str::to_string).unwrap_or_else(|| format!("{owner}{}", crate::build::camel(field)));
                let values: Vec<String> = match option("values") {
                    Some(Value::Array(items)) => items.iter().filter_map(|item| text_of(&item.item)).map(str::to_string).collect(),
                    _ => Vec::new(),
                };
                out.push(Declared { decl: EnumDecl { name, values }, call, file });
            }
            "object" | "array" => {
                if let Some(block) = &call.block {
                    let named = option("as").and_then(text_of).map(str::to_string);
                    let field = symbol_name(call.args.first()).unwrap_or_default();
                    collect(block, &named.unwrap_or_else(|| format!("{owner}{}", crate::build::camel(field))), file, out);
                }
            }
            "ephemeral" => collect(call.block.as_deref().unwrap_or_default(), owner, file, out),
            _ => {}
        }
    }
}

pub(super) fn check_enums(nodes: &[ModelNode], errors: &mut Vec<Found>) {
    let mut declared = Vec::new();
    for node in nodes {
        for schema in node.body.iter().filter(|call| call.word == "schema") {
            collect(schema.block.as_deref().unwrap_or_default(), &node.name, node.file, &mut declared);
        }
    }
    for (index, item) in declared.iter().enumerate() {
        let call = item.call;
        if item.decl.values.is_empty() { continue; }
        let fail = |message: String| at(item.file, call.line, call.column, message);
        if let Err(message) = item.decl.check() {
            errors.push(fail(message));
        } else if nodes.iter().any(|node| node.name == item.decl.name) {
            errors.push(fail(format!("enum `{}` has the name of a model", item.decl.name)));
        } else if let Some(earlier) = declared[..index].iter().find(|earlier| earlier.decl.name == item.decl.name && earlier.decl.values != item.decl.values) {
            errors.push(fail(format!("enum `{}` is declared with different values: {} and {}", item.decl.name, earlier.decl.values.join(", "), item.decl.values.join(", "))));
        }
    }
}
