use super::graphql::{Operations, Schema, Select, TypeKind};
use crate::{FileError, ParseError};

fn failure(file: &str, message: String) -> FileError {
    FileError { file: file.to_string(), error: ParseError::new(0, 0, message) }
}

impl Operations {
    pub fn check_against(&self, schema: &Schema) -> Vec<FileError> {
        let mut found = Vec::new();
        let mut names: Vec<&String> = self.named.keys().collect();
        names.sort();
        for name in names {
            let operation = &self.named[name];
            let place = format!("operation `{name}`");
            for (variable, ty) in &operation.variables {
                let kind = ty.name();
                if !schema.types.contains_key(kind) && !Schema::is_builtin_scalar(kind) {
                    found.push(failure(&operation.file, format!("{place}: variable `${variable}` has type `{kind}` which the schema does not have")));
                }
            }
            let root = match operation.kind {
                "query" => &schema.query,
                "mutation" => &schema.mutation,
                _ => &schema.subscription,
            };
            check_select(schema, root, &operation.select, (&operation.file, &place), &mut found);
        }
        let mut fragments: Vec<&String> = self.fragments.keys().collect();
        fragments.sort();
        for name in fragments {
            let (on, select) = &self.fragments[name];
            let place = format!("fragment `{name}`");
            if schema.types.contains_key(on) {
                check_select(schema, on, select, ("operation documents", &place), &mut found);
            } else {
                found.push(failure("operation documents", format!("{place} is on type `{on}` which the schema does not have")));
            }
        }
        found
    }
}

fn check_select(schema: &Schema, owner: &str, select: &Select, at: (&str, &str), found: &mut Vec<FileError>) {
    let (file, place) = at;
    let owner_fields = matches!(schema.types.get(owner), Some(TypeKind::Fields(_)));
    for field in &select.fields {
        if !owner_fields && (schema.types.contains_key(owner) || Schema::is_builtin_scalar(owner)) {
            found.push(failure(file, format!("{place}: type `{owner}` has no fields, `{}` cannot be selected", field.name)));
            continue;
        }
        match schema.field(owner, &field.name) {
            Ok(ty) => check_select(schema, ty.name(), &field.select, at, found),
            Err(_) if !schema.types.contains_key(owner) => {}
            Err(message) => found.push(failure(file, format!("{place}: {message}"))),
        }
    }
    for (on, inner) in &select.on {
        if schema.types.contains_key(on) {
            check_select(schema, on, inner, at, found);
        } else {
            found.push(failure(file, format!("{place}: the fragment is on type `{on}` which the schema does not have")));
        }
    }
}
