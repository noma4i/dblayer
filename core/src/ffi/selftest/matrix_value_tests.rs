//! Matrix T: every public value type of the Swift wrapper and of the Kotlin wrapper (hand-written files, not the generated bindings): the same name, the same members, the same type of each member after the mapping of the platforms, the same nullability, the same cases of an enum. A type that one platform has and the other lacks is a red cell.
use std::collections::BTreeMap;
use std::path::Path;

use crate::test_matrix::{crate_dir, Matrix};

#[derive(Default, Debug)]
struct Shape {
    fields: Vec<(String, String)>,
    cases: Vec<String>,
    is_enum: bool,
}

fn files(directory: &Path, extension: &str, skip: &str, found: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() { files(&path, extension, skip, found); } else if path.extension().is_some_and(|found| found == extension) && path.file_name().is_some_and(|name| name != skip) { found.push(path); }
    }
}

fn identifier(text: &str) -> String { text.chars().take_while(|character| character.is_alphanumeric() || *character == '_').collect() }

fn swift_types(text: &str) -> BTreeMap<String, Shape> {
    let mut types = BTreeMap::new();
    let lines: Vec<&str> = text.lines().collect();
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index].trim_start();
        let (keyword, is_enum) = if line.starts_with("public struct ") { ("public struct ", false) } else if line.starts_with("public enum ") { ("public enum ", true) } else { index += 1; continue };
        let name = identifier(&line[keyword.len()..]);
        let indent = lines[index].len() - line.len();
        let mut shape = Shape { is_enum, ..Shape::default() };
        let mut depth = 0i32;
        let mut cursor = index;
        loop {
            let current = lines[cursor];
            let trimmed = current.trim_start();
            if depth == 1 {
                if let Some(rest) = trimmed.strip_prefix("public var ").or_else(|| trimmed.strip_prefix("public let ")) {
                    if let Some((field, field_type)) = rest.split_once(':').filter(|(_, tail)| !tail.contains('{') || tail.contains('=')) {
                        let field_type = field_type.split('=').next().unwrap_or("").split('{').next().unwrap_or("").trim().to_string();
                        shape.fields.push((identifier(field.trim()), field_type));
                    }
                }
                if let Some(rest) = trimmed.strip_prefix("case ") {
                    for case in rest.split(',') { let case = identifier(case.trim()); if !case.is_empty() { shape.cases.push(case); } }
                }
            }
            depth += current.matches('{').count() as i32 - current.matches('}').count() as i32;
            cursor += 1;
            if cursor >= lines.len() || (depth <= 0 && cursor > index) { break; }
        }
        let _ = indent;
        types.insert(name, shape);
        index = cursor;
    }
    types
}

fn kotlin_types(text: &str) -> BTreeMap<String, Shape> {
    let mut types = BTreeMap::new();
    let lines: Vec<&str> = text.lines().collect();
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index].trim_start();
        let line = line.strip_prefix("public ").unwrap_or(line);
        if let Some(rest) = line.strip_prefix("data class ") {
            let name = identifier(rest);
            let mut shape = Shape::default();
            let mut cursor = index;
            let mut header = String::new();
            let mut opened = 0i32;
            loop {
                header.push_str(lines[cursor]);
                header.push('\n');
                opened += lines[cursor].matches('(').count() as i32 - lines[cursor].matches(')').count() as i32;
                cursor += 1;
                if cursor >= lines.len() || (opened <= 0 && header.contains('(')) { break; }
            }
            let inside = header.split_once('(').map_or("", |(_, rest)| rest);
            let inside = inside.rsplit_once(')').map_or(inside, |(body, _)| body);
            let mut depth = 0i32;
            let mut current = String::new();
            let mut parts = Vec::new();
            for character in inside.chars() {
                match character { '<' | '(' | '[' => depth += 1, '>' | ')' | ']' => depth -= 1, _ => {} }
                if character == ',' && depth == 0 { parts.push(std::mem::take(&mut current)); } else { current.push(character); }
            }
            parts.push(current);
            for part in parts {
                let part = part.trim();
                let part = part.split('=').next().unwrap_or(part).trim();
                let part = part.strip_prefix("val ").or_else(|| part.strip_prefix("var ")).unwrap_or(part);
                if let Some((field, field_type)) = part.split_once(':') { shape.fields.push((identifier(field.trim()), field_type.trim().to_string())); }
            }
            types.insert(name, shape);
            index = cursor;
            continue;
        }
        if let Some(rest) = line.strip_prefix("enum class ") {
            let name = identifier(rest);
            let mut shape = Shape { is_enum: true, ..Shape::default() };
            let mut cursor = index + 1;
            let mut depth = lines[index].matches('{').count() as i32 - lines[index].matches('}').count() as i32;
            while cursor < lines.len() && depth > 0 {
                let current = lines[cursor].trim();
                if depth == 1 && !current.starts_with("//") && !current.starts_with('*') && !current.starts_with("/*") {
                    for case in current.split(|character| character == ',' || character == ';').take(8) {
                        let case = identifier(case.trim());
                        if !case.is_empty() && case.chars().next().is_some_and(char::is_uppercase) && !case.starts_with("val") { shape.cases.push(case); }
                    }
                }
                depth += lines[cursor].matches('{').count() as i32 - lines[cursor].matches('}').count() as i32;
                cursor += 1;
            }
            types.insert(name, shape);
            index = cursor;
            continue;
        }
        index += 1;
    }
    types
}

fn to_kotlin(swift: &str) -> String {
    let swift = swift.trim();
    if let Some(inner) = swift.strip_suffix('?') { return format!("{}?", to_kotlin(inner)); }
    if swift == "[String:JSONValue]" || swift == "[String: JSONValue]" { return "JsonObject".into(); }
    if let Some(inner) = swift.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')) {
        if let Some((key, value)) = inner.split_once(':') { return format!("Map<{},{}>", to_kotlin(key), to_kotlin(value)); }
        return format!("List<{}>", to_kotlin(inner));
    }
    match swift {
        "Int" | "Int32" => "Int".into(), "Int64" => "Long".into(), "UInt64" => "ULong".into(), "UInt32" | "UInt" => "UInt".into(),
        "Double" => "Double".into(), "JSONValue" => "JsonElement".into(), "Float" => "Float".into(), "Bool" => "Boolean".into(), "Data" => "ByteArray".into(),
        other => other.to_string(),
    }
}

fn squash(text: &str) -> String {
    let text = text.trim();
    let text = if text.starts_with('@') { text.rsplit_once(") ").map_or(text, |(_, rest)| rest) } else { text };
    text.replace("kotlin.", "").split_whitespace().collect()
}

fn declared(text: &str, name: &str, keywords: &[&str]) -> bool {
    keywords.iter().any(|keyword| text.match_indices(&format!("{keyword} {name}")).any(|(at, found)| text[at + found.len()..].chars().next().is_none_or(|next| !(next.is_alphanumeric() || next == '_'))))
}

#[test]
fn matrix_value_platform() {
    let mut matrix = Matrix::new("T-value-platform");
    let root = crate_dir().join("..");
    let (mut swift_files, mut kotlin_files) = (Vec::new(), Vec::new());
    files(&root.join("swift/Sources/DBlayer"), "swift", "dblayer.swift", &mut swift_files);
    files(&root.join("kotlin/src/main/kotlin/com/noma4i/dblayer"), "kt", "dblayer.kt", &mut kotlin_files);
    if swift_files.is_empty() || kotlin_files.is_empty() { matrix.unclassified("wrappers", "-", "-", "the wrapper sources are not readable"); matrix.finish(); return; }
    let mut swift = BTreeMap::new();
    for file in &swift_files { swift.extend(swift_types(&std::fs::read_to_string(file).unwrap_or_default())); }
    let mut kotlin = BTreeMap::new();
    for file in &kotlin_files { kotlin.extend(kotlin_types(&std::fs::read_to_string(file).unwrap_or_default())); }
    let swift_text: String = swift_files.iter().filter_map(|file| std::fs::read_to_string(file).ok()).collect();
    let kotlin_text: String = kotlin_files.iter().filter_map(|file| std::fs::read_to_string(file).ok()).collect();
    let names: std::collections::BTreeSet<&String> = swift.keys().chain(kotlin.keys()).collect();
    for name in names {
        match (swift.get(name), kotlin.get(name)) {
            (Some(left), Some(right)) => {
                if left.is_enum != right.is_enum { matrix.check(name, "kind of type", "enum or struct", "same on both", "enum on one platform only"); continue; }
                if left.is_enum {
                    let (mut a, mut b) = (left.cases.clone(), right.cases.clone());
                    a.iter_mut().for_each(|case| *case = case.to_lowercase());
                    b.iter_mut().for_each(|case| *case = case.to_lowercase());
                    a.sort();
                    b.sort();
                    matrix.check(name, "enum cases", "set of cases", &a.join(","), &b.join(","));
                    continue;
                }
                let kotlin_fields: BTreeMap<&String, &String> = right.fields.iter().map(|(field, field_type)| (field, field_type)).collect();
                for (field, swift_type) in &left.fields {
                    let observed = kotlin_fields.get(field).map_or("member missing on Kotlin".to_string(), |found| squash(found));
                    matrix.check(&format!("{name}.{field}"), "member type", swift_type, &squash(&to_kotlin(swift_type)), &observed);
                }
                for (field, kotlin_type) in &right.fields {
                    if !left.fields.iter().any(|(swift_field, _)| swift_field == field) { matrix.check(&format!("{name}.{field}"), "member type", kotlin_type, "member on Swift", "member missing on Swift"); }
                }
            }
            (Some(_), None) => matrix.check(name, "public value type", "Swift", "on both platforms", if declared(&kotlin_text, name, &["class", "interface", "object", "typealias"]) { "on both platforms" } else { "missing on Kotlin" }),
            (None, Some(_)) => matrix.check(name, "public value type", "Kotlin", "on both platforms", if declared(&swift_text, name, &["struct", "class", "enum", "protocol", "actor", "typealias"]) { "on both platforms" } else { "missing on Swift" }),
            (None, None) => {}
        }
    }
    matrix.finish();
}
