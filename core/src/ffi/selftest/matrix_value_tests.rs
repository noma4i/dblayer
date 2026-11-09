//! Matrix T: every public value type of the Swift wrapper and of the Kotlin wrapper (hand-written files, not the generated bindings): the same name, the same members, the same type of each member after the mapping of the platforms, the same nullability, the same cases of an enum. A type that one platform has and the other lacks is a red cell.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::test_matrix::{crate_dir, Matrix};

type Members = Vec<(String, String)>;

#[derive(Default, Debug)]
struct Shape {
    fields: Members,
    cases: Vec<(String, Members)>,
    is_enum: bool,
    stream: Option<String>,
    skipped: bool,
}

fn files(directory: &Path, extension: &str, skip: &str, found: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() { files(&path, extension, skip, found); } else if path.extension().is_some_and(|found| found == extension) && path.file_name().is_some_and(|name| name != skip) { found.push(path); }
    }
}

fn identifier(text: &str) -> String { text.chars().take_while(|character| character.is_alphanumeric() || *character == '_').collect() }

fn payload(rest: &str) -> Members {
    let Some((_, tail)) = rest.split_once('(') else { return Vec::new() };
    let mut depth = 1i32;
    let close = tail.char_indices().find(|(_, character)| { match character { '(' => depth += 1, ')' => depth -= 1, _ => {} } depth == 0 }).map_or(tail.len(), |(at, _)| at);
    let inside = &tail[..close];
    split_top(inside).into_iter().map(|part| match part.split_once(':') {
        Some((label, field_type)) if !label.contains('<') && !label.contains('[') => (label.trim().to_string(), field_type.trim().to_string()),
        _ => (String::new(), part),
    }).collect()
}

fn strip_annotations(text: &str) -> String {
    let mut result = String::new();
    let characters: Vec<char> = text.chars().collect();
    let mut at = 0;
    while at < characters.len() {
        if characters[at] != '@' { result.push(characters[at]); at += 1; continue; }
        at += 1;
        while at < characters.len() && (characters[at].is_alphanumeric() || characters[at] == '_' || characters[at] == ':') { at += 1; }
        if characters.get(at) == Some(&'(') {
            let mut depth = 0;
            while at < characters.len() { if characters[at] == '(' { depth += 1; } if characters[at] == ')' { depth -= 1; if depth == 0 { at += 1; break; } } at += 1; }
        }
    }
    result
}

fn without_block_comments(text: &str) -> String {
    let mut result = String::new();
    let mut rest = text;
    while let Some(at) = rest.find("/*") {
        result.push_str(&rest[..at]);
        rest = rest[at..].find("*/").map_or("", |end| &rest[at + end + 2..]);
    }
    result.push_str(rest);
    result.lines().map(|line| line.split("//").next().unwrap_or("")).collect::<Vec<_>>().join("\n")
}

fn body_end(lines: &[&str], start: usize) -> usize {
    let (mut depth, mut cursor, mut opened) = (0i32, start, false);
    while cursor < lines.len() {
        depth += lines[cursor].matches('{').count() as i32 - lines[cursor].matches('}').count() as i32;
        opened |= lines[cursor].contains('{');
        cursor += 1;
        if opened && depth <= 0 { break; }
        if !opened && cursor > start && !lines[cursor - 1].trim_end().ends_with([',', '(']) && !lines[cursor - 1].contains('(') { break; }
    }
    cursor
}

fn swift_types(text: &str) -> BTreeMap<String, Shape> {
    let mut types = BTreeMap::new();
    let text = without_block_comments(text);
    let lines: Vec<&str> = text.lines().collect();
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index].trim_start();
        let (keyword, is_enum) = if line.starts_with("public struct ") { ("public struct ", false) } else if line.starts_with("public enum ") { ("public enum ", true) } else { index += 1; continue };
        let name = identifier(&line[keyword.len()..]);
        let mut shape = Shape { is_enum, ..Shape::default() };
        let mut depth = 0i32;
        let mut cursor = index;
        let mut element = None;
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
                if let Some(rest) = trimmed.strip_prefix("public typealias Element = ") { element = Some(rest.trim().to_string()); }
                if let Some(rest) = trimmed.strip_prefix("indirect case ").or_else(|| trimmed.strip_prefix("case ")) {
                    let members = payload(rest);
                    if members.is_empty() { for case in rest.split(',') { let case = identifier(case.trim()); if !case.is_empty() { shape.cases.push((case, Vec::new())); } } } else { shape.cases.push((identifier(rest), members)); }
                }
            }
            depth += current.matches('{').count() as i32 - current.matches('}').count() as i32;
            cursor += 1;
            if cursor >= lines.len() || (depth <= 0 && cursor > index) { break; }
        }
        let header = lines[index];
        shape.stream = header.contains("AsyncSequence").then(|| element.clone().unwrap_or_default());
        shape.skipped = header.contains("CodingKey") || (is_enum && shape.cases.is_empty());
        types.insert(name, shape);
        index = cursor;
    }
    types
}

fn kotlin_types(text: &str) -> BTreeMap<String, Shape> {
    let mut types = BTreeMap::new();
    let text = without_block_comments(text);
    let lines: Vec<&str> = text.lines().collect();
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index].trim_start();
        let line = strip_annotations(line);
        let line = line.trim_start();
        let line = line.strip_prefix("public ").unwrap_or(line);
        let sealed = line.strip_prefix("sealed class ").or_else(|| line.strip_prefix("sealed interface "));
        if let Some(rest) = sealed {
            let name = identifier(rest);
            let end = body_end(&lines, index);
            let mut shape = Shape { is_enum: true, ..Shape::default() };
            let mut at = index + 1;
            while at < end {
                let member = strip_annotations(lines[at].trim());
                let member = member.trim();
                let nested = member.strip_prefix("data class ").or_else(|| member.strip_prefix("data object ")).or_else(|| member.strip_prefix("object ")).or_else(|| member.strip_prefix("class "));
                if let Some(nested) = nested {
                    let mut header = member.to_string();
                    let mut last = at;
                    while header.matches('(').count() > header.matches(')').count() && last + 1 < end { last += 1; header.push(' '); header.push_str(lines[last].trim()); }
                    shape.cases.push((identifier(nested), payload(&header).into_iter().map(|(field, field_type)| (field.trim_start().trim_start_matches("override ").trim_start_matches("val ").trim_start_matches("var ").trim().to_string(), field_type)).collect()));
                    at = last;
                }
                at += 1;
            }
            types.insert(name, shape);
            index = end;
            continue;
        }
        if let Some(rest) = line.strip_prefix("data class ") {
            let name = identifier(rest);
            let mut shape = Shape::default();
            let mut cursor = index;
            let mut header = String::new();
            let mut opened = 0i32;
            loop {
                header.push_str(&strip_annotations(lines[cursor]));
                header.push('\n');
                opened += lines[cursor].matches('(').count() as i32 - lines[cursor].matches(')').count() as i32;
                cursor += 1;
                if cursor >= lines.len() || (opened <= 0 && header.contains('(')) { break; }
            }
            let inside = header.split_once('(').map_or("", |(_, rest)| rest);
            let inside = inside.rsplit_once(')').map_or(inside, |(body, _)| body);
            for part in split_top(inside) {
                let part = part.split('=').next().unwrap_or(&part).trim();
                let part = part.rsplit_once("val ").or_else(|| part.rsplit_once("var ")).map_or(part, |(_, rest)| rest);
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
                if depth == 1 {
                    for case in current.split([',', ';']).take(8) {
                        let case = identifier(case.trim());
                        if !case.is_empty() && case.chars().next().is_some_and(char::is_uppercase) && !case.starts_with("val") { shape.cases.push((case, Vec::new())); }
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
    let mapped = ["JSONValue", "NoFiles"];
    for name in names {
        let (left, right) = (swift.get(name), kotlin.get(name));
        if left.is_some_and(|shape| shape.skipped) || mapped.contains(&name.as_str()) { continue; }
        if let Some(Shape { stream: Some(element), .. }) = left {
            let element = to_kotlin(element);
            let element = element.split('<').next().unwrap_or("");
            let found = kotlin_text.contains(&format!("Flow<{element}"));
            let wanted = format!("Kotlin Flow of {element}");
            matrix.check(name, "stream idiom", &format!("Swift AsyncSequence of {element}"), &wanted, if found { &wanted } else { "no Flow of that element on Kotlin" });
            continue;
        }
        match (left, right) {
            (Some(left), Some(right)) => {
                if left.is_enum != right.is_enum { matrix.check(name, "kind of type", "enum or struct", "same on both", "enum on one platform only"); continue; }
                if left.is_enum {
                    let case_name = |case: &(String, Members)| case.0.to_lowercase();
                    let (mut a, mut b): (Vec<String>, Vec<String>) = (left.cases.iter().map(case_name).collect(), right.cases.iter().map(case_name).collect());
                    a.sort();
                    b.sort();
                    matrix.check(name, "enum cases", "set of cases", &a.join(","), &b.join(","));
                    for (case, swift_members) in &left.cases {
                        let Some((_, kotlin_members)) = right.cases.iter().find(|(other, _)| other.to_lowercase() == case.to_lowercase()) else { continue };
                        let wanted: Vec<String> = swift_members.iter().map(|(_, member_type)| squash(&to_kotlin(member_type))).collect();
                        let observed: Vec<String> = kotlin_members.iter().map(|(_, member_type)| squash(member_type)).collect();
                        matrix.check(&format!("{name}.{case}"), "case payload types", &wanted.join(","), &wanted.join(","), &observed.join(","));
                        for ((label, _), (field, _)) in swift_members.iter().zip(kotlin_members) {
                            if !label.is_empty() { matrix.check(&format!("{name}.{case}.{label}"), "case field name", &label.to_lowercase(), &label.to_lowercase(), &field.to_lowercase()); }
                        }
                    }
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

fn rust_files(directory: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(directory) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "tests" || name == "selftest") { continue; }
            rust_files(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") && !path.to_string_lossy().ends_with("_tests.rs") {
            found.push(path);
        }
    }
}

enum CoreShape {
    Record(Vec<(String, String)>),
    Enum(Vec<String>),
}

/// The text between the braces that follow `start`.
fn block(text: &str, start: usize) -> Option<&str> {
    let open = start + text[start..].find('{')?;
    let mut depth = 0usize;
    for (offset, character) in text[open..].char_indices() {
        match character {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 { return Some(&text[open + 1..open + offset]); }
            }
            _ => {}
        }
    }
    None
}

fn split_top(text: &str) -> Vec<String> {
    let (mut parts, mut depth, mut current) = (Vec::new(), 0i32, String::new());
    for character in text.chars() {
        match character {
            '<' | '(' | '[' | '{' => depth += 1,
            '>' | ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
        if character == ',' && depth == 0 { parts.push(std::mem::take(&mut current)); } else { current.push(character); }
    }
    parts.push(current);
    parts.into_iter().map(|part| part.trim().to_string()).filter(|part| !part.is_empty()).collect()
}

fn without_comments(text: &str) -> String {
    text.lines().filter(|line| { let line = line.trim_start(); !line.starts_with("//") && !line.starts_with("#[") }).collect::<Vec<_>>().join("\n")
}

fn core_shapes() -> Vec<(String, CoreShape)> {
    let mut files = Vec::new();
    rust_files(&crate_dir().join("src"), &mut files);
    let mut shapes = Vec::new();
    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else { continue };
        let mut from = 0;
        while let Some(found) = text[from..].find("uniffi::") {
            let at = from + found;
            from = at + 8;
            let line_end = text[at..].find('\n').map_or(text.len(), |end| at + end);
            let derive = &text[text[..at].rfind('\n').unwrap_or(0)..line_end];
            let is_record = derive.contains("uniffi::Record");
            let is_enum = derive.contains("uniffi::Enum") || derive.contains("uniffi::Error");
            if !is_record && !is_enum { continue; }
            let rest = &text[line_end..];
            let Some(item) = rest.find("pub struct ").filter(|_| is_record).or_else(|| rest.find("pub enum ").filter(|_| is_enum)) else { continue };
            if item > 400 { continue; }
            let head = &rest[item..];
            let name: String = head.split_whitespace().nth(2).unwrap_or("").chars().take_while(|character| character.is_alphanumeric() || *character == '_').collect();
            let Some(body) = block(rest, item) else { continue };
            let body = without_comments(body);
            if is_record {
                let fields = split_top(&body).into_iter().filter_map(|field| {
                    let field = field.trim_start_matches("pub ").trim();
                    let (field_name, field_type) = field.split_once(':')?;
                    Some((field_name.trim().to_string(), field_type.trim().to_string()))
                }).collect();
                shapes.push((name, CoreShape::Record(fields)));
            } else {
                let variants = split_top(&body).into_iter().map(|variant| variant.chars().take_while(|character| character.is_alphanumeric() || *character == '_').collect::<String>()).filter(|variant| !variant.is_empty()).collect();
                shapes.push((name, CoreShape::Enum(variants)));
            }
        }
    }
    shapes.sort_by(|left, right| left.0.cmp(&right.0));
    shapes.dedup_by(|left, right| left.0 == right.0);
    shapes
}

fn camel(name: &str) -> String {
    let mut out = String::new();
    let mut upper = false;
    for character in name.chars() {
        if character == '_' { upper = true; } else if upper { out.extend(character.to_uppercase()); upper = false; } else { out.push(character); }
    }
    out
}

/// The lowercased case names of the enum `name` in a wrapper source: a Swift `enum` (single or multi line), a Kotlin `enum class` or a Kotlin `sealed` type whose cases are its nested types.
fn enum_cases(text: &str, name: &str, swift: bool) -> Vec<String> {
    let declaration = if swift {
        text.find(&format!("enum {name}"))
    } else {
        ["enum class ", "sealed interface ", "sealed class "].iter().find_map(|keyword| text.find(&format!("{keyword}{name}")))
    };
    let Some(at) = declaration else { return Vec::new() };
    let Some(body) = block(text, at) else { return Vec::new() };
    let mut cases = Vec::new();
    if swift {
        for line in body.lines() {
            let Some(rest) = line.trim_start().strip_prefix("case ") else { continue };
            for part in rest.split('(').next().unwrap_or("").split(',') {
                let case = identifier(part.trim());
                if !case.is_empty() { cases.push(case.to_lowercase()); }
            }
        }
    } else if body.contains("class ") || body.contains("object ") {
        for marker in ["data class ", "data object ", "class ", "object "] {
            for piece in body.split(marker).skip(1) {
                let case = identifier(piece.trim_start());
                if !case.is_empty() { cases.push(case.to_lowercase()); }
            }
        }
    } else {
        let constants = strip_annotations(body.split(';').next().unwrap_or(body));
        for part in constants.split(',') {
            let case = identifier(part.trim());
            if !case.is_empty() { cases.push(case.to_lowercase()); }
        }
    }
    cases.sort();
    cases.dedup();
    cases
}

/// The top-level arguments of the first construction `name(` in a wrapper source, so a `wire` mapping can be read for the fields it carries.
fn call_args(text: &str, name: &str) -> Option<Vec<String>> {
    let needle = format!("{name}(");
    let at = text.match_indices(&needle).find(|(at, _)| text[..*at].chars().next_back().is_none_or(|previous| !(previous.is_alphanumeric() || previous == '_'))).map(|(at, _)| at)?;
    let open = at + needle.len() - 1;
    let mut depth = 0i32;
    let mut end = None;
    for (offset, character) in text[open..].char_indices() {
        match character {
            '(' => depth += 1,
            ')' => { depth -= 1; if depth == 0 { end = Some(open + offset); break; } }
            _ => {}
        }
    }
    Some(split_top(&text[open + 1..end?]))
}

enum Projection {
    Enum,
    Produced,
    Decoded,
}

/// Each `Wire*` export of the core (BREC) against its platform projection, matched through the typed DSL `wire` mapping (bridge.md group B), not by name: an enum by its case set, a produced record by the fields its `wire` constructor carries, a decoded record by the projection type the platform declares (its projected `Model` values keep their cross-platform parity in T-value-platform). The generated bindings carry the `Wire*` types verbatim but are regenerated at build and not reliably present or current in the tree, so the committed wrappers are the comparison.
#[test]
fn matrix_value_core_wrapper() {
    let mut matrix = Matrix::new("T-value-core-wrapper");
    let root = crate_dir().join("..");
    let (mut swift_files, mut kotlin_files) = (Vec::new(), Vec::new());
    files(&root.join("swift/Sources/DBlayer"), "swift", "dblayer.swift", &mut swift_files);
    files(&root.join("kotlin/src/main/kotlin/com/noma4i/dblayer"), "kt", "dblayer.kt", &mut kotlin_files);
    let swift_text: String = swift_files.iter().filter_map(|file| std::fs::read_to_string(file).ok()).collect::<Vec<_>>().join("\n");
    let kotlin_text: String = kotlin_files.iter().filter_map(|file| std::fs::read_to_string(file).ok()).collect::<Vec<_>>().join("\n");
    if swift_text.is_empty() || kotlin_text.is_empty() { matrix.unclassified("wrappers", "-", "-", "the hand-written wrapper sources are not readable"); matrix.finish(); return; }
    let shapes: BTreeMap<String, CoreShape> = core_shapes().into_iter().collect();

    let projections: [(&str, &str, Projection); 11] = [
        ("WireSyncState", "SyncState", Projection::Enum),
        ("WireUploadStatus", "UploadStatus", Projection::Enum),
        ("WireOperationOutcome", "OperationOutcome", Projection::Enum),
        ("WireUploadRequest", "UploadRequest", Projection::Produced),
        ("WireUploadItem", "UploadItem", Projection::Produced),
        ("WireUploadSource", "UploadSource", Projection::Produced),
        ("WireUploadBatch", "UploadBatchRequest", Projection::Produced),
        ("WireOperationReceipt", "MutationReceipt", Projection::Decoded),
        ("WireBatchReceipt", "OperationReceipt", Projection::Decoded),
        ("WireItemReceipt", "ItemReceipt", Projection::Decoded),
        ("WireModelEvent", "ModelEvent", Projection::Decoded),
    ];

    for &(wire, projection, ref kind) in &projections {
        let Some(shape) = shapes.get(wire) else { matrix.unclassified(wire, "-", "-", "the core no longer exports this Wire type"); continue };
        for (platform, text, swift_side) in [("swift", swift_text.as_str(), true), ("kotlin", kotlin_text.as_str(), false)] {
            match kind {
                Projection::Enum => {
                    let CoreShape::Enum(variants) = shape else { matrix.unclassified(wire, platform, "-", "the core type is not an enum"); continue };
                    let mut wanted: Vec<String> = variants.iter().map(|variant| variant.to_lowercase()).collect();
                    wanted.sort();
                    matrix.check(wire, platform, "enum cases", &wanted.join(","), &enum_cases(text, projection, swift_side).join(","));
                }
                Projection::Produced => {
                    let CoreShape::Record(fields) = shape else { matrix.unclassified(wire, platform, "-", "the core type is not a record"); continue };
                    let mut wanted: Vec<String> = fields.iter().map(|(field, _)| camel(field)).collect();
                    wanted.sort();
                    match call_args(text, wire) {
                        Some(arguments) if swift_side => {
                            let mut labels: Vec<String> = arguments.iter().filter_map(|argument| argument.split_once(':').map(|(label, _)| identifier(label.trim()))).collect();
                            labels.sort();
                            matrix.check(wire, platform, "fields carried by the wire mapping", &wanted.join(","), &labels.join(","));
                        }
                        Some(arguments) => matrix.check(wire, platform, "fields carried by the wire mapping", &wanted.len().to_string(), &arguments.len().to_string()),
                        None => matrix.check(wire, platform, "fields carried by the wire mapping", &wanted.join(","), "no wire construction in the wrapper"),
                    }
                }
                Projection::Decoded => {
                    let keywords: &[&str] = if swift_side { &["struct", "class", "enum", "protocol", "actor", "typealias"] } else { &["class", "interface", "object", "typealias"] };
                    let found = declared(text, projection, keywords);
                    matrix.check(wire, platform, "decoded into the projection type", projection, if found { projection } else { "projection type missing" });
                }
            }
        }
    }
    matrix.finish();
}
