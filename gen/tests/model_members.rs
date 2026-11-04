use dblayer_gen::{emit_kotlin, emit_swift, parse_files, SchemaTypes, MODEL_MEMBERS};
use std::collections::BTreeSet;
use std::path::PathBuf;

fn normal(name: &str) -> String {
    name.trim_matches('`').replace('_', "").to_lowercase()
}

fn declared(source: &str, indents: &[usize], keywords: &[&str]) -> Vec<String> {
    let mut found = Vec::new();
    for line in source.lines() {
        let indent = line.len() - line.trim_start().len();
        if !indents.contains(&indent) { continue; }
        let mut words = line.split_whitespace().peekable();
        while words.next_if(|word| ["public", "override", "private", "static"].contains(word)).is_some() {}
        if !words.next().is_some_and(|word| keywords.contains(&word)) { continue; }
        let Some(name) = words.next() else { continue };
        found.push(name.split([':', '=', '(']).next().unwrap().to_string());
    }
    found
}

#[test]
fn every_member_a_generated_class_declares_is_a_user_name_or_a_reserved_member() {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(directory).unwrap().map(|entry| entry.unwrap().path()).filter(|path| path.extension().is_some_and(|extension| extension == "dbl")).collect();
    paths.sort();
    let models = parse_files(&paths).unwrap();
    let mut allowed: BTreeSet<String> = MODEL_MEMBERS.iter().map(|name| normal(name)).collect();
    for model in &models {
        allowed.extend(model.fields.iter().map(|field| normal(&field.name)));
        allowed.extend(model.ephemeral.iter().map(|field| normal(&field.field.name)));
        allowed.extend(model.relations.iter().map(|relation| normal(&relation.name)));
    }
    for path in &paths {
        let text = std::fs::read_to_string(path).unwrap();
        for word in text.split(|character: char| !(character.is_alphanumeric() || character == '_' || character == ':')) {
            if let Some(name) = word.strip_prefix(':').filter(|name| !name.is_empty()) { allowed.insert(normal(name)); }
        }
    }
    let swift = emit_swift(&models, &[], &SchemaTypes::default());
    let kotlin = emit_kotlin(&models, &[], &SchemaTypes::default());
    let swift_members = declared(&swift, &[4], &["let", "var"]);
    let kotlin_members = declared(&kotlin, &[4, 8], &["val"]);
    assert!(swift_members.len() > 20 && kotlin_members.len() > 20, "the fixtures declare members: Swift {}, Kotlin {}", swift_members.len(), kotlin_members.len());
    for (platform, members) in [("Swift", swift_members), ("Kotlin", kotlin_members)] {
        let unlisted: BTreeSet<&String> = members.iter().filter(|name| !allowed.contains(&normal(name))).collect();
        assert!(unlisted.is_empty(), "{platform}: generated members that are neither a user name nor in MODEL_MEMBERS: {unlisted:?}");
    }
}

#[test]
fn a_field_named_like_a_generated_member_is_refused() {
    for name in MODEL_MEMBERS {
        let errors = dblayer_gen::parse_models(&format!("model Page do\n  attribute :{name}, :string\nend\n")).unwrap_err();
        let constructor = format!("model `Page` constructor: field `{name}` has the name of the generated parameter `{name}`");
        let core = format!("field name {name} is reserved");
        assert!(errors.iter().any(|error| error.message.contains(&constructor) || error.message.contains(&core)), "{name}: wanted a refusal, got {errors:?}");
    }
}
