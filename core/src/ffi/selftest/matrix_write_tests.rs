//! Matrix W (a nested object reads back positionally, in the order of its layout, M63): every public path that writes a value of a row, against every kind of field the schema knows. Kinds come from the enum of the core (`FieldKind`, `ScalarKind`) and from the fixture `fixtures/matrix/kinds.dbl`; a kind the fixture lacks is a red cell. Oracle: a valid value reads back exactly through the public read, an invalid value is `Invalid` naming the field, a null clears a nullable, an omitted field takes its default.
use std::collections::BTreeSet;

use serde_json::{json, Value};

use super::rig_tests::bound;
use crate::ffi::wire::{Cell, WireRow};
use crate::ffi::{Config, Database, DbError, FileBytes, OpenPorts};
use crate::test_matrix::{crate_dir, Matrix};

const SPEC: &str = include_str!("../../../tests/corpus/matrix/kinds.spec.json");

fn plain(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool { value } => json!(value),
        Cell::Int { value } => json!(value),
        Cell::Real { value } => json!(value),
        Cell::Text { value } => json!(value),
        Cell::Seq { items } => Value::Array(items.iter().map(plain).collect()),
        Cell::Keyed { entries } => Value::Object(entries.iter().map(|entry| (entry.key.clone(), plain(&entry.value))).collect()),
    }
}

fn source_variants(file: &str, name: &str) -> Vec<String> {
    let text = std::fs::read_to_string(crate_dir().join(file)).unwrap_or_default();
    let Some(at) = text.find(&format!("pub enum {name}")) else { return Vec::new() };
    let Some(open) = text[at..].find('{') else { return Vec::new() };
    let mut depth = 0usize;
    let mut body = String::new();
    for character in text[at + open..].chars() {
        match character {
            '{' | '(' => { depth += 1; if depth == 1 { continue; } }
            '}' | ')' => { depth -= 1; if depth == 0 { break; } }
            _ => {}
        }
        if depth == 1 { body.push(character); }
    }
    body.split(',').filter_map(|part| part.trim().split(|character: char| !character.is_alphanumeric()).next().map(str::to_string)).filter(|variant| !variant.is_empty()).collect()
}

fn named(row: &WireRow, order: &[String], field: &str) -> Value {
    order.iter().position(|name| name == field).and_then(|index| row.cells.get(index)).map(plain).unwrap_or(Value::Null)
}

fn invalid_names(result: &Result<WireRow, DbError>, field: &str) -> String {
    match result {
        Err(DbError::Invalid { detail }) if detail.contains(field) => "Invalid".into(),
        Err(DbError::Invalid { detail }) => format!("Invalid without the field name: {detail}"),
        Err(other) => format!("{other:?}"),
        Ok(_) => "accepted".into(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_write_path_kind() {
    let mut matrix = Matrix::new("W-write-path-kind");
    let spec: Value = serde_json::from_str(SPEC).unwrap();
    let model = &spec["models"][0];
    let fields: Vec<&Value> = model["fields"].as_array().unwrap().iter().filter(|field| field["name"] != "id").collect();
    let ephemeral: Vec<&Value> = model["ephemeral"].as_array().unwrap().iter().collect();
    let order: Vec<String> = fields.iter().chain(ephemeral.iter()).map(|field| field["name"].as_str().unwrap().to_string()).collect();

    let signatures: BTreeSet<String> = fields.iter().chain(ephemeral.iter()).map(|field| {
        let kind = &field["kind"];
        match kind["type"].as_str().unwrap() {
            "scalar" => format!("scalar:{}", kind["kind"].as_str().unwrap()),
            other => other.to_string(),
        }
    }).collect();
    let mut wanted: BTreeSet<String> = BTreeSet::new();
    for variant in source_variants("src/schema/field.rs", "FieldKind") {
        let snake: String = variant.chars().enumerate().flat_map(|(index, character)| if character.is_uppercase() && index > 0 { vec!['_', character.to_ascii_lowercase()] } else { vec![character.to_ascii_lowercase()] }).collect();
        if snake == "scalar" {
            for scalar in source_variants("src/value/mod.rs", "ScalarKind") { wanted.insert(format!("scalar:{}", scalar.to_lowercase())); }
        } else {
            wanted.insert(snake);
        }
    }
    for kind in &wanted {
        matrix.check("fixtures/matrix/kinds.dbl", kind, "a field of this kind exists", "declared", if signatures.contains(kind) { "declared" } else { "no field of this kind in the fixture" });
    }

    let directory = tempfile::tempdir().unwrap();
    let config = Config { path: Some(directory.path().join("kinds.sqlite").to_string_lossy().into()), data_version: None };
    let database = Database::open(config, crate::ffi::specs(spec["models"].to_string()), OpenPorts::default()).await.unwrap();
    let epoch = database.session_epoch();

    // path create: valid, invalid, null, omitted per field
    type Row = (&'static str, Value, Value, Option<Value>);
    let valid: Vec<Row> = vec![
        ("title", json!("x"), json!("x"), Some(json!(5))),
        ("count", json!(3), json!(3), Some(json!("one"))),
        ("ratio", json!(1.5), json!(1.5), Some(json!("x"))),
        ("flag", json!(true), json!(true), Some(json!("yes"))),
        ("when", json!("2026-11-07T10:00:00Z"), json!("2026-11-07T10:00:00.000Z"), Some(json!(5))),
        ("ref", json!("a1"), json!("a1"), Some(json!({"a": 1}))),
        ("payload", json!({"k": [1, true, null]}), json!({"k": [1, true, null]}), None),
        ("mood", json!("loud"), json!("loud"), Some(json!(5))),
        ("fallback", json!("z"), json!("z"), Some(json!(5))),
        ("tags", json!(["a", "b"]), json!(["a", "b"]), Some(json!("a"))),
        ("note", json!("n"), Value::Null, None),
        ("box", json!({"label": "L", "inner": {"depth": 2, "leaf": {"value": "v"}}}), json!(["L", null, [2, ["v"]]]), Some(json!({"label": 5}))),
        ("boxes", json!([{"name": "n"}]), json!([["n"]]), Some(json!([{"name": 5}]))),
        ("attachment", json!({"__typename": "KindWord", "text": "t"}), json!(["KindWord", ["t"]]), Some(json!({"__typename": "KindWord", "text": 5}))),
        ("attachments", json!([{"__typename": "KindCount", "amount": 2}]), json!([["KindCount", [2]]]), Some(json!([{"__typename": "KindCount", "amount": "x"}]))),
    ];
    let defaults = |field: &str| -> Value { match field { "fallback" => json!("base"), "tags" | "boxes" | "attachments" => json!([]), "typing" => json!(false), _ => Value::Null } };
    let create = |values: Value| { let database = &database; async move { bound(database.create(epoch, "Kind".into(), values.to_string(), "{}".into())).await } };
    let read = |id: String| { let database = &database; async move { bound(database.read_row(epoch, "Kind".into(), id)).await } };
    for (field, value, expected, bad) in &valid {
        let created = create(json!({"title": "t", *field: value.clone()})).await.map(|receipt| receipt.optimistic_value);
        let observed = match created {
            Ok(Some(row)) => { let back = read(row.id.clone()).await; match back { Ok(Some(stored)) => named(&stored, &order, field).to_string(), other => format!("not read back: {other:?}") } }
            other => format!("not created: {other:?}"),
        };
        let wanted = expected.to_string();
        matrix.check(&format!("create.{field}"), "valid", &value.to_string(), &wanted, &observed);
        if let Some(bad) = bad {
            let result = create(json!({"title": "t", *field: bad.clone()})).await.map(|receipt| receipt.optimistic_value.unwrap());
            matrix.check(&format!("create.{field}"), "invalid", &bad.to_string(), "Invalid", &invalid_names(&result, field));
        } else if *field != "note" {
            matrix.check(&format!("create.{field}"), "invalid", "any JSON", "N/A{json accepts any value}", "N/A{json accepts any value}");
        }
    }
    // null and omitted
    for field in fields.iter().filter(|field| field["nullable"] == true).map(|field| field["name"].as_str().unwrap()) {
        let created = create(json!({"title": "t", field: Value::Null})).await.map(|receipt| receipt.optimistic_value);
        let observed = match created { Ok(Some(row)) => named(&read(row.id).await.ok().flatten().unwrap_or(row_placeholder()), &order, field).to_string(), other => format!("not created: {other:?}") };
        matrix.check(&format!("create.{field}"), "null on a nullable", "null", "null", &observed);
    }
    if let Ok(Some(row)) = create(json!({"title": "t"})).await.map(|receipt| receipt.optimistic_value) {
        let stored = read(row.id).await.ok().flatten().unwrap_or(row_placeholder());
        for field in order.iter().filter(|name| *name != "title") {
            matrix.check(&format!("create.{field}"), "omitted", "field absent", &defaults(field).to_string(), &named(&stored, &order, field).to_string());
        }
    }
    // unknown enum from the app
    let result = create(json!({"title": "t", "mood": "weird"})).await.map(|receipt| receipt.optimistic_value.unwrap());
    matrix.check("create.mood", "unknown enum case", "\"weird\"", "Invalid", &invalid_names(&result, "mood"));
    // the file field is not writable by create; the local field (note) is excluded from create and update and written only by set_local
    let result = create(json!({"title": "t", "photo": "x"})).await.map(|receipt| receipt.optimistic_value.unwrap());
    matrix.check("create.photo", "file", "\"x\"", "Invalid", &invalid_names(&result, "photo"));

    // path update: valid value per field on an existing row
    if let Ok(Some(base)) = create(json!({"title": "t"})).await.map(|receipt| receipt.optimistic_value) {
        for (field, value, expected, bad) in &valid {
            let result = bound(database.update(epoch, "Kind".into(), base.id.clone(), json!({*field: value.clone()}).to_string(), "{}".into())).await;
            let observed = match result { Ok(_) => read(base.id.clone()).await.ok().flatten().map(|row| named(&row, &order, field).to_string()).unwrap_or_default(), Err(error) => format!("{error:?}") };
            matrix.check(&format!("update.{field}"), "valid", &value.to_string(), &expected.to_string(), &observed);
            if let Some(bad) = bad {
                let result = bound(database.update(epoch, "Kind".into(), base.id.clone(), json!({*field: bad.clone()}).to_string(), "{}".into())).await.map(|_| unreachable_row());
                matrix.check(&format!("update.{field}"), "invalid", &bad.to_string(), "Invalid", &invalid_names(&result, field));
            }
        }
        // set_local: the local key and the local field
        for (path, value, expected) in [("note", json!("local"), json!("local")), ("box.stamp", json!("s"), json!(["L", "s", [2, ["v"]]]))] {
            let result = bound(database.set_local(epoch, "Kind".into(), base.id.clone(), path.into(), value.to_string())).await;
            let observed = match result { Ok(()) => read(base.id.clone()).await.ok().flatten().map(|row| named(&row, &order, path.split('.').next().unwrap_or(path)).to_string()).unwrap_or_default(), Err(error) => format!("{error:?}") };
            matrix.check(&format!("set_local.{path}"), "valid", &value.to_string(), &expected.to_string(), &observed);
        }
        let result = bound(database.set_local(epoch, "Kind".into(), base.id.clone(), "title".into(), "\"x\"".into())).await;
        matrix.check("set_local.title", "a server field", "\"x\"", "Invalid", match &result { Err(DbError::Invalid { detail }) if detail.contains("title") => "Invalid", Err(_) => "other error", Ok(()) => "accepted" });
        // attach: the file field
        let result = bound(database.attach(epoch, "Kind".into(), base.id.clone(), "photo".into(), FileBytes { bytes: vec![1, 2], name: "a.png".into(), mime: "image/png".into() })).await;
        matrix.check("attach.photo", "file", "2 bytes", "stored", if result.is_ok() { "stored" } else { "refused" });
        // set_ephemeral
        let result = bound(database.set_ephemeral(epoch, "Kind".into(), base.id.clone(), json!({"typing": true}).to_string())).await;
        let observed = match result { Ok(()) => read(base.id.clone()).await.ok().flatten().map(|row| named(&row, &order, "typing").to_string()).unwrap_or_default(), Err(error) => format!("{error:?}") };
        matrix.check("set_ephemeral.typing", "valid", "true", "true", &observed);
        let result = bound(database.set_ephemeral(epoch, "Kind".into(), base.id.clone(), json!({"typing": "yes"}).to_string())).await.map(|()| unreachable_row());
        matrix.check("set_ephemeral.typing", "invalid", "\"yes\"", "Invalid", &invalid_names(&result, "typing"));
    }
    // the server landing paths run on the schema of the app in consumer/matrix_write_server_tests.rs
    matrix.finish();
}

fn row_placeholder() -> WireRow { unreachable_row() }

fn unreachable_row() -> WireRow {
    WireRow { id: String::new(), revision: 0, estimated_bytes: 0, layout: String::new(), cells: Vec::new(), sync: crate::ffi::wire::WireSync::default(), uploads: Vec::new() }
}
