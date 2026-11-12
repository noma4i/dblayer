//! Every public path that writes a value of a row, against every kind of field of the fixture `kinds.spec.json` (a nested object reads back positionally, in the order of its layout, M63). Oracle: a valid value reads back exactly through the public read, an invalid value is `Invalid` naming the field, a null clears a nullable, an omitted field takes its default.
use serde_json::{json, Value};

use super::cases::Cases;
use super::rig_tests::bound;
use crate::ffi::wire::{Cell, WireRow};
use crate::ffi::{Config, Database, DbError, OpenPorts};

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
async fn every_write_path_stores_and_refuses_each_field_kind() {
    let mut matrix = Cases::new("write path x field kind");
    let spec: Value = serde_json::from_str(SPEC).unwrap();
    let model = &spec["models"][0];
    let fields: Vec<&Value> = model["fields"].as_array().unwrap().iter().filter(|field| field["name"] != "id").collect();
    let ephemeral: Vec<&Value> = model["ephemeral"].as_array().unwrap().iter().collect();
    let order: Vec<String> = fields.iter().chain(ephemeral.iter()).map(|field| field["name"].as_str().unwrap().to_string()).collect();

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
        matrix.cell(&format!("create.{field}"), "valid", &value.to_string(), &wanted, &observed);
        if let Some(bad) = bad {
            let result = create(json!({"title": "t", *field: bad.clone()})).await.map(|receipt| receipt.optimistic_value.unwrap());
            matrix.cell(&format!("create.{field}"), "invalid", &bad.to_string(), "Invalid", &invalid_names(&result, field));
        }
    }
    // null and omitted
    for field in fields.iter().filter(|field| field["nullable"] == true).map(|field| field["name"].as_str().unwrap()) {
        let created = create(json!({"title": "t", field: Value::Null})).await.map(|receipt| receipt.optimistic_value);
        let observed = match created { Ok(Some(row)) => named(&read(row.id).await.ok().flatten().unwrap_or(row_placeholder()), &order, field).to_string(), other => format!("not created: {other:?}") };
        matrix.cell(&format!("create.{field}"), "null on a nullable", "null", "null", &observed);
    }
    if let Ok(Some(row)) = create(json!({"title": "t"})).await.map(|receipt| receipt.optimistic_value) {
        let stored = read(row.id).await.ok().flatten().unwrap_or(row_placeholder());
        for field in order.iter().filter(|name| *name != "title") {
            matrix.cell(&format!("create.{field}"), "omitted", "field absent", &defaults(field).to_string(), &named(&stored, &order, field).to_string());
        }
    }
    // unknown enum text from the app is accepted and kept on create and update (M54)
    let created = create(json!({"title": "t", "mood": "weird"})).await.map(|receipt| receipt.optimistic_value);
    let observed = match created { Ok(Some(row)) => read(row.id).await.ok().flatten().map(|stored| named(&stored, &order, "mood").to_string()).unwrap_or_default(), other => format!("not created: {other:?}") };
    matrix.cell("create.mood", "unknown enum case", "\"weird\"", "\"weird\"", &observed);
    // the file field is not writable by create; the local field (note) is excluded from create and update and written only by set_local
    let result = create(json!({"title": "t", "photo": "x"})).await.map(|receipt| receipt.optimistic_value.unwrap());
    matrix.cell("create.photo", "file", "\"x\"", "Invalid", &invalid_names(&result, "photo"));

    // path update: valid value per field on an existing row
    if let Ok(Some(base)) = create(json!({"title": "t"})).await.map(|receipt| receipt.optimistic_value) {
        for (field, value, expected, bad) in &valid {
            let result = bound(database.update(epoch, "Kind".into(), base.id.clone(), json!({*field: value.clone()}).to_string(), "{}".into())).await;
            let observed = match result { Ok(_) => read(base.id.clone()).await.ok().flatten().map(|row| named(&row, &order, field).to_string()).unwrap_or_default(), Err(error) => format!("{error:?}") };
            matrix.cell(&format!("update.{field}"), "valid", &value.to_string(), &expected.to_string(), &observed);
            if let Some(bad) = bad {
                let result = bound(database.update(epoch, "Kind".into(), base.id.clone(), json!({*field: bad.clone()}).to_string(), "{}".into())).await.map(|_| unreachable_row());
                matrix.cell(&format!("update.{field}"), "invalid", &bad.to_string(), "Invalid", &invalid_names(&result, field));
            }
        }
        let result = bound(database.update(epoch, "Kind".into(), base.id.clone(), json!({"mood": "weird"}).to_string(), "{}".into())).await;
        let observed = match result { Ok(_) => read(base.id.clone()).await.ok().flatten().map(|row| named(&row, &order, "mood").to_string()).unwrap_or_default(), Err(error) => format!("{error:?}") };
        matrix.cell("update.mood", "unknown enum case", "\"weird\"", "\"weird\"", &observed);
        // set_local: the local key and the local field
        for (path, value, expected) in [("note", json!("local"), json!("local")), ("box.stamp", json!("s"), json!(["L", "s", [2, ["v"]]]))] {
            let result = bound(database.set_local(epoch, "Kind".into(), base.id.clone(), path.into(), value.to_string())).await;
            let observed = match result { Ok(()) => read(base.id.clone()).await.ok().flatten().map(|row| named(&row, &order, path.split('.').next().unwrap_or(path)).to_string()).unwrap_or_default(), Err(error) => format!("{error:?}") };
            matrix.cell(&format!("set_local.{path}"), "valid", &value.to_string(), &expected.to_string(), &observed);
        }
        let result = bound(database.set_local(epoch, "Kind".into(), base.id.clone(), "title".into(), "\"x\"".into())).await;
        matrix.cell("set_local.title", "a server field", "\"x\"", "Invalid", match &result { Err(DbError::Invalid { detail }) if detail.contains("title") => "Invalid", Err(_) => "other error", Ok(()) => "accepted" });
        // attach: the file field
        let source = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(source.path(), [1, 2]).unwrap();
        let result = bound(database.attach_file(epoch, "Kind".into(), base.id.clone(), "photo".into(), source.path().to_string_lossy().into(), "a.png".into(), "image/png".into())).await;
        matrix.cell("attach.photo", "file", "2 bytes", "stored", if result.is_ok() { "stored" } else { "refused" });
        // set_ephemeral
        let result = bound(database.set_ephemeral(epoch, "Kind".into(), base.id.clone(), json!({"typing": true}).to_string())).await;
        let observed = match result { Ok(()) => read(base.id.clone()).await.ok().flatten().map(|row| named(&row, &order, "typing").to_string()).unwrap_or_default(), Err(error) => format!("{error:?}") };
        matrix.cell("set_ephemeral.typing", "valid", "true", "true", &observed);
        let result = bound(database.set_ephemeral(epoch, "Kind".into(), base.id.clone(), json!({"typing": "yes"}).to_string())).await.map(|()| unreachable_row());
        matrix.cell("set_ephemeral.typing", "invalid", "\"yes\"", "Invalid", &invalid_names(&result, "typing"));
    }
    matrix.finish();
}

fn row_placeholder() -> WireRow { unreachable_row() }

fn unreachable_row() -> WireRow {
    WireRow { id: String::new(), revision: 0, estimated_bytes: 0, layout: String::new(), cells: Vec::new(), sync: crate::ffi::wire::WireSync::default(), uploads: Vec::new() }
}
