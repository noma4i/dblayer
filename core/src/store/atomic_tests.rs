use crate::db::fixtures::{all_specs, put, rid};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};

fn write(database: &crate::db::Db, ops: Vec<StoreOp>) -> Result<super::batch_write::BatchWritten, super::StoreError> {
    let batch = StoreBatch { author: BatchAuthor::Outbox, id: 1, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
    database.shared.store.write_batch(&batch, &database.shared.files, &database.shared.pointer_guards, None)
}

#[test]
fn store_prop_expiry_any_author() {
    let spec = crate::schema::ModelSpec { storage: crate::schema::Storage::Memory, expire_after: Some("30.seconds".into()), ..crate::schema::ModelSpec::new("presence", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::scalar("room", crate::value::ScalarKind::Str).optional()]) };
    let database = crate::db::Db::open_in_memory(vec![spec], None).unwrap();
    let clock = std::sync::Arc::new(crate::pacing::ManualClock::new(std::time::Duration::from_millis(1000)));
    database.set_clock(clock.clone());
    let row = RowValue { row_id: "p1".into(), values: r#"{"id":"p1","room":"r1"}"#.into() };
    let mut previous = None;
    for author in [BatchAuthor::Window, BatchAuthor::Feed, BatchAuthor::Outbox, BatchAuthor::LocalWrites] {
        let (rows, ops) = match author {
            BatchAuthor::Window => (Vec::new(), vec![StoreOp::Window { key: crate::owners::read::windows::WindowKey::nodes("presence", &["p1"]).name, model: "presence".into(), page: 1, replace_all: false, cursor: None, roots: vec![row.clone()], related: Vec::new(), total: None, base_revision: 0 }]),
            BatchAuthor::Feed => (vec![("presence".into(), serde_json::json!({"op":"upsert", "id":"p1", "row":{"id":"p1","room":"r1"}}).to_string())], Vec::new()),
            BatchAuthor::Outbox => (Vec::new(), vec![StoreOp::Rows { model: "presence".into(), rows: vec![row.clone()] }]),
            BatchAuthor::LocalWrites => (Vec::new(), vec![StoreOp::LocalRows { model: "presence".into(), rows: vec![row.clone()] }]),
            _ => unreachable!(),
        };
        clock.advance(std::time::Duration::from_millis(10));
        let batch = StoreBatch { author, id: 1, epoch: 1, basis: Default::default(), rows, intents: 0, credential: None, ops };
        let result = database.shared.store.write_batch(&batch, &database.shared.files, &database.shared.pointer_guards, None).unwrap();
        assert_eq!(result.expiring.len(), 1, "{author:?}");
        assert_eq!((result.expiring[0].row_id.as_str(), result.expiring[0].expires_after), ("p1", 30_000));
        if let Some(old) = previous {
            write(&database, vec![StoreOp::ExpireRow { model: "presence".into(), row_id: "p1".into(), write_revision: old }]).unwrap();
            assert!(database.shared.store.read(&["presence"], |reader| reader.point("presence", "p1")).unwrap().is_some());
        }
        previous = Some(result.revision);
    }
    write(&database, vec![StoreOp::ExpireRow { model: "presence".into(), row_id: "p1".into(), write_revision: previous.unwrap() }]).unwrap();
    assert!(database.shared.store.read(&["presence"], |reader| reader.point("presence", "p1")).unwrap().is_none());
}

#[test]
fn store_prop_overlay_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let mut specs = all_specs();
    let field = crate::schema::FieldSpec::scalar("typing", crate::value::ScalarKind::Bool).with_default(serde_json::json!(false));
    specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().ephemeral.push(crate::schema::EphemeralSpec::new(field));
    let database = crate::db::Db::open(&directory.path().join("data.sqlite"), specs, None).unwrap();
    write(&database, vec![StoreOp::Rows { model: "moments".into(), rows: vec![RowValue { row_id: "m1".into(), values: r#"{"id":"m1","text":"one"}"#.into() }] }]).unwrap();
    let before = database.shared.store.revision_seq().unwrap();
    let event = |payload: serde_json::Value| StoreBatch { author: BatchAuthor::Feed, id: 1, epoch: 1, basis: Default::default(), rows: vec![("moments".into(), payload.to_string())], intents: 0, credential: None, ops: Vec::new() };
    let ephemeral = event(serde_json::json!({"op":"ephemeral","id":"m1","values":{"typing":true}}));
    let first = database.shared.store.write_batch(&ephemeral, &database.shared.files, &database.shared.pointer_guards, None).unwrap();
    assert_eq!(first.revision, before as u64);
    assert!(first.position > 0);
    database.shared.store.read(&["moments"], |reader| {
        let row = reader.point("moments", "m1")?.unwrap();
        assert_eq!((reader.revision_seq()?, row.get("typing")), (before, Some(&serde_json::json!(true))));
        Ok(())
    }).unwrap();
    let mixed = event(serde_json::json!({"op":"upsert","id":"m1","row":{"id":"m1","text":"two"},"set":{"typing":false}}));
    let second = database.shared.store.write_batch(&mixed, &database.shared.files, &database.shared.pointer_guards, None).unwrap();
    assert_eq!((second.revision, second.position), (before as u64 + 1, 0));
    database.shared.store.read(&["moments"], |reader| {
        let row = reader.point("moments", "m1")?.unwrap();
        assert_eq!((reader.revision_seq()?, row.get("text"), row.get("typing")), (before + 1, Some(&serde_json::json!("two")), Some(&serde_json::json!(false))));
        Ok(())
    }).unwrap();
}

#[test]
fn store_prop_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let database = crate::db::Db::open(&directory.path().join("data.sqlite"), all_specs(), None).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let ops = vec![
        StoreOp::AttachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into(), bytes: vec![7; 300_000], name: "photo".into(), mime: "image/jpeg".into() },
        StoreOp::Rows { model: "missing".into(), rows: vec![RowValue { row_id: "a".into(), values: r#"{"id":"a"}"#.into() }] },
    ];
    assert!(write(&database, ops).is_err());
    assert!(database.file("moments", &rid("m1"), "media").unwrap().is_none());
    let files = directory.path().join("data.sqlite.files");
    assert!(!files.exists() || std::fs::read_dir(files).unwrap().all(|entry| entry.unwrap().file_name() == "cache"));
}

#[test]
fn store_commit_file_orphan() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.sqlite");
    let database = crate::db::Db::open(&path, all_specs(), None).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let attach = |byte| StoreOp::AttachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into(), bytes: vec![byte; 300_000], name: "photo".into(), mime: "image/jpeg".into() };
    write(&database, vec![attach(1)]).unwrap();
    let old = database.file("moments", &rid("m1"), "media").unwrap().unwrap();
    let files = directory.path().join("data.sqlite.files");
    std::fs::create_dir_all(files.join("cache")).unwrap();
    std::fs::write(files.join("cache").join(&old.id), b"cached").unwrap();
    write(&database, vec![attach(2)]).unwrap();
    let new = database.file("moments", &rid("m1"), "media").unwrap().unwrap();
    assert_ne!(old.id, new.id);
    assert!(!files.join(&old.id).exists());
    assert!(!files.join("cache").join(&old.id).exists());
    assert_eq!(database.read("moments", &rid("m1"), "media").unwrap(), Some(vec![2; 300_000]));
}

#[test]
fn store_prop_file_orphans() {
    let directory = tempfile::tempdir().unwrap();
    let database = crate::db::Db::open(&directory.path().join("data.sqlite"), all_specs(), None).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let attach = |field: &str| StoreOp::AttachFile { model: "moments".into(), row_id: "m1".into(), field: field.into(), bytes: vec![3; 300_000], name: "photo".into(), mime: "image/jpeg".into() };
    write(&database, vec![attach("media"), attach("cover")]).unwrap();
    let detached = database.file("moments", &rid("m1"), "media").unwrap().unwrap();
    let retained = database.file("moments", &rid("m1"), "cover").unwrap().unwrap();
    write(&database, vec![StoreOp::DetachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into() }]).unwrap();
    let files = directory.path().join("data.sqlite.files");
    assert!(!files.join(detached.id).exists());
    assert!(files.join(retained.id).exists());
    assert!(database.file("moments", &rid("m1"), "media").unwrap().is_none());
    assert_eq!(database.read("moments", &rid("m1"), "cover").unwrap(), Some(vec![3; 300_000]));
}

#[test]
fn store_boot_finishes_file_rename() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.sqlite");
    let database = crate::db::Db::open(&path, all_specs(), None).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let bytes = vec![7; 300_000];
    let descriptor = database.attach(&database.admit().unwrap(), "moments", &rid("m1"), "media", &bytes, "photo", "image/jpeg").unwrap();
    database.close().unwrap();
    drop(database);
    let files = directory.path().join("data.sqlite.files");
    let target = files.join(&descriptor.id);
    let partial = files.join(format!("{}.partial", descriptor.id));
    std::fs::rename(&target, &partial).unwrap();
    std::fs::write(files.join("orphan.partial"), b"orphan").unwrap();
    std::fs::write(files.join("orphan"), b"orphan").unwrap();
    std::fs::create_dir_all(files.join("cache")).unwrap();
    std::fs::write(files.join("cache/orphan"), b"orphan").unwrap();
    std::fs::create_dir_all(files.join("transfers")).unwrap();
    std::fs::write(files.join("transfers/1"), b"transfer").unwrap();
    let reopened = crate::db::Db::open(&path, all_specs(), None).unwrap();
    assert_eq!(reopened.read("moments", &rid("m1"), "media").unwrap(), Some(bytes));
    assert!(!partial.exists());
    assert!(!files.join("orphan.partial").exists());
    assert!(!files.join("orphan").exists());
    assert!(!files.join("cache/orphan").exists());
    assert!(files.join("transfers/1").exists());
}

#[test]
fn store_batch_same_row_ordered() {
    let mut specs = all_specs();
    let moments = specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap();
    let mut field = crate::schema::FieldSpec::scalar("pinned", crate::value::ScalarKind::Str).optional();
    field.merge = Some(crate::schema::MergeRule::Local);
    moments.fields.push(field);
    let database = crate::db::Db::open_in_memory(specs, None).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1", "text": "before", "pinned": "before"}));
    let before = database.revision_seq().unwrap();
    let row = |id: &str, text: &str, pinned: &str| RowValue { row_id: id.into(), values: serde_json::json!({"id": id, "userId": "u1", "text": text, "pinned": pinned}).to_string() };
    let ops = vec![
        StoreOp::LocalRows { model: "moments".into(), rows: vec![row("m1", "local", "local")] },
        StoreOp::Rows { model: "moments".into(), rows: vec![row("m1", "server", "server")] },
    ];
    write(&database, ops).unwrap();
    let found = database.find("moments", &rid("m1")).unwrap().unwrap();
    assert_eq!(found.text("text"), Some("server"));
    assert_eq!(found.text("pinned"), Some("local"));
    assert_eq!(database.revision_seq().unwrap(), before + 1);
    let changes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = changes.clone();
    *database.shared.store.after_commit.lock().unwrap() = Some(Box::new(move |result| observed.lock().unwrap().extend(result.rows.clone())));
    let transient = vec![
        StoreOp::Rows { model: "moments".into(), rows: vec![row("m2", "transient", "")] },
        StoreOp::RemoveRows { model: "moments".into(), ids: vec!["m2".into()] },
    ];
    write(&database, transient).unwrap();
    assert!(database.find("moments", &rid("m2")).unwrap().is_none());
    assert!(changes.lock().unwrap().iter().all(|change| change.row.id.as_str() != "m2"));
}
