use crate::store::Store;
use crate::store::fixtures::{all_specs, id, put};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreBatch, StoreOp};

fn write(database: &std::sync::Arc<crate::store::Store>, ops: Vec<StoreOp>) -> Result<super::batch_write::BatchWritten, super::StoreError> {
    let batch = StoreBatch { author: BatchAuthor::Outbox, id: 1, epoch: 1, basis: Default::default(), rows: Vec::new(), intents: 0, credential: None, ops };
    database.write_batch(&batch, database.files(), database.pointer_guards(), None)
}

#[test]
fn store_prop_expiry_any_author() {
    let spec = crate::schema::ModelSpec { storage: crate::schema::Storage::Memory, expire_after: Some("30.seconds".into()), ..crate::schema::ModelSpec::new("presence", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id), crate::schema::FieldSpec::scalar("room", crate::value::ScalarKind::Str).optional()]) };
    let database = Store::open_models(None, vec![spec], None).map(std::sync::Arc::new).unwrap();
    database.set_clock(std::sync::Arc::new(crate::runtime::ManualClock::new(std::time::Duration::from_millis(1000))));
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
        let batch = StoreBatch { author, id: 1, epoch: 1, basis: Default::default(), rows, intents: 0, credential: None, ops };
        let result = database.write_batch(&batch, database.files(), database.pointer_guards(), None).unwrap();
        assert_eq!(result.expiring.len(), 1, "{author:?}");
        assert_eq!((result.expiring[0].row_id.as_str(), result.expiring[0].expires_after), ("p1", 30_000));
        if let Some(old) = previous {
            write(&database, vec![StoreOp::ExpireRow { model: "presence".into(), row_id: "p1".into(), write_revision: old }]).unwrap();
            assert!(database.read(&["presence"], |reader| reader.point("presence", "p1")).unwrap().is_some());
        }
        previous = Some(result.revision);
    }
    write(&database, vec![StoreOp::ExpireRow { model: "presence".into(), row_id: "p1".into(), write_revision: previous.unwrap() }]).unwrap();
    assert!(database.read(&["presence"], |reader| reader.point("presence", "p1")).unwrap().is_none());
}

#[test]
fn store_prop_overlay_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let mut specs = all_specs();
    let field = crate::schema::FieldSpec::scalar("typing", crate::value::ScalarKind::Bool).with_default(serde_json::json!(false));
    specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().ephemeral.push(crate::schema::EphemeralSpec::new(field));
    let database = Store::open_models(Some(&directory.path().join("data.sqlite")), specs, None).map(std::sync::Arc::new).unwrap();
    write(&database, vec![StoreOp::Rows { model: "moments".into(), rows: vec![RowValue { row_id: "m1".into(), values: r#"{"id":"m1","text":"one"}"#.into() }] }]).unwrap();
    let before = database.revision_seq().unwrap();
    let event = |payload: serde_json::Value| StoreBatch { author: BatchAuthor::Feed, id: 1, epoch: 1, basis: Default::default(), rows: vec![("moments".into(), payload.to_string())], intents: 0, credential: None, ops: Vec::new() };
    let ephemeral = event(serde_json::json!({"op":"ephemeral","id":"m1","values":{"typing":true}}));
    let first = database.write_batch(&ephemeral, database.files(), database.pointer_guards(), None).unwrap();
    assert_eq!(first.revision, before as u64);
    assert!(first.position > 0);
    database.read(&["moments"], |reader| {
        let row = reader.point("moments", "m1")?.unwrap();
        assert_eq!((reader.revision_seq()?, row.get("typing")), (before, Some(&serde_json::json!(true))));
        Ok(())
    }).unwrap();
    let mixed = event(serde_json::json!({"op":"upsert","id":"m1","row":{"id":"m1","text":"two"},"set":{"typing":false}}));
    let second = database.write_batch(&mixed, database.files(), database.pointer_guards(), None).unwrap();
    assert_eq!((second.revision, second.position), (before as u64 + 1, 0));
    database.read(&["moments"], |reader| {
        let row = reader.point("moments", "m1")?.unwrap();
        assert_eq!((reader.revision_seq()?, row.get("text"), row.get("typing")), (before + 1, Some(&serde_json::json!("two")), Some(&serde_json::json!(false))));
        Ok(())
    }).unwrap();
}

#[test]
fn store_overlay_survives_failed_sql_commits() {
    let directory = tempfile::tempdir().unwrap();
    let mut specs = all_specs();
    let field = crate::schema::FieldSpec::scalar("typing", crate::value::ScalarKind::Bool).with_default(serde_json::json!(false));
    specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().ephemeral.push(crate::schema::EphemeralSpec::new(field));
    let database = Store::open_models(Some(&directory.path().join("data.sqlite")), specs, None).map(std::sync::Arc::new).unwrap();
    write(&database, vec![StoreOp::Rows { model: "moments".into(), rows: vec![RowValue { row_id: "m1".into(), values: r#"{"id":"m1","text":"before"}"#.into() }] }]).unwrap();
    let event = StoreBatch { author: BatchAuthor::Feed, id: 1, epoch: 1, basis: Default::default(), rows: vec![("moments".into(), serde_json::json!({"op":"ephemeral","id":"m1","values":{"typing":true}}).to_string())], intents: 0, credential: None, ops: Vec::new() };
    database.write_batch(&event, database.files(), database.pointer_guards(), None).unwrap();
    let before = database.revision_seq().unwrap();
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reached = attempts.clone();
    database.lock().connection.commit_hook(Some(move || {
        reached.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    })).unwrap();
    for _ in 0..65 {
        let row = RowValue { row_id: "m1".into(), values: r#"{"id":"m1","text":"rejected"}"#.into() };
        assert!(matches!(write(&database, vec![StoreOp::Rows { model: "moments".into(), rows: vec![row] }]), Err(super::StoreError::Constraint(_))));
        assert!(database.overlays.get(before).is_some(), "the committed overlay must remain available");
        assert!(database.overlays.get(before + 1).is_none(), "the aborted overlay must not remain published");
    }
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 65);
    database.lock().connection.commit_hook(None::<fn() -> bool>).unwrap();
    let row = database.find("moments", &id("m1")).unwrap().unwrap();
    assert_eq!(database.revision_seq().unwrap(), before);
    assert_eq!(row.get("text"), Some(&serde_json::json!("before")));
    assert_eq!(row.get("typing"), Some(&serde_json::json!(true)));
    write(&database, vec![StoreOp::Rows { model: "moments".into(), rows: vec![RowValue { row_id: "m1".into(), values: r#"{"id":"m1","text":"after"}"#.into() }] }]).unwrap();
    assert_eq!(database.revision_seq().unwrap(), before + 1);
}

#[test]
fn store_prop_atomic() {
    let directory = tempfile::tempdir().unwrap();
    let database = Store::open_models(Some(&directory.path().join("data.sqlite")), all_specs(), None).map(std::sync::Arc::new).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let ops = vec![
        StoreOp::AttachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into(), bytes: vec![7; 300_000], name: "photo".into(), mime: "image/jpeg".into() },
        StoreOp::Rows { model: "missing".into(), rows: vec![RowValue { row_id: "a".into(), values: r#"{"id":"a"}"#.into() }] },
    ];
    assert!(write(&database, ops).is_err());
    assert!(database.file("moments", &id("m1"), "media").unwrap().is_none());
    let files = directory.path().join("data.sqlite.files");
    assert!(!files.exists() || std::fs::read_dir(files).unwrap().all(|entry| entry.unwrap().file_name() == "cache"));
}

#[test]
fn store_commit_file_orphan() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.sqlite");
    let database = Store::open_models(Some(&path), all_specs(), None).map(std::sync::Arc::new).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let attach = |byte| StoreOp::AttachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into(), bytes: vec![byte; 300_000], name: "photo".into(), mime: "image/jpeg".into() };
    write(&database, vec![attach(1)]).unwrap();
    let old = database.file("moments", &id("m1"), "media").unwrap().unwrap();
    let files = directory.path().join("data.sqlite.files");
    std::fs::create_dir_all(files.join("cache")).unwrap();
    std::fs::write(files.join("cache").join(&old.id), b"cached").unwrap();
    write(&database, vec![attach(2)]).unwrap();
    let new = database.file("moments", &id("m1"), "media").unwrap().unwrap();
    assert_ne!(old.id, new.id);
    assert!(!files.join(&old.id).exists());
    assert!(!files.join("cache").join(&old.id).exists());
    assert_eq!(database.read_file("moments", &id("m1"), "media").unwrap(), Some(vec![2; 300_000]));
}

#[test]
fn store_prop_file_orphans() {
    let directory = tempfile::tempdir().unwrap();
    let database = Store::open_models(Some(&directory.path().join("data.sqlite")), all_specs(), None).map(std::sync::Arc::new).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let attach = |field: &str| StoreOp::AttachFile { model: "moments".into(), row_id: "m1".into(), field: field.into(), bytes: vec![3; 300_000], name: "photo".into(), mime: "image/jpeg".into() };
    write(&database, vec![attach("media"), attach("cover")]).unwrap();
    let detached = database.file("moments", &id("m1"), "media").unwrap().unwrap();
    let retained = database.file("moments", &id("m1"), "cover").unwrap().unwrap();
    write(&database, vec![StoreOp::DetachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into() }]).unwrap();
    let files = directory.path().join("data.sqlite.files");
    assert!(!files.join(detached.id).exists());
    assert!(files.join(retained.id).exists());
    assert!(database.file("moments", &id("m1"), "media").unwrap().is_none());
    assert_eq!(database.read_file("moments", &id("m1"), "cover").unwrap(), Some(vec![3; 300_000]));
}

#[test]
fn store_boot_finishes_file_rename() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.sqlite");
    let database = Store::open_models(Some(&path), all_specs(), None).map(std::sync::Arc::new).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let bytes = vec![7; 300_000];
    write(&database, vec![StoreOp::AttachFile { model: "moments".into(), row_id: "m1".into(), field: "media".into(), bytes: bytes.clone(), name: "photo".into(), mime: "image/jpeg".into() }]).unwrap();
    let descriptor = database.file("moments", &id("m1"), "media").unwrap().unwrap();
    drop(database);
    let files = directory.path().join("data.sqlite.files");
    let target = files.join(&descriptor.id);
    let partial = files.join(format!("{}.partial", descriptor.id));
    std::fs::rename(&target, &partial).unwrap();
    std::fs::write(files.join("orphan.partial"), b"orphan").unwrap();
    std::fs::write(files.join("orphan"), b"orphan").unwrap();
    std::fs::create_dir_all(files.join("cache")).unwrap();
    std::fs::write(files.join("cache/orphan"), b"orphan").unwrap();
    let reopened = Store::open_models(Some(&path), all_specs(), None).map(std::sync::Arc::new).unwrap();
    assert_eq!(reopened.read_file("moments", &id("m1"), "media").unwrap(), Some(bytes));
    assert!(!partial.exists());
    assert!(!files.join("orphan.partial").exists());
    assert!(!files.join("orphan").exists());
    assert!(!files.join("cache/orphan").exists());
}

#[test]
fn store_boot_transfer_file_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.sqlite");
    let database = Store::open_models(Some(&path), all_specs(), None).map(std::sync::Arc::new).unwrap();
    for (transfer_id, size) in [(1, 10), (3, 0)] {
        database.lock().connection.execute(
            "INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts, seq, source, size, transfer_id) VALUES ('moments', ?1, 'upload', 'queued', 'prepare', 0, 1, 0, ?2, 'source', ?3, ?2)",
            rusqlite::params![format!("m{transfer_id}"), transfer_id, size],
        ).unwrap();
    }
    drop(database);
    let copies = directory.path().join("data.sqlite.files/transfers");
    std::fs::create_dir_all(&copies).unwrap();
    for name in ["1", "2", "3.part", "4.part"] {
        std::fs::write(copies.join(name), b"copy").unwrap();
    }
    let reopened = Store::open_models(Some(&path), all_specs(), None).map(std::sync::Arc::new).unwrap();
    let mut left: Vec<String> = std::fs::read_dir(&copies).unwrap().map(|entry| entry.unwrap().file_name().into_string().unwrap()).collect();
    left.sort();
    assert_eq!(left, ["1", "3.part"], "a copy with a sync_upload row stays, also while it is still copying, a copy without a holder goes");
    drop(reopened);
}

#[test]
fn store_batch_same_row_ordered() {
    let mut specs = all_specs();
    let moments = specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap();
    let mut field = crate::schema::FieldSpec::scalar("pinned", crate::value::ScalarKind::Str).optional();
    field.merge = Some(crate::schema::MergeRule::Local);
    moments.fields.push(field);
    let database = Store::open_models(None, specs, None).map(std::sync::Arc::new).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1", "text": "before", "pinned": "before"}));
    let before = database.revision_seq().unwrap();
    let row = |id: &str, text: &str, pinned: &str| RowValue { row_id: id.into(), values: serde_json::json!({"id": id, "userId": "u1", "text": text, "pinned": pinned}).to_string() };
    let ops = vec![
        StoreOp::LocalRows { model: "moments".into(), rows: vec![row("m1", "local", "local")] },
        StoreOp::Rows { model: "moments".into(), rows: vec![row("m1", "server", "server")] },
    ];
    write(&database, ops).unwrap();
    let found = database.find("moments", &id("m1")).unwrap().unwrap();
    assert_eq!(found.text("text"), Some("server"));
    assert_eq!(found.text("pinned"), Some("local"));
    assert_eq!(database.revision_seq().unwrap(), before + 1);
    let changes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = changes.clone();
    *database.after_commit.lock().unwrap() = Some(Box::new(move |result| observed.lock().unwrap().extend(result.rows.clone())));
    let transient = vec![
        StoreOp::Rows { model: "moments".into(), rows: vec![row("m2", "transient", "")] },
        StoreOp::RemoveRows { model: "moments".into(), ids: vec!["m2".into()] },
    ];
    write(&database, transient).unwrap();
    assert!(database.find("moments", &id("m2")).unwrap().is_none());
    assert!(changes.lock().unwrap().iter().all(|change| change.row.id.as_str() != "m2"));
}

#[test]
fn out_ephemeral_reset_by_position() {
    let mut specs = all_specs();
    let field = crate::schema::FieldSpec::scalar("typing", crate::value::ScalarKind::Bool).with_default(serde_json::json!(false));
    specs.iter_mut().find(|spec| spec.key.as_str() == "moments").unwrap().ephemeral.push(crate::schema::EphemeralSpec::new(field).resetting_after("5.seconds"));
    let database = Store::open_models(None, specs, None).map(std::sync::Arc::new).unwrap();
    put(&database, "moments", serde_json::json!({"id": "m1", "userId": "u1"}));
    let typing = |database: &Store| database.read(&["moments"], |reader| Ok(reader.point("moments", "m1")?.unwrap().get("typing").cloned())).unwrap();
    let set = || StoreBatch { author: BatchAuthor::Feed, id: 1, epoch: 1, basis: Default::default(), rows: vec![("moments".into(), serde_json::json!({"op":"ephemeral","id":"m1","values":{"typing":true}}).to_string())], intents: 0, credential: None, ops: Vec::new() };
    let started = u64::try_from(database.lock().now_ms()).unwrap();
    let first = database.write_batch(&set(), database.files(), database.pointer_guards(), None).unwrap();
    let second = database.write_batch(&set(), database.files(), database.pointer_guards(), None).unwrap();
    let (old, current) = (first.resets[0].clone(), second.resets[0].clone());
    assert!(old.deadline >= started + 5000 && old.field == "typing");
    assert_ne!(old.position, current.position);
    let reset = |reset: &crate::owners::store::EphemeralReset| write(&database, vec![StoreOp::ResetEphemeral { model: reset.model.clone(), row_id: reset.row_id.clone(), field: reset.field.clone(), position: reset.position }]).unwrap();
    reset(&old);
    assert_eq!(typing(&database), Some(serde_json::json!(true)), "a later write keeps the value, its own reset decides");
    reset(&current);
    assert_eq!(typing(&database), Some(serde_json::json!(false)), "the reset of the current position restores the default");
}
