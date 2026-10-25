use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::files::sha256_hex;
use crate::identity::ModelKey;
use serde_json::json;

const SMALL: &[u8] = b"small bytes";

fn large() -> Vec<u8> {
    (0..300 * 1024u32).map(|index| (index % 251) as u8).collect()
}

fn on_disk() -> (tempfile::TempDir, Db) {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(&root.path().join("42.sqlite"), all_specs(), None).unwrap();
    db.set_clock(std::sync::Arc::new(crate::pacing::ManualClock::new(std::time::Duration::from_millis(1_000_000))));
    (root, db)
}

fn count_files(db: &Db) -> i64 {
    db.shared.store.lock().connection.query_row("SELECT COUNT(*) FROM file", [], |row| row.get(0)).unwrap()
}

fn bytes_in_table(db: &Db) -> i64 {
    db.shared.store.lock().connection.query_row("SELECT COUNT(*) FROM file WHERE data IS NOT NULL", [], |row| row.get(0)).unwrap()
}

fn media(db: &Db, id: &str) -> Option<serde_json::Value> {
    db.find("moments", &rid(id)).unwrap().and_then(|row| row.get("media").cloned())
}

fn seeded() -> Db {
    let db = booted();
    seed_tree(&db);
    db
}

fn attach(db: &Db, row: &str, field: &str, bytes: &[u8]) -> crate::files::FileDescriptor {
    db.attach("moments", &rid(row), field, bytes, "a.png", "image/png").unwrap()
}

#[test]
fn small_file_round_trips_with_its_description() {
    let db = seeded();
    let descriptor = attach(&db, "m1", "media", SMALL);
    assert_eq!((descriptor.size, descriptor.name.as_deref(), descriptor.mime.as_deref()), (SMALL.len() as u64, Some("a.png"), Some("image/png")));
    assert_eq!(descriptor.sha256, Some(sha256_hex(SMALL)));
    assert_eq!(db.file("moments", &rid("m1"), "media").unwrap(), Some(descriptor.clone()));
    assert_eq!(db.read("moments", &rid("m1"), "media").unwrap(), Some(SMALL.to_vec()));
    assert_eq!(media(&db, "m1"), Some(descriptor.to_value()));
}

#[test]
fn nothing_attached_reads_as_none() {
    let db = seeded();
    assert_eq!(db.file("moments", &rid("m1"), "media").unwrap(), None);
    assert_eq!(db.read("moments", &rid("m1"), "media").unwrap(), None);
    assert_eq!(db.url("moments", &rid("m1"), "media").unwrap(), None);
}

#[test]
fn attach_needs_an_existing_row_and_a_file_field() {
    let db = seeded();
    assert!(matches!(db.attach("moments", &rid("m1"), "text", SMALL, "a", "b/c"), Err(DbError::NotFileField { .. })));
    assert!(matches!(db.attach("moments", &rid("m1"), "ghost", SMALL, "a", "b/c"), Err(DbError::NotFileField { .. })));
    assert!(matches!(db.attach("moments", &rid("nope"), "media", SMALL, "a", "b/c"), Err(DbError::NotFound { .. })));
    assert!(matches!(db.attach("ghosts", &rid("m1"), "media", SMALL, "a", "b/c"), Err(DbError::UnknownModel(_))));
    assert_eq!(count_files(&db), 0);
}

#[test]
fn large_file_lives_beside_the_database_and_not_in_it() {
    let (root, db) = on_disk();
    seed_tree(&db);
    let bytes = large();
    let descriptor = attach(&db, "m1", "media", &bytes);
    assert_eq!(db.read("moments", &rid("m1"), "media").unwrap(), Some(bytes.clone()));
    assert_eq!(bytes_in_table(&db), 0);
    let path = db.url("moments", &rid("m1"), "media").unwrap().unwrap();
    assert_eq!(path, root.path().join("42.sqlite.files").join(&descriptor.id));
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[test]
fn memory_database_keeps_large_files_in_the_table() {
    let db = seeded();
    let bytes = large();
    attach(&db, "m1", "media", &bytes);
    assert_eq!(db.read("moments", &rid("m1"), "media").unwrap(), Some(bytes));
    assert_eq!(bytes_in_table(&db), 1);
}

#[test]
fn small_file_url_is_created_on_demand_and_kept_until_detach() {
    let db = seeded();
    attach(&db, "m1", "media", SMALL);
    let path = db.url("moments", &rid("m1"), "media").unwrap().unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), SMALL);
    assert!(path.ends_with("a.png"));
    assert_eq!(db.url("moments", &rid("m1"), "media").unwrap().unwrap(), path);
    assert!(db.detach("moments", &rid("m1"), "media").unwrap());
    assert!(!path.exists());
}

#[test]
fn inv_file_02_attaching_again_replaces_the_file_and_its_leftovers() {
    let (root, db) = on_disk();
    seed_tree(&db);
    let first = attach(&db, "m1", "media", &large());
    let small = attach(&db, "m1", "media", SMALL);
    let cached = db.url("moments", &rid("m1"), "media").unwrap().unwrap();
    let second = db.attach("moments", &rid("m1"), "media", &large(), "b.png", "image/png").unwrap();
    assert!(first.id != second.id && small.id != second.id);
    assert_eq!(count_files(&db), 1);
    assert_eq!(db.file("moments", &rid("m1"), "media").unwrap().unwrap().id, second.id);
    assert_eq!(db.shared.files.directory_entries().unwrap(), vec![second.id.clone()]);
    assert!(!cached.exists());
    assert_eq!(std::fs::read_dir(root.path().join("42.sqlite.files")).unwrap().count(), 1);
}

#[test]
fn files_of_different_fields_and_rows_are_independent() {
    let db = seeded();
    attach(&db, "m1", "media", b"one");
    attach(&db, "m1", "cover", b"two");
    attach(&db, "m2", "media", b"three");
    assert_eq!(db.read("moments", &rid("m1"), "media").unwrap(), Some(b"one".to_vec()));
    assert_eq!(db.read("moments", &rid("m1"), "cover").unwrap(), Some(b"two".to_vec()));
    assert_eq!(db.read("moments", &rid("m2"), "media").unwrap(), Some(b"three".to_vec()));
}

#[test]
fn detach_clears_the_description_the_bytes_and_the_directory_file() {
    let (_root, db) = on_disk();
    seed_tree(&db);
    attach(&db, "m1", "media", &large());
    assert!(db.detach("moments", &rid("m1"), "media").unwrap());
    assert_eq!(db.file("moments", &rid("m1"), "media").unwrap(), None);
    assert_eq!(db.read("moments", &rid("m1"), "media").unwrap(), None);
    assert_eq!(media(&db, "m1"), None);
    assert_eq!(count_files(&db), 0);
    assert!(db.shared.files.directory_entries().unwrap().is_empty());
    assert!(!db.detach("moments", &rid("m1"), "media").unwrap());
}

#[test]
fn inv_file_06_destroying_a_row_hides_its_files_and_those_of_cascaded_children_but_keeps_their_bytes() {
    let (_root, db) = on_disk();
    seed_tree(&db);
    attach(&db, "m1", "media", &large());
    db.attach("vibes", &rid("v1"), "media", &large(), "v.bin", "a/b").unwrap();
    db.attach("vibes", &rid("v2"), "media", SMALL, "w.bin", "a/b").unwrap();
    attach(&db, "m2", "media", &large());
    db.destroy("moments", &rid("m1")).unwrap();
    for (model, id) in [("moments", "m1"), ("vibes", "v1"), ("vibes", "v2")] {
        assert_eq!(db.read(model, &rid(id), "media").unwrap(), None);
    }
    assert_eq!(count_files(&db), 4);
    assert_eq!(db.shared.files.directory_entries().unwrap().len(), 3);
    db.sweep_files().unwrap();
    assert_eq!(count_files(&db), 4);
}

#[test]
fn inv_file_01_failed_attach_leaves_neither_a_file_nor_a_description() {
    let (_root, db) = on_disk();
    seed_tree(&db);
    let before = attach(&db, "m1", "media", &large());
    db.execute_for_test("CREATE TRIGGER refuse BEFORE INSERT ON file BEGIN SELECT RAISE(ABORT, 'refused'); END");
    assert!(db.attach("moments", &rid("m1"), "media", &large(), "x", "y/z").is_err());
    assert!(db.attach("moments", &rid("m1"), "cover", &large(), "x", "y/z").is_err());
    assert_eq!(db.file("moments", &rid("m1"), "media").unwrap(), Some(before.clone()));
    assert_eq!(db.file("moments", &rid("m1"), "cover").unwrap(), None);
    assert_eq!(count_files(&db), 1);
    assert_eq!(db.shared.files.directory_entries().unwrap(), vec![before.id]);
}

#[test]
fn attach_notifies_observers_of_the_row_and_refreshes_the_cache() {
    let db = seeded();
    let (log, callback) = recorder();
    let _subscription = db.observe(crate::query::Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    attach(&db, "m1", "media", SMALL);
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log), vec![2, 2]);
    assert!(media(&db, "m1").is_some());
}

#[test]
fn inv_file_04_temp_row_files_follow_the_row_to_its_real_id() {
    let db = booted();
    let temp = db.create("moments", obj(json!({"text": "new"}))).unwrap();
    let descriptor = db.attach("moments", &temp.id, "media", SMALL, "a", "b/c").unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Replace, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("moments"), id: temp.id.clone(), reason: DestroyReason::Replaced { by: rid("m7") } });
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m7"), row: obj(json!({"id": "m7", "text": "new", "media": descriptor.to_value()})) });
    db.land(envelope).unwrap();
    assert_eq!(db.read("moments", &rid("m7"), "media").unwrap(), Some(SMALL.to_vec()));
    assert_eq!(db.read("moments", &temp.id, "media").unwrap(), None);
    assert_eq!(db.maintain(&[]).unwrap().temp_rows_dropped, 0);
    assert_eq!(db.read("moments", &rid("m7"), "media").unwrap(), Some(SMALL.to_vec()));
}

#[test]
fn inv_file_10_sweep_reports_directory_and_removal_failures() {
    for (suffix, listing) in [("files", true), ("cache", true), ("files", false), ("cache", false)] {
        let (root, db) = on_disk();
        let directory = root.path().join(format!("42.sqlite.{suffix}"));
        if listing {
            std::fs::write(&directory, b"not a directory").unwrap();
        } else {
            std::fs::create_dir_all(&directory).unwrap();
            if suffix == "files" {
                std::fs::create_dir(directory.join("stray")).unwrap();
            } else {
                std::fs::write(directory.join("stray"), b"not a directory").unwrap();
            }
        }
        assert!(db.sweep_files().is_err(), "{suffix} listing={listing} reported success");
    }
}

#[test]
fn maintain_removes_files_of_vanished_rows_and_unreferenced_fields() {
    let (_root, db) = on_disk();
    seed_tree(&db);
    let gone = attach(&db, "m1", "media", &large());
    let overwritten = attach(&db, "m2", "media", &large());
    let kept = attach(&db, "m2", "cover", &large());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("moments"), id: rid("m1"), reason: DestroyReason::Server });
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m2"), row: obj(json!({"id": "m2", "userId": "u2", "text": "two", "media": null, "cover": kept.to_value()})) });
    db.land(envelope).unwrap();
    assert_eq!(db.shared.files.directory_entries().unwrap().len(), 3);
    db.maintain(&[]).unwrap();
    assert_eq!(db.shared.files.directory_entries().unwrap(), vec![kept.id.clone()]);
    assert_eq!(count_files(&db), 1);
    assert!(db.read("moments", &rid("m2"), "media").unwrap().is_none());
    assert_eq!(db.read("moments", &rid("m2"), "cover").unwrap().unwrap().len(), 300 * 1024);
    assert!(!db.shared.files.directory_entries().unwrap().contains(&gone.id) && !db.shared.files.directory_entries().unwrap().contains(&overwritten.id));
}

#[test]
fn maintain_removes_stray_directory_files_partial_writes_and_stale_cache() {
    let (root, db) = on_disk();
    seed_tree(&db);
    let kept = attach(&db, "m1", "media", &large());
    let small = attach(&db, "m2", "media", SMALL);
    let live_cache = db.url("moments", &rid("m2"), "media").unwrap().unwrap();
    let directory = root.path().join("42.sqlite.files");
    std::fs::write(directory.join("stray"), b"x").unwrap();
    std::fs::write(directory.join("half.part"), b"x").unwrap();
    db.shared.files.materialize("stale", "x.bin", b"x").unwrap();
    db.sweep_files().unwrap();
    assert_eq!(db.shared.files.directory_entries().unwrap(), vec![kept.id]);
    assert_eq!(db.shared.files.cache_entries().unwrap(), vec![small.id]);
    assert!(live_cache.exists());
}

#[test]
fn logout_style_wipe_leaves_no_files_behind() {
    let (root, db) = on_disk();
    seed_tree(&db);
    attach(&db, "m1", "media", &large());
    db.shared.store.reset_incompatible().unwrap();
    assert_eq!(count_files(&db), 0);
    db.sweep_files().unwrap();
    assert!(db.shared.files.directory_entries().unwrap().is_empty());
    assert!(root.path().join("42.sqlite").exists());
}

#[test]
fn inv_file_07_the_bytes_read_back_the_same_on_both_sides_of_the_storage_threshold() {
    let limit = crate::files::TABLE_LIMIT;
    let (_root, db) = on_disk();
    seed_tree(&db);
    for size in [0, 1, limit - 1, limit, limit + 1] {
        let bytes: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
        let descriptor = attach(&db, "m1", "media", &bytes);
        assert_eq!(descriptor.size, size as u64, "size {size}");
        assert_eq!(db.read("moments", &rid("m1"), "media").unwrap(), Some(bytes.clone()), "size {size}");
        let url = db.url("moments", &rid("m1"), "media").unwrap().unwrap();
        assert_eq!(std::fs::read(url).unwrap(), bytes, "size {size}");
        assert_eq!((bytes_in_table(&db), db.shared.files.directory_entries().unwrap().len()), if size <= limit { (1, 0) } else { (0, 1) }, "size {size}");
    }
}
