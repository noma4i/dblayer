use super::fixtures::*;
use super::*;
use crate::commit::{BatchPlan, DestroyReason, EntityOp, FileOp};
use crate::files::{find, FileDescriptor, FileRecord, Location};

fn file(id: &str, row: &str, field: &str, data: &[u8]) -> FileRecord {
    FileRecord {
        id: id.into(),
        model: "messages".into(),
        row_id: row.into(),
        field: field.into(),
        descriptor: FileDescriptor { id: id.into(), size: data.len() as u64, name: Some("a".into()), mime: Some("b/c".into()), sha256: Some("h".into()) },
        location: Location::Table,
        data: data.to_vec(),
        created_at: 1,
    }
}

fn stored(store: &Store, row: &str, field: &str) -> Option<FileRecord> {
    find(&store.lock().connection, "messages", row, field).unwrap()
}

fn with_files(store: &Store, ops: Vec<EntityOp>, files: Vec<FileOp>) -> BatchPlan {
    let mut envelope = envelope(store, ops, vec![]);
    envelope.file_ops = files;
    envelope
}

#[test]
fn put_commits_with_the_row_in_one_transaction() {
    let (store, _) = booted_store();
    let result = store.commit(with_files(&store, vec![upsert("messages", message("m1", "c1"))], vec![FileOp::Put(file("f1", "m1", "media", b"abc"))])).unwrap();
    assert_eq!(result.revision, Some(1));
    assert_eq!(stored(&store, "m1", "media").unwrap().data, b"abc");
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 1);
}

#[test]
fn file_only_envelope_is_committed() {
    let (store, _) = booted_store();
    let result = store.commit(with_files(&store, vec![], vec![FileOp::Put(file("f1", "m1", "media", b"abc"))])).unwrap();
    assert!(!result.is_noop());
    assert!(stored(&store, "m1", "media").is_some());
}

#[test]
fn failed_file_write_rolls_the_row_back() {
    let (store, _) = booted_store();
    exec(&store, "CREATE TRIGGER refuse BEFORE INSERT ON file BEGIN SELECT RAISE(ABORT, 'refused'); END");
    let outcome = store.commit(with_files(&store, vec![upsert("messages", message("m1", "c1"))], vec![FileOp::Put(file("f1", "m1", "media", b"abc"))]));
    assert!(outcome.is_err());
    assert_eq!(count(&store, "SELECT COUNT(*) FROM row_messages"), 0);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM file"), 0);
}

#[test]
fn put_replaces_the_file_of_the_pair_and_remove_deletes_it() {
    let (store, _) = booted_store();
    store.commit(with_files(&store, vec![], vec![FileOp::Put(file("f1", "m1", "media", b"one"))])).unwrap();
    store.commit(with_files(&store, vec![], vec![FileOp::Put(file("f2", "m1", "media", b"two"))])).unwrap();
    assert_eq!(stored(&store, "m1", "media").unwrap().id, "f2");
    let remove = FileOp::Remove { model: model("messages"), row_id: id("m1"), field: "media".into() };
    store.commit(with_files(&store, vec![], vec![remove])).unwrap();
    assert!(stored(&store, "m1", "media").is_none());
}

#[test]
fn destroying_a_row_deletes_its_files_unless_a_local_destroy_holds_them() {
    let (store, _) = booted_store();
    for (index, reason) in [DestroyReason::Server, DestroyReason::Expired].into_iter().enumerate() {
        let row = format!("m{index}");
        store.commit(with_files(&store, vec![upsert("messages", message(&row, "c1"))], vec![FileOp::Put(file(&format!("f{index}"), &row, "media", b"x"))])).unwrap();
        store.commit(envelope(&store, vec![destroy("messages", &row, reason)], vec![])).unwrap();
        assert!(stored(&store, &row, "media").is_none());
    }
    store.commit(with_files(&store, vec![upsert("messages", message("m9", "c1"))], vec![FileOp::Put(file("f9", "m9", "media", b"x"))])).unwrap();
    store.commit(envelope(&store, vec![destroy("messages", "m9", DestroyReason::Local)], vec![])).unwrap();
    assert_eq!(stored(&store, "m9", "media").unwrap().id, "f9");
}

#[test]
fn replacing_a_row_hands_its_files_to_the_new_id() {
    let (store, _) = booted_store();
    store.commit(with_files(&store, vec![upsert("messages", message("temp-1", "c1"))], vec![FileOp::Put(file("f1", "temp-1", "media", b"x"))])).unwrap();
    let ops = vec![destroy("messages", "temp-1", DestroyReason::Replaced { by: id("m7") }), upsert("messages", message("m7", "c1"))];
    store.commit(envelope(&store, ops, vec![])).unwrap();
    assert_eq!(stored(&store, "m7", "media").unwrap().id, "f1");
    assert!(stored(&store, "temp-1", "media").is_none());
}

#[test]
fn resets_clear_the_file_table() {
    let (store, _) = booted_store();
    store.commit(with_files(&store, vec![], vec![FileOp::Put(file("f1", "m1", "media", b"x"))])).unwrap();
    store.erase_models(store.generation().next(), &["messages"].map(String::from)).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM file"), 0);
    store.commit(with_files(&store, vec![], vec![FileOp::Put(file("f2", "m1", "media", b"x"))])).unwrap();
    store.erase_account(store.generation().next(), false).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM file"), 0);
}
