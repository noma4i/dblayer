use super::fixtures::{booted_store, exec, failed_restore};
use crate::owners::store::StoreOp;

thread_local! { static ORIGIN_QUERIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

fn count_origin_query(event: rusqlite::trace::TraceEvent<'_>) {
    if let rusqlite::trace::TraceEvent::Stmt(_, sql) = event {
        if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") { ORIGIN_QUERIES.with(|count| count.set(count.get() + 1)); }
    }
}

fn origin_work(size: usize) -> usize {
    let (store, _) = booted_store();
    let connection = store.lock();
    for index in 1..=size {
        connection.connection.execute("INSERT INTO op (seq, action_key, status, created_at, since, origin_kind, transfer_id) VALUES (?1, 'messages:upload', 'pending', 0, 0, 'transfer', ?1)", [index as i64]).unwrap();
        connection.connection.execute("INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts, seq, transfer_id, cycle_id) VALUES ('messages', ?1, 'upload', 'queued', 'prepare', 0, 1, 0, ?2, ?2, 1)", rusqlite::params![format!("r{index}"), index as i64]).unwrap();
    }
    ORIGIN_QUERIES.with(|count| count.set(0));
    connection.connection.trace_v2(rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT, Some(count_origin_query));
    let count = super::restore_read::restored_origin_count(&connection.connection).unwrap();
    connection.connection.trace_v2(rusqlite::trace::TraceEventCodes::empty(), None);
    assert_eq!(count, size);
    ORIGIN_QUERIES.with(|queries| queries.get())
}

#[test]
fn store_restore_origins_work_linear() {
    let small = origin_work(2);
    let large = origin_work(40);
    assert_eq!(small, large, "SQL query count must not grow with restored transfers");
}

fn discard(table: &str, key: &str) -> StoreOp {
    StoreOp::DiscardRestoreFailed { table: table.into(), key: key.into() }
}

#[test]
fn store_discard_restore_failed_absent() {
    let (store, _) = booted_store();
    exec(&store, "INSERT INTO restore_failed VALUES ('op', 'kept', 'constraint', '{}')");
    let before = failed_restore(&store);
    store.write_queue_ops(&[discard("op", "missing")]).unwrap();
    assert_eq!(failed_restore(&store), before, "a batch for an absent element changes no row");
}

#[test]
fn store_discard_restore_failed() {
    let (store, _) = booted_store();
    exec(&store, "INSERT INTO restore_failed VALUES ('op', 'first', 'constraint', '{}'), ('op', 'second', 'type', '{}')");
    let report = store.open_report().clone();
    store.write_queue_ops(&[discard("op", "first")]).unwrap();
    let remaining = failed_restore(&store);
    assert_eq!(remaining.iter().map(|row| (row.table.as_str(), row.key.as_str(), row.reason.as_str())).collect::<Vec<_>>(), vec![("op", "second", "type")]);
    assert_eq!(store.open_report(), &report, "the open report does not change after the commit");
}

/// lifecycle.md:519 (D-280): the held abort pair of a multipart debt survives a restart with its Orphan owner.
#[test]
fn store_pending_orphan_owner_round_trip() {
    let db = crate::store::Store::open_models(None, Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    let pending = crate::store::session_state::Pending { credential_digest: "d".into(), request_id: "r".into(), response_key: "k".into(), sequence: 7, path: "/p".into(), body: "{}".into(), document: None, owner: Some(crate::transport::RequestOwner::Orphan { upload_id: "u1".into() }), purpose: crate::transport::NetworkPurpose::Transfer, req_id: 11 };
    let inner = db.lock();
    crate::store::session_state::start(&inner.connection, "d").unwrap();
    crate::store::session_state::store_pending(&inner.connection, &pending).unwrap();
    assert_eq!(crate::store::session_state::pending(&inner.connection).unwrap(), Some(pending), "the Orphan owner is read back as stored");
}

/// store.md store_read_intents_held_orphan (D-280): the restore snapshot names the upload of the abort pair Session holds; without an Orphan pending it names none.
#[test]
fn store_read_intents_held_orphan() {
    let db = crate::store::Store::open_models(None, Vec::new(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    assert_eq!(db.read_restore().unwrap().held_orphan, None, "no pending, no held pair");
    let pending = crate::store::session_state::Pending { credential_digest: "d".into(), request_id: "r".into(), response_key: "k".into(), sequence: 7, path: "/p".into(), body: "{}".into(), document: None, owner: Some(crate::transport::RequestOwner::Orphan { upload_id: "u9".into() }), purpose: crate::transport::NetworkPurpose::Transfer, req_id: 11 };
    {
        let inner = db.lock();
        crate::store::session_state::start(&inner.connection, "d").unwrap();
        crate::store::session_state::store_pending(&inner.connection, &pending).unwrap();
    }
    assert_eq!(db.read_restore().unwrap().held_orphan, Some("u9".into()), "the same snapshot names the held pair");
}

/// store_commit_control_reply_and_restart (D-277, store.md): the Control batch written before a restart is read by the restore with the same upload_id and key, in the next epoch.
#[test]
fn store_commit_control_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("control.sqlite");
    let batch = |id: u64, ops: Vec<crate::owners::store::StoreOp>| crate::owners::store::StoreBatch {
        author: crate::owners::contract::BatchAuthor::Transfer, id, stamp: crate::runtime::EpochStamp::Control, rows: Vec::new(), intents: 0, credential: None, ops,
    };
    {
        let store = crate::store::Store::open(&path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
        store.boot(&[super::fixtures::messages_spec(), super::fixtures::items_spec()]).unwrap();
        store.mark_ready();
        let first = batch(1, vec![StoreOp::UploadOrphan { upload_id: "u1".into(), key: "Moment:r1:upload".into() }]);
        store.write_batch(&first, store.files(), None).unwrap();
    }
    let reopened = crate::store::Store::open(&path, std::sync::Arc::new(crate::runtime::SystemClock)).unwrap();
    reopened.boot(&[super::fixtures::messages_spec(), super::fixtures::items_spec()]).unwrap();
    reopened.mark_ready();
    assert_eq!(reopened.read_restore().unwrap().orphans, vec![("u1".to_string(), "Moment:r1:upload".to_string())], "the debt of the abort survives the restart with its key");

    let removal = batch(2, vec![StoreOp::RemoveOrphan { upload_id: "u1".into() }]);
    reopened.write_batch(&removal, reopened.files(), None).unwrap();
    assert!(reopened.read_restore().unwrap().orphans.is_empty(), "RemoveOrphan of the same stamp clears the debt");
}

fn put_upload(store: &crate::store::Store, row_id: &str, seq: i64, batch: Option<i64>, fields: Option<&str>) {
    store.lock().connection.execute(
        "INSERT INTO sync_upload (model, row_id, action, status, stage, sent, total, attempts, seq, transfer_id, cycle_id, batch, fields, completed, created_at, inline, size, sha256, mutation_path, mutation_body, on_failure) VALUES ('messages', ?1, 'upload', 'queued', 'prepare', 0, 1, 0, ?2, ?2, 1, ?3, ?4, 0, 0, 0, 1, 'x', 'p', '{}', 'keep')",
        rusqlite::params![row_id, seq, batch, fields],
    ).unwrap();
}

fn restored_failure(store: &crate::store::Store, row_id: &str) -> Option<String> {
    let restore = store.read_restore().unwrap();
    restore.transfers.unwrap().into_iter().find(|row| row.key == format!("messages:{row_id}:upload")).unwrap().restore_failed
}

/// D-461: a stored upload whose `fields` are not JSON restores as `RestoreFailed`, the other upload restores as it was.
#[test]
fn store_restore_upload_fields_not_json_fails_the_element() {
    let (store, _) = booted_store();
    put_upload(&store, "bad", 1, None, Some("not json"));
    put_upload(&store, "good", 2, None, Some("{\"a\":1}"));
    assert!(restored_failure(&store, "bad").is_some(), "the upload with unreadable fields restored as it was written");
    assert_eq!(restored_failure(&store, "good"), None);
}

/// D-461: a group whose stored rule does not parse fails its members, the members of a group with a readable rule restore as they were.
#[test]
fn store_restore_group_rule_not_json_fails_its_members() {
    let (store, _) = booted_store();
    let rule = r#"{"minimum":2,"maximum":10,"items_key":"items","renumber":null,"mutation":{"model":"messages","row_id":"r","action":"upload","keep":true,"path":"p","body":"{}"}}"#;
    store.lock().connection.execute("INSERT INTO sync_upload_batch (batch_id, op_id, joint) VALUES (1, NULL, 'not json'), (2, NULL, ?1)", [rule]).unwrap();
    put_upload(&store, "bad", 1, Some(1), Some("{}"));
    put_upload(&store, "good", 2, Some(2), Some("{}"));
    assert!(restored_failure(&store, "bad").is_some(), "the member of a group with an unreadable rule restored as it was written");
    assert_eq!(restored_failure(&store, "good"), None);
}
