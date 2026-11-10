use super::fixtures::{booted_store, exec};
use crate::owners::store::StoreOp;

fn discard(table: &str, key: &str) -> StoreOp {
    StoreOp::DiscardRestoreFailed { table: table.into(), key: key.into() }
}

#[test]
fn store_discard_restore_failed_absent() {
    let (store, _) = booted_store();
    exec(&store, "INSERT INTO restore_failed VALUES ('op', 'kept', 'constraint', '{}')");
    let before = store.restore_failed().unwrap();
    store.write_queue_ops(&[discard("op", "missing")]).unwrap();
    assert_eq!(store.restore_failed().unwrap(), before, "a batch for an absent element changes no row");
}

#[test]
fn store_discard_restore_failed() {
    let (store, _) = booted_store();
    exec(&store, "INSERT INTO restore_failed VALUES ('op', 'first', 'constraint', '{}'), ('op', 'second', 'type', '{}')");
    let report = store.open_report().clone();
    store.write_queue_ops(&[discard("op", "first")]).unwrap();
    let remaining = store.restore_failed().unwrap();
    assert_eq!(remaining.iter().map(|row| (row.table.as_str(), row.key.as_str(), row.reason.as_str())).collect::<Vec<_>>(), vec![("op", "second", "type")]);
    assert_eq!(store.open_report(), &report, "the open report does not change after the commit");
}

/// lifecycle.md:519 (D-280): the held abort pair of a multipart debt survives a restart with its Orphan owner.
#[test]
fn store_pending_orphan_owner_round_trip() {
    let db = crate::store::Store::open_models(None, Vec::new(), None).unwrap();
    let pending = crate::store::session_state::Pending { credential_digest: "d".into(), request_id: "r".into(), response_key: "k".into(), sequence: 7, path: "/p".into(), body: "{}".into(), document: None, owner: Some(crate::transport::RequestOwner::Orphan { upload_id: "u1".into() }), purpose: crate::transport::NetworkPurpose::Transfer, req_id: 11 };
    let inner = db.lock();
    crate::store::session_state::start(&inner.connection, "d").unwrap();
    crate::store::session_state::store_pending(&inner.connection, &pending).unwrap();
    assert_eq!(crate::store::session_state::pending(&inner.connection).unwrap(), Some(pending), "the Orphan owner is read back as stored");
}

/// store.md store_read_intents_held_orphan (D-280): the restore snapshot names the upload of the abort pair Session holds; without an Orphan pending it names none.
#[test]
fn store_read_intents_held_orphan() {
    let db = crate::store::Store::open_models(None, Vec::new(), None).unwrap();
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
        author: crate::owners::contract::BatchAuthor::Transfer, id, stamp: crate::runtime::EpochStamp::Control, basis: std::collections::BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops,
    };
    {
        let store = crate::store::Store::open(&path).unwrap();
        store.boot(&[super::fixtures::messages_spec(), super::fixtures::items_spec()], None).unwrap();
        store.mark_ready();
        let first = batch(1, vec![StoreOp::UploadOrphan { upload_id: "u1".into(), key: "Moment:r1:upload".into() }]);
        store.write_batch(&first, store.files(), store.pointer_guards(), None).unwrap();
    }
    let reopened = crate::store::Store::open(&path).unwrap();
    reopened.boot(&[super::fixtures::messages_spec(), super::fixtures::items_spec()], None).unwrap();
    reopened.mark_ready();
    assert_eq!(reopened.read_restore().unwrap().orphans, vec![("u1".to_string(), "Moment:r1:upload".to_string())], "the debt of the abort survives the restart with its key");

    let removal = batch(2, vec![StoreOp::RemoveOrphan { upload_id: "u1".into() }]);
    reopened.write_batch(&removal, reopened.files(), reopened.pointer_guards(), None).unwrap();
    assert!(reopened.read_restore().unwrap().orphans.is_empty(), "RemoveOrphan of the same stamp clears the debt");
}
