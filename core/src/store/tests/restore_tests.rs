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
