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
