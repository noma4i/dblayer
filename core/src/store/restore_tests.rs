use super::fixtures::{booted_store, exec};

#[test]
fn store_discard_restore_failed_absent() {
    let (store, _) = booted_store();
    exec(&store, "INSERT INTO restore_failed VALUES ('op', 'kept', 'constraint', '{}')");
    let before = store.restore_failed().unwrap();
    assert!(!store.discard_restore_failed("op", "missing").unwrap());
    assert_eq!(store.restore_failed().unwrap(), before);
}

#[test]
fn store_discard_restore_failed() {
    let (store, _) = booted_store();
    exec(&store, "INSERT INTO restore_failed VALUES ('op', 'first', 'constraint', '{}'), ('op', 'second', 'type', '{}')");
    assert!(store.discard_restore_failed("op", "first").unwrap());
    let remaining = store.restore_failed().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!((remaining[0].table.as_str(), remaining[0].key.as_str(), remaining[0].reason.as_str()), ("op", "second", "type"));
    assert!(!store.discard_restore_failed("op", "first").unwrap());
}
