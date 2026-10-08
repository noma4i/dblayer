use super::support::*;

#[test]
fn drop_scope_on_empty_scope_returns_zero() {
    let (store, _) = booted_store();
    assert_eq!(store.drop_scope("items", scope("items", "list").key.as_str()).unwrap(), 0);
}

#[test]
fn drop_scope_removes_all_members_and_keeps_other_scopes() {
    let (store, _) = booted_store();
    let first = scope("items", "a");
    let second = scope("items", "b");
    store
        .commit(envelope(&store, vec![], vec![append(&first, "i1", "a"), append(&first, "i2", "b"), append(&second, "i3", "a")]))
        .unwrap();
    assert_eq!(store.drop_scope("items", first.key.as_str()).unwrap(), 2);
    assert!(store.scope_members("items", first.key.as_str()).unwrap().is_empty());
    assert_eq!(store.scope_members("items", second.key.as_str()).unwrap().len(), 1);
}

#[test]
fn drop_row_memberships_removes_row_from_all_model_scopes() {
    let (store, _) = booted_store();
    let first = scope("messages", "t1");
    let second = scope("messages", "t2");
    store.commit(envelope(&store, vec![], vec![append(&first, "m1", "a"), append(&second, "m1", "a")])).unwrap();
    assert_eq!(store.drop_row_memberships("messages", "m1").unwrap(), 2);
    assert!(store.scope_members("messages", first.key.as_str()).unwrap().is_empty());
    assert!(store.scope_members("messages", second.key.as_str()).unwrap().is_empty());
}

#[test]
fn same_row_id_in_other_model_is_not_a_member() {
    let (store, _) = booted_store();
    store.commit(envelope(&store, vec![], vec![append(&scope("messages", "t"), "1", "a"), append(&scope("items", "t"), "1", "a")])).unwrap();
    store.drop_row_memberships("messages", "1").unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM scope_member WHERE model = 'items'"), 1);
}
