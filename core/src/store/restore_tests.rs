use super::fixtures::{booted_store, exec};
use super::Store;

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

fn pending_call(request_id: u64, owner: crate::transport::RequestOwner) -> crate::owners::session::PendingCall {
    crate::owners::session::PendingCall { request_id, sequence: 4, class: crate::owners::session::OperationClass::Authenticated, operation: String::new(), path: "/graphql".into(), body: "{}".into(), credential: None, purpose: 0, author: None, owner: Some(owner), epoch: 1, reply: None }
}

fn pending_row(store: &Store) -> crate::transport::encrypted::state::Pending {
    let inner = store.lock();
    crate::transport::encrypted::state::pending(&inner.connection).unwrap().expect("a pending row")
}

#[test]
fn store_release_pending_owner() {
    use crate::owners::store::StoreOp;
    use crate::transport::RequestOwner;
    let (store, _) = booted_store();
    let owner = RequestOwner::Operation { seq: 9 };
    let put = StoreOp::Pending { pending: pending_call(41, owner.clone()), credential_digest: "digest".into(), wire_id: "wire".into(), response_key: "key".into() };
    store.write_queue_ops(&[put]).unwrap();
    store.write_queue_ops(&[StoreOp::ReleasePendingOwner { request_id: 99 }]).unwrap();
    assert_eq!(pending_row(&store).owner, Some(owner), "another request id changes nothing");
    store.write_queue_ops(&[StoreOp::ReleasePendingOwner { request_id: 41 }]).unwrap();
    let released = pending_row(&store);
    assert_eq!((released.owner, released.req_id, released.sequence, released.request_id.as_str(), released.path.as_str()), (Some(RequestOwner::Detached), 41, 4, "wire", "/graphql"), "only the owner changes");
}
