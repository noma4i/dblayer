use crate::store::Store;

/// A test database with a plain transport boots with a stored session, as a database that restored its sign-in: the windows live in an epoch with an identity.
pub(super) fn signed_session(db: &Store) {
    let inner = db.lock();
    if crate::store::session_state::session(&inner.connection).unwrap().is_some() { return; }
    crate::store::session_state::start(&inner.connection, "00000000000000010000000000000000000000000000000000000000000000ff").unwrap();
    crate::store::session_state::commit(&inner.connection, 1).unwrap();
}
