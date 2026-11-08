//! The secret of the session: read from the `CredentialStore` on every use, erased by one path that waits for a load in progress.

use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};

use super::exchange::digest_of;
use crate::store::session_state;
use super::CredentialSlot;
use crate::store::Store;
use crate::transport::{SessionCode, TransportError};

#[derive(Default)]
struct Loads {
    loading: Option<ThreadId>,
}

#[derive(Default)]
struct Cell {
    state: Mutex<Loads>,
    settled: Condvar,
}

struct Loading<'a>(&'a Cell);

impl Drop for Loading<'_> {
    fn drop(&mut self) {
        crate::lock::lock(&self.0.state).loading = None;
        self.0.settled.notify_all();
    }
}

pub(crate) struct Secret {
    slot: Arc<dyn CredentialSlot>,
    cell: Cell,
}

impl Secret {
    pub(crate) fn new(slot: Arc<dyn CredentialSlot>) -> Self { Self { slot, cell: Cell::default() } }

    pub(crate) fn slot(&self) -> Arc<dyn CredentialSlot> { self.slot.clone() }

    fn hold(&self) -> Result<Loading<'_>, TransportError> {
        let mut state = crate::lock::lock(&self.cell.state);
        loop {
            match state.loading {
                None => {
                    state.loading = Some(thread::current().id());
                    return Ok(Loading(&self.cell));
                }
                Some(owner) if owner == thread::current().id() => return Err(TransportError::Network("the credential callback re-entered the credential load".into())),
                Some(_) => state = crate::lock::wait(&self.cell.settled, state),
            }
        }
    }

    /// Erases the secret in the `CredentialStore`: the one path, held against a load that mints.
    pub(crate) fn erase(&self) -> Result<(), TransportError> {
        let _loading = self.hold()?;
        crate::lock::assert_no_library_lock("CredentialSlot::clear");
        crate::blocking::blocking_unit(|| self.slot.clear())
    }

    /// The stored secret. The owner Session mints it: a store without one is lost state of a session, and no session has no credential.
    pub(crate) fn read(&self, store: &Store) -> Result<String, TransportError> {
        let _loading = self.hold()?;
        crate::lock::assert_no_library_lock("CredentialSlot::get");
        match crate::blocking::blocking_unit(|| self.slot.get())? {
            Some(stored) => verified(store, stored),
            None => Err(match stored_session(store)? {
                Some(_) => TransportError::Session { code: SessionCode::StateLost, detail: "the stored session has no secret".into() },
                None => TransportError::Session { code: SessionCode::NoCredential, detail: "the call needs a session and the database has none".into() },
            }),
        }
    }
}

fn verified(store: &Store, credential: String) -> Result<String, TransportError> {
    match stored_session(store)? {
        Some(session) if session.credential_digest == digest_of(&credential) => Ok(credential),
        _ => Err(TransportError::Session { code: SessionCode::StateLost, detail: "lost session sequence state for the stored credential".into() }),
    }
}

fn storage(error: rusqlite::Error) -> TransportError {
    match error {
        rusqlite::Error::FromSqlConversionFailure(..) => TransportError::Decode(format!("session storage: {error}")),
        _ => TransportError::Storage(format!("session storage: {error}")),
    }
}

pub(crate) fn stored_session(store: &Store) -> Result<Option<session_state::Session>, TransportError> { store.read_locked(session_state::session).map_err(storage) }

pub(crate) fn stored_pending(store: &Store) -> Result<Option<session_state::Pending>, TransportError> { store.read_locked(session_state::pending).map_err(storage) }

pub(crate) fn stored_answers(store: &Store) -> Result<std::collections::BTreeMap<u64, session_state::StoredAnswer>, TransportError> { store.read_locked(session_state::answers).map_err(storage) }

/// A stored request could not be carried over a format change: Session stays blocked until `session_reset`.
pub(crate) fn dropped_pending(store: &Store) -> Result<bool, TransportError> {
    store.read_locked(session_state::dropped_pending).map_err(storage)
}

/// The state of the session at open: a session end that did not finish clears the tables and the secret.
pub(crate) fn finish_session_end(store: &Store, secret: &Secret) -> Result<(), TransportError> {
    if store.take_session_end_replay() {
        store.write(|inner| Ok(session_state::clear(&inner.connection)?)).map_err(|error| TransportError::Storage(format!("session storage: {error}")))?;
        secret.slot.clear()?;
    }
    Ok(())
}
