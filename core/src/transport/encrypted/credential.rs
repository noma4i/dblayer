//! The secret of the session: read from the `CredentialStore` on every use, minted once, erased by one path that waits for a load in progress.

use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};

use super::exchange::digest_of;
use super::jwe::random_token;
use super::state;
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

    /// The stored secret, or a new one when the store has none. A secret that does not match the stored session state is lost state.
    pub(crate) fn read(&self, store: &Store) -> Result<String, TransportError> {
        let _loading = self.hold()?;
        crate::lock::assert_no_library_lock("CredentialSlot::get");
        match crate::blocking::blocking_unit(|| self.slot.get())? {
            Some(stored) => verified(store, stored),
            None => {
                let credential = random_token();
                crate::lock::assert_no_library_lock("CredentialSlot::set");
                crate::blocking::blocking_unit(|| self.slot.set(&credential))?;
                store.write_store(&crate::lifecycle::authority::Authority::Owner, |inner| Ok(state::start(&inner.connection, &digest_of(&credential))?)).map_err(|error| TransportError::Network(format!("session storage: {error}")))?;
                Ok(credential)
            }
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
        _ => TransportError::Network(format!("session storage: {error}")),
    }
}

pub(crate) fn stored_session(store: &Store) -> Result<Option<state::Session>, TransportError> { store.read_locked(state::session).map_err(storage) }

pub(crate) fn stored_pending(store: &Store) -> Result<Option<state::Pending>, TransportError> { store.read_locked(state::pending).map_err(storage) }

pub(crate) fn stored_answers(store: &Store) -> Result<std::collections::BTreeMap<u64, state::StoredAnswer>, TransportError> { store.read_locked(state::answers).map_err(storage) }

/// A stored request could not be carried over a format change: Session stays blocked until `session_reset`.
pub(crate) fn dropped_pending(store: &Store) -> Result<bool, TransportError> {
    store.read_locked(|connection| connection.query_row("SELECT EXISTS (SELECT 1 FROM restore_failed WHERE source_table = 'encrypted_pending')", [], |row| row.get(0))).map_err(storage)
}

/// The state of the session at open: a session end that did not finish clears the tables and the secret.
pub(crate) fn finish_session_end(store: &Store, secret: &Secret) -> Result<(), TransportError> {
    if store.take_session_end_replay() {
        store.write_store(&crate::lifecycle::authority::Authority::Owner, |inner| Ok(state::clear(&inner.connection)?)).map_err(|error| TransportError::Network(format!("session storage: {error}")))?;
        secret.slot.clear()?;
    }
    Ok(())
}
