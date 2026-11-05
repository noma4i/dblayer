//! Direction A install (`design.md` F1-F3): the owners Network, Lifecycle, Store and Calls on one `Runtime`, each with the executor of its effects.
//! An executor does IO and returns the result as an input, it takes no transition. `Session` joins when its table gets `Mutate` and `GenerateCredential`.

mod runners;
mod session_runner;
#[cfg(test)]
mod life_tests;

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use super::calls::{CallAnswer, CallError, Calls, CallsInput, CallsState};
use super::contract::{BatchAuthor, BatchId, Committed, CredentialCommitted, Published, Rejected, RequestId};
use super::session::{AuthorId, AuthorOutcome, SessionAnswer, SessionCalls, SessionInput, SessionState};
use super::lifecycle::{Lifecycle, LifecycleError, LifecycleInput, LifecycleState, Peer};
use super::network::{Network, NetworkInput, NetworkState};
use super::store::{BootFound, StoreInput, StoreReply, StoreState, StoreWriter};
use crate::db::Db;
use crate::runtime::{Address, Epoch, EpochStamp, Handle, Outcomes, Runtime, RuntimeBuilder, Waiter};

/// The result of a platform call, the payload of `Outcomes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifeReply {
    Life(Result<(), LifecycleError>),
    Online { closed: bool },
    Call(CallAnswer),
    Session(SessionAnswer),
    Closed,
}

/// The document and the answer root of a declared operation.
pub type OperationLookup = Arc<dyn Fn(&str) -> Option<(String, String)> + Send + Sync>;

pub struct Ports {
    pub db: Db,
    pub transport: Option<Arc<dyn crate::transport::Transport>>,
    pub executor: tokio::runtime::Handle,
    pub operations: OperationLookup,
    pub credentials: Option<Arc<dyn crate::transport::CredentialSlot>>,
    /// The encrypted client: Session installs only with it and `credentials`. It does the encoding and the HTTP exchange, Session decides.
    pub session: Option<Arc<crate::transport::EncryptedClient>>,
}

#[derive(Clone, Copy)]
pub(crate) struct Addresses {
    pub lifecycle: Address<Lifecycle>,
    pub store: Address<StoreWriter>,
    pub network: Address<Network>,
    pub calls: Address<Calls>,
    pub session: Option<Address<SessionCalls>>,
}

#[derive(Default)]
struct PublishedCell {
    value: Mutex<Option<Published>>,
    changed: Condvar,
}

/// What every runner shares: the address book, the registry of waiters and the published value.
pub(crate) struct Wiring {
    pub handle: Handle,
    pub addresses: OnceLock<Addresses>,
    pub outcomes: Outcomes<LifeReply>,
    published: PublishedCell,
    ending: AtomicBool,
    store_closed: (Mutex<bool>, Condvar),
    faults: Mutex<Vec<String>>,
    open_failed: Mutex<Option<String>>,
    /// Where the answer to a `Mutate` goes, by author: `Transport{req_id, Response/Error}`.
    mutate_sinks: Mutex<HashMap<AuthorId, Box<dyn Fn(RequestId, AuthorOutcome) + Send + Sync>>>,
}

impl Wiring {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            addresses: OnceLock::new(),
            outcomes: Outcomes::new(),
            published: PublishedCell::default(),
            ending: AtomicBool::new(false),
            store_closed: (Mutex::new(false), Condvar::new()),
            faults: Mutex::new(Vec::new()),
            open_failed: Mutex::new(None),
            mutate_sinks: Mutex::new(HashMap::new()),
        }
    }

    /// `None` only before `install` finished: no effect runs by then.
    pub fn addresses(&self) -> Option<Addresses> { self.addresses.get().copied() }

    pub fn publish(&self, published: Published) {
        *self.published.value.lock().unwrap_or_else(PoisonError::into_inner) = Some(published);
        self.published.changed.notify_all();
    }

    pub fn set_ending(&self, ending: bool) { self.ending.store(ending, Ordering::SeqCst); }

    pub fn fault(&self, what: String) { self.faults.lock().unwrap_or_else(PoisonError::into_inner).push(what); }

    pub fn open_failed(&self, error: String) {
        *self.open_failed.lock().unwrap_or_else(PoisonError::into_inner) = Some(error);
        self.published.changed.notify_all();
    }

    pub fn store_closed(&self) {
        *self.store_closed.0.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.store_closed.1.notify_all();
    }

    pub fn to_lifecycle(&self, input: LifecycleInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.lifecycle, EpochStamp::Control, input); }
    }

    pub fn to_store(&self, input: StoreInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.store, EpochStamp::Control, input); }
    }

    pub fn to_calls(&self, input: CallsInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.calls, EpochStamp::Control, input); }
    }

    pub fn to_session(&self, input: SessionInput) {
        if let Some(Addresses { session: Some(session), .. }) = self.addresses() { self.handle.post(session, EpochStamp::Control, input); }
    }

    /// The answer of Store to the batch `(author, batch)`.
    pub fn route_store_reply(&self, author: BatchAuthor, batch: BatchId, reply: StoreReply) {
        if author == BatchAuthor::Session {
            self.to_session(match reply {
                StoreReply::Committed { revision } => SessionInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), models: Vec::new() }),
                StoreReply::Rejected(reason) => SessionInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        self.fault(format!("no owner installed for the batches of {author:?}"));
    }

    pub fn on_mutate_answer(&self, author: AuthorId, sink: impl Fn(RequestId, AuthorOutcome) + Send + Sync + 'static) {
        self.mutate_sinks.lock().unwrap_or_else(PoisonError::into_inner).insert(author, Box::new(sink));
    }

    /// `Transport{req_id, Response/Error{Permanent, X}}` to the author of a `Mutate`.
    pub fn answer_author(&self, author: AuthorId, req_id: RequestId, outcome: AuthorOutcome) {
        match self.mutate_sinks.lock().unwrap_or_else(PoisonError::into_inner).get(&author) {
            Some(sink) => sink(req_id, outcome),
            None => self.fault(format!("no author {author} for the answer to request {req_id}")),
        }
    }

    pub fn to_network(&self, input: NetworkInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.network, EpochStamp::Control, input); }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LifeError {
    Storage(String),
    Timeout,
}

pub struct Life {
    wiring: Arc<Wiring>,
    _runtime: Runtime,
}

/// Peers of `all owners` without an installed owner answer `Drained` at `Closing`.
fn absent(session: bool) -> Vec<Peer> {
    let mut peers: Vec<Peer> = super::read::ABSENT.into_iter().chain(super::write::ABSENT).collect();
    if !session {
        peers.push(Peer::Session);
    }
    peers
}

impl Life {
    /// Builds the owners, boots the Store from the booted `Db` and returns after the first `Publish`.
    pub fn start(ports: Ports, wait: Duration) -> Result<Self, LifeError> {
        let found = boot_found(&ports.db)?;
        let session = ports.session.clone().zip(ports.credentials.clone());
        let mut builder = RuntimeBuilder::new();
        let wiring = Arc::new(Wiring::new(builder.handle()));
        let lifecycle = builder.add::<Lifecycle>("lifecycle", LifecycleState::new(), runners::LifecycleRunner { wiring: wiring.clone(), absent: absent(session.is_some()), held_published: None, held_replies: Vec::new() });
        let store = builder.add::<StoreWriter>("store", StoreState::new(), runners::StoreRunner { wiring: wiring.clone(), db: ports.db.clone(), credentials: ports.credentials.clone() });
        let network = builder.add::<Network>("network", NetworkState::new(), runners::NetworkRunner { wiring: wiring.clone(), db: ports.db.clone(), held_replies: Vec::new() });
        let calls = builder.add::<Calls>("calls", CallsState::new(0, false, &[]), runners::CallsRunner::new(wiring.clone(), &ports));
        let session_address = session.as_ref().map(|(client, credentials)| {
            let open = session_runner::SessionRunner::stored(client);
            let state = SessionState::with_operations(client.session_operations(), true);
            let address = builder.add::<SessionCalls>("session", state, session_runner::SessionRunner::new(wiring.clone(), client.clone(), credentials.clone(), ports.executor.clone()));
            (address, open)
        });
        let _ = wiring.addresses.set(Addresses { lifecycle, store, network, calls, session: session_address.as_ref().map(|(address, _)| *address) });
        let runtime = builder.start().map_err(|error| LifeError::Storage(error.to_string()))?;
        if let Some((_, open)) = session_address {
            let (credential, pending) = open.map_err(|error| LifeError::Storage(error.to_string()))?;
            wiring.to_session(SessionInput::Open { credential, pending });
        }
        wiring.to_store(StoreInput::Boot(found));
        let started = std::time::Instant::now();
        let mut guard = wiring.published.value.lock().unwrap_or_else(PoisonError::into_inner);
        while guard.is_none() {
            if let Some(error) = wiring.open_failed.lock().unwrap_or_else(PoisonError::into_inner).clone() {
                return Err(LifeError::Storage(error));
            }
            let left = wait.checked_sub(started.elapsed()).ok_or(LifeError::Timeout)?;
            guard = crate::blocking::blocking_unit(|| wiring.published.changed.wait_timeout(guard, left).unwrap_or_else(PoisonError::into_inner).0);
        }
        drop(guard);
        Ok(Self { wiring, _runtime: runtime })
    }

    pub fn published(&self) -> Published {
        self.wiring.published.value.lock().unwrap_or_else(PoisonError::into_inner).clone().unwrap_or(Published { epoch: 0, identity: None, generation: 0, closed: false })
    }

    pub fn faults(&self) -> Vec<String> { self.wiring.faults.lock().unwrap_or_else(PoisonError::into_inner).clone() }

    fn settled(&self, reply: LifeReply) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.outcomes.settle(waiter.id(), reply);
        waiter
    }

    /// `set_online`: Network answers after its `step`.
    pub fn set_online(&self, online: bool) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_network(NetworkInput::SetOnline { online, reply: waiter.id().raw() });
        waiter
    }

    /// `run`: the platform call rule of `lifecycle.md`, then the owner Calls.
    pub fn run(&self, epoch: Epoch, operation: String, variables: String) -> Waiter<LifeReply> {
        let published = self.published();
        if published.closed {
            return self.settled(LifeReply::Closed);
        }
        if published.identity.is_none() || self.wiring.ending.load(Ordering::SeqCst) || published.epoch != epoch {
            return self.settled(LifeReply::Call(Err(CallError::NoSession)));
        }
        let waiter = self.wiring.outcomes.register(epoch);
        self.wiring.to_calls(CallsInput::Run { epoch, operation, variables, reply: waiter.id().raw() });
        waiter
    }

    /// `session_call`: the owner Session, the answer by `Outcomes`.
    pub fn session_call(&self, operation: String, body: String) -> Waiter<LifeReply> {
        let published = self.published();
        if published.closed {
            return self.settled(LifeReply::Closed);
        }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_session(SessionInput::SessionCall { operation, body, reply: waiter.id().raw() });
        waiter
    }

    /// `Mutate` of Outbox and Transfer: the answer comes to the sink the author registered with `on_mutate_answer`.
    pub fn mutate(&self, author: AuthorId, owner: Option<super::session::MutateOwner>, req_id: RequestId, path: String, body: String, purpose: u8) {
        self.wiring.to_session(SessionInput::Mutate { author, owner, req_id, path, body, purpose });
    }

    pub fn on_mutate_answer(&self, author: AuthorId, sink: impl Fn(RequestId, AuthorOutcome) + Send + Sync + 'static) { self.wiring.on_mutate_answer(author, sink); }

    pub fn declare(&self, names: Vec<String>) { self.wiring.to_calls(CallsInput::Declare(names)); }

    pub fn session_reset(&self) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_lifecycle(LifecycleInput::SessionReset { reply: waiter.id().raw() });
        waiter
    }

    pub fn reset_models(&self, models: BTreeSet<String>) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_lifecycle(LifecycleInput::ResetModels { models, reply: Some(waiter.id().raw()) });
        waiter
    }

    /// Until `Session` executes the sign-in, the old path reports the committed credential here.
    pub fn credential_committed(&self, identity: u64) { self.wiring.to_lifecycle(LifecycleInput::CredentialCommitted(CredentialCommitted { identity })); }

    pub fn close(&self) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_lifecycle(LifecycleInput::Close { reply: waiter.id().raw() });
        waiter
    }

    /// Returns when the Store closed its connections, which follows the answer of `close`.
    pub fn wait_store_closed(&self, wait: Duration) -> bool {
        let guard = self.wiring.store_closed.0.lock().unwrap_or_else(PoisonError::into_inner);
        crate::blocking::blocking_unit(|| *self.wiring.store_closed.1.wait_timeout_while(guard, wait, |closed| !*closed).unwrap_or_else(PoisonError::into_inner).0)
    }
}

fn boot_found(db: &Db) -> Result<BootFound, LifeError> {
    let (saved_epoch, saved_generation, saved_identity) = db.shared.store.saved().map_err(|error| LifeError::Storage(error.to_string()))?;
    let changed: BTreeSet<String> = db.open_report().changed_models.iter().cloned().collect();
    Ok(if changed.is_empty() {
        BootFound::Same { saved_epoch, saved_generation, saved_identity }
    } else {
        BootFound::ChangedModels { saved_epoch, saved_generation, saved_identity, changed_models: changed }
    })
}
