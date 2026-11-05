//! Direction A install: the owners Network, Lifecycle, Store and Calls on one `Runtime`, each with the executor of its effects.
//! An executor does IO and returns the result as an input, it takes no transition. `Session` joins when its table gets `Mutate` and `GenerateCredential`.

mod observers;
mod read;
mod call_rule;
#[cfg(test)]
mod attempt_tests;
#[cfg(test)]
mod call_rule_tests;
mod api;
mod transfer_api;
mod calls_runner;
mod runners;
pub(crate) mod session_runner;
#[cfg(test)]
mod life_tests;
#[cfg(test)]
mod session_exchange_tests;

pub use observers::{ObserverPort, StopDone};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use super::calls::{CallAnswer, Calls, CallsInput, CallsState};
use super::contract::{BatchAuthor, BatchId, Committed, CommittedBroadcast, NetworkEvent, Published, Rejected, RequestId};
use super::session::{AuthorId, AuthorOutcome, SessionAnswer, SessionCalls, SessionInput, SessionState};
use super::lifecycle::{Lifecycle, LifecycleError, LifecycleInput, LifecycleState, Peer};
use super::network::{Network, NetworkInput, NetworkState};
use super::write::outbox::{Outbox, OutboxAnswer, OutboxInput, OutboxPeers, OutboxState};
use super::write::outbox_executor::OutboxExecutor;
use super::write::transfer::{TransferAnswer, TransferInput, TransferPeers, TransferState, Transfers};
use super::write::rounds::{Rounds, RoundsInput, RoundsState};
use super::write::rounds_executor::RoundsExecutor;
use super::write::transfer_executor::{LifeSignal, TransferExecutor, TransferPort};
use super::store::{BootFound, StoreInput, StoreReply, StoreState, StoreWriter};
use crate::db::Db;
use crate::runtime::{Address, Epoch, EpochStamp, Handle, Outcomes, PublishedCell, Runtime, RuntimeBuilder, Waiter};

/// The result of a platform call, the payload of `Outcomes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifeReply {
    Life(Result<(), LifecycleError>),
    Online { closed: bool },
    Call(CallAnswer),
    Session(SessionAnswer),
    Mutated(AuthorOutcome),
    Outbox(OutboxAnswer),
    Transfer(TransferAnswer),
    /// The outcome of a load that a platform call waited for in `Windows`.
    Window(super::read::windows::WindowOutcome),
    /// `Deliveries` closed the sink of a stream: the `cancel` of the platform returns.
    Cancelled,
    Closed,
}

const OPEN_WAIT: Duration = Duration::from_secs(365 * 24 * 3600);

/// The first author id of the calls that the old stack makes: below it the ids of Outbox, Transfer and Session.
const LEGACY_AUTHORS: AuthorId = 1 << 32;

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
    /// Transfer installs only with the session and this port.
    pub(crate) transfer: Option<TransferPort>,
    /// Feed opens its connections through the cable; without it the owner runs no connection.
    pub cable: Option<Arc<dyn crate::transport::CableLines>>,
    /// The operations of the schema by name, for the requests of the windows.
    pub read_operations: Option<super::read::ReadOperations>,
}

#[derive(Clone, Copy)]
pub(crate) struct Addresses {
    pub lifecycle: Address<Lifecycle>,
    pub store: Address<StoreWriter>,
    pub network: Address<Network>,
    pub calls: Address<Calls>,
    pub session: Option<Address<SessionCalls>>,
    /// Installed with the session: an operation needs the sequence of the session.
    pub outbox: Option<Address<Outbox>>,
    /// The owner LocalWrites: it lives in every epoch, with a session or without.
    pub local_writes: Address<Outbox>,
    pub transfer: Option<Address<Transfers>>,
    pub rounds: Option<Address<Rounds>>,
    pub windows: Option<Address<super::read::windows::Windows>>,
    pub deliveries: Option<Address<super::read::deliveries::Deliveries>>,
    pub feed: Option<Address<super::read::feed::Feed>>,
}

/// What every runner shares: the address book, the registry of waiters and the published value.
pub(crate) struct Wiring {
    pub handle: Handle,
    pub addresses: OnceLock<Addresses>,
    pub outcomes: Outcomes<LifeReply>,
    /// The answers of `Pull`: the registry of the platform calls that wait for the next value of a stream.
    pub pulls: Outcomes<super::read::deliveries::PullAnswer>,
    /// The ids of streams: they stay below the bit that marks the waiters of platform calls in `Windows`.
    pub next_stream: std::sync::atomic::AtomicU64,
    published: PublishedCell<Published>,
    /// The id of the wait of `open` and its answer, set at `Saved` of the first publication.
    open: Mutex<OpenWait>,
    faults: Mutex<Vec<String>>,
    open_failed: Mutex<Option<(String, bool)>>,
    /// Where the answer to a `Mutate` goes, by author: `Transport{req_id, Response/Error}`.
    mutate_sinks: Mutex<HashMap<AuthorId, Arc<dyn Fn(RequestId, AuthorOutcome) + Send + Sync>>>,
    /// Where the answer of Store to a batch goes, by author: the `install` of Window, Feed, Outbox and Transfer registers its owner here.
    store_sinks: Mutex<HashMap<BatchAuthor, Arc<dyn Fn(BatchId, StoreReply) + Send + Sync>>>,
    /// The answer of a call that ended the session waits for the next epoch: a caller that heard it finds the new epoch.
    after_publish: Mutex<Vec<(Awaited, crate::runtime::OutcomeId, LifeReply)>>,
    network_sinks: Mutex<Vec<(Peer, Arc<dyn Fn(NetworkEvent) + Send + Sync>)>>,
    /// Every subscriber of the Store broadcast `Committed`, in the order of subscription.
    committed_sinks: Mutex<Vec<Arc<dyn Fn(CommittedBroadcast) + Send + Sync>>>,
    session_open: Mutex<Option<SessionInput>>,
    restore_failures: Mutex<Vec<(Peer, String)>>,
}

#[derive(Default)]
struct OpenWait {
    id: Option<u64>,
    answer: Option<Result<(), LifecycleError>>,
}

/// What a held answer waits for: the epoch to move past the one that ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Awaited {
    EpochAbove(u64),
}

impl Wiring {
    fn new(handle: Handle) -> Self {
        Self {
            handle,
            addresses: OnceLock::new(),
            outcomes: Outcomes::new(),
            pulls: Outcomes::new(),
            next_stream: std::sync::atomic::AtomicU64::new(1),
            published: PublishedCell::default(),
            open: Mutex::new(OpenWait::default()),
            faults: Mutex::new(Vec::new()),
            open_failed: Mutex::new(None),
            mutate_sinks: Mutex::new(HashMap::new()),
            store_sinks: Mutex::new(HashMap::new()),
            after_publish: Mutex::new(Vec::new()),
            network_sinks: Mutex::new(Vec::new()),
            committed_sinks: Mutex::new(Vec::new()),
            session_open: Mutex::new(None),
            restore_failures: Mutex::new(Vec::new()),
        }
    }

    /// `None` only before `install` finished: no effect runs by then.
    pub fn addresses(&self) -> Option<Addresses> { self.addresses.get().copied() }

    pub fn publish(&self, published: Published) {
        let published_epoch = published.epoch;
        self.published.publish(published);
        let due: Vec<_> = {
            let mut held = self.after_publish.lock().unwrap_or_else(PoisonError::into_inner);
            let (due, rest): (Vec<_>, Vec<_>) = held.drain(..).partition(|(waiting, ..)| match waiting {
                Awaited::EpochAbove(epoch) => published_epoch > *epoch,
            });
            *held = rest;
            due
        };
        for (_, id, reply) in due {
            self.outcomes.settle(id, reply);
        }
    }

    /// Settles the answer of a call that ended the session once the epoch moved past `epoch`. Nothing to wait for when the session already ended or the epoch is past.
    pub fn settle_after_epoch(&self, epoch: u64, id: crate::runtime::OutcomeId, reply: LifeReply) {
        let mut held = self.after_publish.lock().unwrap_or_else(PoisonError::into_inner);
        let published = self.published.get();
        if published.as_ref().is_none_or(|published| published.epoch > epoch || published.identity.is_none() || published.closed) {
            drop(held);
            self.outcomes.settle(id, reply);
        } else {
            held.push((Awaited::EpochAbove(epoch), id, reply));
        }
    }

    /// The close answers every held answer: no publication follows.
    pub fn release_held(&self) {
        let held: Vec<_> = self.after_publish.lock().unwrap_or_else(PoisonError::into_inner).drain(..).collect();
        for (_, id, reply) in held {
            self.outcomes.settle(id, reply);
        }
    }

    /// Whether the published epoch is already past `epoch`: an answer addressed to that epoch has no receiver left.
    pub fn epoch_passed(&self, epoch: Epoch) -> bool { self.published.read(|published| published.is_some_and(|published| published.epoch > epoch)) }

    /// The wait of `open`: the id Lifecycle answers when the first publication is saved.
    pub fn open_reply(&self) -> Option<u64> { self.open.lock().unwrap_or_else(PoisonError::into_inner).id }

    /// Takes the answer of `open` when `id` is its wait. Returns whether it was.
    pub fn open_answered(&self, id: u64, result: Result<(), LifecycleError>) -> bool {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        if open.id != Some(id) || open.answer.is_some() {
            return false;
        }
        open.answer = Some(result);
        drop(open);
        self.published.notify();
        true
    }

    pub fn has_transfer(&self) -> bool { self.addresses().is_some_and(|addresses| addresses.transfer.is_some()) }

    pub fn fault(&self, what: String) { self.faults.lock().unwrap_or_else(PoisonError::into_inner).push(what); }

    pub fn open_failed(&self, error: String, corrupt: bool) {
        *self.open_failed.lock().unwrap_or_else(PoisonError::into_inner) = Some((error, corrupt));
        self.published.notify();
    }

    pub fn to_lifecycle(&self, input: LifecycleInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.lifecycle, EpochStamp::Control, input); }
    }

    pub fn to_store(&self, input: StoreInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.store, EpochStamp::Control, input); }
    }

    pub fn to_calls(&self, input: CallsInput) { self.to_calls_at(EpochStamp::Control, input); }

    pub fn to_calls_at(&self, stamp: EpochStamp, input: CallsInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.calls, stamp, input); }
    }

    pub fn to_session(&self, input: SessionInput) { self.to_session_at(EpochStamp::Control, input); }

    pub fn to_session_at(&self, stamp: EpochStamp, input: SessionInput) {
        if let Some(Addresses { session: Some(session), .. }) = self.addresses() { self.handle.post(session, stamp, input); }
    }

    pub fn open_session(&self) {
        let open = self.session_open.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(open) = open { self.to_session(open); }
    }

    pub fn on_network(&self, owner: Peer, sink: impl Fn(NetworkEvent) + Send + Sync + 'static) {
        self.network_sinks.lock().unwrap_or_else(PoisonError::into_inner).push((owner, Arc::new(sink)));
    }

    pub fn tell_network(&self, owners: &[Peer], event: NetworkEvent) {
        let sinks: Vec<_> = self.network_sinks.lock().unwrap_or_else(PoisonError::into_inner).iter().filter(|(owner, _)| owners.contains(owner)).map(|(_, sink)| sink.clone()).collect();
        for sink in sinks {
            crate::db::lock::effect(|| sink(event));
        }
    }

    pub fn on_committed(&self, sink: impl Fn(CommittedBroadcast) + Send + Sync + 'static) {
        self.committed_sinks.lock().unwrap_or_else(PoisonError::into_inner).push(Arc::new(sink));
    }

    pub fn tell_committed(&self, broadcast: CommittedBroadcast) {
        let sinks = self.committed_sinks.lock().unwrap_or_else(PoisonError::into_inner).clone();
        for sink in sinks {
            crate::db::lock::effect(|| sink(broadcast.clone()));
        }
    }

    pub fn restore_failed_for(&self, owner: Peer, error: String) {
        self.fault(format!("{} could not restore its intents: {error}", owner.name()));
        self.restore_failures.lock().unwrap_or_else(PoisonError::into_inner).push((owner, error));
    }

    /// The answer of Store to the batch `(author, batch)`.
    pub fn route_store_reply(&self, author: BatchAuthor, batch: BatchId, epoch: Epoch, reply: StoreReply) {
        let stamp = EpochStamp::Epoch(epoch);
        if author == BatchAuthor::Session {
            self.to_session_at(stamp, match reply {
                StoreReply::Committed { revision } => SessionInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), models: Default::default() }),
                StoreReply::Rejected(reason) => SessionInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        if author == BatchAuthor::Transfer {
            self.to_transfer_at(stamp, match reply {
                StoreReply::Committed { revision } => TransferInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), models: Default::default() }),
                StoreReply::Rejected(reason) => TransferInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        if author == BatchAuthor::LocalWrites {
            self.to_local_writes_at(stamp, match reply {
                StoreReply::Committed { revision } => OutboxInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), models: Default::default() }),
                StoreReply::Rejected(reason) => OutboxInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        if author == BatchAuthor::Outbox {
            self.to_outbox_at(stamp, match reply {
                StoreReply::Committed { revision } => OutboxInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), models: Default::default() }),
                StoreReply::Rejected(reason) => OutboxInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        let sink = self.store_sinks.lock().unwrap_or_else(PoisonError::into_inner).get(&author).cloned();
        match sink {
            Some(sink) => sink(batch, reply),
            None => self.fault(format!("no owner installed for the batches of {author:?}")),
        }
    }

    pub fn on_store_reply(&self, author: BatchAuthor, sink: impl Fn(BatchId, StoreReply) + Send + Sync + 'static) {
        self.store_sinks.lock().unwrap_or_else(PoisonError::into_inner).insert(author, Arc::new(sink));
    }

    pub fn on_mutate_answer(&self, author: AuthorId, sink: impl Fn(RequestId, AuthorOutcome) + Send + Sync + 'static) {
        self.mutate_sinks.lock().unwrap_or_else(PoisonError::into_inner).insert(author, Arc::new(sink));
    }

    /// `Transport{req_id, Response/Error{Permanent, X}}` to the author of a `Mutate`.
    pub fn answer_author(&self, author: AuthorId, req_id: RequestId, outcome: AuthorOutcome) {
        let sink = self.mutate_sinks.lock().unwrap_or_else(PoisonError::into_inner).get(&author).cloned();
        match sink {
            Some(sink) => sink(req_id, outcome),
            None => self.fault(format!("no author {author} for the answer to request {req_id}")),
        }
    }

    pub fn to_rounds(&self, input: RoundsInput) {
        if let Some(Addresses { rounds: Some(rounds), .. }) = self.addresses() { self.handle.post(rounds, EpochStamp::Control, input); }
    }

    pub fn to_transfer(&self, input: TransferInput) { self.to_transfer_at(EpochStamp::Control, input); }

    pub fn to_transfer_at(&self, stamp: EpochStamp, input: TransferInput) {
        if let Some(Addresses { transfer: Some(transfer), .. }) = self.addresses() { self.handle.post(transfer, stamp, input); }
    }

    pub fn to_local_writes(&self, input: OutboxInput) { self.to_local_writes_at(EpochStamp::Control, input); }

    pub fn to_local_writes_at(&self, stamp: EpochStamp, input: OutboxInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.local_writes, stamp, input); }
    }

    pub fn to_outbox(&self, input: OutboxInput) { self.to_outbox_at(EpochStamp::Control, input); }

    pub fn to_outbox_at(&self, stamp: EpochStamp, input: OutboxInput) {
        if let Some(Addresses { outbox: Some(outbox), .. }) = self.addresses() { self.handle.post(outbox, stamp, input); }
    }

    pub fn to_windows(&self, input: super::read::windows::WindowsInput) {
        if let Some(Addresses { windows: Some(windows), .. }) = self.addresses() { self.handle.post(windows, EpochStamp::Control, input); }
    }

    pub fn to_windows_in(&self, epoch: Epoch, input: super::read::windows::WindowsInput) {
        if let Some(Addresses { windows: Some(windows), .. }) = self.addresses() { self.handle.post(windows, EpochStamp::Epoch(epoch), input); }
    }

    pub fn to_deliveries(&self, input: super::read::deliveries::DeliveryInput) {
        if let Some(Addresses { deliveries: Some(deliveries), .. }) = self.addresses() { self.handle.post(deliveries, EpochStamp::Control, input); }
    }

    pub fn to_feed(&self, input: super::read::feed::FeedInput) {
        if let Some(Addresses { feed: Some(feed), .. }) = self.addresses() { self.handle.post(feed, EpochStamp::Control, input); }
    }

    pub fn to_network(&self, input: NetworkInput) {
        if let Some(addresses) = self.addresses() { self.handle.post(addresses.network, EpochStamp::Control, input); }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum LifeError {
    Storage(String),
    CorruptManifest(String),
}

pub struct Life {
    wiring: Arc<Wiring>,
    runtime: Mutex<Option<Runtime>>,
    shut: AtomicBool,
    /// The database has a session client: the rule of the calls reads `Published`. A database without one hands the calls to the owners.
    with_session: bool,
    alive: Arc<std::sync::atomic::AtomicUsize>,
}

/// Installed owners that send `Started` and `RestoreClassified` to Lifecycle.
fn restore_participants(session: bool) -> Vec<Peer> {
    if session { vec![Peer::Session] } else { Vec::new() }
}

/// Peers of `all owners` without an installed owner answer `Drained` at `Closing`.
fn absent(session: bool, transfer: bool) -> Vec<Peer> {
    let mut peers: Vec<Peer> = Vec::new();
    if !session {
        peers.push(Peer::Session);
        peers.push(Peer::Outbox);
    }
    if !transfer {
        peers.push(Peer::Transfer);
        peers.push(Peer::Rounds);
    }
    peers
}

impl Life {
    /// Builds the owners, boots the Store from the booted `Db` and returns after the first `Publish`.
    pub fn start(ports: Ports) -> Result<Self, LifeError> {
        let found = boot_found(&ports.db)?;
        let session = ports.session.clone().zip(ports.credentials.clone());
        let restored = session.as_ref().map(|(client, _)| session_runner::SessionRunner::stored(client)).transpose().map_err(|error| LifeError::Storage(error.to_string()))?;
        let mut builder = RuntimeBuilder::new();
        builder.with_clock(ports.db.shared.store.lock().clock.clone());
        let wiring = Arc::new(Wiring::new(builder.handle()));
        let open_wait = wiring.outcomes.register(Epoch::MAX);
        let lifecycle = builder.add::<Lifecycle>("lifecycle", LifecycleState::new().with_restore_participants(restore_participants(session.is_some())), runners::LifecycleRunner { wiring: wiring.clone(), leases: ports.db.shared.store.lifecycle.clone(), absent: absent(session.is_some(), session.is_some() && ports.transfer.is_some()), db: ports.db.clone(), held_published: None, held_replies: Vec::new() });
        builder.on_executor_failed(lifecycle);
        let store = builder.add::<StoreWriter>("store", StoreState::new(), runners::StoreRunner { wiring: wiring.clone(), db: ports.db.clone(), credentials: ports.credentials.clone(), client: ports.session.clone(), executor: ports.executor.clone() });
        let network = builder.add::<Network>("network", NetworkState::new(), runners::NetworkRunner { wiring: wiring.clone(), db: ports.db.clone(), held_replies: Vec::new() });
        let calls = builder.add::<Calls>("calls", CallsState::new(0, None, &[]), calls_runner::CallsRunner::new(wiring.clone(), &ports));
        let session_address = session.as_ref().zip(restored).map(|((client, credentials), restored)| {
            let state = SessionState::with_operations(client.session_operations(), None);
            let address = builder.add::<SessionCalls>("session", state, session_runner::SessionRunner::new(wiring.clone(), client.clone(), credentials.clone(), ports.executor.clone(), &restored));
            (address, restored)
        });
        let transfer_at = session_address.as_ref().zip(ports.transfer.as_ref()).map(|((session, _), _)| Address::<Transfers>::new(session.index() + 2));
        let rounds_at = transfer_at.map(|at| Address::<Rounds>::new(at.index() + 1));
        let outbox = session_address.as_ref().map(|(session, _)| {
            let peers = OutboxPeers { store, session: Some(*session), transfer: transfer_at };
            let (settle_wiring, drained_wiring) = (wiring.clone(), wiring.clone());
            let executor = OutboxExecutor {
                settle: Arc::new(move |reply, answer| { settle_wiring.outcomes.settle(crate::runtime::OutcomeId::from_raw(reply), LifeReply::Outbox(answer)); }),
                drained: Arc::new(move |drained| drained_wiring.to_lifecycle(LifecycleInput::Drained(drained))),
                copy: None,
                stale: Some({
                    let stale_wiring = wiring.clone();
                    Arc::new(move |families: Vec<(String, String)>, revision: i64| {
                        stale_wiring.to_windows(super::read::windows::WindowsInput::StoreCommitted { models: Vec::new(), revision });
                        for (model, relation) in families {
                            stale_wiring.to_windows(super::read::windows::WindowsInput::Stale { model, keys: super::read::windows::StaleKeys::Relations(std::collections::BTreeSet::from([relation])) });
                        }
                    })
                }),
                decode: ports.read_operations.clone().map(|operations| {
                    let db = ports.db.clone();
                    let decode: Arc<dyn Fn(&super::write::answer::DecodeJob) -> super::write::answer::Decoded + Send + Sync> = Arc::new(move |job| super::write::answer::decode_answer(&db, &|name| operations.get(name).map(|operation| operation.root_path.clone()), job));
                    (ports.executor.clone(), decode)
                }),
            };
            let answers = wiring.clone();
            wiring.on_mutate_answer(super::session::OUTBOX_AUTHOR, move |req_id, outcome| answers.to_outbox(OutboxInput::Answered { req_id, outcome }));
            builder.add::<Outbox>("outbox", OutboxState::new(0, peers), executor)
        });
        let transfer = match (outbox, ports.transfer.as_ref(), ports.transport.as_ref()) {
            (Some(outbox), Some(port), Some(transport)) => {
                let (settle_wiring, drained_wiring, admit_db, admit_wiring, signal_wiring) = (wiring.clone(), wiring.clone(), ports.db.clone(), wiring.clone(), wiring.clone());
                let wire = super::write::wire::Wire {
                    transport: transport.clone(),
                    admit: Arc::new(move || match admit_db.admit() {
                        Ok(admitted) => Some(admitted),
                        Err(error) => {
                            admit_wiring.fault(format!("a step of a transfer found no session to run in: {error}"));
                            None
                        }
                    }),
                    tasks: Arc::new(super::write::wire::TokioTasks(ports.executor.clone())),
                };
                let executor = TransferExecutor::new(
                    wire,
                    port.codec.clone(),
                    ports.db.shared.files.clone(),
                    Arc::new(move |reply, answer| { settle_wiring.outcomes.settle(crate::runtime::OutcomeId::from_raw(reply), LifeReply::Transfer(answer)); }),
                    port.shows.clone(),
                    Arc::new(move |drained| drained_wiring.to_lifecycle(LifecycleInput::Drained(drained))),
                    Arc::new(move |signal| match signal {
                        LifeSignal::Started(epoch) => signal_wiring.to_lifecycle(LifecycleInput::Started { owner: Peer::Transfer, epoch }),
                        LifeSignal::SnapshotRequest(epoch) => signal_wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Transfer, epoch }),
                        LifeSignal::RestoreClassified(epoch) => signal_wiring.to_lifecycle(LifecycleInput::RestoreClassified { owner: Peer::Transfer, epoch }),
                    }),
                );
                let address = builder.add::<Transfers>("transfer", TransferState::new(0, port.slots, TransferPeers { store, outbox, rounds: rounds_at }), executor);
                if Some(address.index()) != transfer_at.map(|at| at.index()) {
                    return Err(LifeError::Storage("the address of Transfer is not the one Outbox was given".into()));
                }
                let rounds_drained = wiring.clone();
                let rounds = builder.add::<Rounds>("rounds", RoundsState::new(address), RoundsExecutor::new(port.jobs.clone(), Arc::new(move |drained| rounds_drained.to_lifecycle(LifecycleInput::Drained(drained)))));
                if Some(rounds.index()) != rounds_at.map(|at| at.index()) {
                    return Err(LifeError::Storage("the address of Rounds is not the one Transfer was given".into()));
                }
                Some((address, rounds))
            }
            _ => None,
        };
        let local_writes = {
            let (settle_wiring, drained_wiring) = (wiring.clone(), wiring.clone());
            let executor = OutboxExecutor {
                settle: Arc::new(move |reply, answer| { settle_wiring.outcomes.settle(crate::runtime::OutcomeId::from_raw(reply), LifeReply::Outbox(answer)); }),
                drained: Arc::new(move |drained| drained_wiring.to_lifecycle(LifecycleInput::Drained(drained))),
                copy: None,
                stale: None,
                decode: None,
            };
            builder.add::<Outbox>("local_writes", OutboxState::local_writes(0, store), executor)
        };
        {
            let reads_wiring = wiring.clone();
            ports.db.shared.store.reads().attach(move |event| reads_wiring.to_store(match event {
                crate::store::reads::ReadEvent::Closed(ids) => StoreInput::ReadsClosed { ids },
                crate::store::reads::ReadEvent::Finished(read_id) => StoreInput::ReadFinished { read_id },
            }));
        }
        if session_address.is_some() {
            let weak = Arc::downgrade(&wiring);
            wiring.on_network(Peer::Session, move |event| {
                if let (NetworkEvent::Foreground(seq), Some(wiring)) = (event, weak.upgrade()) { wiring.to_session(SessionInput::Foreground { seq }); }
            });
        }
        let last = [Some(calls.index()), session_address.as_ref().map(|(address, _)| address.index()), outbox.map(|address| address.index()), transfer.map(|(address, _)| address.index()), Some(local_writes.index())].into_iter().flatten().max().unwrap_or(0);
        let read = read::add(&mut builder, &wiring, &ports, store, last + 1);
        let _ = wiring.addresses.set(Addresses { lifecycle, store, network, calls, session: session_address.as_ref().map(|(address, _)| *address), outbox, local_writes, transfer: transfer.map(|(address, _)| address), rounds: transfer.map(|(_, rounds)| rounds), windows: None, deliveries: None, feed: None }.with_read(&read));
        let runtime = builder.start().map_err(|error| LifeError::Storage(error.to_string()))?;
        if let Some((_, restored)) = session_address {
            *wiring.session_open.lock().unwrap_or_else(PoisonError::into_inner) = Some(SessionInput::Open { credential: restored.credential, pending: restored.pending, dropped_pending: restored.dropped_pending, secret: restored.secret });
        }
        wiring.open.lock().unwrap_or_else(PoisonError::into_inner).id = Some(open_wait.id().raw());
        wiring.to_store(StoreInput::Boot(found));
        let settled = wiring.published.wait_for(OPEN_WAIT, || {
            if let Some(answer) = wiring.open.lock().unwrap_or_else(PoisonError::into_inner).answer {
                return Some(Ok(answer));
            }
            wiring.open_failed.lock().unwrap_or_else(PoisonError::into_inner).clone().map(Err)
        });
        let answer = match settled {
            Some(Ok(answer)) => answer,
            Some(Err((error, corrupt))) => return Err(if corrupt { LifeError::CorruptManifest(error) } else { LifeError::Storage(error) }),
            None => return Err(LifeError::Storage("the first publication did not come".to_string())),
        };
        answer.map_err(|error| LifeError::Storage(format!("the first publication was not saved: {error:?}")))?;
        let alive = runtime.alive();
        Ok(Self { wiring, runtime: Mutex::new(Some(runtime)), shut: AtomicBool::new(false), with_session: session.is_some(), alive })
    }

    /// Stops the owner threads and joins them. The caller is not an owner thread. A second call does nothing.
    pub fn shutdown(&self) {
        self.shut.store(true, Ordering::SeqCst);
        let runtime = self.runtime.lock().unwrap_or_else(PoisonError::into_inner).take();
        drop(runtime);
    }

    /// Whether `shutdown` ran: the database is closed for good.
    pub fn is_shut(&self) -> bool { self.shut.load(Ordering::SeqCst) }

    /// The owner threads of this `Life` that have not ended.
    pub fn owners_alive(&self) -> usize { self.alive.load(Ordering::SeqCst) }

    pub fn published(&self) -> Published {
        self.wiring.published.get().unwrap_or_else(Published::initial)
    }

    pub fn faults(&self) -> Vec<String> { self.wiring.faults.lock().unwrap_or_else(PoisonError::into_inner).clone() }

    pub fn restore_failures(&self) -> Vec<(Peer, String)> { self.wiring.restore_failures.lock().unwrap_or_else(PoisonError::into_inner).clone() }

    pub fn on_network(&self, owner: Peer, sink: impl Fn(NetworkEvent) + Send + Sync + 'static) { self.wiring.on_network(owner, sink); }

    /// Subscribes to the Store broadcast `Committed`: one call per committed batch, clear and models reset, in the order of revisions.
    pub fn on_committed(&self, sink: impl Fn(CommittedBroadcast) + Send + Sync + 'static) { self.wiring.on_committed(sink); }

    fn settled(&self, reply: LifeReply) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.outcomes.settle(waiter.id(), reply);
        waiter
    }
}

fn boot_found(db: &Db) -> Result<BootFound, LifeError> {
    let (saved_epoch, saved_generation, saved_identity) = db.shared.store.saved().map_err(|error| match error {
        crate::store::StoreError::CorruptManifest(detail) => LifeError::CorruptManifest(detail),
        other => LifeError::Storage(other.to_string()),
    })?;
    let revision = db.shared.store.revision_seq().ok().and_then(|revision| u64::try_from(revision).ok()).ok_or_else(|| LifeError::Storage("the stored revision is not a revision".into()))?;
    let report = db.open_report();
    let server_model_changed = report.changed_models.iter().any(|model| db.shared.specs.get(model).is_none_or(|spec| !spec.is_local()));
    Ok(if report.recreated == Some(crate::ddl::RecreateReason::UnreadableFile) {
        BootFound::Fresh
    } else if report.recreated.is_some() {
        BootFound::FormatChange { saved_epoch, saved_generation, saved_identity, revision }
    } else if server_model_changed {
        BootFound::ChangedModels { saved_epoch, saved_generation, saved_identity, revision }
    } else if !report.changed_models.is_empty() {
        BootFound::LocalModelChanged { saved_epoch, saved_generation, saved_identity, revision }
    } else {
        BootFound::Same { saved_epoch, saved_generation, saved_identity, revision }
    })
}
