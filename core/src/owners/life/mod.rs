//! Direction A install: the owners Network, Lifecycle, Store and Calls on one `Runtime`, each with the executor of its effects.
//! An executor does IO and returns the result as an input, it takes no transition. `Session` joins when its table gets `Mutate` and `GenerateCredential`.

mod observers;
mod read;
mod call_rule;
#[cfg(test)]
mod tests;
mod api;
mod transfer_api;
mod calls_runner;
mod runners;
pub(crate) mod session_runner;

pub use observers::{ObserverPort, StopDone};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use super::calls::{CallAnswer, Calls, CallsInput, CallsState};
use super::contract::{BatchAuthor, BatchId, Committed, CommittedBroadcast, NetworkEvent, Published, Rejected, RequestId};
use super::session::{AuthorId, AuthorOutcome, SessionAnswer, SessionCalls, SessionInput, SessionState};
use super::lifecycle::{Lifecycle, LifecycleError, LifecycleInput, LifecycleState, Peer};
use super::network::{Network, NetworkInput, NetworkState};
use super::write::outbox::{Outbox, OutboxAnswer, OutboxInput, OutboxPeers, OutboxState};
use super::write::outbox_executor::{OutboxExecutor, OutboxSignal};
use super::write::transfer::{TransferAnswer, TransferInput, TransferPeers, TransferState, Transfers};
use super::write::rounds::{Rounds, RoundsInput, RoundsState};
use super::write::rounds_executor::RoundsExecutor;
use super::write::transfer_executor::{LifeSignal, TransferExecutor, TransferPort, TransferSinks};
use super::store::{BootFound, StoreInput, StoreReply, StoreState, StoreWriter};
use crate::store::Store;
use crate::runtime::{Address, AppendLog, Epoch, EpochStamp, Handle, OutcomeId, Outcomes, PublishedCell, Runtime, RuntimeBuilder, Waiter};

mod reply;
mod boot;
pub use reply::LifeReply;
pub(crate) use call_rule::CallVerdict;
use boot::boot_found;


/// The first author id of the direct calls: below it the ids of Outbox, Transfer and Calls.
const WAITER_AUTHOR: AuthorId = 1 << 32;

/// The document and the answer root of a declared operation.
pub type OperationLookup = Arc<dyn Fn(&str) -> Option<(String, String)> + Send + Sync>;

pub struct Ports {
    pub db: std::sync::Arc<Store>,
    pub transport: Option<Arc<dyn crate::transport::Upstream>>,
    pub executor: tokio::runtime::Handle,
    pub operations: OperationLookup,
    pub(crate) secret: Option<Arc<crate::transport::encrypted::credential::Secret>>,
    /// The encrypted exchange: Session installs only with it and `secret`. It does the encoding and the HTTP exchange, Session decides.
    pub exchange: Option<Arc<crate::transport::Exchange>>,
    pub(crate) seal: Option<Arc<super::session_seal::SealConfig>>,
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
    /// The clock of the database in ms: the platform calls stamp the inputs that decide freshness with it.
    pub now: Arc<dyn Fn() -> i64 + Send + Sync>,
    pub addresses: OnceLock<Addresses>,
    pub outcomes: Outcomes<LifeReply>,
    /// The answers of `Pull`: the registry of the platform calls that wait for the next value of a stream.
    pub pulls: Outcomes<super::read::deliveries::PullAnswer>,
    /// The ids of streams: they stay below the bit that marks the waiters of platform calls in `Windows`.
    pub next_stream: std::sync::atomic::AtomicU64,
    published: PublishedCell<Published>,
    /// The epoch the requests to the network may leave in: it closes before any owner steps on `EpochEnded`.
    pub(crate) gate: Arc<crate::runtime::EpochGate>,
    #[cfg(test)]
    pub(crate) flights: super::read::RequestsOnTheWay,
    /// The id of the wait of `open`, set before the owners start, and its answer, set at `Saved` of the first publication.
    open_id: OnceLock<u64>,
    open_answer: OnceLock<Result<(), LifecycleError>>,
    faults: AppendLog<String>,
    open_failed: OnceLock<(String, bool)>,
    /// The answers to `Mutate{reply_to = Calls, req_id}`: the executor of Calls waits for its request under `req_id`.
    pub(crate) calls_answers: Outcomes<AuthorOutcome>,
    /// The waits of direct calls by author id.
    pub(crate) mutations: Outcomes<LifeReply>,
    /// The request id of the unanswered `Mutate` of a transfer step: the repeat of the step is the same `Mutate` under the same id until an answer settles it.
    held_requests: crate::runtime::HeldIds<crate::transport::RequestOwner>,
    /// Where the answer of Store to a batch goes, by author: the `install` of Window, Feed, Outbox and Transfer registers its owner here, the last registration of an author answers.
    store_sinks: AppendLog<(BatchAuthor, StoreSink)>,
    network_sinks: AppendLog<(Peer, NetworkSink)>,
    /// Every subscriber of the Store broadcast `Committed`, in the order of subscription.
    committed_sinks: AppendLog<CommittedSink>,
    restore_failures: AppendLog<(Peer, String)>,
}

impl Wiring {
    /// The epoch is the published one, open, and has an identity: a request to the server needs a session.
    pub(crate) fn admits_session(&self, epoch: Epoch) -> bool { self.published.read(|published| published.is_some_and(|published| published.epoch == epoch && !published.closed && published.identity.is_some())) }

    fn new(handle: Handle, now: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        Self {
            now,
            addresses: OnceLock::new(),
            outcomes: Outcomes::new(),
            pulls: Outcomes::new(),
            handle,
            next_stream: std::sync::atomic::AtomicU64::new(1),
            published: PublishedCell::default(),
            gate: Arc::default(),
            #[cfg(test)]
            flights: Default::default(),
            open_id: OnceLock::new(),
            open_answer: OnceLock::new(),
            faults: AppendLog::default(),
            open_failed: OnceLock::new(),
            calls_answers: Outcomes::new(),
            mutations: Outcomes::new(),
            held_requests: Default::default(),
            store_sinks: AppendLog::default(),
            network_sinks: AppendLog::default(),
            committed_sinks: AppendLog::default(),
            restore_failures: AppendLog::default(),
        }
    }

    /// The id a repeat of the step of `owner` carries: the held one, else a new one that stays held.
    pub(crate) fn request_of(&self, owner: &crate::transport::RequestOwner) -> RequestId { self.held_requests.id_of(owner, super::contract::next_request_id) }

    pub(crate) fn hold_request(&self, owner: crate::transport::RequestOwner, id: RequestId) { self.held_requests.hold(owner, id); }

    pub(crate) fn settle_request(&self, id: RequestId) { self.held_requests.settle(id); }

    /// `None` only before `install` finished: no effect runs by then.
    pub fn addresses(&self) -> Option<Addresses> { self.addresses.get().copied() }

    pub fn publish(&self, published: Published) { self.published.publish(published); }

    /// Ends every open wait with `Closing` and answers each later wait the same way: no owner runs after the stop to settle one.
    pub fn close_waits(&self) {
        self.outcomes.close();
        self.pulls.close();
        self.calls_answers.close();
        self.mutations.close();
    }

    /// Whether the published epoch is already past `epoch`: an answer addressed to that epoch has no receiver left.
    pub fn epoch_passed(&self, epoch: Epoch) -> bool { self.published.read(|published| published.is_some_and(|published| published.epoch > epoch)) }

    /// The wait of `open`: the id Lifecycle answers when the first publication is saved.
    pub fn open_reply(&self) -> Option<u64> { self.open_id.get().copied() }

    /// Takes the answer of `open` when `id` is its wait. Returns whether it was.
    pub fn open_answered(&self, id: u64, result: Result<(), LifecycleError>) -> bool {
        if self.open_id.get() != Some(&id) || self.open_answer.set(result).is_err() {
            return false;
        }
        self.published.notify();
        true
    }

    pub fn has_transfer(&self) -> bool { self.addresses().is_some_and(|addresses| addresses.transfer.is_some()) }

    pub fn fault(&self, what: String) { self.faults.push(what); }

    pub fn open_failed(&self, error: String, corrupt: bool) {
        if self.open_failed.set((error, corrupt)).is_err() { self.fault("the open failed twice".into()); }
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

    pub fn on_network(&self, owner: Peer, sink: impl Fn(NetworkEvent) + Send + Sync + 'static) {
        self.network_sinks.push((owner, Arc::new(sink)));
    }

    pub fn tell_network(&self, owners: &[Peer], event: NetworkEvent) {
        for (_, sink) in self.network_sinks.iter().filter(|(owner, _)| owners.contains(owner)) {
            crate::lock::effect(|| sink(event));
        }
    }

    pub fn on_committed(&self, sink: impl Fn(CommittedBroadcast) + Send + Sync + 'static) {
        self.committed_sinks.push(Arc::new(sink));
    }

    pub fn tell_committed(&self, broadcast: CommittedBroadcast) {
        for sink in self.committed_sinks.iter() {
            crate::lock::effect(|| sink(broadcast.clone()));
        }
    }

    pub fn restore_failed_for(&self, owner: Peer, error: String) {
        self.fault(format!("{} could not restore its intents: {error}", owner.name()));
        self.restore_failures.push((owner, error));
    }

    /// The answer of Store to the batch `(author, batch)`.
    pub fn route_store_reply(&self, author: BatchAuthor, batch: BatchId, epoch: Epoch, reply: StoreReply) {
        let stamp = EpochStamp::Epoch(epoch);
        if author == BatchAuthor::Session {
            self.to_session_at(stamp, match reply {
                StoreReply::Committed { revision, .. } => SessionInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }),
                StoreReply::Rejected(reason) => SessionInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        if author == BatchAuthor::Transfer {
            self.to_transfer_at(stamp, match reply {
                StoreReply::Committed { revision, shown, .. } => TransferInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown, files_removed: 0 }),
                StoreReply::Rejected(reason) => TransferInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        if author == BatchAuthor::LocalWrites {
            self.to_local_writes_at(stamp, match reply {
                StoreReply::Committed { revision, shown, files_removed, .. } => OutboxInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown, files_removed }),
                StoreReply::Rejected(reason) => OutboxInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        if author == BatchAuthor::Outbox {
            self.to_outbox_at(stamp, match reply {
                StoreReply::Committed { revision, shown, files_removed, .. } => OutboxInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown, files_removed }),
                StoreReply::Rejected(reason) => OutboxInput::Rejected(Rejected { batch, reason }),
            });
            return;
        }
        let sink = self.store_sinks.iter().filter(|(registered, _)| *registered == author).map(|(_, sink)| sink.clone()).last();
        match sink {
            Some(sink) => sink(batch, reply),
            None => self.fault(format!("no owner installed for the batches of {author:?}")),
        }
    }

    pub fn on_store_reply(&self, author: BatchAuthor, sink: impl Fn(BatchId, StoreReply) + Send + Sync + 'static) {
        self.store_sinks.push((author, Arc::new(sink)));
    }

    /// `Transport{req_id, Response/Error{Permanent, X}}` to the `reply_to` of a `Mutate`: Outbox, Calls or the waiter of a direct call.
    pub(crate) fn answer_author(&self, author: AuthorId, req_id: RequestId, outcome: AuthorOutcome) {
        match author {
            super::session::OUTBOX_AUTHOR => self.to_outbox(OutboxInput::Answered { req_id, outcome }),
            super::session::CALLS_AUTHOR => { self.calls_answers.settle(OutcomeId::from_raw(req_id), outcome); }
            super::session::TRANSFER_AUTHOR => {
                self.to_transfer(TransferInput::StepAnswer { request_id: req_id, outcome });
            }
            WAITER_AUTHOR.. => {
                if !matches!(&outcome, AuthorOutcome::Dropped | AuthorOutcome::Error { pending_retained: true, .. } | AuthorOutcome::Response { kind: crate::transport::AnswerKind::Transient { .. }, .. }) { self.settle_request(req_id); }
                self.mutations.settle(OutcomeId::from_raw(author), LifeReply::Mutated(outcome));
            }
            _ => self.fault(format!("no author {author} for the answer to request {req_id}")),
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
        self.to_feed_at(EpochStamp::Control, input);
    }

    pub fn to_feed_at(&self, stamp: EpochStamp, input: super::read::feed::FeedInput) {
        if let Some(Addresses { feed: Some(feed), .. }) = self.addresses() { self.handle.post(feed, stamp, input); }
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

type StoreSink = Arc<dyn Fn(BatchId, StoreReply) + Send + Sync>;
type NetworkSink = Arc<dyn Fn(NetworkEvent) + Send + Sync>;
type CommittedSink = Arc<dyn Fn(CommittedBroadcast) + Send + Sync>;

pub struct Life {
    wiring: Arc<Wiring>,
    runtime: Runtime,
    shut: AtomicBool,
    /// The database has a session client: the rule of the calls reads `Published`. A database without one hands the calls to the owners.
    with_session: bool,
    /// The path of the requests of the server, when the database has a session client.
    graphql_path: Option<String>,
    alive: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(test)]
    rounds_needed: Arc<std::sync::atomic::AtomicU64>,
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
        #[cfg(test)] let rounds_needed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let session = ports.exchange.clone().zip(ports.secret.clone()).zip(ports.seal.clone()).map(|((exchange, secret), seal)| (exchange, secret, seal));
        let restored = session.as_ref().map(|(exchange, secret, _)| session_runner::SessionRunner::stored(exchange, secret, &ports.db)).transpose().map_err(|error| LifeError::Storage(error.to_string()))?;
        let saved_unowned_pending = restored.as_ref().is_some_and(|state| state.unowned_pending);
        let restore_error = restored.as_ref().and_then(|state| state.restore_error.clone());
        let found = boot_found(&ports.db, saved_unowned_pending)?;
        let clock = ports.db.lock().clock.clone();
        let mut builder = RuntimeBuilder::new(clock.clone());
        let wiring = Wiring::new(builder.handle(), Arc::new(move || i64::try_from(clock.now().as_millis()).unwrap_or(i64::MAX)));
        if let Some(transport) = &ports.transport { wiring.gate.attach(transport.clone()); }
        let wiring = Arc::new(wiring);
        let open_wait = wiring.outcomes.register(Epoch::MAX);
        wiring.open_id.set(open_wait.id().raw()).map_err(|_| LifeError::Storage("the wait of open is set twice".into()))?;
        let lifecycle = builder.add::<Lifecycle>("lifecycle", LifecycleState::new(), runners::LifecycleRunner { wiring: wiring.clone(), absent: absent(session.is_some(), session.is_some() && ports.transfer.is_some()), db: ports.db.clone(), held_published: None, held_replies: Vec::new(), held_read: None });
        builder.on_executor_failed(lifecycle);
        let store = builder.reserve::<StoreWriter>();
        let network = builder.add::<Network>("network", NetworkState::new(), runners::NetworkRunner { wiring: wiring.clone(), db: ports.db.clone(), held_replies: Vec::new() });
        let calls = builder.add::<Calls>("calls", CallsState::new(0, None, &[]), calls_runner::CallsRunner::new(wiring.clone(), &ports));
        let session_address = session.as_ref().zip(restored).map(|((exchange, secret, seal), restored)| {
            let state = SessionState::with_operations(exchange.session_operations(), None);
            let address = builder.add::<SessionCalls>("session", state, session_runner::SessionRunner::new(wiring.clone(), (exchange.clone(), secret.clone(), ports.db.clone()), seal.clone(), ports.executor.clone(), &restored));
            (address, restored)
        });
        let has_transfer = session_address.is_some() && ports.transfer.is_some() && ports.transport.is_some();
        let transfer_at = has_transfer.then(|| builder.reserve::<Transfers>());
        let rounds_at = has_transfer.then(|| builder.reserve::<Rounds>());
        let outbox = session_address.as_ref().map(|(session, _)| {
            let peers = OutboxPeers { store, session: Some(*session), transfer: transfer_at };
            let (settle_wiring, drained_wiring, signal_wiring) = (wiring.clone(), wiring.clone(), wiring.clone());
            let executor = OutboxExecutor {
                settle: Arc::new(move |reply, answer| { settle_wiring.outcomes.settle(crate::runtime::OutcomeId::from_raw(reply), LifeReply::Outbox(answer)); }),
                drained: Arc::new(move |drained| drained_wiring.to_lifecycle(LifecycleInput::Drained(drained))),
                signal: Arc::new(move |signal| match signal {
                    OutboxSignal::Started(epoch) => signal_wiring.to_lifecycle(LifecycleInput::Started { owner: Peer::Outbox, epoch }),
                    OutboxSignal::SnapshotRequest(epoch) => signal_wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Outbox, epoch }),
                    OutboxSignal::RestoreClassified(epoch) => signal_wiring.to_lifecycle(LifecycleInput::RestoreClassified { owner: Peer::Outbox, epoch }),
                }),
                copy: None,
                counted: Some(builder.handle()),
                stale: Some({
                    let stale_wiring = wiring.clone();
                    Arc::new(move |families: Vec<(String, String)>, revision: i64| {
                        stale_wiring.to_windows(super::read::windows::WindowsInput::StoreCommitted { models: Vec::new(), revision });
                        for (model, relation) in families {
                            stale_wiring.to_windows(super::read::windows::WindowsInput::Stale { model, keys: super::read::windows::StaleKeys::Relations(std::collections::BTreeSet::from([relation])), cause: super::read::windows::StaleCause::Change });
                        }
                    })
                }),
                decode: ports.read_operations.clone().map(|operations| {
                    let db = ports.db.clone();
                    let decode: Arc<dyn Fn(&super::write::answer::DecodeJob) -> super::write::answer::Decoded + Send + Sync> = Arc::new(move |job| super::write::answer::decode_answer(&db, &|name| operations.get(name).map(|operation| operation.root_path.clone()), job));
                    (ports.executor.clone(), decode)
                }),
            };
            builder.add::<Outbox>("outbox", OutboxState::new(0, peers), executor)
        });
        let transfer = match (outbox, ports.transfer.as_ref(), ports.transport.as_ref(), transfer_at, rounds_at) {
            (Some(outbox), Some(port), Some(_), Some(address), Some(rounds)) => {
                let (settle_wiring, drained_wiring, signal_wiring, status_wiring) = (wiring.clone(), wiring.clone(), wiring.clone(), wiring.clone());
                let wire = super::write::wire::Wire {
                    gate: wiring.gate.clone(),
                    tasks: Arc::new(super::write::wire::TokioTasks(ports.executor.clone(), Some(builder.handle()))),
                    ids: super::write::wire::StepIds { mint: { let ids = wiring.clone(); Arc::new(move |step| ids.request_of(step)) }, settle: { let ids = wiring.clone(); Arc::new(move |id| ids.settle_request(id)) } },
                };
                let executor = TransferExecutor::new(
                    wire,
                    port.codec.clone(),
                    ports.db.files().clone(),
                    TransferSinks {
                        settle: Arc::new(move |reply, answer| { settle_wiring.outcomes.settle(crate::runtime::OutcomeId::from_raw(reply), LifeReply::Transfer(answer)); }),
                        status: Arc::new(move |stream, shown, revision| status_wiring.to_deliveries(super::read::deliveries::DeliveryInput::TransferStatus { stream, blob: super::read::deliveries::BlobStatus::of(&shown), revision })),
                        drained: Arc::new(move |drained| drained_wiring.to_lifecycle(LifecycleInput::Drained(drained))),
                        signal: Arc::new(move |signal| match signal {
                            LifeSignal::Started(epoch) => signal_wiring.to_lifecycle(LifecycleInput::Started { owner: Peer::Transfer, epoch }),
                            LifeSignal::SnapshotRequest(epoch) => signal_wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Transfer, epoch }),
                            LifeSignal::RestoreClassified(epoch) => signal_wiring.to_lifecycle(LifecycleInput::RestoreClassified { owner: Peer::Transfer, epoch }),
                            LifeSignal::ReleaseStep(req_id) => {
                                signal_wiring.settle_request(req_id);
                                signal_wiring.to_session(SessionInput::Release { req_id });
                            }
                        }),
                    },
                    { let db = ports.db.clone(); Arc::new(move |sha256: &str| db.blob_cache_lookup(sha256)) },
                );
                let mut transfers = TransferState::new(0, port.slots, TransferPeers { store, outbox, rounds: rounds_at });
                transfers.put_timeout = port.put_timeout;
                builder.install(address, "transfer", transfers, executor);
                let rounds_drained = wiring.clone();
                let round_state = RoundsState::new(address);
                #[cfg(test)] let round_state = { let mut state = round_state; state.needed_count = rounds_needed.clone(); state };
                builder.install(rounds, "rounds", round_state, RoundsExecutor::new(port.jobs.clone(), Arc::new(move |drained| rounds_drained.to_lifecycle(LifecycleInput::Drained(drained)))));
                Some((address, rounds))
            }
            _ => None,
        };
        let local_writes = {
            let (settle_wiring, drained_wiring) = (wiring.clone(), wiring.clone());
            let executor = OutboxExecutor {
                settle: Arc::new(move |reply, answer| { settle_wiring.outcomes.settle(crate::runtime::OutcomeId::from_raw(reply), LifeReply::Outbox(answer)); }),
                drained: Arc::new(move |drained| drained_wiring.to_lifecycle(LifecycleInput::Drained(drained))),
                signal: Arc::new(|_| {}),
                copy: None,
                stale: None,
                decode: None,
                counted: None,
            };
            builder.add::<Outbox>("local_writes", OutboxState::local_writes(0, store), executor)
        };
        {
            let reads_wiring = wiring.clone();
            ports.db.reads().attach(move |event| reads_wiring.to_store(match event {
                crate::store::reads::ReadEvent::Closed(ids) => StoreInput::ReadsClosed { ids },
                crate::store::reads::ReadEvent::Finished(read_id) => StoreInput::ReadFinished { read_id },
            })).map_err(|error| LifeError::Storage(error.to_string()))?;
        }
        if session_address.is_some() {
            let weak = Arc::downgrade(&wiring);
            wiring.on_network(Peer::Session, move |event| {
                if let (NetworkEvent::Foreground(seq), Some(wiring)) = (event, weak.upgrade()) { wiring.to_session(SessionInput::Foreground { seq }); }
            });
        }
        let read = read::add(&mut builder, &wiring, &ports, store);
        wiring.addresses.set(Addresses { lifecycle, store, network, calls, session: session_address.as_ref().map(|(address, _)| *address), outbox, local_writes, transfer: transfer.map(|(address, _)| address), rounds: transfer.map(|(_, rounds)| rounds), windows: None, deliveries: None, feed: None }.with_read(&read)).map_err(|_| LifeError::Storage("the addresses are set twice".into()))?;
        let session_open = session_address.map(|(_, restored)| SessionInput::Open { credential: restored.credential, pending: restored.pending, dropped_pending: restored.dropped_pending, secret: restored.secret });
        builder.install(store, "store", StoreState::new(), runners::StoreRunner { wiring: wiring.clone(), db: ports.db.clone(), secret: ports.secret.clone(), exchange: ports.exchange.clone(), executor: ports.executor.clone(), restore_error, session_open });
        let runtime = builder.start().map_err(|error| LifeError::Storage(error.to_string()))?;
        wiring.to_store(StoreInput::Boot(found));
        let settled = wiring.published.wait_until(|| {
            if let Some(answer) = wiring.open_answer.get() {
                return Some(Ok(*answer));
            }
            wiring.open_failed.get().cloned().map(Err)
        });
        let answer = match settled {
            Ok(answer) => answer,
            Err((error, corrupt)) => return Err(if corrupt { LifeError::CorruptManifest(error) } else { LifeError::Storage(error) }),
        };
        answer.map_err(|error| LifeError::Storage(format!("the first publication was not saved: {error:?}")))?;
        let alive = runtime.alive();
        Ok(Self { wiring, runtime, shut: AtomicBool::new(false), with_session: session.is_some(), graphql_path: session.as_ref().map(|session| session.0.graphql_path().to_string()), alive, #[cfg(test)] rounds_needed })
    }

    #[cfg(test)]
    pub(crate) fn rounds_needed_count(&self) -> u64 { self.rounds_needed.load(std::sync::atomic::Ordering::Relaxed) }

    /// Stops the owner threads and joins them. The caller is not an owner thread. A second call waits for the first to finish.
    pub fn shutdown(&self) {
        self.shut.store(true, Ordering::SeqCst);
        self.runtime.stop();
        self.wiring.close_waits();
    }

    /// Returns once every input posted to an owner was handled.
    #[cfg(test)]
    pub fn barrier(&self) { self.wiring.handle.barrier() }

    /// The address book of the owners: it outlives the `Life` and still waits for the tasks that hold it.
    #[cfg(test)]
    pub fn owners_handle(&self) -> crate::runtime::Handle { self.wiring.handle.clone() }

    /// The inputs and calls that are in flight now.
    #[cfg(test)]
    pub fn pending(&self) -> usize { self.wiring.handle.pending() }

    /// Counts a platform call as in flight until it registered with the owners.
    pub fn launch(&self) -> crate::runtime::Launch { self.wiring.handle.launch() }

    /// Whether `shutdown` ran: the database is closed for good.
    pub fn is_shut(&self) -> bool { self.shut.load(Ordering::SeqCst) }

    /// The owner threads of this `Life` that have not ended.
    pub fn owners_alive(&self) -> usize { self.alive.load(Ordering::SeqCst) }

    pub fn published(&self) -> Published {
        self.wiring.published.get().unwrap_or_else(Published::initial)
    }

    /// The requests of the network that are on their way.
    #[cfg(test)]
    pub fn requests_in_flight(&self) -> usize { self.wiring.flights.load(Ordering::SeqCst) }

    /// Returns when no request of the network is on its way.
    #[cfg(test)]
    pub async fn requests_none(&self) { self.wiring.flights.none().await }

    pub(crate) fn fault(&self, what: String) { self.wiring.fault(what); }

    pub fn faults(&self) -> Vec<String> { self.wiring.faults.iter().cloned().collect() }

    pub fn restore_failures(&self) -> Vec<(Peer, String)> { self.wiring.restore_failures.iter().cloned().collect() }

    pub fn on_network(&self, owner: Peer, sink: impl Fn(NetworkEvent) + Send + Sync + 'static) { self.wiring.on_network(owner, sink); }

    /// Subscribes to the Store broadcast `Committed`: one call per committed batch, clear and models reset, in the order of revisions.
    pub fn on_committed(&self, sink: impl Fn(CommittedBroadcast) + Send + Sync + 'static) { self.wiring.on_committed(sink); }

    #[cfg(test)]
    pub(crate) fn outcomes(&self) -> &Outcomes<LifeReply> { &self.wiring.outcomes }

    fn settled(&self, reply: LifeReply) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.outcomes.settle(waiter.id(), reply);
        waiter
    }
}
