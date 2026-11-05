//! Install of the owners of reading (`design.md` F2): `Windows`, `Deliveries` and `Feed` with the executors of their effects.
//! An executor does IO and returns the result as an input; the platform side settles its waiters through `Outcomes`.

use std::sync::Arc;

use super::{Addresses, LifeReply, Ports, Wiring};
use crate::db::Db;
use crate::owners::contract::{BatchAuthor, Committed, Drained, Models, Rejected};
use crate::owners::lifecycle::LifecycleInput;
use crate::owners::read::deliveries::{DeliveriesState, Deliveries, DeliveryEffect, DeliveryInput, DeliveryPeers, PullAnswer, StreamEffect, StreamId, View};
use crate::owners::read::feed::{Feed, FeedEffect, FeedPeers, FeedState};
use crate::owners::read::windows::{Goal, Windows, WindowEffect, WindowKey, WindowsEffect, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::read::{max_pages, preload_keys, preload_sources, refreshes_on_foreground, DbSnapshots, DeliveryIo, SchemaWindowCodec, WindowIo};
use crate::owners::store::{StoreReply, StoreWriter};
use crate::runtime::{Address, Back, EffectRunner, Epoch, EpochStamp, OutcomeId, RuntimeBuilder, Waiter};
use crate::transport::{GraphQLRequest, PutRequest, PutResponse, Response, ServerSubscription, Transport, TransportError};

/// The ids of waiters that platform calls register in `Windows` carry this bit, the ids of streams stay below it.
pub(crate) const CALL: u64 = 1 << 62;

/// The transport of a database without one: every request fails as a network error.
struct Unconnected;

#[async_trait::async_trait]
impl Transport for Unconnected {
    async fn query(&self, _admitted: &crate::ffi::legacy::Admitted, _request: GraphQLRequest) -> Result<Response, TransportError> { Err(TransportError::Network("the database has no transport".into())) }
    async fn mutate(&self, _admitted: &crate::ffi::legacy::Admitted, _request: GraphQLRequest) -> Result<Response, TransportError> { Err(TransportError::Network("the database has no transport".into())) }
    async fn subscribe(&self, _admitted: &crate::ffi::legacy::Admitted, _request: GraphQLRequest) -> Result<ServerSubscription, TransportError> { Err(TransportError::Network("the database has no transport".into())) }
    async fn put(&self, _admitted: &crate::ffi::legacy::Admitted, _request: PutRequest) -> Result<PutResponse, TransportError> { Err(TransportError::Network("the database has no transport".into())) }
}

/// `Feed` runs no IO here: the owner exists for its address and ends with the database. The runner of its subscription replaces this one.
struct FeedWithoutIo {
    wiring: Arc<Wiring>,
}

impl EffectRunner<Feed> for FeedWithoutIo {
    fn run(&mut self, _stamp: EpochStamp, effect: FeedEffect, _back: &Back<Feed>) {
        if let FeedEffect::Drained(drained) = effect {
            self.wiring.to_lifecycle(LifecycleInput::Drained(drained));
        }
    }
}

pub(super) struct ReadAt {
    pub windows: Address<Windows>,
    pub deliveries: Address<Deliveries>,
    pub feed: Address<Feed>,
}

fn drained(wiring: &Wiring, drained: Drained) { wiring.to_lifecycle(LifecycleInput::Drained(drained)); }

/// Adds the owners at the indexes `base`, `base + 1`, `base + 2`; each owner holds the addresses of the others.
pub(super) fn add(builder: &mut RuntimeBuilder, wiring: &Arc<Wiring>, ports: &Ports, store: Address<StoreWriter>, base: usize) -> ReadAt {
    let (windows_at, deliveries_at, feed_at) = (Address::<Windows>::new(base), Address::<Deliveries>::new(base + 1), Address::<Feed>::new(base + 2));
    let db: Db = ports.db.clone();
    let handle = builder.handle();
    let transport: Arc<dyn Transport> = ports.transport.clone().unwrap_or_else(|| Arc::new(Unconnected));
    let admit_db = db.clone();
    let admit: crate::owners::read::Admit = Arc::new(move || admit_db.admit().ok());
    let codec = Arc::new(SchemaWindowCodec::new(db.clone(), ports.read_operations.clone().unwrap_or_else(|| Arc::new(|_| None))));

    let preload = preload_keys(&db, false).unwrap_or_else(|error| {
        wiring.fault(format!("the preload of the schema is invalid: {error}"));
        Vec::new()
    });
    let state = WindowsState::new(0, None, WindowsPeers { store, deliveries: deliveries_at }).with_preload(preload).with_sources(preload_sources(&db)).with_max_pages(max_pages(db.clone())).with_foreground(refreshes_on_foreground(db.clone()));
    let windows_wiring = wiring.clone();
    let (derive_db, derive_executor, derive_handle) = (db.clone(), ports.executor.clone(), handle.clone());
    let windows_io = WindowIo::new(
        transport,
        codec,
        admit,
        ports.executor.clone(),
        Box::new(move |_stamp, effect| match effect {
            WindowsEffect::Window { effect: WindowEffect::Outcome { id, outcome }, .. } if id & CALL != 0 => {
                windows_wiring.outcomes.settle(OutcomeId::from_raw(id & !CALL), LifeReply::Window(outcome));
            }
            WindowsEffect::Window { effect: WindowEffect::DerivePreload { .. }, .. } => {
                let (db, handle, wiring) = (derive_db.clone(), derive_handle.clone(), windows_wiring.clone());
                crate::task::spawn_blocking_on(crate::task::Carry::Detached, &derive_executor, move || match preload_keys(&db, true) {
                    Ok(keys) => handle.post(windows_at, EpochStamp::Control, WindowsInput::PreloadKeys { keys }),
                    Err(error) => wiring.fault(format!("the derived preload is invalid: {error}")),
                });
            }
            WindowsEffect::Drained(done) => drained(&windows_wiring, done),
            WindowsEffect::Window { .. } => {}
        }),
    );
    let windows = builder.add::<Windows>("windows", state, windows_io);

    let deliveries_wiring = wiring.clone();
    let delivery_io = DeliveryIo::new(
        Arc::new(DbSnapshots::new(db.clone())),
        wiring.pulls.clone(),
        ports.executor.clone(),
        Box::new(move |_stamp, effect| match effect {
            DeliveryEffect::Stream { stream, effect: StreamEffect::CloseSink } => deliveries_wiring.to_deliveries(DeliveryInput::SinkClosed { stream }),
            DeliveryEffect::Stream { effect: StreamEffect::Cancelled { reply }, .. } => {
                deliveries_wiring.outcomes.settle(OutcomeId::from_raw(reply), LifeReply::Cancelled);
            }
            DeliveryEffect::Drained(done) => drained(&deliveries_wiring, done),
            DeliveryEffect::Stream { .. } => {}
        }),
    );
    let deliveries = builder.add::<Deliveries>("deliveries", DeliveriesState::new(0, DeliveryPeers { windows: windows_at, feed: feed_at }), delivery_io);
    let feed = builder.add::<Feed>("feed", FeedState::new(0, false, FeedPeers { store, windows: windows_at, deliveries: deliveries_at }), FeedWithoutIo { wiring: wiring.clone() });
    debug_assert!(windows.index() == base && deliveries.index() == base + 1 && feed.index() == base + 2);

    let reply_wiring = wiring.clone();
    wiring.on_store_reply(BatchAuthor::Window, move |batch, reply| {
        reply_wiring.to_windows(match reply {
            StoreReply::Committed { revision } => WindowsInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), models: Models::default() }),
            StoreReply::Rejected(reason) => WindowsInput::Rejected(Rejected { batch, reason }),
        });
    });
    let commit_wiring = wiring.clone();
    db.on_commit(move |notice| {
        let models = if notice.all { Models::All } else { Models::Set(Default::default()) };
        commit_wiring.to_deliveries(DeliveryInput::Committed(Committed { batch: 0, revision: notice.revision, tables: notice.tables.clone(), models }));
        commit_wiring.to_windows(WindowsInput::StoreCommitted { models: notice.models.clone(), revision: notice.revision });
    });
    ReadAt { windows, deliveries, feed }
}

impl Addresses {
    pub(super) fn with_read(mut self, read: &ReadAt) -> Self {
        self.windows = Some(read.windows);
        self.deliveries = Some(read.deliveries);
        self.feed = Some(read.feed);
        self
    }
}

impl super::Life {
    /// A platform call that waits for a load of `key`: the waiter is the id of the call in `Windows`.
    pub fn load(&self, key: WindowKey, goal: Goal, force: bool, store_revision: i64) -> (Waiter<LifeReply>, u64) {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        let id = waiter.id().raw() | CALL;
        if self.is_shut() {
            self.wiring.outcomes.settle(waiter.id(), LifeReply::Closed);
        } else {
            self.wiring.to_windows(WindowsInput::Load { key, id, goal, force, store_revision });
        }
        (waiter, id)
    }

    /// The caller of `id` gives up: the window decides whether the request goes on for the others.
    pub fn leave_load(&self, key: WindowKey, id: u64) { self.wiring.to_windows(WindowsInput::InterestMinus { key, id }); }

    pub fn next_stream_id(&self) -> StreamId { self.wiring.next_stream.fetch_add(1, std::sync::atomic::Ordering::SeqCst) }

    pub fn open_stream(&self, stream: StreamId, view: View, epoch: Epoch, revision: i64, valid: bool) {
        let identity = self.published().identity.is_some();
        self.wiring.to_deliveries(DeliveryInput::Open { stream, view, epoch, revision, identity, valid });
    }

    /// The next value of a stream: the answer comes with the next value of the journal.
    pub fn pull(&self, stream: StreamId) -> Waiter<PullAnswer> {
        let waiter = self.wiring.pulls.register(Epoch::MAX);
        if self.is_shut() {
            self.wiring.pulls.settle(waiter.id(), PullAnswer::Done);
        } else {
            self.wiring.to_deliveries(DeliveryInput::Pull { stream, outcome: waiter.id() });
        }
        waiter
    }

    /// `cancel` of a stream returns once no callback of its sink can run.
    pub fn cancel_stream(&self, stream: StreamId) -> Waiter<LifeReply> {
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        if self.is_shut() {
            self.wiring.outcomes.settle(waiter.id(), LifeReply::Cancelled);
        } else {
            self.wiring.to_deliveries(DeliveryInput::Cancel { stream, reply: waiter.id().raw() });
        }
        waiter
    }

    /// The last handle of the stream is gone.
    pub fn release_stream(&self, stream: StreamId) { self.wiring.to_deliveries(DeliveryInput::Release { stream }); }
}
