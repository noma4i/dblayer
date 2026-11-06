//! Install of the owners of reading: `Windows`, `Deliveries` and `Feed` with the executors of their effects.
//! An executor does IO and returns the result as an input; the platform side settles its waiters through `Outcomes`.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use super::{Addresses, LifeReply, Ports, Wiring};
use crate::db::Db;
use crate::owners::contract::{BatchAuthor, Committed, Drained, Models, Rejected};
use crate::owners::lifecycle::{LifecycleInput, Peer};
use crate::owners::network::NetworkInput;
use crate::owners::read::deliveries::{DeliveriesState, Deliveries, DeliveryEffect, DeliveryInput, DeliveryPeers, PullAnswer, StreamEffect, StreamId, View};
use crate::owners::read::feed::{Feed, FeedEffect, FeedInput, FeedPeers, FeedState, FramedEvent};
use crate::owners::read::windows::{Goal, Windows, WindowEffect, WindowKey, WindowsEffect, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::read::{max_pages, preload_keys, preload_sources, refreshes_on_foreground, DbSnapshots, DeliveryIo, FeedCodec, FeedIo, ReadOperations, Rest, SchemaFeedCodec, SchemaWindowCodec, WindowIo};
use crate::owners::store::{StoreReply, StoreWriter};
use crate::runtime::{Address, Back, EffectRunner, Epoch, EpochStamp, OutcomeId, RuntimeBuilder, Waiter};
use crate::transport::{GraphQLRequest, Response, Transport};

/// The ids of waiters that platform calls register in `Windows` carry this bit, the ids of streams stay below it.
pub(crate) const CALL: u64 = 1 << 62;

/// Without a cable (a database without a session) `Feed` runs no connection: the owner exists for its address and ends with the database.
struct FeedWithoutIo {
    rest: Rest<FeedEffect>,
}

impl EffectRunner<Feed> for FeedWithoutIo {
    fn run(&mut self, stamp: EpochStamp, effect: FeedEffect, _back: &Back<Feed>) { (self.rest)(stamp, effect); }
}

/// The request and the events of the subscription come from the schema; the operation of the subscription is registered after the open.
struct LazyFeedCodec {
    db: Db,
    operations: ReadOperations,
    wiring: Arc<Wiring>,
    built: OnceLock<Arc<SchemaFeedCodec>>,
}

impl LazyFeedCodec {
    fn codec(&self) -> Option<&Arc<SchemaFeedCodec>> {
        if let Some(codec) = self.built.get() { return Some(codec); }
        match SchemaFeedCodec::new(self.db.clone(), self.operations.clone()) {
            Ok(codec) => Some(self.built.get_or_init(|| Arc::new(codec))),
            Err(error) => {
                self.wiring.fault(format!("the subscription could not be built: {error}"));
                None
            }
        }
    }
}

impl FeedCodec for LazyFeedCodec {
    fn request(&self, models: &[String]) -> GraphQLRequest {
        self.codec().map_or_else(|| GraphQLRequest::new("", "", serde_json::json!({})), |codec| codec.request(models))
    }

    fn events(&self, response: Response) -> Vec<FramedEvent> { self.codec().map(|codec| codec.events(response)).unwrap_or_default() }
}

/// `updates.debounce` of each model, in milliseconds.
fn debounce_of(db: &Db) -> BTreeMap<String, u64> {
    db.shared.specs.values().filter_map(|spec| {
        let declared = spec.relations.iter().flat_map(|relation| &relation.updates).chain(&spec.listens);
        let longest = declared.filter_map(|updates| updates.debounce.as_deref().and_then(crate::pacing::parse_duration)).max()?;
        Some((spec.key.to_string(), u64::try_from(longest.as_millis()).unwrap_or(u64::MAX)))
    }).collect()
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
    let transport: Option<Arc<dyn Transport>> = ports.transport.clone();
    let admit_db = db.clone();
    let admit: crate::owners::read::Admit = Arc::new(move |epoch| admit_db.admit_at(epoch).map_err(|error| crate::owners::read::Refused(error.to_string())));
    let codec = Arc::new(SchemaWindowCodec::new(db.clone(), ports.read_operations.clone().unwrap_or_default()));

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
        Box::new(move |stamp, effect| match effect {
            WindowsEffect::Window { effect: WindowEffect::Outcome { id, outcome }, .. } if id & CALL != 0 => {
                windows_wiring.outcomes.settle(OutcomeId::from_raw(id & !CALL), LifeReply::Window(outcome));
            }
            WindowsEffect::Window { effect: WindowEffect::DerivePreload { .. }, .. } => {
                let (db, handle, wiring) = (derive_db.clone(), derive_handle.clone(), windows_wiring.clone());
                let launch = derive_handle.launch();
                crate::task::spawn_blocking_on(crate::task::Carry::Detached, &derive_executor, move || { let _launch = launch; match preload_keys(&db, true) {
                    Ok(keys) => handle.post(windows_at, stamp, WindowsInput::PreloadKeys { keys }),
                    Err(error) => wiring.fault(format!("the derived preload is invalid: {error}")),
                } });
            }
            WindowsEffect::SnapshotRequest { epoch } => windows_wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Windows, epoch }),
            WindowsEffect::Drained(done) => drained(&windows_wiring, done),
            WindowsEffect::Window { .. } => {}
        }),
    );
    let windows = builder.add::<Windows>("windows", state, windows_io.counted_by(handle.clone()));

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
    ).counted_by(handle.clone());
    let deliveries = builder.add::<Deliveries>("deliveries", DeliveriesState::new(0, DeliveryPeers { windows: windows_at, feed: feed_at }), delivery_io);
    let feed_wiring = wiring.clone();
    let feed_rest: Rest<FeedEffect> = Box::new(move |_stamp, effect| match effect {
        FeedEffect::Drained(done) => drained(&feed_wiring, done),
        FeedEffect::SnapshotRequest { epoch } => feed_wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Feed, epoch }),
        _ => {}
    });
    let feed_state = FeedState::new(0, FeedPeers { store, windows: windows_at, deliveries: deliveries_at }, debounce_of(&db));
    let feed = match &ports.cable {
        Some(lines) => {
            let codec = Arc::new(LazyFeedCodec { db: db.clone(), operations: ports.read_operations.clone().unwrap_or_default(), wiring: wiring.clone(), built: OnceLock::new() });
            builder.add::<Feed>("feed", feed_state, FeedIo::new(lines.clone(), codec, ports.executor.clone(), feed_rest).counted_by(handle.clone()))
        }
        None => builder.add::<Feed>("feed", feed_state, FeedWithoutIo { rest: feed_rest }),
    };
    debug_assert!(windows.index() == base && deliveries.index() == base + 1 && feed.index() == base + 2);

    let reply_wiring = wiring.clone();
    wiring.on_store_reply(BatchAuthor::Window, move |batch, reply| {
        reply_wiring.to_windows(match reply {
            StoreReply::Committed { revision } => WindowsInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), models: Models::default() }),
            StoreReply::Rejected(reason) => WindowsInput::Rejected(Rejected { batch, reason }),
        });
    });
    let feed_reply = wiring.clone();
    wiring.on_store_reply(BatchAuthor::Feed, move |batch, reply| {
        feed_reply.to_feed(match reply {
            StoreReply::Committed { revision } => FeedInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), models: Models::default() }),
            StoreReply::Rejected(reason) => FeedInput::Rejected(Rejected { batch, reason }),
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
    pub fn load(&self, epoch: Epoch, key: WindowKey, goal: Goal, force: bool, store_revision: i64) -> (Waiter<LifeReply>, u64) {
        let waiter = self.wiring.outcomes.register(epoch);
        let id = waiter.id().raw() | CALL;
        if self.is_shut() {
            self.wiring.outcomes.settle(waiter.id(), LifeReply::Closed);
        } else {
            self.wiring.to_windows_in(epoch, WindowsInput::Load { key, id, goal, force, store_revision });
        }
        (waiter, id)
    }

    /// The keys of the schema declared `preload` start loading once their operations are registered.
    pub fn preload(&self, keys: Vec<WindowKey>) { self.wiring.to_windows(WindowsInput::PreloadKeys { keys }); }

    /// The window of a stream that is about to open outlived its `stale_time`: Windows loads it again for the new interest.
    pub fn stale_window(&self, key: WindowKey) {
        let model = key.model.clone();
        self.wiring.to_windows(WindowsInput::Stale { model, keys: crate::owners::read::windows::StaleKeys::Set(std::collections::BTreeSet::from([key])) });
    }

    /// The platform calls that wait for an outcome of `Windows` now.
    #[cfg(test)]
    pub fn waiting_calls(&self) -> usize { self.wiring.outcomes.waiting() }

    /// A call of the platform was refused at once because the network is away: the window shows the error.
    pub fn offline_refused(&self, key: WindowKey, message: String) { self.wiring.to_windows(WindowsInput::OfflineRefused { key, message }); }

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
        if self.is_shut() || self.published().closed {
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

    /// The transfer a stream of kind `Blob` follows changed its status.
    pub fn push_status(&self, stream: StreamId, blob: crate::owners::read::deliveries::BlobStatus) { self.wiring.to_deliveries(DeliveryInput::TransferStatus { stream, blob }); }

    /// The last handle of the stream is gone.
    pub fn release_stream(&self, stream: StreamId) { self.wiring.to_deliveries(DeliveryInput::Release { stream }); }
}
