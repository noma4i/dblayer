//! Install of the owners of reading: `Windows`, `Deliveries` and `Feed` with the executors of their effects.
//! An executor does IO and returns the result as an input; the platform side settles its waiters through `Outcomes`.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::{Addresses, LifeReply, Ports, Wiring};
use crate::store::Store;
use crate::owners::contract::{BatchAuthor, Committed, Drained, Models, Rejected};
use crate::owners::lifecycle::{LifecycleInput, Peer};
use crate::owners::network::NetworkInput;
use crate::owners::read::deliveries::{BlobStatus, DeliveriesState, Deliveries, DeliveryEffect, DeliveryInput, DeliveryPeers, PullAnswer, StreamEffect, StreamId, View};
use crate::owners::read::feed::{Feed, FeedEffect, FeedInput, FeedPeers, FeedState};
use crate::owners::read::windows::{Goal, Windows, WindowEffect, WindowKey, WindowsEffect, WindowsInput, WindowsPeers, WindowsState};
use crate::owners::read::{channels_of, has_media, lifetimes, max_pages, members_complete, preload_keys, preload_sources, refreshes_on_foreground, DbSnapshots, DeliveryIo, FeedIo, Rest, SchemaFeedCodec, SchemaWindowCodec, WindowIo};
use crate::owners::store::{StoreReply, StoreWriter};
use crate::owners::write::transfer::{Shown, TransferInput};
use crate::runtime::{Address, Back, EffectRunner, Epoch, EpochStamp, Outcome, OutcomeId, RuntimeBuilder, Waiter};

/// The ids of waiters that platform calls register in `Windows` carry this bit, the ids of streams stay below it.
pub(crate) const CALL: u64 = 1 << 62;

/// Without a cable (a database without a session) `Feed` runs no connection: the owner exists for its address and ends with the database.
struct FeedWithoutIo {
    rest: Rest<FeedEffect>,
}

impl EffectRunner<Feed> for FeedWithoutIo {
    fn run(&mut self, stamp: EpochStamp, effect: FeedEffect, _back: &Back<Feed>) { (self.rest)(stamp, effect); }
}

/// `updates.debounce` of each model, in milliseconds.
fn debounce_of(db: &Store) -> BTreeMap<String, u64> {
    db.specs().values().filter_map(|spec| {
        let declared = spec.relations.iter().flat_map(|relation| &relation.updates).chain(&spec.listens);
        let longest = declared.filter_map(|updates| updates.debounce.as_deref().and_then(crate::runtime::parse_duration)).max()?;
        Some((spec.key.to_string(), u64::try_from(longest.as_millis()).unwrap_or(u64::MAX)))
    }).collect()
}

pub(super) struct ReadAt {
    pub windows: Address<Windows>,
    pub deliveries: Address<Deliveries>,
    pub feed: Address<Feed>,
}

fn drained(wiring: &Wiring, drained: Drained) { wiring.to_lifecycle(LifecycleInput::Drained(drained)); }

pub(super) fn add(builder: &mut RuntimeBuilder, wiring: &Arc<Wiring>, ports: &Ports, store: Address<StoreWriter>) -> ReadAt {
    let windows_at = builder.reserve::<Windows>();
    let deliveries_at = builder.reserve::<Deliveries>();
    let feed_at = builder.reserve::<Feed>();
    let db: std::sync::Arc<Store> = ports.db.clone();
    let handle = builder.handle();
    let admit_wiring = wiring.clone();
    let admit: crate::owners::read::Admit = Arc::new(move |epoch| if admit_wiring.admits_session(epoch) { Ok(()) } else { Err(crate::owners::read::Refused("the epoch of the request ended".into())) });
    let feed_admit = admit.clone();
    let codec = Arc::new(SchemaWindowCodec::new(db.clone(), ports.read_operations.clone().unwrap_or_default()));

    let preload = preload_keys(&db, false).unwrap_or_else(|error| {
        wiring.fault(format!("the preload of the schema is invalid: {error}"));
        Vec::new()
    });
    let state = WindowsState::new(0, None, WindowsPeers { store, deliveries: deliveries_at }).with_preload(preload).with_operations_awaited().with_sources(preload_sources(&db)).with_lifetimes(lifetimes(db.clone())).with_max_pages(max_pages(db.clone())).with_members_complete(members_complete(db.clone())).with_media(has_media(db.clone())).with_foreground(refreshes_on_foreground(db.clone()));
    let windows_wiring = wiring.clone();
    let (derive_db, derive_executor, derive_handle) = (db.clone(), ports.executor.clone(), handle.clone());
    let read_db = db.clone();
    let windows_io = WindowIo::new(
        codec,
        (admit, wiring.gate.clone()),
        (ports.executor.clone(), wiring.now.clone()),
        Box::new(move |stamp, effect| match effect {
            WindowsEffect::Window { effect: WindowEffect::Outcome { id, outcome }, .. } if id & CALL != 0 => {
                windows_wiring.outcomes.settle(OutcomeId::from_raw(id & !CALL), LifeReply::Window(Box::new(outcome)));
            }
            WindowsEffect::Window { effect: WindowEffect::DerivePreload { .. }, .. } => {
                let (db, handle, wiring) = (derive_db.clone(), derive_handle.clone(), windows_wiring.clone());
                let launch = derive_handle.launch();
                crate::task::spawn_blocking_on(&derive_executor, Some(launch), move || { match preload_keys(&db, true) {
                    Ok(keys) => { let now = db.lock().now_ms(); handle.post(windows_at, stamp, WindowsInput::PreloadKeys { keys, now }) }
                    Err(error) => wiring.fault(format!("the derived preload is invalid: {error}")),
                } });
            }
            WindowsEffect::SnapshotRequest { epoch } => windows_wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Windows, epoch }),
            WindowsEffect::Drained(done) => drained(&windows_wiring, done),
            WindowsEffect::Window { .. } | WindowsEffect::ReadWindow { .. } => {}
        }),
    );
    builder.install(windows_at, "windows", state, {
        let io = windows_io.reading(Arc::new(move |key| match key.kind() {
            crate::owners::read::windows::WindowKind::Relation { relation, params } => read_db.window_saved(&key.model, &relation, &params).map_err(|error| error.to_string()),
            crate::owners::read::windows::WindowKind::Nodes { .. } => Ok(None),
        })).counted_by(handle.clone());
        #[cfg(test)]
        let io = io.flights_in(wiring.flights.clone());
        io
    });

    let deliveries_wiring = wiring.clone();
    let delivery_io = DeliveryIo::new(
        Arc::new(DbSnapshots::new(db.clone())),
        wiring.pulls.clone(),
        ports.executor.clone(),
        Box::new(move |stamp, effect| match effect {
            DeliveryEffect::WantPlus { model, id, stream, scoped } => deliveries_wiring.to_feed_at(stamp, FeedInput::WantPlus { model, id, stream, scoped, now: crate::runtime::Time((deliveries_wiring.now)() as u64) }),
            DeliveryEffect::WantMinus { model, id } => deliveries_wiring.to_feed_at(stamp, FeedInput::WantMinus { model, id, now: crate::runtime::Time((deliveries_wiring.now)() as u64) }),
            DeliveryEffect::Stream { stream, effect: StreamEffect::CloseSink } => deliveries_wiring.to_deliveries(DeliveryInput::SinkClosed { stream }),
            DeliveryEffect::CancelAnswered { reply } | DeliveryEffect::Stream { effect: StreamEffect::Cancelled { reply }, .. } => {
                deliveries_wiring.outcomes.settle(OutcomeId::from_raw(reply), LifeReply::Cancelled);
            }
            DeliveryEffect::Stream { effect: StreamEffect::ReaderClaimed { reply, claimed }, .. } => {
                deliveries_wiring.outcomes.settle(OutcomeId::from_raw(reply), LifeReply::Reader { claimed });
            }
            DeliveryEffect::Stream { stream, effect: StreamEffect::StatusInterest { plus, key: Some(key) } } => {
                let owned = deliveries_wiring.addresses().is_some_and(|addresses| addresses.transfer.is_some());
                match (plus, owned) {
                    (true, true) => deliveries_wiring.to_transfer(TransferInput::StatusInterestPlus { key, stream_id: stream }),
                    (false, true) => deliveries_wiring.to_transfer(TransferInput::StatusInterestMinus { key, stream_id: stream }),
                    (true, false) => deliveries_wiring.to_deliveries(DeliveryInput::TransferStatus { stream, blob: BlobStatus::of(&Shown::Gone), revision: 0 }),
                    (false, false) => {}
                }
            }
            DeliveryEffect::Drained(done) => drained(&deliveries_wiring, done),
            DeliveryEffect::Census { reply, observed, retained } => {
                deliveries_wiring.outcomes.settle(OutcomeId::from_raw(reply), LifeReply::Streams { observed, retained });
            }
            DeliveryEffect::Stream { .. } => {}
        }),
    ).counted_by(handle.clone());
    builder.install(deliveries_at, "deliveries", DeliveriesState::new(0, DeliveryPeers { windows: windows_at, feed: feed_at }), delivery_io);
    let feed_wiring = wiring.clone();
    let feed_rest: Rest<FeedEffect> = Box::new(move |_stamp, effect| match effect {
        FeedEffect::Drained(done) => drained(&feed_wiring, done),
        FeedEffect::Census { reply, subscriptions, refused } => {
            feed_wiring.outcomes.settle(OutcomeId::from_raw(reply), LifeReply::Channels { subscriptions, refused });
        }
        FeedEffect::SnapshotRequest { epoch } => feed_wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Feed, epoch }),
        _ => {}
    });
    let feed_state = FeedState::new(0, FeedPeers { store, windows: windows_at, deliveries: deliveries_at }, debounce_of(&db), channels_of(&db));
    match &ports.cable {
        Some(lines) => {
            let codec = Arc::new(SchemaFeedCodec::new(db.clone(), ports.read_operations.clone().unwrap_or_default()));
            builder.install(feed_at, "feed", feed_state, FeedIo::new(lines.clone(), codec, ports.executor.clone(), feed_rest, wiring.gate.clone(), wiring.now.clone()).counted_by(handle.clone()).admitted_by(feed_admit.clone()))
        }
        None => builder.install(feed_at, "feed", feed_state, FeedWithoutIo { rest: feed_rest }),
    };

    let reply_wiring = wiring.clone();
    wiring.on_store_reply(BatchAuthor::Window, move |batch, reply| {
        reply_wiring.to_windows(match reply {
            StoreReply::Committed { revision, .. } => WindowsInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), rows: None, models: Models::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0 }),
            StoreReply::Rejected(reason) => WindowsInput::Rejected(Rejected { batch, reason }),
        });
    });
    let feed_reply = wiring.clone();
    wiring.on_store_reply(BatchAuthor::Feed, move |batch, reply| {
        feed_reply.to_feed(match reply {
            StoreReply::Committed { revision, stale_parents, .. } => FeedInput::Committed(Committed { batch, revision: i64::try_from(revision).unwrap_or(i64::MAX), tables: Vec::new(), rows: None, models: Models::default(), stale_parents, position: 0, shown: Vec::new(), files_removed: 0 }),
            StoreReply::Rejected(reason) => FeedInput::Rejected(Rejected { batch, reason }),
        });
    });
    let commit_wiring = wiring.clone();
    db.on_commit(move |notice| {
        let windows = match &notice.models { Models::All => Vec::new(), Models::Set(models) => models.iter().cloned().collect() };
        let committed = notice.for_deliveries();
        let revision = committed.revision;
        commit_wiring.to_deliveries(DeliveryInput::Committed(committed));
        commit_wiring.to_windows(WindowsInput::StoreCommitted { models: windows, revision });
    }).unwrap_or_else(|error| wiring.fault(error.to_string()));
    ReadAt { windows: windows_at, deliveries: deliveries_at, feed: feed_at }
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
    /// Takes the place of a platform call in the order of the loads: the id and the waiter of its outcome. `Windows` registers the loads in the order of their ids.
    pub fn reserve_load(&self, epoch: Epoch) -> (Waiter<LifeReply>, u64) {
        let waiter = self.wiring.outcomes.register(epoch);
        let id = waiter.id().raw() | CALL;
        if self.is_shut() {
            self.wiring.outcomes.settle(waiter.id(), LifeReply::Closed);
        } else {
            self.wiring.to_windows_in(epoch, WindowsInput::Reserve { id });
        }
        (waiter, id)
    }

    /// The call of `id` ended before its load was registered.
    pub fn unreserve_load(&self, id: u64) {
        if !self.is_shut() { self.wiring.to_windows(WindowsInput::Unreserve { id }); }
    }

    pub fn load(&self, epoch: Epoch, id: u64, key: WindowKey, (goal, force): (Goal, bool), store_revision: i64, no_wait: Option<&'static str>) {
        if self.is_shut() { return; }
        let now = (self.wiring.now)();
        self.wiring.to_windows_in(epoch, match no_wait {
            Some(verb) => WindowsInput::LoadNoWait { key, id, goal, force, store_revision, now, verb },
            None => WindowsInput::Load { key, id, goal, force, store_revision, now },
        });
    }

    /// The platform calls that wait for an outcome of `Windows` now.
    #[cfg(test)]
    pub fn waiting_calls(&self) -> usize { self.wiring.outcomes.waiting() }

    /// The caller of `id` gives up: the window decides whether the request goes on for the others.
    pub fn leave_load(&self, key: WindowKey, id: u64) { self.wiring.to_windows(WindowsInput::InterestMinus { key, id }); }

    pub fn next_stream_id(&self) -> StreamId { self.wiring.next_stream.fetch_add(1, std::sync::atomic::Ordering::SeqCst) }

    pub fn request_snapshot(&self, stream: StreamId) { self.wiring.to_deliveries(DeliveryInput::Resync { stream }); }

    pub fn open_stream(&self, stream: StreamId, view: View, epoch: Epoch, valid: bool, revision: i64) {
        let identity = self.published().identity.is_some();
        self.wiring.to_deliveries(DeliveryInput::Open { stream, view, epoch, identity, valid, now: (self.wiring.now)(), revision });
    }

    /// The next value of a stream: the answer comes with the next value of the journal.
    pub fn pull(&self, stream: StreamId, reader: u64) -> Waiter<PullAnswer> {
        let waiter = self.wiring.pulls.register(Epoch::MAX);
        if self.is_shut() || self.published().closed {
            self.wiring.pulls.settle(waiter.id(), PullAnswer::Done);
        } else {
            self.wiring.to_deliveries(DeliveryInput::Pull { stream, outcome: waiter.id(), reader });
        }
        waiter
    }

    pub fn cancel_pull(&self, stream: StreamId, outcome: OutcomeId) {
        self.wiring.to_deliveries(DeliveryInput::PullCancelled { stream, outcome });
    }

    /// The streams of Deliveries and the channels of Feed now, read from the fields of the owners: `(observed_queries, subscriptions, retained_observers)`.
    pub async fn subscription_census(&self) -> Option<(usize, usize, usize, u64)> {
        if self.is_shut() {
            return Some((0, 0, 0, 0));
        }
        let streams = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_deliveries(DeliveryInput::Census { reply: streams.id().raw() });
        let streams = streams.wait().await;
        let channels = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_feed(FeedInput::Census { reply: channels.id().raw() });
        match (streams, channels.wait().await) {
            (Outcome::Value(LifeReply::Streams { observed, retained }), Outcome::Value(LifeReply::Channels { subscriptions, refused })) => Some((observed, subscriptions, retained, refused)),
            (Outcome::Closing, _) | (_, Outcome::Closing) => Some((0, 0, 0, 0)),
            _ => None,
        }
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
    pub fn release_stream(&self, stream: StreamId) { self.wiring.to_deliveries(DeliveryInput::Release { stream, reader: None, now: crate::runtime::Time((self.wiring.now)() as u64) }); }

    /// A reader takes the stream: the first claim wins and a later one is refused.
    pub async fn claim_reader(&self, stream: StreamId, reader: u64) -> bool {
        if self.is_shut() { return false; }
        let waiter = self.wiring.outcomes.register(Epoch::MAX);
        self.wiring.to_deliveries(DeliveryInput::ClaimReader { stream, reader, reply: waiter.id().raw() });
        matches!(waiter.wait().await, Outcome::Value(LifeReply::Reader { claimed: true }))
    }

    /// The reader left the stream and its handle is still held: the interest of the stream ends.
    pub fn release_reader(&self, stream: StreamId, reader: u64) { self.wiring.to_deliveries(DeliveryInput::Release { stream, reader: Some(reader), now: crate::runtime::Time((self.wiring.now)() as u64) }); }
}
