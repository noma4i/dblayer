use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::db::dispatch::OrderedEffects;
use crate::db::locked;
use crate::identity::RowId;
use crate::lifecycle::{Holder, Lifecycle, Registration, Snapshot};
use crate::query::{Query, Where};
use crate::sync::LoadOptions;
use super::live::{LiveChange, LivePhase, LoadMoreResult, Placement, QueryListener, Replacement};
use super::wire::WireRow;
use super::{Database, DbError, LiveSubscription};

/// What a stream reads. Both kinds deliver rows as values; an entity is a query that finds one row.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum StreamRequest {
    /// The JSON of a query: model, filters, order, and for a server relation its name and parameters.
    Query { query_json: String },
    /// One row by id.
    Entity { model: String, id: String },
}

/// The loading state that goes with a value. It is the state of the whole stream, not of one row.
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct StreamState {
    pub phase: LivePhase,
    pub initial: bool,
    pub refreshing: bool,
    pub fetching_more: bool,
    pub has_more: bool,
    pub complete: bool,
    pub total: Option<i64>,
    pub loaded_at: Option<i64>,
    pub error: Option<DbError>,
    pub error_kind: Option<String>,
}

impl StreamState {
    fn of(change: &LiveChange) -> Self {
        Self {
            phase: change.phase.unwrap_or(LivePhase::NotLoaded), initial: change.initial, refreshing: change.refreshing, fetching_more: change.fetching_more, has_more: change.has_more,
            complete: change.complete, total: change.total, loaded_at: change.loaded_at, error: change.error.clone(), error_kind: change.error_kind.clone(),
        }
    }
}

/// Why a stream ended on its own. A stream the reader cancels ends without a delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum StreamEnd {
    /// The session of its epoch ended, or the reader asked for an epoch that had gone.
    SessionEnded,
    /// The database closed.
    Closed,
    /// The query is not valid. The state of the last delivery carries the error.
    Invalid,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum StreamValue {
    /// The whole result at a revision. The first value of a stream, and the answer to a missed revision.
    Snapshot { revision: i64, rows: Vec<WireRow>, state: StreamState },
    /// The rows that changed since `base`, keyed by id. `rows` are the inserted, updated and replaced rows with their final index in `row_indexes`.
    /// Apply in this order: rename every `replaced.from` to its `to`, drop `removed`, take every `placed` id out and insert the placed rows in ascending index, write every row at its index. The result has `size` rows.
    Delta {
        base: i64,
        revision: i64,
        size: u32,
        rows: Vec<WireRow>,
        row_indexes: Vec<u32>,
        removed: Vec<String>,
        placed: Vec<Placement>,
        replaced: Vec<Replacement>,
        renamed: Vec<Replacement>,
        state: StreamState,
    },
    /// The state changed and the rows did not.
    State { state: StreamState },
    /// The last value of a stream that ended by itself. Exactly one per stream.
    Ended { reason: StreamEnd },
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct StreamDelivery {
    pub stream: u64,
    /// The session epoch the stream was opened in. A delivery whose epoch the reader does not hold belongs to an account that has gone.
    pub epoch: u64,
    /// Counts the deliveries of this stream from 1, with no gap.
    pub seq: u64,
    pub value: StreamValue,
}

#[uniffi::export(rust, foreign)]
pub trait ValueSink: Send + Sync {
    fn delivered(&self, delivery: StreamDelivery);
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StreamCensus {
    pub open: u64,
}

struct Flow {
    seq: u64,
    revision: Option<i64>,
    ended: bool,
    sink: Option<Arc<dyn ValueSink>>,
}

impl Drop for Flow {
    fn drop(&mut self) { crate::db::lock::assert_no_library_lock("Flow::drop"); }
}

/// The core side of one reader: it turns the feed of a query into values and ends with the epoch it was opened in.
pub(super) struct Stream {
    id: u64,
    epoch: u64,
    life: Arc<Lifecycle>,
    table: Weak<StreamTable>,
    flow: Mutex<Flow>,
    outbox: OrderedEffects,
    subscription: OnceLock<Weak<LiveSubscription>>,
    registration: OnceLock<Registration>,
    pub(super) request: Request,
}

/// What a call on a stream needs to name its server side.
#[derive(Clone, Debug)]
pub(super) struct Request {
    pub(super) model: String,
    pub(super) relation: Option<String>,
    pub(super) params_json: String,
    pub(super) entity: Option<String>,
}

struct Open {
    stream: Arc<Stream>,
    subscription: Arc<LiveSubscription>,
}

#[derive(Default)]
pub(super) struct StreamTable {
    next: AtomicU64,
    open: Mutex<HashMap<u64, Open>>,
}

impl StreamTable {
    fn issue(&self) -> u64 { self.next.fetch_add(1, Ordering::SeqCst) + 1 }

    fn remove(&self, id: u64) -> Option<Open> { locked(&self.open).remove(&id) }

    fn find(&self, id: u64) -> Option<Arc<Stream>> { locked(&self.open).get(&id).map(|open| open.stream.clone()) }

    fn count(&self) -> usize { locked(&self.open).len() }
}

impl Stream {
    fn new(id: u64, epoch: u64, life: Arc<Lifecycle>, table: &Arc<StreamTable>, sink: Arc<dyn ValueSink>, request: Request) -> Arc<Self> {
        Arc::new(Self {
            id, epoch, life, table: Arc::downgrade(table), flow: Mutex::new(Flow { seq: 0, revision: None, ended: false, sink: Some(sink) }), outbox: OrderedEffects::default(),
            subscription: OnceLock::new(), registration: OnceLock::new(), request,
        })
    }

    pub(super) fn epoch(&self) -> u64 { self.epoch }

    fn holds(&self, snapshot: &Snapshot) -> bool { !snapshot.closed && snapshot.session_epoch == self.epoch }

    /// Queues one delivery and runs the queue outside every library lock.
    fn announce(&self, make: impl FnOnce(&mut Flow) -> Option<(StreamValue, bool)>) {
        self.outbox.under(&self.flow, |flow, effects| {
            if flow.ended { return; }
            let Some((value, last)) = make(flow) else { return };
            flow.seq += 1;
            let Some(sink) = flow.sink.clone() else { return };
            let delivery = StreamDelivery { stream: self.id, epoch: self.epoch, seq: flow.seq, value };
            if last { flow.ended = true; flow.sink = None; }
            effects.push(move || {
                crate::db::lock::assert_no_library_lock("ValueSink::delivered");
                sink.delivered(delivery);
            });
        });
    }

    /// Ends the stream with its one outcome and frees what it holds.
    fn end(&self, reason: StreamEnd) {
        self.announce(|flow| {
            if flow.ended { return None; }
            Some((StreamValue::Ended { reason }, true))
        });
        self.release();
    }

    /// The reader asked to stop: nothing is delivered after this.
    fn cancelled(&self) {
        locked(&self.flow).ended = true;
        let sink = locked(&self.flow).sink.take();
        drop(sink);
        self.release();
    }

    fn release(&self) {
        let open = self.table.upgrade().and_then(|table| table.remove(self.id));
        if let Some(registration) = self.registration.get() { registration.remove(); }
        if let Some(open) = open { open.subscription.cancel(); }
        else if let Some(subscription) = self.subscription.get().and_then(Weak::upgrade) { subscription.cancel(); }
    }

    fn resync(&self) {
        if let Some(subscription) = self.subscription.get().and_then(Weak::upgrade) { subscription.report_desync(); }
    }

    fn receive(&self, change: LiveChange) {
        if !self.holds(&self.life.snapshot()) {
            let closed = self.life.snapshot().closed;
            self.end(if closed { StreamEnd::Closed } else { StreamEnd::SessionEnded });
            return;
        }
        if change.phase == Some(LivePhase::Closed) {
            self.end(StreamEnd::Closed);
            return;
        }
        let state = StreamState::of(&change);
        let rows_moved = change.full || !(change.rows.is_empty() && change.removed.is_empty() && change.placed.is_empty() && change.replaced.is_empty() && change.renamed.is_empty());
        let mut orphan = false;
        self.announce(|flow| {
            if !rows_moved { return Some((StreamValue::State { state }, false)); }
            if change.full {
                flow.revision = Some(change.revision);
                return Some((StreamValue::Snapshot { revision: change.revision, rows: change.rows, state }, false));
            }
            let Some(base) = flow.revision else {
                orphan = true;
                return None;
            };
            flow.revision = Some(change.revision);
            Some((StreamValue::Delta {
                base, revision: change.revision, size: change.size, rows: change.rows, row_indexes: change.row_indexes, removed: change.removed, placed: change.placed, replaced: change.replaced, renamed: change.renamed, state,
            }, false))
        });
        if orphan { self.resync(); }
    }
}

impl QueryListener for Stream {
    fn changed(&self, change: LiveChange) { self.receive(change); }
}

impl Holder for Stream {
    fn close(&self) { self.end(StreamEnd::Closed); }
    fn registration(&self) -> &OnceLock<Registration> { &self.registration }
    fn changed(&self, snapshot: &Snapshot) {
        if !self.holds(snapshot) { self.end(if snapshot.closed { StreamEnd::Closed } else { StreamEnd::SessionEnded }); }
    }
}

fn call_of(request: &StreamRequest) -> Result<Request, DbError> {
    match request {
        StreamRequest::Entity { model, id } => Ok(Request { model: model.clone(), relation: None, params_json: "{}".into(), entity: Some(id.clone()) }),
        StreamRequest::Query { query_json } => {
            let description: serde_json::Value = serde_json::from_str(query_json)?;
            let model = description["model"].as_str().ok_or_else(|| DbError::Invalid { detail: "a query names its model".into() })?.to_string();
            let relation = description["relation"].as_str().map(str::to_string);
            let params_json = description.get("params").cloned().unwrap_or_else(|| serde_json::json!({})).to_string();
            Ok(Request { model, relation, params_json, entity: None })
        }
    }
}

#[uniffi::export]
impl Database {
    /// The epoch of the session now. A reader that opens streams for an account asks for it once, after the sign-in, and hands it to every stream and command of that account.
    pub fn session_epoch(&self) -> u64 { self.streams.snapshot().session_epoch }

    /// Opens a stream and returns its number at once. The first value arrives as a snapshot, then deltas follow. A stream opened for an epoch that has gone ends with one `Ended` value.
    pub fn subscribe(&self, epoch: u64, request: StreamRequest, sink: Arc<dyn ValueSink>) -> u64 {
        let id = self.stream_table.issue();
        let call = match call_of(&request) {
            Ok(call) => call,
            Err(error) => return self.refuse(id, epoch, sink, error),
        };
        let stream = Stream::new(id, epoch, self.streams.clone(), &self.stream_table, sink, call);
        let admitted = self.streams.enroll(&stream);
        if !stream.holds(&admitted) {
            stream.end(if admitted.closed { StreamEnd::Closed } else { StreamEnd::SessionEnded });
            return id;
        }
        let subscription = self.subscribed(stream.clone(), |feed| match &request {
            StreamRequest::Query { query_json } => self.connect_json(query_json, false, None, feed),
            StreamRequest::Entity { model, id } => {
                let row = RowId::new(id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
                self.connect(Query::new(model).filtered(Where::equal("id", serde_json::json!(row))), None, false, None, LoadOptions::default(), feed)
            }
        });
        let _ = stream.subscription.set(Arc::downgrade(&subscription));
        locked(&self.stream_table.open).insert(id, Open { stream: stream.clone(), subscription });
        if locked(&stream.flow).ended { stream.release(); }
        id
    }

    /// The reader stopped. The stream ends in the core and nothing is delivered after this call returns.
    pub fn cancel_stream(&self, stream: u64) {
        if let Some(open) = self.stream_table.find(stream) { open.cancelled(); }
    }

    /// The reader could not apply a delta (its revision is not the delta's base). The core sends one new snapshot.
    pub fn resync_stream(&self, stream: u64) {
        if let Some(open) = self.stream_table.find(stream) { open.resync(); }
    }

    pub fn stream_census(&self) -> StreamCensus { StreamCensus { open: self.stream_table.count() as u64 } }

    /// Loads the relation of the stream again from the server. The answer belongs to the epoch of the stream: it ends as `Interrupted` when the session changed.
    pub async fn refresh_stream(&self, stream: u64) -> Result<(), DbError> {
        let (open, lease) = self.stream_call(stream, "refresh a stream")?;
        let sync = self.sync()?.clone();
        let request = open.request.clone();
        let work = super::database::on_executor(async move {
            let params = serde_json::from_str(&request.params_json).map_err(DbError::from)?;
            match (&request.relation, &request.entity) {
                (Some(relation), _) => sync.refresh(&request.model, relation, &params).await.map(|_| ()).map_err(DbError::from),
                (None, Some(id)) => sync.load_entity(&request.model, id).await.map_err(DbError::from),
                (None, None) => Err(DbError::Invalid { detail: "the stream reads local rows and has nothing to refresh".into() }),
            }
        });
        lease.run(work).await.map_err(|_| stale())?
    }

    /// Loads the next page of the relation of the stream.
    pub async fn load_more_stream(&self, stream: u64) -> Result<LoadMoreResult, DbError> {
        let (open, lease) = self.stream_call(stream, "load more of a stream")?;
        let sync = self.sync()?.clone();
        let request = open.request.clone();
        let Some(relation) = request.relation.clone() else { return Err(DbError::Invalid { detail: "the stream has no relation to page".into() }) };
        let work = super::database::on_executor(async move {
            let params = serde_json::from_str(&request.params_json).map_err(DbError::from)?;
            let outcome = sync.load_more(&request.model, &relation, &params).await.map_err(DbError::from)?;
            Ok::<_, DbError>(match outcome.status {
                crate::sync::LoadStatus::NoMorePages(crate::read::LoadMoreVerdict::AlreadyLoading) => LoadMoreResult::AlreadyLoading,
                crate::sync::LoadStatus::NoMorePages(_) => LoadMoreResult::NoMore,
                _ => LoadMoreResult::Loaded { has_more: outcome.has_next },
            })
        });
        lease.run(work).await.map_err(|_| stale())?
    }

    /// Returns when the first load of the stream settled. Fails when the epoch of the stream ended first.
    pub async fn stream_loaded(&self, stream: u64) -> Result<(), DbError> {
        let subscription = self.stream_table.subscription(stream).ok_or_else(stale)?;
        subscription.wait_for_initial_load().await
    }
}

fn stale() -> DbError { DbError::Interrupted { detail: "the session of this stream ended".into() } }

impl StreamTable {
    fn subscription(&self, id: u64) -> Option<Arc<LiveSubscription>> { locked(&self.open).get(&id).map(|open| open.subscription.clone()) }
}

impl Database {
    fn refuse(&self, id: u64, epoch: u64, sink: Arc<dyn ValueSink>, error: DbError) -> u64 {
        let stream = Stream::new(id, epoch, self.streams.clone(), &self.stream_table, sink, Request { model: String::new(), relation: None, params_json: "{}".into(), entity: None });
        let mut change = LiveChange::empty(0);
        change.error = Some(error);
        change.error_kind = Some("invalid".into());
        change.phase = Some(LivePhase::Failed);
        stream.announce(|_| Some((StreamValue::State { state: StreamState::of(&change) }, false)));
        stream.end(StreamEnd::Invalid);
        id
    }

    fn stream_call(&self, stream: u64, target: &'static str) -> Result<(Arc<Stream>, crate::lifecycle::Lease), DbError> {
        let open = self.stream_table.find(stream).ok_or_else(stale)?;
        let lease = self.streams.lease(crate::lifecycle::Origin::Session, target).map_err(|_| stale())?;
        if lease.epoch() != open.epoch() { return Err(stale()); }
        Ok((open, lease))
    }
}
