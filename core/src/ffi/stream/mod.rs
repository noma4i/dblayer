use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::db::locked;
use crate::lifecycle::{Lease, Origin};
use super::wire::{WireModel, WireRow};
use super::{Database, DbError, WireBlobSnapshot, WireModelEvent};

mod frame;
mod open;
mod owner;
#[cfg(test)]
mod owner_tests;
#[cfg(test)]
#[path = "tape_tests.rs"]
pub(crate) mod tape;

pub use crate::sync::EntityFetch;
pub(super) use frame::Frame;
pub(super) use owner::Stream;
#[cfg(test)]
pub(super) use open::follow;

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum LivePhase {
    NotLoaded,
    Loading,
    Loaded,
    Empty,
    Failed,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum LoadMoreResult {
    Loaded { has_more: bool },
    NoMore,
    AlreadyLoading,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct Placement {
    pub id: String,
    pub index: u32,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct Replacement {
    pub from: String,
    pub to: String,
}

/// What a stream reads. Every kind delivers values: rows, a count, events or the status of an upload.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum StreamRequest {
    /// The JSON of a query: model, filters, order, and for a server relation its name and parameters.
    Query { query_json: String },
    /// The number of rows of a query.
    Count { query_json: String },
    /// The rows that an association of the rows of a query points to.
    Related { query_json: String, name: String },
    /// One row by id. `fetch` tells when the library loads the row from the server.
    Entity { model: String, id: String, fetch: EntityFetch },
    /// The events of a model: changes the server pushes and `changes` of other models.
    Events { model: String },
    /// The status of a media upload.
    Blob { id: String },
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

/// Why a stream ended on its own. A stream the reader cancels ends without a delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum StreamEnd {
    /// The session of its epoch ended, or the reader asked for an epoch that had gone.
    SessionEnded,
    /// The database closed.
    Closed,
    /// The request is not valid. The state of the last delivery carries the error.
    Invalid,
    /// The stream could not apply a delta twice in a row and a new snapshot did not help.
    OutOfSync,
}

#[derive(Clone, Debug, uniffi::Enum)]
#[allow(clippy::large_enum_variant)]
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
    /// The number of rows of a count stream at a revision.
    Count { revision: i64, count: i64, state: StreamState },
    /// The state changed and the rows did not.
    State { state: StreamState },
    /// One event of a model. Events before the stream opened are not repeated.
    Event { event: WireModelEvent },
    /// Events were dropped because the reader was slow.
    Missed { count: u64 },
    /// The status of a media upload when it changed.
    Status { blob: WireBlobSnapshot },
    /// The last value of a stream that ended by itself. Exactly one per stream.
    Ended { reason: StreamEnd },
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct StreamDelivery {
    pub stream: u64,
    /// The session epoch the stream was opened in. A delivery whose epoch the reader does not hold belongs to an account that has gone.
    pub epoch: u64,
    /// Counts the events of this stream from 1, with no gap: a `Missed` advances it by its count, every other value by 1.
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

/// The rows of a query that were read without a stream.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CachedQuery {
    pub revision: i64,
    pub rows: Vec<WireRow>,
}

/// What a call on a stream needs to name its server side.
#[derive(Clone, Debug)]
pub(in crate::ffi) struct Request {
    pub(in crate::ffi) model: String,
    pub(in crate::ffi) relation: Option<String>,
    pub(in crate::ffi) params_json: String,
    pub(in crate::ffi) entity: Option<String>,
}

#[derive(Default)]
pub(in crate::ffi) struct StreamTable {
    next: AtomicU64,
    open: Mutex<HashMap<u64, Arc<Stream>>>,
}

impl StreamTable {
    fn issue(&self) -> u64 { self.next.fetch_add(1, Ordering::SeqCst) + 1 }

    pub(super) fn remove(&self, id: u64) -> Option<Arc<Stream>> { locked(&self.open).remove(&id) }

    fn find(&self, id: u64) -> Option<Arc<Stream>> { locked(&self.open).get(&id).cloned() }

    fn count(&self) -> usize { locked(&self.open).len() }
}

fn call_of(request: &StreamRequest) -> Result<Request, DbError> {
    let named = |json: &str| -> Result<Request, DbError> {
        let description: serde_json::Value = serde_json::from_str(json)?;
        let model = description["model"].as_str().ok_or_else(|| DbError::Invalid { detail: "a query names its model".into() })?.to_string();
        let relation = description["relation"].as_str().map(str::to_string);
        let params_json = description.get("params").cloned().unwrap_or_else(|| serde_json::json!({})).to_string();
        Ok(Request { model, relation, params_json, entity: None })
    };
    match request {
        StreamRequest::Entity { model, id, .. } => Ok(Request { model: model.clone(), relation: None, params_json: "{}".into(), entity: Some(id.clone()) }),
        StreamRequest::Query { query_json } | StreamRequest::Count { query_json } | StreamRequest::Related { query_json, .. } => named(query_json),
        StreamRequest::Events { model } => Ok(Request { model: model.clone(), relation: None, params_json: "{}".into(), entity: None }),
        StreamRequest::Blob { .. } => Ok(Request { model: String::new(), relation: None, params_json: "{}".into(), entity: None }),
    }
}

fn stale() -> DbError { DbError::Interrupted { detail: "the session of this stream ended".into() } }

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
        locked(&self.stream_table.open).insert(id, stream.clone());
        if let Ok(sync) = self.sync() { stream.hold_first_screen(sync.first_screen()); }
        match self.open_stream(&stream, &request) {
            Ok(handles) => stream.attach(handles),
            Err(error) => self.fail_open(&stream, error),
        }
        id
    }

    /// The reader stopped. The stream ends in the core and no new delivery is admitted after this call returns. A delivery already admitted may finish, and the call does not wait for the reader.
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
        if let Err(error) = self.require_online("refresh a stream") {
            if open.request.relation.is_some() || open.request.entity.is_some() { open.load_finished(Some(error.clone())); }
            return Err(error);
        }
        let sync = self.sync()?.clone();
        let request = open.request.clone();
        let work = super::database::cancellable(async move {
            let params = serde_json::from_str(&request.params_json).map_err(DbError::from)?;
            match (&request.relation, &request.entity) {
                (Some(relation), _) => sync.refresh(&request.model, relation, &params).await.map(|_| ()).map_err(DbError::from),
                (None, Some(id)) => sync.load_entity(&request.model, id).await.map_err(DbError::from),
                (None, None) => Err(DbError::Invalid { detail: "the stream reads local rows and has nothing to refresh".into() }),
            }
        });
        lease.run(work).await.map_err(|_| stale())??
    }

    /// Loads the next page of the relation of the stream.
    pub async fn load_more_stream(&self, stream: u64) -> Result<LoadMoreResult, DbError> {
        let (open, lease) = self.stream_call(stream, "load more of a stream")?;
        self.require_online("load more of a stream")?;
        let sync = self.sync()?.clone();
        let request = open.request.clone();
        let Some(relation) = request.relation.clone() else { return Err(DbError::Invalid { detail: "the stream has no relation to page".into() }) };
        let work = super::database::cancellable(async move {
            let params = serde_json::from_str(&request.params_json).map_err(DbError::from)?;
            let outcome = sync.load_more(&request.model, &relation, &params).await.map_err(DbError::from)?;
            Ok::<_, DbError>(match outcome.status {
                crate::sync::LoadStatus::NoMorePages(crate::read::LoadMoreVerdict::AlreadyLoading) => LoadMoreResult::AlreadyLoading,
                crate::sync::LoadStatus::NoMorePages(_) => LoadMoreResult::NoMore,
                _ => LoadMoreResult::Loaded { has_more: outcome.has_next },
            })
        });
        lease.run(work).await.map_err(|_| stale())??
    }

    /// Returns when the first load of the stream settled. Fails when the epoch of the stream ended first.
    pub async fn stream_loaded(&self, stream: u64) -> Result<(), DbError> {
        let open = self.stream_table.find(stream).ok_or_else(stale)?;
        open.loaded().await
    }

    /// The rows of a query the core already holds, without a stream and without reading SQLite on the calling thread.
    pub async fn peek_query(&self, epoch: u64, query_json: String) -> Result<Option<CachedQuery>, DbError> {
        self.read_in_epoch(epoch, "peek query", move |this| {
            let (query, _) = this.parsed_query(&query_json)?;
            let wire = this.wire(query.model.as_str())?;
            Ok(this.db.peek_query(&query).map(|peeked| CachedQuery { revision: peeked.revision, rows: peeked.rows.iter().map(|row| wire.row(row)).collect() }))
        }).await
    }

    /// The rows of a query read from SQLite once, off the calling thread. Nothing is loaded from the server and no stream stays open.
    pub async fn read_query(&self, epoch: u64, query_json: String) -> Result<Vec<WireRow>, DbError> {
        self.read_in_epoch(epoch, "read query", move |this| {
            let (query, _) = this.parsed_query(&query_json)?;
            let wire = this.wire(query.model.as_str())?;
            Ok(this.db.query(&query)?.iter().map(|row| wire.row(row)).collect())
        }).await
    }

    /// One row read from SQLite once, off the calling thread.
    pub async fn read_row(&self, epoch: u64, model: String, row_id: String) -> Result<Option<WireRow>, DbError> {
        self.read_in_epoch(epoch, "read row", move |this| {
            let wire = this.wire(&model)?;
            Ok(this.db.find(&model, &super::database::id(&row_id)?)?.map(|row| wire.row(&row)))
        }).await
    }
}

#[cfg(test)]
impl Database {
    pub(crate) fn stream_probe(&self, stream: u64) -> Option<(bool, bool)> { self.stream_table.find(stream).map(|open| open.probe()) }
}

impl Database {
    fn refuse(&self, id: u64, epoch: u64, sink: Arc<dyn ValueSink>, error: DbError) -> u64 {
        let stream = Stream::new(id, epoch, self.streams.clone(), &self.stream_table, sink, Request { model: String::new(), relation: None, params_json: "{}".into(), entity: None });
        stream.invalid(error);
        id
    }

    fn fail_open(&self, stream: &Arc<Stream>, error: DbError) {
        let failed = stream.clone();
        match super::database::executor() {
            Ok(executor) => { crate::task::spawn_on(crate::task::Carry::Detached, executor.handle(), async move { failed.invalid(error) }); }
            Err(_) => failed.invalid(error),
        }
    }

    fn stream_call(&self, stream: u64, target: &'static str) -> Result<(Arc<Stream>, Lease), DbError> {
        let open = self.stream_table.find(stream).ok_or_else(stale)?;
        let lease = self.streams.lease(Origin::Session, target).map_err(|_| stale())?;
        if lease.epoch() != open.epoch { return Err(stale()); }
        Ok((open, lease))
    }

    pub(super) fn wire_of(&self, model: &str) -> Result<Arc<WireModel>, DbError> { self.wire(model).cloned() }
}
