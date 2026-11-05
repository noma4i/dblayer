//! The values of reading as the platform gets them (`delivery.md`, "Значения для платформы"): the bridge carries the values of the owner `Deliveries`,
//! the wrappers compute nothing. A stream is an object: `pull` waits for the next value, `cancel` returns when no value follows, the last handle gone releases it.

use std::sync::Arc;

use super::database::Database;
use super::wire::{WireModel, WireRow};
use super::DbError;
use crate::db::Db;
use crate::owners::contract::{WindowPhase, WindowState};
use crate::owners::life::Life;
use crate::owners::read::deliveries::{EndCause, Fetch, Kind, PullAnswer, StreamRow, Value, View};
use crate::owners::read::windows::WindowKey;
use crate::runtime::Outcome;

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ReadPhase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

/// The state of the window that goes with a value: `Failed` only without rows, an error with rows is `Loaded` with the error.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ReadState {
    pub phase: ReadPhase,
    pub error: Option<String>,
    pub loaded: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ReadEnd {
    SessionEnded,
    Closed,
    Cancelled,
    Invalid,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RowPlace {
    pub id: String,
    pub index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RowRename {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
#[allow(clippy::large_enum_variant)]
pub enum ReadValue {
    /// The whole result at a revision: the first value of a stream of rows.
    Snapshot { revision: i64, rows: Vec<WireRow>, state: ReadState },
    /// The change after the last value the reader took. `rows[i]` stands at `indexes[i]` of the final list of `size` rows, the core fixed the order.
    Delta { base: i64, revision: i64, size: u32, rows: Vec<WireRow>, indexes: Vec<u32>, removed: Vec<String>, placed: Vec<RowPlace>, replaced: Vec<RowRename>, renamed: Vec<RowRename>, state: ReadState },
    Count { revision: i64, count: u64, state: ReadState },
    State { revision: i64, state: ReadState },
    Ready,
    Event { position: u64, payload: String },
    Lost { first: u64, last: u64 },
    Gap,
    Status { revision: i64, state: String, progress: u64, error: Option<String> },
    Retry { attempts: u32, phase: String },
    /// The last value of a stream, exactly one.
    Ended { cause: ReadEnd },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ReadFetch {
    Never,
    Stale,
    Force,
    IfMissing,
}

/// What a stream reads.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum ReadRequest {
    /// The JSON of a query: model, filters, order, and for a server relation its name and parameters.
    List { query_json: String },
    Count { query_json: String },
    Entity { model: String, id: String, fetch: ReadFetch },
    Events { model: String },
    Blob { id: String },
    RetryStatus,
}

fn state_of(state: WindowState) -> ReadState {
    ReadState {
        phase: match state.phase {
            WindowPhase::Idle => ReadPhase::Idle,
            WindowPhase::Loading => ReadPhase::Loading,
            WindowPhase::Loaded => ReadPhase::Loaded,
            WindowPhase::Failed => ReadPhase::Failed,
        },
        error: state.error,
        loaded: state.loaded,
    }
}

fn end_of(cause: EndCause) -> ReadEnd {
    match cause {
        EndCause::SessionEnded => ReadEnd::SessionEnded,
        EndCause::Closed => ReadEnd::Closed,
        EndCause::Cancelled => ReadEnd::Cancelled,
        EndCause::Invalid => ReadEnd::Invalid,
    }
}

/// The row of an entry: the owner reads ids with their stamps, the full row comes from the cache of the database.
fn full_row(db: &Db, wire: Option<&WireModel>, model: &str, row: &StreamRow) -> Option<WireRow> {
    if let Some(row) = &row.row {
        return Some(row.clone());
    }
    let wire = wire?;
    let id = crate::identity::RowId::new(&row.id).ok()?;
    db.find(model, &id).ok().flatten().map(|found| wire.row(&found))
}

fn read_value(db: &Db, wire: Option<&WireModel>, model: &str, value: Value) -> ReadValue {
    match value {
        Value::Snapshot { revision, rows, state } => ReadValue::Snapshot { revision, rows: rows.iter().filter_map(|row| full_row(db, wire, model, row)).collect(), state: state_of(state) },
        Value::Delta(delta) => {
            let (mut rows, mut indexes) = (Vec::with_capacity(delta.rows.len()), Vec::with_capacity(delta.rows.len()));
            for (row, index) in delta.rows.iter().zip(&delta.indexes) {
                if let Some(row) = full_row(db, wire, model, row) {
                    rows.push(row);
                    indexes.push(*index);
                }
            }
            let pairs = |pairs: Vec<(String, String)>| pairs.into_iter().map(|(from, to)| RowRename { from, to }).collect();
            ReadValue::Delta {
                base: delta.base,
                revision: delta.revision,
                size: delta.size,
                rows,
                indexes,
                removed: delta.removed,
                placed: delta.placed.into_iter().map(|(id, index)| RowPlace { id, index }).collect(),
                replaced: pairs(delta.replaced),
                renamed: pairs(delta.renamed),
                state: state_of(delta.state),
            }
        }
        Value::Count { revision, count, state } => ReadValue::Count { revision, count, state: state_of(state) },
        Value::State { revision, state } => ReadValue::State { revision, state: state_of(state) },
        Value::Ready => ReadValue::Ready,
        Value::Event { position, payload } => ReadValue::Event { position, payload },
        Value::Lost { first, last } => ReadValue::Lost { first, last },
        Value::Gap => ReadValue::Gap,
        Value::Status { revision, blob } => ReadValue::Status { revision, state: blob.state, progress: blob.progress, error: blob.error },
        Value::Retry(retry) => ReadValue::Retry { attempts: retry.attempts, phase: retry.phase },
        Value::Ended(cause) => ReadValue::Ended { cause: end_of(cause) },
    }
}

/// An open stream. Dropping the last handle releases it in the core.
#[derive(uniffi::Object)]
pub struct ReadStream {
    life: Arc<Life>,
    db: Db,
    wire: Option<Arc<WireModel>>,
    model: String,
    stream: u64,
}

#[uniffi::export]
impl ReadStream {
    /// The next value of the stream, `None` once the stream ended and its last value was taken.
    pub async fn pull(&self) -> Result<Option<ReadValue>, DbError> {
        match self.life.pull(self.stream).wait().await {
            Outcome::Value(PullAnswer::Value(value)) => Ok(Some(read_value(&self.db, self.wire.as_deref(), &self.model, value))),
            Outcome::Value(PullAnswer::Done) | Outcome::EpochEnded(_) | Outcome::Closing => Ok(None),
            Outcome::Value(PullAnswer::Invalid) => Err(DbError::Invalid { detail: "a pull of this stream is waiting already".into() }),
        }
    }

    /// Returns when no value of this stream is delivered any more.
    pub async fn cancel(&self) {
        let _ = self.life.cancel_stream(self.stream).wait().await;
    }
}

impl Drop for ReadStream {
    fn drop(&mut self) { self.life.release_stream(self.stream); }
}

fn fetch_of(fetch: ReadFetch) -> Fetch {
    match fetch {
        ReadFetch::Never => Fetch::Never,
        ReadFetch::Stale => Fetch::Stale,
        ReadFetch::Force => Fetch::Force,
        ReadFetch::IfMissing => Fetch::IfMissing,
    }
}

impl Database {
    /// The view of a request: what the owner reads, and the window it watches.
    fn view_of(&self, request: &ReadRequest) -> Result<View, DbError> {
        match request {
            ReadRequest::List { query_json } | ReadRequest::Count { query_json } => {
                let (query, remote) = self.parsed_query(query_json)?;
                let model = query.model.as_str().to_string();
                let spec = self.db.spec(&model)?;
                let kind = if matches!(request, ReadRequest::Count { .. }) { Kind::Count } else { Kind::List };
                let window = match &remote {
                    Some((relation, params)) => WindowKey::relation(&model, relation, params),
                    None => WindowKey { model: model.clone(), name: String::new() },
                };
                Ok(View { kind, model: model.clone(), tables: vec![crate::ddl::table_name(&model)], local: spec.is_local(), remote: remote.is_some(), window, subject: query_json.clone() })
            }
            ReadRequest::Entity { model, id, fetch } => {
                let spec = self.db.spec(model)?;
                let node = spec.relations.iter().find(|relation| relation.node == Some(crate::schema::NodeRead::One));
                let fetch = fetch_of(*fetch);
                if fetch != Fetch::Never && node.is_none() {
                    return Err(DbError::Invalid { detail: format!("{model} declares no node read to fetch from") });
                }
                let window = match node {
                    Some(relation) => WindowKey::relation(model, &relation.name, &serde_json::Map::from_iter([(crate::schema::NODE_PARAM.to_string(), serde_json::Value::String(id.clone()))])),
                    None => WindowKey { model: model.clone(), name: String::new() },
                };
                Ok(View { kind: Kind::Entity { fetch }, model: model.clone(), tables: vec![crate::ddl::table_name(model)], local: spec.is_local(), remote: fetch != Fetch::Never, window, subject: id.clone() })
            }
            ReadRequest::Events { model } => {
                self.db.spec(model)?;
                Ok(View { kind: Kind::Events, model: model.clone(), tables: Vec::new(), local: false, remote: false, window: WindowKey { model: model.clone(), name: String::new() }, subject: String::new() })
            }
            ReadRequest::Blob { id } => Ok(View { kind: Kind::Blob, model: String::new(), tables: Vec::new(), local: true, remote: false, window: WindowKey { model: String::new(), name: String::new() }, subject: id.clone() }),
            ReadRequest::RetryStatus => Ok(View { kind: Kind::RetryStatus, model: String::new(), tables: Vec::new(), local: true, remote: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new() }),
        }
    }
}

#[uniffi::export]
impl Database {
    /// Opens a stream of the session epoch of the caller. A request that cannot be read opens a stream that ends as `Invalid`.
    pub fn subscribe_values(&self, epoch: u64, request: ReadRequest) -> Arc<ReadStream> {
        let stream = self.life.next_stream_id();
        let (view, valid) = match self.view_of(&request) {
            Ok(view) => (view, true),
            Err(_) => (View { kind: Kind::List, model: String::new(), tables: Vec::new(), local: true, remote: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new() }, false),
        };
        let wire = self.wire(&view.model).ok().cloned();
        let model = view.model.clone();
        let revision = self.db.revision_seq().unwrap_or(0);
        self.life.open_stream(stream, view, epoch, revision, valid);
        Arc::new(ReadStream { life: self.life.clone(), db: self.db.clone(), wire, model, stream })
    }
}
