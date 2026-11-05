//! The values of reading as the platform gets them (`delivery.md`, the values for the platform): the bridge carries the values of the owner `Deliveries`,
//! the wrappers compute nothing. A stream is an object: `pull` waits for the next value, `cancel` returns when no value follows, the last handle gone releases it.

use std::sync::Arc;

use super::database::Database;
use super::wire::WireRow;
use super::DbError;
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

fn full_row(row: &StreamRow) -> Result<WireRow, DbError> {
    row.row.clone().ok_or_else(|| DbError::Database { detail: format!("the read gave row {} without its values", row.id) })
}

fn read_value(value: Value) -> Result<ReadValue, DbError> {
    Ok(match value {
        Value::Snapshot { revision, rows, state } => ReadValue::Snapshot { revision, rows: rows.iter().map(full_row).collect::<Result<_, _>>()?, state: state_of(state) },
        Value::Delta(delta) => {
            let rows = delta.rows.iter().map(full_row).collect::<Result<Vec<_>, _>>()?;
            let indexes = delta.indexes.clone();
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
    })
}

/// An open stream. Dropping the last handle releases it in the core.
#[derive(uniffi::Object)]
pub struct ReadStream {
    life: Arc<Life>,
    stream: u64,
    refusal: std::sync::Mutex<Option<DbError>>,
    ended: std::sync::atomic::AtomicBool,
}

impl ReadStream {
    fn finished(&self, cause: ReadEnd) -> Option<ReadValue> {
        (!self.ended.swap(true, std::sync::atomic::Ordering::SeqCst)).then_some(ReadValue::Ended { cause })
    }
}

#[uniffi::export]
impl ReadStream {
    /// The next value of the stream, `None` once the stream ended and its last value was taken.
    pub async fn pull(&self) -> Result<Option<ReadValue>, DbError> {
        if let Some(error) = self.refusal.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            return Err(error);
        }
        match self.life.pull(self.stream).wait().await {
            Outcome::Value(PullAnswer::Value(value)) => {
                if matches!(value, Value::Ended(_)) {
                    self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                read_value(value).map(Some)
            }
            Outcome::Value(PullAnswer::Done) | Outcome::Closing => Ok(self.finished(ReadEnd::Closed)),
            Outcome::EpochEnded(_) => Ok(self.finished(ReadEnd::SessionEnded)),
            Outcome::Value(PullAnswer::Invalid) => Err(DbError::Invalid { detail: "a pull of this stream is waiting already".into() }),
        }
    }

    /// Returns when no value of this stream is delivered any more.
    pub async fn cancel(&self) {
        let _ = self.life.cancel_stream(self.stream).wait().await;
        self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
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
        }
    }
}

#[uniffi::export]
impl Database {
    pub fn subscribe_values(&self, epoch: u64, request: ReadRequest) -> Arc<ReadStream> {
        let stream = self.life.next_stream_id();
        let opened = self.view_of(&request).and_then(|view| Ok((view, self.db.revision_seq()?)));
        let (view, revision, valid, refusal) = match opened {
            Ok((view, revision)) => (view, revision, true, None),
            Err(error) => (View { kind: Kind::List, model: String::new(), tables: Vec::new(), local: true, remote: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new() }, 0, false, Some(error)),
        };
        self.life.open_stream(stream, view, epoch, revision, valid);
        Arc::new(ReadStream { life: self.life.clone(), stream, refusal: std::sync::Mutex::new(refusal), ended: std::sync::atomic::AtomicBool::new(false) })
    }
}
