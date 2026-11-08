//! The values of reading as the platform gets them: the bridge carries the values of the owner `Deliveries`,
//! the wrappers compute nothing. A stream is an object: `pull` waits for the next value, `cancel` returns when no value follows, the last handle gone releases it.

use std::sync::{Arc, Weak};

use super::database::Database;
use super::LoadMoreResult;
use super::queue::OperationInfo;
use super::wire::{WireModel, WireProcessing, WireRow};
use super::QuerySpec;
use super::DbError;
use crate::owners::contract::{WindowPhase, WindowState};
use crate::owners::life::Life;
use crate::owners::read::deliveries::{EndCause, Edit, Fetch, Kind, PullAnswer, StreamRow, Value, View};
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
    pub error: Option<DbError>,
    pub loaded: Option<i64>,
    /// The window has a next page.
    pub has_more: bool,
    /// The first load of the interest of the stream ended with an outcome.
    pub settled: bool,
    /// The rows of the membership at the revision of the value, for a list.
    pub total: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ReadEnd {
    SessionEnded,
    Closed,
    Cancelled,
    Invalid,
    Storage { error: String },
    Internal { detail: String },
}

/// One step on the list of the reader: an index is a place in the list as it stands after the steps before.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum RowEdit {
    Insert { index: u32, row: WireRow },
    /// `temporary` = the id the row had when the reader first got it, set when this step settles the swap of a temporary id.
    Rewrite { index: u32, row: WireRow, temporary: Option<String> },
    Remove { index: u32, id: String },
    Move { from: u32, to: u32 },
}

#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum ReadValue {
    /// The whole result at a revision: the first value of a stream of rows.
    Snapshot { revision: i64, rows: Vec<WireRow>, edits: Vec<RowEdit>, state: ReadState },
    /// The change after the last value the reader took: the steps from the rows it holds to the rows of `revision`.
    Delta { base: i64, revision: i64, edits: Vec<RowEdit>, state: ReadState },
    Count { revision: i64, count: u64, state: ReadState },
    /// The whole queue of operations at a revision.
    Operations { revision: i64, items: Vec<OperationInfo> },
    State { revision: i64, state: ReadState },
    Ready,
    Changes { revision: i64, rows: Vec<ChangedRow> },
    /// One event of a model: the rows before and after its landing, read only while a stream of events is open.
    Event { position: u64, area: String, kind: String, id: String, before: Option<Box<WireRow>>, after: Option<Box<WireRow>> },
    Lost { first: u64, last: u64 },
    Gap,
    Status { revision: i64, state: String, progress: u64, error: Option<String>, local_url: Option<String>, processing: Option<WireProcessing> },
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
    List { query: QuerySpec },
    Count { query: QuerySpec },
    Entity { model: String, id: String, fetch: ReadFetch },
    /// The events of a model after the stream opened.
    Events { model: String },
    /// The status of the transfer of `uploads[action]` of a row.
    Blob { model: String, id: String, action: String },
    /// The queue of operations of the database.
    Operations,
    Changes,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ChangedRow {
    pub model: String,
    pub id: String,
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
        has_more: state.has_more,
        settled: state.settled,
        total: state.total,
    }
}

fn end_of(cause: EndCause) -> ReadEnd {
    match cause {
        EndCause::SessionEnded => ReadEnd::SessionEnded,
        EndCause::Closed => ReadEnd::Closed,
        EndCause::Cancelled => ReadEnd::Cancelled,
        EndCause::Invalid => ReadEnd::Invalid,
        EndCause::Storage { error } => ReadEnd::Storage { error },
    }
}

fn full_row(row: &StreamRow) -> Result<WireRow, DbError> {
    row.row.clone().ok_or_else(|| DbError::Internal { detail: format!("the read gave row {} without its values", row.id) })
}

fn edit_of(edit: &Edit) -> Result<RowEdit, DbError> {
    Ok(match edit {
        Edit::Insert { index, row } => RowEdit::Insert { index: *index, row: full_row(row)? },
        Edit::Rewrite { index, row, temporary } => RowEdit::Rewrite { index: *index, row: full_row(row)?, temporary: temporary.clone() },
        Edit::Remove { index, id } => RowEdit::Remove { index: *index, id: id.clone() },
        Edit::Move { from, to } => RowEdit::Move { from: *from, to: *to },
    })
}

fn damaged_event(detail: &str) -> DbError { DbError::Internal { detail: format!("the payload of an event is damaged: {detail}") } }

fn event_row(wire: Option<&WireModel>, id: &str, value: Option<&serde_json::Value>) -> Result<Option<Box<WireRow>>, DbError> {
    let Some(value) = value.filter(|value| !value.is_null()) else { return Ok(None) };
    let Some(wire) = wire else { return Ok(None) };
    let data = value.as_object().ok_or_else(|| damaged_event("a row is not an object"))?;
    let id = crate::identity::RowId::new(id).map_err(|error| damaged_event(&error.to_string()))?;
    Ok(Some(Box::new(wire.row(&crate::store::Record { id, revision: 0, data: data.clone(), sync: Default::default() }))))
}

fn event_of(position: u64, payload: &str, wire: Option<&WireModel>) -> Result<ReadValue, DbError> {
    let fields: serde_json::Value = serde_json::from_str(payload).map_err(|error| damaged_event(&error.to_string()))?;
    if !fields.is_object() { return Err(damaged_event("the payload is not an object")); }
    let text = |name: &str| match fields.get(name) {
        None | Some(serde_json::Value::Null) => Ok(String::new()),
        Some(serde_json::Value::String(text)) => Ok(text.clone()),
        Some(_) => Err(damaged_event(&format!("{name} is not text"))),
    };
    let (id, area, kind) = (text("id")?, text("area")?, text("kind")?);
    Ok(ReadValue::Event { position, area, kind, before: event_row(wire, &id, fields.get("before"))?, after: event_row(wire, &id, fields.get("after"))?, id })
}

fn read_value(value: Value, wire: Option<&WireModel>) -> Result<ReadValue, DbError> {
    Ok(match value {
        Value::Snapshot { revision, rows, edits, state, .. } => ReadValue::Snapshot { revision, rows: rows.iter().map(full_row).collect::<Result<_, _>>()?, edits: edits.iter().map(edit_of).collect::<Result<_, _>>()?, state: state_of(state) },
        Value::Delta(delta) => ReadValue::Delta {
            base: delta.base,
            revision: delta.revision,
            edits: delta.edits.iter().map(edit_of).collect::<Result<_, _>>()?,
            state: state_of(delta.state),
        },
        Value::Count { revision, count, state } => ReadValue::Count { revision, count, state: state_of(state) },
        Value::Operations { revision, items } => ReadValue::Operations { revision, items },
        Value::State { revision, state } => ReadValue::State { revision, state: state_of(state) },
        Value::Ready => ReadValue::Ready,
        Value::Changes { revision, rows } => ReadValue::Changes { revision, rows: rows.into_iter().map(|(table, id)| ChangedRow { model: table.strip_prefix("row_").unwrap_or(&table).to_string(), id }).collect() },
        Value::Event { position, payload } => event_of(position, &payload, wire)?,
        Value::Lost { first, last } => ReadValue::Lost { first, last },
        Value::Gap => ReadValue::Gap,
        Value::Status { revision, blob } => ReadValue::Status { revision, state: blob.state, progress: blob.progress, error: blob.error, local_url: blob.local_url, processing: blob.processing },
        Value::Retry(retry) => ReadValue::Retry { attempts: retry.attempts, phase: retry.phase },
        Value::Ended(cause) => ReadValue::Ended { cause: end_of(cause) },
    })
}

/// An open stream. Dropping the last handle releases it in the core.
#[derive(uniffi::Object)]
pub struct ReadStream {
    database: Weak<Database>,
    stream: u64,
    ended: std::sync::atomic::AtomicBool,
    /// The model of an events stream, to turn the rows of its events into wire rows.
    wire: Option<Arc<WireModel>>,
    /// The epoch of the open, and the relation the request reads from the server.
    epoch: u64,
    load: Option<RelationLoad>,
}

struct RelationLoad {
    model: String,
    relation: String,
    params_json: String,
}

impl ReadStream {
    #[cfg(test)]
    pub(crate) fn id(&self) -> u64 { self.stream }

    /// The next value for a test that reads with the one reader of its stream.
    #[cfg(test)]
    pub(crate) async fn pull_next(&self) -> Option<ReadValue> { self.pull(0).await }

    fn life(&self) -> Option<Arc<Life>> { self.database.upgrade().map(|database| database.life.clone()) }

    fn relation(&self) -> Result<&RelationLoad, DbError> {
        self.load.as_ref().ok_or_else(|| DbError::Invalid { detail: "the stream has no relation to load".into() })
    }

    fn finished(&self, cause: ReadEnd) -> Option<ReadValue> {
        (!self.ended.swap(true, std::sync::atomic::Ordering::SeqCst)).then_some(ReadValue::Ended { cause })
    }
}

struct PullWait {
    life: Arc<Life>,
    stream: u64,
    outcome: crate::runtime::OutcomeId,
    armed: bool,
}

impl Drop for PullWait {
    fn drop(&mut self) {
        if self.armed { self.life.cancel_pull(self.stream, self.outcome); }
    }
}

#[uniffi::export]
impl ReadStream {
    pub fn resync(&self) {
        if let Some(life) = self.life() { life.request_snapshot(self.stream); }
    }

    /// The next value of the stream, `None` once the stream ended and its last value was taken. A failure of the stream arrives as its last value `Ended`.
    pub async fn pull(&self, reader: u64) -> Option<ReadValue> {
        let Some(life) = self.life() else { return self.finished(ReadEnd::Closed) };
        let waiter = life.pull(self.stream, reader);
        let mut waiting = PullWait { life, stream: self.stream, outcome: waiter.id(), armed: true };
        let answer = waiter.wait().await;
        waiting.armed = false;
        match answer {
            Outcome::Value(PullAnswer::Value(value)) => {
                let value = *value;
                if matches!(value, Value::Ended(_)) {
                    self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                match read_value(value, self.wire.as_deref()) {
                    Ok(value) => Some(value),
                    Err(error) => self.finished(ReadEnd::Internal { detail: error.to_string() }),
                }
            }
            Outcome::Value(PullAnswer::Done) | Outcome::Closing => self.finished(ReadEnd::Closed),
            Outcome::EpochEnded(_) => self.finished(ReadEnd::SessionEnded),
            Outcome::Internal => self.finished(ReadEnd::Internal { detail: "the answer of the owner was lost".into() }),
            Outcome::Value(PullAnswer::Invalid) => Some(ReadValue::Ended { cause: ReadEnd::Invalid }),
            Outcome::Value(PullAnswer::Interrupted) => self.finished(ReadEnd::Cancelled),
        }
    }

    /// Loads the first page of the relation of this stream again. `Invalid` when the stream reads no relation, `Interrupted` when its session ended.
    pub async fn refresh(&self) -> Result<(), DbError> {
        let database = self.database.upgrade().ok_or_else(super::command::closed)?;
        let load = self.relation()?;
        database.refresh(self.epoch, load.model.clone(), Some(load.relation.clone()), load.params_json.clone()).await
    }

    /// Loads the next page of the relation of this stream.
    pub async fn load_more(&self) -> Result<LoadMoreResult, DbError> {
        let database = self.database.upgrade().ok_or_else(super::command::closed)?;
        let load = self.relation()?;
        database.load_more(self.epoch, load.model.clone(), Some(load.relation.clone()), load.params_json.clone()).await
    }

    /// Takes the stream for one reader and returns its number. `Invalid` when another reader holds the stream.
    pub async fn claim_reader(&self) -> Result<u64, DbError> {
        static READERS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let life = self.life().ok_or_else(super::command::closed)?;
        let reader = READERS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if life.claim_reader(self.stream, reader).await { Ok(reader) } else if life.is_shut() { Err(super::command::closed()) } else { Err(DbError::Invalid { detail: "the stream has a reader".into() }) }
    }

    /// The reader left the stream: its interest ends while the handle lives. Another reader's number changes nothing.
    pub fn release_reader(&self, reader: u64) {
        if let Some(life) = self.life() { life.release_reader(self.stream, reader); }
    }

    /// Returns when no value of this stream is delivered any more.
    pub async fn cancel(&self) {
        if let Some(life) = self.life() { let _ = life.cancel_stream(self.stream).wait().await; }
        self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Drop for ReadStream {
    fn drop(&mut self) {
        if let Some(life) = self.life() { life.release_stream(self.stream); }
    }
}

/// The tables a stream of `model` reads: its own and those of the rows its nested references read.
fn read_tables(db: &crate::store::Store, model: &str) -> Vec<String> {
    let linked = db.spec(model).map(crate::store::plan::nested_ref::link_targets).unwrap_or_default();
    std::iter::once(model.to_string()).chain(linked).map(|name| crate::store::ddl::table_name(&name)).collect()
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
            ReadRequest::List { query: asked } | ReadRequest::Count { query: asked } => {
                let (query, remote) = self.parsed_query(asked)?;
                let model = query.model.as_str().to_string();
                let spec = self.db.spec(&model)?;
                let kind = if matches!(request, ReadRequest::Count { .. }) { Kind::Count } else { Kind::List };
                let live = !spec.listens.is_empty() || !spec.changes.is_empty() || remote.as_ref().is_some_and(|(relation, _)| spec.relations.iter().any(|declared| declared.name == *relation && !declared.updates.is_empty()));
                let remote = remote.filter(|(relation, _)| spec.relations.iter().any(|declared| declared.name == *relation && declared.remote.is_some()));
                let scoped = remote.as_ref().map(|(relation, params)| crate::owners::read::scoped_channels(spec, relation, params)).unwrap_or_default().into();
                let window = match &remote {
                    Some((relation, params)) => WindowKey::relation(&model, relation, params),
                    None => WindowKey { model: model.clone(), name: String::new() },
                };
                Ok(View { kind, model: model.clone(), tables: read_tables(&self.db, &model), local: spec.is_local(), remote: remote.is_some(), live, scoped, reload: asked.refresh_on_connect, window, subject: String::new(), query: Some(Arc::new(query)) })
            }
            ReadRequest::Events { model } => {
                self.db.spec(model)?;
                Ok(View { kind: Kind::Events, model: model.clone(), tables: Vec::new(), local: false, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: model.clone(), name: String::new() }, subject: model.clone(), query: None })
            }
            ReadRequest::Changes => Ok(View { kind: Kind::Changes, model: String::new(), tables: Vec::new(), local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), query: None }),
            ReadRequest::Operations => Ok(View { kind: Kind::Operations, model: String::new(), tables: vec![crate::store::operation::QUEUE_TABLE.to_string()], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), query: None }),
            ReadRequest::Blob { model, id, action } => Ok(View { kind: Kind::Blob, model: model.clone(), tables: vec![crate::store::ddl::table_name(model)], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: crate::store::transfer_key(model, id, action), query: None }),
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
                let scoped = node.map(|relation| crate::owners::read::scoped_channels(spec, &relation.name, &serde_json::Map::from_iter([(crate::schema::NODE_PARAM.to_string(), serde_json::Value::String(id.clone()))]))).unwrap_or_default().into();
                Ok(View { kind: Kind::Entity { fetch }, model: model.clone(), tables: read_tables(&self.db, model), local: spec.is_local(), remote: fetch != Fetch::Never, live: fetch != Fetch::Never && spec.is_live(), scoped: if fetch == Fetch::Never { Default::default() } else { scoped }, reload: false, window, subject: id.clone(), query: None })
            }
        }
    }
}

#[uniffi::export]
impl Database {
    /// The epoch of the session now. A reader that opens streams for an account asks for it once, after the sign-in, and hands it to every stream and command of that account.
    pub fn session_epoch(&self) -> u64 { self.life.published().epoch }

    pub fn subscribe_values(&self, epoch: u64, request: ReadRequest) -> Arc<ReadStream> {
        let stream = self.life.next_stream_id();
        let (view, valid) = match self.view_of(&request) {
            Ok(view) => (view, true),
            Err(_) => (View { kind: Kind::List, model: String::new(), tables: Vec::new(), local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), query: None }, false),
        };
        let revision = self.db.published_revision().load(std::sync::atomic::Ordering::Acquire);
        self.life.open_stream(stream, view, epoch, valid, revision);
        let wire = match (&request, valid) {
            (ReadRequest::Events { model }, true) => self.wire(model).ok().cloned(),
            _ => None,
        };
        let load = match (&request, valid) {
            (ReadRequest::List { query } | ReadRequest::Count { query }, true) => query.relation.as_ref().map(|relation| RelationLoad { model: query.model.clone(), relation: relation.clone(), params_json: query.params_json.clone() }),
            _ => None,
        };
        Arc::new(ReadStream { database: self.me.clone(), stream, ended: std::sync::atomic::AtomicBool::new(false), wire, epoch, load })
    }
}

#[cfg(test)]
impl RowEdit {
    pub(crate) fn replay(rows: &mut Vec<WireRow>, edits: &[RowEdit]) {
        for edit in edits {
            match edit {
                RowEdit::Insert { index, row } => rows.insert(*index as usize, row.clone()),
                RowEdit::Rewrite { index, row, .. } => rows[*index as usize] = row.clone(),
                RowEdit::Remove { index, id } => assert_eq!(&rows.remove(*index as usize).id, id),
                RowEdit::Move { from, to } => {
                    let row = rows.remove(*from as usize);
                    rows.insert(*to as usize, row);
                }
            }
        }
    }

    pub(crate) fn carried(edits: &[RowEdit]) -> Vec<WireRow> {
        edits.iter().filter_map(|edit| match edit { RowEdit::Insert { row, .. } | RowEdit::Rewrite { row, .. } => Some(row.clone()), _ => None }).collect()
    }

    pub(crate) fn removed(edits: &[RowEdit]) -> Vec<String> {
        edits.iter().filter_map(|edit| match edit { RowEdit::Remove { id, .. } => Some(id.clone()), _ => None }).collect()
    }
}
