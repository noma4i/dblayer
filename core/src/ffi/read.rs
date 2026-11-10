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
pub use crate::owners::read::deliveries::EndCause;
use crate::owners::read::deliveries::{Edit, Fetch, Kind, PullAnswer, StreamRow, Value, View};
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
    CableClose { position: u64, event: crate::owners::read::feed::CableClose },
    Lost { first: u64, last: u64 },
    Gap,
    Status { revision: i64, state: String, wait_reason: Option<String>, progress: u64, error: Option<String>, local_url: Option<String>, processing: Option<WireProcessing> },
    Retry { attempts: u32, phase: String },
    /// The last value of a stream, exactly one.
    Ended { cause: EndCause },
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
    CableCloses,
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

pub(super) fn read_value(value: Value, wire: Option<&WireModel>) -> Result<ReadValue, DbError> {
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
        Value::CableClose { position, event } => ReadValue::CableClose { position, event },
        Value::Lost { first, last } => ReadValue::Lost { first, last },
        Value::Gap => ReadValue::Gap,
        Value::Status { revision, blob } => ReadValue::Status { revision, state: blob.state, wait_reason: blob.wait_reason.and_then(|reason| crate::store::operation::enum_text(&reason)), progress: blob.progress, error: blob.error, local_url: blob.local_url, processing: blob.processing },
        Value::Retry(retry) => ReadValue::Retry { attempts: retry.attempts, phase: retry.phase },
        Value::Ended(cause) => ReadValue::Ended { cause },
    })
}

/// An open stream. Dropping the last handle releases it in the core.
#[derive(uniffi::Object)]
pub struct ReadStream {
    database: Weak<Database>,
    stream: u64,
    ended: std::sync::Mutex<Option<EndCause>>,
    readers: std::sync::Mutex<ReadReaders>,
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

#[derive(Default)]
struct ReadReaders {
    claimed: Option<u64>,
    released: std::collections::HashSet<u64>,
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

    fn finished(&self, cause: EndCause) -> Option<ReadValue> {
        let mut ended = self.ended.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if ended.is_some() { return None; }
        *ended = Some(cause.clone());
        Some(ReadValue::Ended { cause })
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
        let readers = self.readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if readers.claimed.is_some_and(|id| readers.released.contains(&id)) { return; }
        drop(readers);
        if let Some(life) = self.life() { life.request_snapshot(self.stream); }
    }

    /// The next value of the stream, `None` once the stream ended and its last value was taken. A failure of the stream arrives as its last value `Ended`.
    pub async fn pull(&self, reader: u64) -> Option<ReadValue> {
        if self.readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner).released.contains(&reader) { return Some(ReadValue::Ended { cause: EndCause::Closed }); }
        let Some(life) = self.life() else { return self.finished(EndCause::Closed) };
        let waiter = life.pull(self.stream, reader);
        let mut waiting = PullWait { life, stream: self.stream, outcome: waiter.id(), armed: true };
        let answer = waiter.wait().await;
        waiting.armed = false;
        if self.readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner).released.contains(&reader) { return Some(ReadValue::Ended { cause: EndCause::Closed }); }
        match answer {
            Outcome::Value(PullAnswer::Value(value)) => {
                let mut value = *value;
                if let Value::Ended(cause) = &mut value {
                    let mut ended = self.ended.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    *cause = ended.get_or_insert_with(|| cause.clone()).clone();
                }
                match read_value(value, self.wire.as_deref()) {
                    Ok(value) => Some(value),
                    Err(error) => self.finished(EndCause::Internal { detail: error.to_string() }),
                }
            }
            Outcome::Value(PullAnswer::Done) | Outcome::Closing => self.finished(EndCause::Closed),
            Outcome::EpochEnded(_) => self.finished(EndCause::SessionEnded),
            Outcome::Internal => self.finished(EndCause::Internal { detail: "the answer of the owner was lost".into() }),
            Outcome::Value(PullAnswer::Interrupted) => self.finished(EndCause::Cancelled),
        }
    }

    /// Loads the first page of the relation of this stream again. `Invalid` when the stream reads no relation, `NoSession` when its session ended.
    pub async fn refresh(&self) -> Result<(), DbError> {
        let database = self.database.upgrade().ok_or_else(super::command::closed)?;
        database.account_call(self.epoch)?;
        let load = self.relation()?;
        database.refresh(self.epoch, load.model.clone(), Some(load.relation.clone()), load.params_json.clone()).await
    }

    /// Loads the next page of the relation of this stream.
    pub async fn load_more(&self) -> Result<LoadMoreResult, DbError> {
        let database = self.database.upgrade().ok_or_else(super::command::closed)?;
        database.account_call(self.epoch)?;
        let load = self.relation()?;
        database.load_more(self.epoch, load.model.clone(), Some(load.relation.clone()), load.params_json.clone()).await
    }

    /// Takes the stream for one reader and returns its number or the reason the claim ended.
    pub async fn claim_reader(&self) -> ReadClaim {
        {
            let readers = self.readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(reader) = readers.claimed.filter(|id| readers.released.contains(id)) { return ReadClaim::Claimed { reader }; }
        }
        let Some(life) = self.life() else { return ReadClaim::Ended { cause: EndCause::Closed } };
        let reader = crate::runtime::next_id(crate::runtime::IdSpace::Reader);
        if life.claim_reader(self.stream, reader).await {
            self.readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner).claimed = Some(reader);
            ReadClaim::Claimed { reader }
        } else if life.is_shut() { ReadClaim::Ended { cause: EndCause::Closed } } else { ReadClaim::Ended { cause: EndCause::Invalid { detail: "the stream has a reader".into() } } }
    }

    /// The reader left the stream: its interest ends while the handle lives. Another reader's number changes nothing.
    pub fn release_reader(&self, reader: u64) {
        if !self.readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner).released.insert(reader) { return; }
        if let Some(life) = self.life() { life.release_reader(self.stream, reader); }
    }

    /// Returns when no value of this stream is delivered any more.
    pub async fn cancel(&self) {
        {
            let readers = self.readers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if readers.claimed.is_some_and(|id| readers.released.contains(&id)) { return; }
        }
        if let Some(life) = self.life() { let _ = life.cancel_stream(self.stream).wait().await; }
        self.ended.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(EndCause::Cancelled);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ReadClaim { Claimed { reader: u64 }, Ended { cause: EndCause } }

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
            ReadRequest::List { .. } | ReadRequest::Count { .. } | ReadRequest::Entity { .. } => self.row_view(request).map(|(view, _)| view),
            ReadRequest::Events { model } => {
                self.db.spec(model)?;
                Ok(View { kind: Kind::Events, model: model.clone(), tables: Vec::new(), local: false, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: model.clone(), name: String::new() }, subject: model.clone(), query: None })
            }
            ReadRequest::Changes | ReadRequest::CableCloses => Ok(View { kind: if matches!(request, ReadRequest::CableCloses) { Kind::CableCloses } else { Kind::Changes }, model: String::new(), tables: Vec::new(), local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), query: None }),
            ReadRequest::Operations => Ok(View { kind: Kind::Operations, model: String::new(), tables: vec![crate::store::operation::QUEUE_TABLE.to_string()], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), query: None }),
            ReadRequest::Blob { model, id, action } => Ok(View { kind: Kind::Blob, model: model.clone(), tables: vec![crate::store::ddl::table_name(model)], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: crate::store::transfer_key(model, id, action), query: None }),
        }
    }

    /// The stream waits for the swap of `waiting`: `rebind` builds its view again from the same request.
    fn deferred_of(&self, request: &ReadRequest, waiting: Vec<(String, String)>) -> Option<crate::owners::read::deliveries::Deferred> {
        if waiting.is_empty() { return None; }
        let (me, request) = (self.me.clone(), request.clone());
        Some(crate::owners::read::deliveries::Deferred { temps: waiting, rebind: crate::owners::read::deliveries::Rebind::new(move || me.upgrade()?.row_view(&request).ok().filter(|(_, waiting)| waiting.is_empty()).map(|(view, _)| view)) })
    }

    /// The view of a read of rows and the temporary ids of its request that have no server id yet. Every id passes `resolve` first: a request with such an id opens no window and sends no request, the stream reads the local rows and opens its window when a swap gives the server ids (`Deferred`).
    pub(super) fn row_view(&self, request: &ReadRequest) -> Result<(View, Vec<(String, String)>), DbError> {
        match request {
            ReadRequest::List { query: asked } | ReadRequest::Count { query: asked } => {
                let mut asked = asked.clone();
                let mut waiting = Vec::new();
                if asked.relation.is_some() && asked.params_json.contains(crate::identity::TEMP_ID_PREFIX) {
                    let mut params: serde_json::Map<String, serde_json::Value> = serde_json::from_str(&asked.params_json).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
                    waiting = super::loading::resolve_params(&self.db, &asked.model, &mut params)?;
                    asked.params_json = serde_json::Value::Object(params).to_string();
                }
                let (query, remote) = self.parsed_query(&asked)?;
                let model = query.model.as_str().to_string();
                let spec = self.db.spec(&model)?;
                let kind = if matches!(request, ReadRequest::Count { .. }) { Kind::Count } else { Kind::List };
                let live = !spec.listens.is_empty() || !spec.changes.is_empty() || remote.as_ref().is_some_and(|(relation, _)| spec.relations.iter().any(|declared| declared.name == *relation && !declared.updates.is_empty()));
                let remote = remote.filter(|(relation, _)| spec.relations.iter().any(|declared| declared.name == *relation && declared.remote.is_some()));
                let waits = !waiting.is_empty();
                let scoped = remote.as_ref().filter(|_| !waits).map(|(relation, params)| crate::owners::read::scoped_channels(spec, relation, params)).unwrap_or_default().into();
                let window = match &remote {
                    Some((relation, params)) if !waits => WindowKey::relation(&model, relation, params),
                    _ => WindowKey { model: model.clone(), name: String::new() },
                };
                let remote = remote.is_some() && !waits;
                Ok((View { kind, model: model.clone(), tables: read_tables(&self.db, &model), local: spec.is_local(), remote, live: live && !waits, scoped, reload: asked.refresh_on_connect, window, subject: String::new(), query: Some(Arc::new(query)) }, waiting))
            }
            ReadRequest::Entity { model, id, fetch } => {
                let fetch = fetch_of(*fetch);
                let spec = self.db.spec(model)?;
                let node = spec.relations.iter().find(|relation| relation.node == Some(crate::schema::NodeRead::One));
                if fetch != Fetch::Never && node.is_none() {
                    return Err(DbError::Invalid { detail: format!("{model} declares no node read to fetch from") });
                }
                let row = crate::identity::RowId::new(id).map_err(|error| DbError::Invalid { detail: error.to_string() })?;
                let row = self.db.resolve(model, &row)?;
                let id = row.as_str();
                let waits = row.is_temp() && fetch != Fetch::Never;
                let remote = fetch != Fetch::Never && !waits;
                let params = serde_json::Map::from_iter([(crate::schema::NODE_PARAM.to_string(), serde_json::Value::String(id.to_string()))]);
                let window = match node {
                    Some(relation) if remote => WindowKey::relation(model, &relation.name, &params),
                    _ => WindowKey { model: model.clone(), name: String::new() },
                };
                let scoped = match node {
                    Some(relation) if remote => crate::owners::read::scoped_channels(spec, &relation.name, &params).into(),
                    _ => Default::default(),
                };
                Ok((View { kind: Kind::Entity { fetch }, model: model.clone(), tables: read_tables(&self.db, model), local: spec.is_local(), remote, live: remote && spec.is_live(), scoped, reload: false, window, subject: id.to_string(), query: None }, if waits { vec![(model.clone(), id.to_string())] } else { Vec::new() }))
            }
            _ => Err(DbError::Internal { detail: "a stream of this kind reads no rows".into() }),
        }
    }
}

/// What the session is: no session, a signed-in one, or one that is ending.
#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum SessionStanding {
    None,
    Active,
    Ending,
}

/// The session of the database: its epoch, the identity of its credential (equal readings mean the same session, not a server id) and its phase.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SessionStatus {
    pub epoch: u64,
    pub identity: Option<u64>,
    pub phase: SessionStanding,
}

/// The status of a published value: a closed database refuses, a session without an identity is none.
pub(crate) fn status_of_published(published: &crate::owners::contract::Published) -> Result<SessionStatus, DbError> {
    use crate::owners::contract::PublishedState as State;
    let (phase, identity) = match published.state {
        _ if published.closed => return Err(super::command::closed()),
        State::Closing => return Err(super::command::closed()),
        State::NoSession => (SessionStanding::None, None),
        State::Session | State::Resetting | State::StorageFailed if published.identity.is_none() => (SessionStanding::None, None),
        State::Session | State::Resetting | State::StorageFailed => (SessionStanding::Active, published.identity),
        State::Ending | State::EndingFailed => (SessionStanding::Ending, published.identity),
    };
    Ok(SessionStatus { epoch: published.epoch, identity, phase })
}

#[uniffi::export]
impl Database {
    pub fn cable_closes(&self) -> Arc<ReadStream> { self.subscribe_values(self.session_epoch(), ReadRequest::CableCloses) }
    /// The epoch of the session now. A reader that opens streams for an account asks for it once, after the sign-in, and hands it to every stream and command of that account.
    pub fn session_epoch(&self) -> u64 { self.life.published().epoch }

    /// The session of the database now, read from the published value: no network and no storage.
    pub fn session_status(&self) -> Result<SessionStatus, DbError> { status_of_published(&self.life.published()) }

    pub fn subscribe_values(&self, epoch: u64, request: ReadRequest) -> Arc<ReadStream> {
        let stream = self.life.next_stream_id();
        let (view, valid, deferred) = match match &request {
            ReadRequest::List { .. } | ReadRequest::Count { .. } | ReadRequest::Entity { .. } => self.row_view(&request).map(|(view, waiting)| (view, self.deferred_of(&request, waiting))),
            _ => self.view_of(&request).map(|view| (view, None)),
        } {
            Ok((view, deferred)) => (view, true, deferred),
            Err(_) => (View { kind: Kind::List, model: String::new(), tables: Vec::new(), local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), query: None }, false, None),
        };
        let revision = self.db.published_revision().load(std::sync::atomic::Ordering::Acquire);
        self.life.open_stream(stream, view, epoch, valid, revision, deferred);
        let wire = match (&request, valid) {
            (ReadRequest::Events { model }, true) => self.wire(model).ok().cloned(),
            _ => None,
        };
        let load = match (&request, valid) {
            (ReadRequest::List { query } | ReadRequest::Count { query }, true) => query.relation.as_ref().map(|relation| RelationLoad { model: query.model.clone(), relation: relation.clone(), params_json: query.params_json.clone() }),
            _ => None,
        };
        Arc::new(ReadStream { database: self.me.clone(), stream, ended: std::sync::Mutex::new(None), readers: std::sync::Mutex::new(ReadReaders::default()), wire, epoch, load })
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
