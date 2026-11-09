use super::*;

/// `(model, relation, params)`, `(model, node, id)` or `(model, node, ids)` as one name; `model` is the model of the window.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WindowKey {
    pub model: String,
    pub name: String,
}

impl WindowKey {
    pub(super) const NODES: &'static str = "nodes:";

    /// The key `(model, node, ids)`: the order of `ids` is the order of the call.
    pub fn nodes(model: &str, ids: &[&str]) -> Self { Self { model: model.into(), name: format!("{}{}", Self::NODES, serde_json::to_string(ids).unwrap_or_default()) } }

    pub(super) fn is_empty_nodes(&self) -> bool { matches!(self.kind(), WindowKind::Nodes { ids } if ids.is_empty()) }

    /// The key `(model, relation, params)`: `params` in the canonical form, keys in alphabetical order.
    pub fn relation(model: &str, relation: &str, params: &serde_json::Map<String, serde_json::Value>) -> Self {
        let canonical: BTreeMap<&String, &serde_json::Value> = params.iter().collect();
        Self { model: model.into(), name: format!("{relation}|{}", serde_json::to_string(&canonical).unwrap_or_default()) }
    }

    pub fn kind(&self) -> WindowKind {
        if let Some(ids) = self.name.strip_prefix(Self::NODES) {
            return WindowKind::Nodes { ids: serde_json::from_str(ids).unwrap_or_default() };
        }
        let (relation, params) = self.name.split_once('|').unwrap_or((self.name.as_str(), "{}"));
        let params = serde_json::from_str(params).unwrap_or_default();
        WindowKind::Relation { relation: relation.to_string(), params }
    }
}

/// What a key names: a relation with params (a `node` read is a relation with the param `NODE_PARAM`) or several nodes of the root of the model.
#[derive(Clone, Debug, PartialEq)]
pub enum WindowKind {
    Relation { relation: String, params: serde_json::Map<String, serde_json::Value> },
    Nodes { ids: Vec<String> },
}

/// An id of a stream that watches the window or of a caller that waits for it. `PRELOAD` is the interest of a key declared `preload`.
pub type InterestId = u64;

pub const PRELOAD: InterestId = InterestId::MAX;

/// What a `Load` asks for: the first page, the page after the last one the caller saw, or every page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Goal {
    First,
    Next,
    All,
}

/// A stored goal: `Next(c)` waits for the landing of the page `c`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Want {
    First,
    Next(u32),
    All,
}

/// The rows of a page as the server sent them, normalized by the codec of the runner. The owner passes them to the Store untouched.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageRows {
    pub roots: Vec<crate::owners::store::RowValue>,
    pub related: Vec<(String, crate::owners::store::RowValue)>,
    pub total: Option<i64>,
}

/// The answer of the server for one page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnswerPage {
    pub page: u32,
    pub rows: Vec<String>,
    pub cursor: Option<String>,
    pub dependency_models: BTreeSet<String>,
    pub payload: PageRows,
    /// The answer of a marker request: `sequence` is the newest sequence of the server, `values` are `revision` and the counts of the prefix.
    pub marker: Option<Marker>,
    /// `pageInfo.hasNextPage`: a page of a media chain that has more pages but names no cursor is an invalid answer.
    pub more: bool,
}

impl AnswerPage {
    pub fn new(page: u32, rows: Vec<String>, cursor: Option<String>) -> Self { let more = cursor.is_some(); Self { page, rows, cursor, dependency_models: BTreeSet::new(), payload: PageRows::default(), marker: None, more } }

    pub fn marked(marker: Marker) -> Self { Self { marker: Some(marker), ..Self::new(1, Vec::new(), None) } }
}

/// The marker of a media window: `revision`, the counts of its rows by kind and the sequence they stand for.
pub use crate::store::WindowMarker as Marker;

/// What a request of a window asks: a page, the marker of the media chain up to a sequence, or the tail after a sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    Page,
    Marker { up_to: i64 },
    Tail { after: i64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaPhase {
    MarkerCheck { up_to: i64 },
    Tail { after: i64 },
    Full,
    Confirm { up_to: i64 },
}

/// The pages of one load of a media window: they land in 1 batch after the marker of the end of the chain confirmed them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chain {
    pub phase: MediaPhase,
    pub cursor: Option<String>,
    pub ids: Vec<String>,
    pub rows: PageRows,
    pub models: BTreeSet<String>,
    pub latest: i64,
    pub stale: bool,
    /// The members the chain keeps: all of the saved window for a tail, none for a full read.
    pub base: i64,
}

impl Chain {
    pub fn kind(&self) -> RequestKind {
        match self.phase {
            MediaPhase::MarkerCheck { up_to } | MediaPhase::Confirm { up_to } => RequestKind::Marker { up_to },
            MediaPhase::Tail { after } => RequestKind::Tail { after },
            MediaPhase::Full => RequestKind::Page,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowOutcome {
    Loaded { revision: i64, has_more: bool, fetched: bool },
    Failed { failure: crate::ffi::DbError },
    PageLimit,
    Offline { message: String },
    Ended { cause: crate::owners::read::deliveries::EndCause },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowEffect {
    Request { request: RequestId, attempt: u32, page: u32, cursor: Option<String>, epoch: Epoch, kind: RequestKind },
    Cancel { request: RequestId, attempt: u32 },
    Outcome { id: InterestId, outcome: WindowOutcome },
    DerivePreload { revision: i64 },
}

pub use crate::store::SavedWindow;

pub(super) const REQUEST_TASK: EffectId = 1 << 62;
pub(super) const READ_TASK: EffectId = 1 << 61;

pub fn request_task(request: RequestId, attempt: u32) -> EffectId { REQUEST_TASK | (request << 20) | u64::from(attempt) }

pub(super) fn task_request(id: EffectId) -> (RequestId, u32) { ((id & !REQUEST_TASK) >> 20, (id & 0xF_FFFF) as u32) }

pub fn read_task(read_id: u64) -> EffectId { READ_TASK | read_id }

/// What a machine gives out: an effect of the runner, or an input for another owner or a timer that the owner `Windows` turns into `Effect`.
pub(super) enum Emit {
    Run(WindowEffect),
    Pin { epoch: Epoch, request: RequestId },
    Unpin { request: RequestId },
    Commit { request: RequestId, basis: BTreeMap<String, u64>, page: AnswerPage, number: u32, replace_all: bool, base_revision: i64, marker: Option<Marker>, keep_rows: bool },
    Retry { request: RequestId, attempt: u32, class: ErrorClass },
    CancelRetry { request: RequestId },
    Announce { state: WindowState, streams: Vec<InterestId> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowsEffect {
    Window { key: WindowKey, effect: WindowEffect },
    SnapshotRequest { epoch: Epoch },
    /// Read the saved window of `key` in the read pool; the answer is `WindowsInput::WindowRead`.
    ReadWindow { key: WindowKey, read_id: u64 },
    Drained(Drained),
}

/// Which keys of a model a `Stale` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StaleKeys {
    All,
    Set(BTreeSet<WindowKey>),
    /// Every window of the model whose relation is named, whatever its params.
    Relations(BTreeSet<String>),
}

impl StaleKeys {
    pub fn none() -> Self { Self::Set(BTreeSet::new()) }

    pub fn is_empty(&self) -> bool { matches!(self, Self::Set(set) if set.is_empty()) || matches!(self, Self::Relations(set) if set.is_empty()) }

    pub(super) fn names(&self, key: &WindowKey) -> bool {
        match self {
            Self::All => true,
            Self::Set(set) => set.contains(key),
            Self::Relations(relations) => matches!(key.kind(), WindowKind::Relation { relation, .. } if relations.contains(&relation)),
        }
    }

    pub fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Set(mut left), Self::Set(right)) => { left.extend(right); Self::Set(left) }
            (Self::Relations(mut left), Self::Relations(right)) => { left.extend(right); Self::Relations(left) }
            _ => Self::All,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StaleCause {
    Change,
    Gap { online_seq: u64 },
}

#[derive(Clone, Debug)]
pub enum WindowsInput {
    /// A stream opened the key (Delivery open) or the preload of the key began.
    InterestPlus { key: WindowKey, id: InterestId, now: i64 },
    /// An interest that only watches: the phase goes to the stream, no load starts.
    InterestPassive { key: WindowKey, id: InterestId },
    /// A call that goes to the server only was refused at once because the network is away: the error shows in the state of the window.
    OfflineRefused { key: WindowKey, message: String },
    /// The stream of `id` asks for a load of the key each time the network returns.
    ReloadOnConnect { key: WindowKey, id: InterestId },
    InterestMinus { key: WindowKey, id: InterestId },
    /// A platform call takes its place in the order of the loads when it begins, before its load is ready.
    Reserve { id: InterestId },
    /// The call of `id` ended before its `Load` came: its place and its held `Load` are void.
    Unreserve { id: InterestId },
    /// `begin_load`, `begin_load_all`, `refresh`, `load_more`, `node`, `nodes`. `store_revision` is the Published revision of the Store at the entry.
    Load { key: WindowKey, id: InterestId, goal: Goal, force: bool, store_revision: i64, now: i64 },
    /// `refresh`, `load_more`, `node(force)`: a `Load` that does not wait for the network. Without `online = Some(true)` the owner answers `Offline` and applies `OfflineRefused`.
    LoadNoWait { key: WindowKey, id: InterestId, goal: Goal, force: bool, store_revision: i64, now: i64, verb: &'static str },
    Pinned { request_id: RequestId, base_revision: u64 },
    Response { request: RequestId, attempt: u32, page: AnswerPage, now: i64 },
    Failure { request: RequestId, attempt: u32, error: RequestError },
    Dropped { request: RequestId, attempt: u32 },
    Committed(Committed),
    Rejected(Rejected),
    Stale { model: String, keys: StaleKeys, cause: StaleCause },
    RetryDue { request: RequestId },
    /// The executor of `DerivePreload` found these keys in the rows of a source window.
    PreloadKeys { keys: Vec<WindowKey>, now: i64 },
    /// The operations of the `.dbl` are declared: the preload keys of a live session start now.
    RegisterOperations { now: i64 },
    /// A Store broadcast `Committed{tables}`: the models whose tables changed and the revision.
    StoreCommitted { models: Vec<String>, revision: i64 },
    Online { online: Online, online_seq: u64, now: i64 },
    /// The device saves power: a key that only the preload watches waits (Network snapshot and broadcast).
    PowerSaving { saving: bool },
    /// The application came to the front; `seq` grows by 1 with every return.
    Foreground(u64),
    /// The answer of `ReadWindow`: the saved window of a key that had no machine.
    WindowRead { key: WindowKey, read_id: u64, result: Result<Option<SavedWindow>, String> },
    TaskFailed { id: EffectId },
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    ModelsReset(ModelsReset),
    Closing,
    Tick(Time),
}

/// An input of one machine.
#[derive(Clone, Debug)]
pub(super) enum Ev {
    Pinned { request: RequestId, base_revision: u64 },
    InterestPlus { id: InterestId, now: i64 },
    InterestPassive { id: InterestId },
    ReloadOnConnect { id: InterestId },
    OfflineRefused { message: String },
    InterestMinus { id: InterestId },
    Load { id: InterestId, goal: Goal, force: bool, store_revision: i64, now: i64 },
    /// The Store published this revision: a request issued from now on is based on it.
    StoreSeen { revision: i64 },
    Response { request: RequestId, attempt: u32, page: AnswerPage, now: i64 },
    /// `Failure` and `Dropped`: the cells are the same, a dropped request is a temporary network error.
    Failure { request: RequestId, attempt: u32, error: RequestError },
    Committed { request: RequestId, revision: i64 },
    Rejected { request: RequestId, reason: RejectReason },
    Stale { cause: StaleCause },
    Foreground,
    SourceChanged { revision: i64 },
    RetryDue { request: RequestId },
    Online { online: bool, online_seq: u64, now: i64 },
    PowerSaving { saving: bool },
    /// The saved window of the key: `None` before its first page or after a reset of its generation.
    Saved(Option<SavedWindow>),
    ModelsReset { generation: u64, models: Vec<String> },
    EpochEnded { cause: crate::owners::read::deliveries::EndCause },
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowStage {
    Opening,
    Idle,
    Loading,
    Landing,
    Waiting,
    Ended,
}

/// The policy that retries a failure of this class; a permanent failure has none.
pub(super) fn retry_policy(class: ErrorClass) -> Option<crate::runtime::RetryPolicy> {
    match class {
        ErrorClass::Network => Some(crate::runtime::RetryPolicy::QueryNetwork),
        ErrorClass::Server => Some(crate::runtime::RetryPolicy::QueryServer),
        ErrorClass::Permanent | ErrorClass::InvalidCursor => None,
    }
}

/// Whether the send after `sends` sends may go out for a failure of this class.
pub(super) fn may_retry(class: ErrorClass, sends: u32) -> bool { retry_policy(class).is_some_and(|policy| policy.allows(sends.saturating_add(1))) }

/// The request ids of the windows: the counter of the process, so no two lives of a database in 1 process share an id. A test of the owner counts its own ids from 1.
#[derive(Clone, Copy, Debug)]
pub enum RequestIds {
    Process,
    Local(RequestId),
}

impl RequestIds {
    pub(super) fn next(&mut self) -> RequestId {
        match self {
            Self::Process => crate::owners::contract::next_request_id(),
            Self::Local(next) => {
                *next += 1;
                *next - 1
            }
        }
    }
}

pub(super) struct Ctx<'a> {
    pub(super) request_ids: &'a mut RequestIds,
    pub(super) next_attempt: &'a mut u32,
}

impl Ctx<'_> {
    pub(super) fn request(&mut self) -> (RequestId, u32) {
        let request = self.request_ids.next();
        (request, self.attempt())
    }

    pub(super) fn attempt(&mut self) -> u32 {
        let attempt = *self.next_attempt;
        *self.next_attempt += 1;
        attempt
    }
}

/// What the state of a window that lands keeps: the request in the Store, the page it carries, the cursor and whether it has rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Landing {
    pub request: RequestId,
    pub page: u32,
    pub cursor: Option<String>,
    pub rows: bool,
    pub models: BTreeSet<String>,
}

/// What the state of a window that loads keeps: the request in flight and how often it was tried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Load {
    pub request: RequestId,
    pub step: RequestId,
    pub attempt: u32,
    pub retry: bool,
    pub pinning: bool,
    pub page: u32,
    pub force: bool,
    pub attempts: u32,
    pub basis: GenerationsAtIssue,
    pub chain: Option<Chain>,
    pub confirms: u32,
}

impl Load {
    /// A request that waits for its pin.
    pub fn issued(request: RequestId, attempt: u32, page: u32, force: bool, basis: GenerationsAtIssue) -> Self {
        Self { request, step: request, attempt, retry: false, pinning: true, page, force, attempts: 0, basis, chain: None, confirms: 0 }
    }
}

/// The generations of the models at the moment a request left.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationsAtIssue {
    pub(super) base: u64,
    pub(super) generations: BTreeMap<String, u64>,
}

impl GenerationsAtIssue {
    pub(super) fn of(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base) }
}
