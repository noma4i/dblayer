/// `(model, relation, params)`, `(model, node, id)` or `(model, node, ids)` as one name; `model` is the model of the window.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WindowKey {
    pub model: String,
    pub name: String,
}

impl WindowKey {
    const NODES: &'static str = "nodes:";

    /// The key `(model, node, ids)`: the order of `ids` is the order of the call.
    pub fn nodes(model: &str, ids: &[&str]) -> Self { Self { model: model.into(), name: format!("{}{}", Self::NODES, serde_json::to_string(ids).unwrap_or_default()) } }

    fn is_empty_nodes(&self) -> bool { matches!(self.kind(), WindowKind::Nodes { ids } if ids.is_empty()) }

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
enum Want {
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
}

impl AnswerPage {
    pub fn new(page: u32, rows: Vec<String>, cursor: Option<String>) -> Self { Self { page, rows, cursor, dependency_models: BTreeSet::new(), payload: PageRows::default() } }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowOutcome {
    Loaded { revision: i64, has_more: bool, fetched: bool },
    Failed { error: String, source: Option<crate::transport::TransportError> },
    PageLimit,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowEffect {
    Request { request: RequestId, attempt: u32, page: u32, cursor: Option<String>, epoch: Epoch },
    Cancel { request: RequestId, attempt: u32 },
    Outcome { id: InterestId, outcome: WindowOutcome },
    DerivePreload { revision: i64 },
}

/// What a machine gives out: an effect of the runner, or an input for another owner or a timer that the owner `Windows` turns into `Effect`.
enum Emit {
    Run(WindowEffect),
    Commit { request: RequestId, basis: BTreeMap<String, u64>, page: AnswerPage, number: u32, replace_all: bool, base_revision: i64 },
    Retry { request: RequestId, attempt: u32, class: ErrorClass },
    CancelRetry { request: RequestId },
    Announce { state: WindowState, streams: Vec<InterestId> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowsEffect {
    Window { key: WindowKey, effect: WindowEffect },
    SnapshotRequest { epoch: Epoch },
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

    fn names(&self, key: &WindowKey) -> bool {
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

#[derive(Clone, Debug)]
pub enum WindowsInput {
    /// A stream opened the key (Delivery open) or the preload of the key began.
    InterestPlus { key: WindowKey, id: InterestId },
    /// An interest that only watches: the phase goes to the stream, no load starts.
    InterestPassive { key: WindowKey, id: InterestId },
    /// A call that goes to the server only was refused at once because the network is away: the error shows in the state of the window.
    OfflineRefused { key: WindowKey, message: String },
    /// The stream of `id` asks for a load of the key each time the network returns.
    ReloadOnConnect { key: WindowKey, id: InterestId },
    InterestMinus { key: WindowKey, id: InterestId },
    /// `begin_load`, `begin_load_all`, `refresh`, `load_more`, `node`, `nodes`. `store_revision` is the Published revision of the Store at the entry.
    Load { key: WindowKey, id: InterestId, goal: Goal, force: bool, store_revision: i64 },
    Response { request: RequestId, attempt: u32, page: AnswerPage },
    Failure { request: RequestId, attempt: u32, error: RequestError },
    Dropped { request: RequestId, attempt: u32 },
    Committed(Committed),
    Rejected(Rejected),
    Stale { model: String, keys: StaleKeys },
    RetryDue { request: RequestId },
    /// The executor of `DerivePreload` found these keys in the rows of a source window.
    PreloadKeys { keys: Vec<WindowKey> },
    /// A Store broadcast `Committed{tables}`: the models whose tables changed and the revision.
    StoreCommitted { models: Vec<String>, revision: i64 },
    Online(Online),
    /// The application came to the front; `seq` grows by 1 with every return.
    Foreground(u64),
    EpochStarted(EpochStarted),
    EpochEnded(EpochEnded),
    ModelsReset(ModelsReset),
    Closing,
    Tick(Time),
}

/// An input of one machine.
#[derive(Clone, Debug)]
enum Ev {
    InterestPlus { id: InterestId },
    InterestPassive { id: InterestId },
    ReloadOnConnect { id: InterestId },
    OfflineRefused { message: String },
    InterestMinus { id: InterestId },
    Load { id: InterestId, goal: Goal, force: bool, store_revision: i64 },
    /// The Store published this revision: a request issued from now on is based on it.
    StoreSeen { revision: i64 },
    Response { request: RequestId, attempt: u32, page: AnswerPage },
    /// `Failure` and `Dropped`: the cells are the same, a dropped request is a temporary network error.
    Failure { request: RequestId, attempt: u32, error: RequestError },
    Committed { request: RequestId, revision: i64 },
    Rejected { request: RequestId, reason: RejectReason },
    Stale,
    Foreground,
    SourceChanged { revision: i64 },
    RetryDue { request: RequestId },
    Online(bool),
    ModelsReset { generation: u64, models: Vec<String> },
    EpochEnded,
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowStage {
    Idle,
    Loading,
    Landing,
    Waiting,
    Ended,
}

/// Attempts of a window per error class.
fn attempt_limit(class: ErrorClass) -> u32 {
    match class {
        ErrorClass::Network => 3,
        ErrorClass::Server => 2,
        ErrorClass::Permanent | ErrorClass::InvalidCursor => 0,
    }
}

struct Ctx<'a> {
    next_request: &'a mut RequestId,
    next_attempt: &'a mut u32,
}

impl Ctx<'_> {
    fn request(&mut self) -> (RequestId, u32) {
        let request = *self.next_request;
        *self.next_request += 1;
        (request, self.attempt())
    }

    fn attempt(&mut self) -> u32 {
        let attempt = *self.next_attempt;
        *self.next_attempt += 1;
        attempt
    }
}

/// The generations of the models at the moment a request left.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationsAtIssue {
    base: u64,
    generations: BTreeMap<String, u64>,
}

impl GenerationsAtIssue {
    fn of(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base) }
}
