//! Inputs shared by the owners. Directions A and C take these types from here.
//! Changing a type goes through the coordinator.

pub use crate::runtime::{Cause, Epoch, Time};

pub type RequestId = u64;

static NEXT_REQUEST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// The request id of a `Mutate`: one counter of the process for every author.
pub fn next_request_id() -> RequestId { NEXT_REQUEST.fetch_add(1, std::sync::atomic::Ordering::SeqCst) }

/// A request id read from the base is never issued again.
pub fn seen_request_id(id: RequestId) { NEXT_REQUEST.fetch_max(id.saturating_add(1), std::sync::atomic::Ordering::SeqCst); }
pub type ReplyId = u64;
pub type BatchId = u64;
pub type ConnectionId = u64;

/// The owner that wrote a batch. Store answers the pair `(author, id)`: `id` is unique within one author.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BatchAuthor {
    Lifecycle,
    Session,
    Window,
    Feed,
    Outbox,
    LocalWrites,
    Transfer,
}

/// `EpochStarted(e, identity, generation)`: a new epoch, `epoch` grows strictly. `identity = None` is an epoch without a session.
/// `generation` is the generation of every model at the start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochStarted {
    pub epoch: Epoch,
    pub identity: Option<u64>,
    pub generation: u64,
}

/// `EpochEnded(e, cause)`: the terminal outcome of every id of the epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochEnded {
    pub epoch: Epoch,
    pub cause: Cause,
}

/// `Online(bool)`: the network changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Online(pub bool);

/// `Committed(revision, tables, models)`: the Store reply to the author of `batch`, and the Store broadcast to Deliveries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub batch: BatchId,
    pub revision: i64,
    pub tables: Vec<String>,
    pub rows: ChangedRows,
    pub models: Models,
    /// Parent models whose `sets:` fields the batch did not write because the parent row is not in the base.
    pub stale_parents: Vec<String>,
    /// The version of the overlay of ephemeral values at `revision`.
    pub position: u64,
    /// The rows the batch showed, one per `Show` and per intent that names a row, read in the transaction of the batch.
    pub shown: Vec<Option<crate::store::Row>>,
    /// The files the batch deleted.
    pub files_removed: usize,
    /// P-DATA-99: the events of a Feed batch that the Store refused alone, by their place in the batch, with the reason; the rest of the batch landed.
    pub refused: Vec<(usize, RejectReason)>,
}

impl CommittedBroadcast {
    /// The input of `Deliveries`: a reset (`rows` none) is `All` and reads no row stream, a write touches the streams of `tables` and `rows`.
    pub fn for_deliveries(self) -> Committed {
        let models = if self.models == Models::All || self.rows.is_none() { Models::All } else { Models::default() };
        Committed { batch: 0, revision: i64::try_from(self.revision).unwrap_or(i64::MAX), tables: self.tables, rows: self.rows, models, stale_parents: Vec::new(), position: self.position, shown: Vec::new(), files_removed: 0, refused: Vec::new() }
    }
}

/// The `(table, row_id)` pairs a commit changed, from the Store journal of changed rows. `None` = every row of the tables.
pub type ChangedRows = Option<std::collections::BTreeSet<(String, String)>>;

/// The models a commit touched: `All` touches every stream, a `Set` touches the streams of its tables (the empty set touches none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Models {
    All,
    Set(std::collections::BTreeSet<String>),
}

impl Default for Models {
    fn default() -> Self { Models::Set(std::collections::BTreeSet::new()) }
}

/// The one notice of a commit, `Committed{revision, tables, rows, models, position}`: built by `CommittedBroadcast::of_commit` and `of_reset`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommittedBroadcast {
    pub revision: u64,
    pub position: u64,
    pub tables: Vec<String>,
    pub rows: ChangedRows,
    pub models: Models,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    NoSession,
    StaleGeneration { models: Vec<String>, generations: std::collections::BTreeMap<String, u64> },
    Storage(String),
    Closed,
    Invalid,
}

/// `Rejected`: the Store refused `batch`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejected {
    pub batch: BatchId,
    pub reason: RejectReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    Network,
    Server,
    Permanent,
    InvalidCursor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestError {
    pub class: ErrorClass,
    pub failure: crate::ffi::DbError,
}

impl RequestError {
    pub fn temporary(&self) -> bool { matches!(self.class, ErrorClass::Network | ErrorClass::Server) }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportOutcome<R> {
    Response(R),
    Error(RequestError),
    Dropped,
}

/// `Tick(now)`: the only clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tick(pub Time);

/// `Closing`: every owner gives its terminal outcomes and answers `Drained`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Closing;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Drained {
    pub owner: &'static str,
}

/// `Interest+(id, force)` to Windows; `all` marks `begin_load_all`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InterestPlus {
    pub id: u64,
    pub force: bool,
    pub all: bool,
}

/// `Interest-(id)` to Windows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InterestMinus {
    pub id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowPhase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

/// `WindowState{phase, error, loaded, has_more, settled}`: Windows to the Deliveries of the key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowState {
    pub phase: WindowPhase,
    pub error: Option<crate::ffi::DbError>,
    pub loaded: Option<i64>,
    pub has_more: bool,
    pub settled: bool,
    /// The rows of the membership at the revision of the value: set by `Deliveries` for a list.
    pub total: Option<u64>,
}

impl WindowState {
    pub fn idle() -> Self { Self { phase: WindowPhase::Idle, error: None, loaded: None, has_more: false, settled: false, total: None } }
}

/// `Want+(model)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WantPlus {
    pub model: String,
}

/// `Want-(model)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WantMinus {
    pub model: String,
}

/// `Joined(model, position)`: Feed reply to `Want+`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Joined {
    pub model: String,
    pub position: u64,
}

/// `ModelReady(model)`: the server subscription is alive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelReady {
    pub model: String,
}

/// `ModelLost{model, first, last}`: Feed positions that did not arrive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelLost {
    pub model: String,
    pub first: u64,
    pub last: u64,
}

/// `Published.state`: the one value the platform call rule reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishedState {
    NoSession,
    Session,
    Ending,
    EndingFailed,
    StorageFailed,
    Closing,
}

/// `Published{epoch, identity, generation, state, closed}`: Lifecycle to Store and Session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub epoch: Epoch,
    pub identity: Option<u64>,
    pub generation: u64,
    pub state: PublishedState,
    pub closed: bool,
}

impl Published {
    pub fn initial() -> Self { Self { epoch: 0, identity: None, generation: 0, state: PublishedState::NoSession, closed: false } }
}

/// `CredentialCommitted(identity)`: Session to Lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CredentialCommitted {
    pub identity: u64,
}

/// `ServerEnded{epoch}`: Session to Lifecycle, the server answered `Revoked`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerEnded {
    pub epoch: Epoch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreResult {
    Restored,
    WaitingDeclaration,
    RestoreFailed { error: String, retained: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkEvent {
    Online { online: bool, epoch: Option<Epoch> },
    Foreground(u64),
    PowerSaving { saving: bool, epoch: Option<Epoch> },
}
