//! Inputs shared by the owners. Directions A and C take these types from here.
//! Changing a type goes through the coordinator.

pub use crate::runtime::{Cause, Epoch, Time};

pub type RequestId = u64;
pub type ReplyId = u64;
pub type BatchId = u64;
pub type ConnectionId = u64;

/// The owner that wrote a batch. Store answers the pair `(author, id)` (`store.md`): `id` is unique within one author.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BatchAuthor {
    Lifecycle,
    Session,
    Window,
    Feed,
    Outbox,
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

/// `ModelsReset(e, generation, models)`: the model cache was erased, the generation grew.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelsReset {
    pub epoch: Epoch,
    pub generation: u64,
    pub models: Vec<String>,
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
    pub models: Models,
}

/// The models a commit touched: `All` touches every stream, a `Set` touches the streams of its tables (the empty set touches none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Models {
    All,
    Set(std::collections::BTreeSet<String>),
}

impl Default for Models {
    fn default() -> Self { Models::Set(std::collections::BTreeSet::new()) }
}

/// `Committed.models` (D-34): `All` touches every stream, an empty `Set` touches none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommittedModels {
    All,
    Set(Vec<String>),
}

/// The Store broadcast `Committed{revision, tables, models}` to every subscriber (Deliveries).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedBroadcast {
    pub revision: u64,
    pub tables: Vec<String>,
    pub models: CommittedModels,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    NoSession,
    /// `generation` is the current one of the stale models: the author sends the batch again with it.
    StaleGeneration { models: Vec<String>, generation: u64 },
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
    pub message: String,
    pub class: ErrorClass,
    pub source: Option<crate::transport::TransportError>,
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

/// `Transport{req_id, Response/Error/Dropped}`: the outcome of a request, to its author.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transport<R> {
    pub req_id: RequestId,
    pub outcome: TransportOutcome<R>,
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

/// `WindowState{phase, error, loaded}`: Windows to the Deliveries of the key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowState {
    pub phase: WindowPhase,
    pub error: Option<String>,
    pub loaded: Option<i64>,
}

impl WindowState {
    pub fn idle() -> Self { Self { phase: WindowPhase::Idle, error: None, loaded: None } }
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

/// `ModelEvent{model, position, payload}`: Feed to the Deliveries of the model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventAtPosition {
    pub model: String,
    pub position: u64,
    pub payload: String,
}

/// `ModelLost{model, first, last}`: Feed positions that did not arrive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelLost {
    pub model: String,
    pub first: u64,
    pub last: u64,
}

/// `Published.state` of `lifecycle.md`: the one value the platform call rule reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishedState {
    NoSession,
    Session,
    Ending,
    EndingFailed,
    Resetting,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreItem {
    pub key: String,
    pub result: RestoreResult,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkEvent {
    Online { online: bool, epoch: Option<Epoch> },
    Foreground(u64),
    PowerSaving { saving: bool, epoch: Option<Epoch> },
}
