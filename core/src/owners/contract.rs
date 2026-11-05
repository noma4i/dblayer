//! Inputs shared by the owners (`specs/machines/design.md` section 3). Directions A and C take these types from here.
//! Changing a type goes through the coordinator.

pub use crate::runtime::{Cause, Epoch, Time};

pub type RequestId = u64;
pub type BatchId = u64;
pub type ConnectionId = u64;

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
    pub models: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    NoSession,
    StaleGeneration { models: Vec<String> },
    Storage(String),
    Closed,
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestError {
    pub message: String,
    pub class: ErrorClass,
}

impl RequestError {
    pub fn temporary(&self) -> bool { self.class != ErrorClass::Permanent }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Ready,
    Event { model: String, payload: String },
    ChannelLost { dropped: u64, models: Vec<String> },
    Dropped,
    Ended,
}

/// `SubscriptionFrame{connection_id, Ready/Event/ChannelLost/Dropped/Ended}`: a frame of the subscription connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionFrame {
    pub connection_id: ConnectionId,
    pub frame: Frame,
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

/// `Stale(model)`: Feed to Windows, the windows of the model are out of date.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stale {
    pub model: String,
}

/// `Published{epoch, identity, generation, closed}`: Lifecycle to Store and Session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub epoch: Epoch,
    pub identity: Option<u64>,
    pub generation: u64,
    pub closed: bool,
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
