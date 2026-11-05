//! Inputs shared by the owners (`specs/machines/design.md` section 3). Directions A and C take these types from here.
//! Changing a type goes through the coordinator.

pub use crate::runtime::{Cause, Epoch, Time};

pub type RequestId = u64;
pub type BatchId = u64;
pub type ConnectionId = u64;

/// `EpochStarted(e, identity)`: a session began, `epoch` grows strictly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochStarted {
    pub epoch: Epoch,
    pub identity: Option<u64>,
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

/// `Committed`: the Store reply to the author of `batch`, and the Store broadcast of `revision` and the touched `tables`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub batch: BatchId,
    pub revision: i64,
    pub tables: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    NoSession,
    StaleGeneration { models: Vec<String>, generation: u64 },
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
    Lost { dropped: u64, models: Vec<String> },
    Dropped,
    Ended,
}

/// `SubscriptionFrame{epoch, gen, Event/Lost/Ready/Ended}`: a frame of the subscription connection `connection`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionFrame {
    pub epoch: Epoch,
    pub connection: ConnectionId,
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
