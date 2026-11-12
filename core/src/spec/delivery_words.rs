//! The inputs and the effects the `Deliveries` row tests name: a vocabulary, no second implementation.
//! A row is `(id, stamp)`: the stamp changes when the content of the row changes. A value a `Pull` reads is the effect `Answer`.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Ws {
    pub(super) phase: Phase,
    pub(super) error: bool,
    pub(super) loaded: Option<i64>,
}

impl Ws {
    pub(super) fn idle() -> Self { Self { phase: Phase::Idle, error: false, loaded: None } }

    pub(super) fn loaded_at(revision: i64) -> Self { Self { phase: Phase::Loaded, error: false, loaded: Some(revision) } }

    pub(super) fn loading_after(revision: i64) -> Self { Self { phase: Phase::Loading, error: false, loaded: Some(revision) } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cause {
    SessionEnded,
    Closed,
    Cancelled,
    Invalid,
    Storage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fetch {
    Never,
    Stale,
    Force,
    IfMissing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    List,
    Entity(Fetch),
    Count,
    Events,
    Changes,
    Blob,
}

pub(super) type Row = (String, i64);

pub(super) fn row(id: &str) -> Row { (id.to_string(), 0) }

pub(super) fn rows(ids: &[&str]) -> Vec<Row> { ids.iter().map(|id| row(id)).collect() }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    Open { kind: Kind, epoch_current: bool },
    OpenNoSession,
    OpenInvalid,
    Committed { revision: i64, touches: bool },
    Changed { revision: i64, rows: Vec<(String, String)>, reset: bool },
    ReadDone { revision: i64, rows: Vec<Row> },
    ReadRelevant { revision: i64, position: u64, relevant: Option<(i64, u64)>, rows: Vec<Row> },
    ReadOverflow { revision: i64 },
    CountDone { revision: i64, count: u64 },
    ReadStale { revision: i64, rows: Vec<Row> },
    ReadFailed { revision: i64 },
    ReadFailedStale,
    WindowState(Ws),
    Joined { position: u64 },
    ModelReady,
    ModelEvent { position: u64, payload: String },
    ModelLost { first: u64, last: u64 },
    ModelGap,
    Resync,
    Transfer { state: String, revision: i64 },
    Pull,
    PullCancelled { matched: bool },
    Cancel { reply: u64 },
    Release,
    SinkClosed,
    EpochEnded { own: bool },
    EpochClosed,
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Ed {
    Insert { index: u32, id: String },
    Rewrite { index: u32, id: String },
    Remove { index: u32, id: String },
    Move { from: u32, to: u32 },
}

pub(super) fn insert(index: u32, id: &str) -> Ed { Ed::Insert { index, id: id.to_string() } }

pub(super) fn remove(index: u32, id: &str) -> Ed { Ed::Remove { index, id: id.to_string() } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Val {
    Snapshot { revision: i64, rows: Vec<String>, state: Ws },
    Delta { base: i64, revision: i64, size: usize, edits: Vec<Ed>, state: Ws },
    Count { revision: i64, count: u64, state: Ws },
    State { revision: i64, state: Ws },
    Ready,
    Changes { revision: i64, rows: Vec<(String, String)> },
    Event { position: u64, payload: String },
    Lost { first: u64, last: u64 },
    Gap,
    Status { revision: i64, state: String },
    Ended(Cause),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Pulled {
    Value(Val),
    Done,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Read(i64),
    InterestPlus,
    InterestPassive,
    Load { force: bool },
    InterestMinus,
    WantPlus,
    WantMinus,
    StatusInterest(bool),
    Available,
    CloseSink,
    Cancelled(u64),
    Answer(Pulled),
    Drained,
}
