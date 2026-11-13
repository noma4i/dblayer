//! The vocabulary of the Windows stand: the inputs it feeds to the owner and the effects it reads back. No logic.

/// Observables shared by the references and the core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Phase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

pub(in super::super) const PRELOAD: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Result3 {
    Loaded,
    Failed,
    PageLimit,
    Interrupted,
}

/// Failure classes of a transport error: the first two are temporary and retried up to `retry_limit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Failure {
    Network,
    Server,
    Permanent,
    InvalidCursor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Reject {
    NoSession,
    Closed,
    Storage,
    Invalid,
}

/// What a `Load` asks for: the first page, the page after the last one the caller saw, or every page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Goal {
    First,
    Next,
    All,
}

/// The marker of a media window as the reference counts it: the sequence, the revision and the number of the rows its counts name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) struct Marker3 {
    pub(in super::super) sequence: i64,
    pub(in super::super) revision: i64,
    pub(in super::super) count: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) enum Input {
    /// The answer of a marker request.
    MarkerAnswer { request: u64, attempt: u32, marker: Marker3 },
    /// A page of a media chain that says it has more pages and names no cursor.
    MoreWithoutCursor { request: u64, attempt: u32 },
    InterestPlus { id: u32 },
    InterestPassive { id: u32 },
    InterestMinus { id: u32 },
    Load { id: u32, goal: Goal, force: bool },
    Response { request: u64, attempt: u32, cursor: Option<u32>, rows: bool },
    TransportError { request: u64, attempt: u32, failure: Failure },
    Dropped { request: u64, attempt: u32 },
    Retry { request: u64 },
    Committed { request: u64, revision: i64 },
    Rejected { request: u64, reason: Reject },
    Stale { in_keys: bool },
    StaleGap,
    Foreground { seq: u64 },
    SourceChanged { revision: i64 },
    Online(bool),
    EpochEnded,
    Closing,
    /// The answer of the read pool for the saved window of the key.
    Read(Read),
    PowerSaving(bool),
    OfflineRefused,
}

/// What the read pool answers: the saved window (`generation_ok` = the generation did not change), nothing, an error, or the answer of another read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum Read {
    Saved { landed_at: i64, fresh_until: i64, stale: bool, generation_ok: bool },
    /// A saved media window that is not fresh: its marker and its member count.
    Media { marker: Marker3, members: i64 },
    /// A saved media window that still owes a refetch (`stale_since`): its marker and its member count.
    MediaMarked { marker: Marker3, members: i64 },
    None,
    Failed,
    Stray,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) enum Effect {
    ReadWindow,
    Request { request: u64, attempt: u32, page: u32 },
    Marker { request: u64, attempt: u32, up_to: i64 },
    Tail { request: u64, attempt: u32, after: i64 },
    Cancel { request: u64, attempt: u32 },
    Commit { request: u64, replace_all: bool },
    ScheduleRetry { request: u64 },
    WindowState,
    Drained,
    DerivePreload { revision: i64 },
}
