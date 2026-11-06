// The inputs, the effects and the state of the `window` reference.

/// Observables shared by the references and the core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Idle,
    Loading,
    Loaded,
    Failed,
}

pub(super) const PRELOAD: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Result3 {
    Loaded,
    Failed,
    PageLimit,
    Interrupted,
}

/// Failure classes of a transport error: the first two are temporary and retried up to `retry_limit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Failure {
    Network,
    Server,
    Permanent,
    InvalidCursor,
}

impl Failure {
    /// Attempts of a window per class.
    pub(super) fn retry_limit(self) -> u32 {
        match self {
            Failure::Network => 3,
            Failure::Server => 2,
            Failure::Permanent | Failure::InvalidCursor => 0,
        }
    }

    fn temporary(self) -> bool { matches!(self, Failure::Network | Failure::Server) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Reject {
    StaleGeneration,
    NoSession,
    Closed,
    Storage,
    Invalid,
}

/// What a `Load` asks for: the first page, the page after the last one the caller saw, or every page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Goal {
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

/// The marker of a media window as the reference counts it: the sequence, the revision and the number of the rows its counts name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Marker3 {
    pub(super) sequence: i64,
    pub(super) revision: i64,
    pub(super) count: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage3 {
    MarkerCheck { up_to: i64 },
    Tail { after: i64 },
    Full,
    Confirm { up_to: i64 },
}

/// The pages of one load of a media window: `rows` pages counted, `base` rows of the saved window the tail keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Chain3 {
    stage: Stage3,
    cursor: Option<u32>,
    rows: i64,
    latest: i64,
    base: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
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
    Foreground { seq: u64 },
    SourceChanged { revision: i64 },
    Online(bool),
    EpochEnded,
    ModelsReset { own_model: bool },
    Closing,
    /// The answer of the read pool for the saved window of the key.
    Read(Read),
    PowerSaving(bool),
    OfflineRefused,
}

/// What the read pool answers: the saved window (`generation_ok` = the generation did not change), nothing, an error, or the answer of another read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Read {
    Saved { landed_at: i64, fresh_until: i64, stale: bool, generation_ok: bool },
    /// A saved media window that is not fresh: its marker and its member count.
    Media { marker: Marker3, members: i64 },
    None,
    Failed,
    Stray,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum State {
    Opening,
    Idle,
    Loading { request: u64, attempt: u32, retry: bool, page: u32, force: bool, attempts: u32 },
    Landing { request: u64, page: u32, cursor: Option<u32>, rows: bool },
    Waiting { force: bool, page: u32 },
    Ended,
}

pub(super) struct Window {
    pub(super) state: State,
    pub(super) online: Option<bool>,
    pub(super) fresh: bool,
    /// The page the next cursor points at, `None` = no more pages.
    pub(super) cursor: Option<u32>,
    pub(super) last_error: Option<bool>,
    pub(super) interest: BTreeSet<u32>,
    pub(super) waiters: BTreeMap<u32, Want>,
    pub(super) reread_due: bool,
    /// The last page that landed.
    pub(super) page: u32,
    pub(super) page_count: u32,
    pub(super) max_pages: u32,
    pub(super) members_complete: bool,
    pub(super) cursor_reset_used: bool,
    pub(super) replace_all: bool,
    pub(super) has_rows: bool,
    pub(super) landed_revision: i64,
    pub(super) foreground_seen: u64,
    pub(super) phase: Phase,
    pub(super) settled: bool,
    pub(super) outcomes: Vec<(u32, Result3)>,
    /// The `now` of the input, set by the test before every step.
    pub(super) now: i64,
    pub(super) landed_at: i64,
    pub(super) fresh_until: i64,
    answered_at: i64,
    /// `expires_in` and `empty_in`.
    pub(super) lifetime: (i64, i64),
    pub(super) saving: bool,
    /// The key has had no machine: its first creating input reads the saved window.
    unread: bool,
    held: Vec<Input>,
    next_request: u64,
    next_attempt: u32,
    pub(super) media: bool,
    pub(super) marker: Option<Marker3>,
    pub(super) members: i64,
    chain: Option<Chain3>,
    landing_marker: Option<(Marker3, i64)>,
}
