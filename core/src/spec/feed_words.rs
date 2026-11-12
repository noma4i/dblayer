//! The inputs and the effects the `Feed` row tests name: a vocabulary, no second implementation.

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Reject {
    StaleGeneration(Vec<String>),
    Storage,
    Invalid,
    NoSession,
    Closed,
}

/// The connection an input names: the active one, the opening one, or one that is neither.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum To {
    Active,
    Opening,
    /// The connection that `Closing` left to drain.
    Draining,
    Stray,
}

/// The counter of an opened frame against the `recv_counter` of the connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Counter {
    Expected,
    Replayed,
}

/// A frame of the server that carries no model event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Ping,
    /// `complete` of the subscription of the connection.
    Complete,
    /// `complete` and `next` of another subscription id.
    OtherId,
    /// `ready`, `subscribed`, `pong`.
    Quiet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Reread {
    None,
    All,
    Set(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Input {
    WantPlus { model: String, id: u64, stream: bool },
    WantMinus { model: String, id: u64 },
    HandshakeDone,
    FrameOpened { to: To, counter: Counter, events: Vec<(String, String)> },
    Frame { to: To, counter: Counter, kind: Kind },
    ProtocolError { to: To },
    Event { to: To, model: String, payload: String },
    Dropped { to: To },
    /// `SubscriptionEnded`: the cells of `Dropped`.
    Ended { to: To },
    Resubscribe { to: To },
    /// The deadline of a `Connect`: a connection in `Handshake` fails like a protocol error.
    HandshakeDeadline { to: To },
    Committed,
    CommittedAt(usize),
    RejectedAt(usize, Reject),
    Refused { model: String },
    /// `Committed` of a batch whose children left parents without a row: the windows of these models turn `Stale`.
    CommittedStale(Vec<String>),
    /// `Committed` of a batch whose events at these places the Store refused alone (P-DATA-99).
    CommittedRefused(Vec<usize>),
    Rejected(Reject),
    /// `Committed` or `Rejected` of a batch that is not in `landing`.
    StrayBatch,
    StrayRepeat,
    Tick,
    /// `Tick(keepalive{connection_id})` of the active connection.
    Keepalive,
    /// `Tick(pong_deadline{connection_id})` of the active connection.
    PongDeadline,
    Online(bool),
    EpochEnded,
    Closing,
    /// `Tick(drain{connection_id})`: `own` names the draining connection, otherwise a stray one.
    Drain { own: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Effect {
    Connect,
    SealSubscribe,
    SealUnsubscribe,
    SealPong,
    SealPing,
    SchedulePong,
    Close,
    Cancel,
    ScheduleDrain,
    CancelDrain,
    ScheduleTick,
    ScheduleResubscribe,
    Gap(Vec<String>),
    Commit,
    Stale { model: String, all: bool },
    StaleSet { model: String, keys: Vec<String> },
    Joined(u64),
    ModelReady(u64),
    Drained,
}

/// What the stream `stream` of Deliveries sees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Obs {
    Event { stream: u64, position: u64, payload: String },
    Lost { stream: u64, first: u64, last: u64 },
    Gap { stream: u64 },
}
