use super::*;

/// The result of a platform call, the payload of `Outcomes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifeReply {
    CloudEnabled(Result<bool, String>),
    Life(Result<(), LifecycleError>),
    Online { closed: bool },
    Call(CallAnswer),
    Session(SessionAnswer),
    SessionResume(Result<Option<String>, crate::owners::session::SessionError>),
    Mutated(AuthorOutcome),
    Outbox(OutboxAnswer),
    Transfer(TransferAnswer),
    /// The outcome of a load that a platform call waited for in `Windows`.
    Window(Box<crate::owners::read::windows::WindowOutcome>),
    /// `Deliveries` closed the sink of a stream: the `cancel` of the platform returns.
    Cancelled,
    /// `Deliveries` answered the claim of the reader of a stream.
    Reader { claimed: bool },
    /// `Deliveries` counted its streams for `subscription_census`.
    Streams { observed: usize, retained: usize },
    /// `Feed` counted its channels for `subscription_census`.
    Channels { subscriptions: usize, refused: u64 },
    Closed,
}

