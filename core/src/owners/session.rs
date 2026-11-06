//! Owner `SessionCalls`: the credential, `sequence`, `encrypted_session` and `encrypted_pending`.

use std::collections::{BTreeSet, VecDeque};
use std::time::Duration;

use super::contract::{BatchId, Committed, CredentialCommitted, Drained, EpochEnded, ErrorClass, Online, Published, PublishedState, RejectReason, Rejected, ReplyId, RequestId, ServerEnded};
use super::read::deliveries::StreamId;
pub use crate::store::session_state::StoredAnswer;
use crate::runtime::{Cause, Effect, EffectId, Effects, Epoch, ScheduleId, StateOwner, Time};

pub type Digest = u64;
pub type AuthorId = u64;
pub type AttemptId = u64;
pub type SecretOpId = u64;

pub const OUTBOX_AUTHOR: AuthorId = 1;
pub const TRANSFER_AUTHOR: AuthorId = 2;
pub const CALLS_AUTHOR: AuthorId = 3;

/// The owner of a `Mutate`: `encrypted_pending` keeps it, and a pending restored at open answers the author this owner names.
pub type MutateOwner = crate::transport::RequestOwner;

pub fn author_of(owner: &MutateOwner) -> Option<AuthorId> {
    match owner {
        MutateOwner::Operation { .. } | MutateOwner::Unmatched { .. } => Some(OUTBOX_AUTHOR),
        MutateOwner::Transfer { .. } => Some(TRANSFER_AUTHOR),
        MutateOwner::Detached => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionError {
    Invalid,
    Rejected(RejectReason),
    Session(crate::transport::SessionCode),
    Storage(String),
    SessionExists,
    NoCredential,
    Revoked,
    StateLost,
    SequenceConflict,
    Protocol,
    Unauthorized,
    Security { code: crate::transport::SecurityCode, detail: String },
    Http { status: u16 },
    TooLarge { size: u64, limit: u64 },
    Interrupted,
    NoSession,
    Closed,
}

pub type SessionAnswer = Result<String, SessionError>;

/// The row of `encrypted_answer` for a terminal outcome of a `Mutate`; the other errors of a call never land.
pub fn stored_answer(answer: &SessionAnswer) -> Option<StoredAnswer> {
    match answer {
        Ok(body) => Some(StoredAnswer::Ok(body.clone())),
        Err(SessionError::Http { status }) => Some(StoredAnswer::Http { status: *status }),
        Err(SessionError::TooLarge { size, limit }) => Some(StoredAnswer::TooLarge { size: *size, limit: *limit }),
        Err(SessionError::Unauthorized) => Some(StoredAnswer::Unauthorized),
        Err(SessionError::Security { code, detail }) => Some(StoredAnswer::Security { code: *code, detail: detail.clone() }),
        Err(SessionError::Revoked) => Some(StoredAnswer::Revoked),
        Err(SessionError::Session(code)) => Some(StoredAnswer::Session(*code)),
        Err(_) => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    pub digest: Digest,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationClass {
    Login,
    Authenticated,
    Sessionless,
    Logout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionOperation {
    pub name: String,
    pub path: String,
    pub class: OperationClass,
}

/// `pending` of the table: the row `encrypted_pending`, by value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCall {
    pub request_id: RequestId,
    pub sequence: u64,
    pub class: OperationClass,
    pub operation: String,
    pub path: String,
    pub body: String,
    pub credential: Option<Digest>,
    pub purpose: u8,
    pub author: Option<AuthorId>,
    pub owner: Option<MutateOwner>,
    pub epoch: Epoch,
    pub reply: Option<ReplyId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Queued {
    Call { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<MutateOwner>, req_id: RequestId, epoch: Epoch, path: String, body: String, purpose: u8 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoginCall {
    pub operation: SessionOperation,
    pub body: String,
    pub reply: ReplyId,
}

impl From<LoginCall> for Queued {
    fn from(call: LoginCall) -> Self {
        Self::Call { operation: call.operation.name, body: call.body, reply: call.reply }
    }
}

/// What `CredentialStore.get` found: the digest of the secret, nothing, or an access error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretState {
    Present(Digest),
    Missing,
    Error(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptPhase {
    InFlight(AttemptId),
    WaitingRetry,
    WaitingNetwork,
    WaitingSecret,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Block {
    Storage,
    SequenceConflict,
    Protocol,
    StateLost,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AfterStore {
    Persist,
    Clear(SessionError),
    Drain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AfterClear {
    Idle,
    Drain,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionPhase {
    Idle,
    Sealing { pending: PendingCall, seal_id: u64 },
    Generating { call: LoginCall, op: SecretOpId },
    Storing { call: LoginCall, credential_new: Digest, op: SecretOpId, after: AfterStore },
    ClearingSecret { call: Option<Queued>, reason: Option<SessionError>, op: SecretOpId, after: AfterClear },
    PendingHeld { pending: PendingCall },
    SecretMismatch,
    SecretUnavailable { error: String, read: Option<SecretOpId> },
    Persisting { pending: PendingCall, batch: BatchId },
    Calling { pending: PendingCall, attempts: u64, phase: AttemptPhase, document_sent: bool },
    Landing { pending: PendingCall, batch: BatchId, answer: SessionAnswer, identity: Option<Digest>, sequence: Option<u64>, server_ended: bool },
    Blocked { pending: Option<PendingCall>, reason: Block, landing: Option<Landed> },
    Closed,
}

/// The answer of a landing that the store refused: the same batch commits again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Landed {
    pub answer: SessionAnswer,
    pub identity: Option<Digest>,
    pub sequence: Option<u64>,
    pub server_ended: bool,
}

/// What the author of a `Mutate` hears: `Transport{req_id, Response(body) / Error{class, X} / Dropped}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorOutcome {
    Response(String),
    Error { class: ErrorClass, error: SessionError, pending_retained: bool },
    Dropped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryPhase {
    Idle,
    InFlight,
    WaitingRetry,
    WaitingNetwork,
    WaitingSecret,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionEffect {
    SealProbe { seal_id: u64, pending: PendingCall, credential: Option<Digest> },
    /// Store `Commit{encrypted_pending = pending}`, with `Session{digest, 0}` at the first sign-in.
    CommitPending { batch: BatchId, epoch: Epoch, pending: PendingCall, session: Option<Digest> },
    /// Store `Commit{Session{digest or the previous one, sequence}, RemovePending, Answer}`; no `sequence` for a call without a session.
    /// `erase_session`: the sign-in failed, `encrypted_session` of its new credential goes with the pending.
    CommitLanding { batch: BatchId, epoch: Epoch, digest: Option<Digest>, sequence: Option<u64>, answer: Option<(RequestId, SessionAnswer)>, erase_session: bool },
    HttpRequest { pending: PendingCall, attempt_id: AttemptId, full_document: bool },
    HttpCancel { attempt_id: AttemptId },
    /// `Schedule Tick` by the Backoff of Http for the attempt number `attempts`. `step` turns it into the Runtime effect `Schedule` (R5).
    ScheduleTick { attempts: u64 },
    GenerateCredential { secret_op_id: SecretOpId },
    StoreSecret { credential: Digest, secret_op_id: SecretOpId },
    ClearSecret { secret_op_id: SecretOpId },
    ReadSecret { secret_op_id: SecretOpId },
    CredentialCommitted(CredentialCommitted),
    ServerEnded(ServerEnded),
    /// Lifecycle `SessionReset{reply = None}`.
    SessionReset,
    /// Lifecycle `RestoreClassified{Session, epoch}`.
    RestoreClassified { epoch: Epoch },
    /// Network `SnapshotRequest{Session, epoch}`.
    SnapshotRequest { epoch: Epoch },
    Reply { id: ReplyId, answer: SessionAnswer },
    ResumeReply { id: ReplyId, answer: Result<Option<String>, SessionError> },
    ExecutorFailed,
    Answer { author: AuthorId, req_id: RequestId, outcome: AuthorOutcome },
    RetryState { stream_id: StreamId, attempts: u64, phase: RetryPhase },
    Drained(Drained),
}

#[derive(Clone, Debug)]
pub enum SessionInput {
    Sealed { seal_id: u64, size: u64 },
    SealFailed { seal_id: u64, error: SessionError },
    Open { credential: Option<Credential>, pending: Option<PendingCall>, dropped_pending: bool, secret: SecretState },
    SessionCall { operation: String, body: String, reply: ReplyId },
    SessionResume { reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<MutateOwner>, req_id: RequestId, epoch: Epoch, path: String, body: String, purpose: u8 },
    /// Outbox, Transfer and Calls take their request back: the one in the network goes on, the one in the queue is removed.
    Cancel { req_id: RequestId },
    /// The author gives up the outcome of its request: a held pair goes on with no author.
    Release { req_id: RequestId },
    CredentialGenerated { secret_op_id: SecretOpId, credential: Digest },
    SecretStored { secret_op_id: SecretOpId, ok: bool },
    SecretCleared { secret_op_id: SecretOpId, ok: bool },
    SecretRead { secret_op_id: SecretOpId, read: SecretState },
    SecretMissing { request_id: RequestId, attempt_id: AttemptId, lost: bool },
    /// The gate of the epoch refused the request before the transport call.
    GateClosed { request_id: RequestId, attempt_id: AttemptId },
    Foreground { seq: u64 },
    InterestPlus { stream_id: StreamId },
    InterestMinus { stream_id: StreamId },
    RestoreReady { epoch: Epoch },
    RestoreFailed { epoch: Epoch, error: String },
    /// The snapshot of `ReadIntents`: Session classifies `pending`; `answers` go to the authors, `pending_lost` is the dropped `encrypted_pending` (also `Open.dropped_pending`).
    Restored { epoch: Epoch, pending: Option<PendingCall>, answers: std::collections::BTreeMap<RequestId, StoredAnswer>, pending_lost: bool },
    Response { request_id: RequestId, attempt_id: AttemptId, body: String, sequence: Option<u64>, refusal: Option<crate::transport::SessionCode> },
    Revoked { request_id: RequestId, attempt_id: AttemptId, body: String },
    HttpError { request_id: RequestId, attempt_id: AttemptId, temporary: bool, auth: bool, persisted_missing: bool, status: u16, too_large: Option<(u64, u64)>, security: Option<(crate::transport::SecurityCode, String)> },
    Dropped { request_id: RequestId, attempt_id: AttemptId },
    /// The executor of the HTTP exchange of `attempt_id` died.
    HttpFailed { attempt_id: AttemptId },
    Committed(Committed),
    Rejected(Rejected),
    Tick(Time),
    Online(Online),
    /// The first online value that Network addresses to Session for `epoch`; one of an ended epoch is dropped.
    Snapshot { epoch: Epoch, online: bool },
    Published(Published),
    EpochEnded(EpochEnded),
    Idle,
    Closing,
}

/// What the answer of a call waits for: the publication that confirms the new credential, or the first epoch after the one the answer ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishWait {
    Identity,
    EpochAfter(Epoch),
}

struct AnswerAfterPublish {
    reply: ReplyId,
    result: SessionAnswer,
    wait: PublishWait,
}

pub struct SessionState {
    phase: SessionPhase,
    credential: Option<Credential>,
    queue: VecDeque<Queued>,
    online: Option<bool>,
    epoch: Epoch,
    published: PublishedState,
    after_publish: Option<AnswerAfterPublish>,
    subscribers: BTreeSet<StreamId>,
    restored_direct: Option<PendingCall>,
    restored_detached: Option<PendingCall>,
    released: BTreeSet<RequestId>,
    restored_active: Option<RequestId>,
    restored_answer: Option<SessionAnswer>,
    restore_error: Option<String>,
    resume_waiters: Vec<ReplyId>,
    resume_published: Option<String>,
    resume_clearing: Option<SessionError>,
    failed_batch: Option<BatchId>,
    operations: Vec<SessionOperation>,
    next_request: RequestId,
    next_batch: BatchId,
    next_attempt: AttemptId,
    next_secret: SecretOpId,
    restore_ready: bool,
    /// The first epoch whose `Restored` and `RestoreReady` count: an ended epoch raises it.
    restore_floor: Epoch,
    adopt: bool,
    held: Option<SessionPhase>,
    reported: (u64, RetryPhase),
}

fn is_ended(cause: Cause) -> bool { matches!(cause, Cause::SessionReset | Cause::ServerEnded) }

fn author_error(fx: &mut Vec<SessionEffect>, author: AuthorId, req_id: RequestId, class: ErrorClass, error: SessionError) {
    author_error_retained(fx, author, req_id, class, error, false);
}

fn author_error_retained(fx: &mut Vec<SessionEffect>, author: AuthorId, req_id: RequestId, class: ErrorClass, error: SessionError, pending_retained: bool) {
    fx.push(SessionEffect::Answer { author, req_id, outcome: AuthorOutcome::Error { class, error, pending_retained } });
}

/// A sign-in that the server did not accept: the credential its call issued goes, the server did not end the session.
fn login_failed(pending: &PendingCall, answer: &SessionAnswer, server_ended: bool) -> bool {
    pending.class == OperationClass::Login && pending.credential.is_some() && answer.is_err() && !server_ended
}

fn reply(fx: &mut Vec<SessionEffect>, id: ReplyId, answer: SessionAnswer) { fx.push(SessionEffect::Reply { id, answer }); }

fn blocked_error(reason: Block) -> SessionError {
    match reason {
        Block::Storage => SessionError::Storage("the stored request is not confirmed".into()),
        Block::SequenceConflict => SessionError::SequenceConflict,
        Block::Protocol => SessionError::Protocol,
        Block::StateLost => SessionError::StateLost,
    }
}

fn pending_of(phase: &SessionPhase) -> Option<PendingCall> {
    match phase {
        SessionPhase::Persisting { pending, .. } | SessionPhase::Calling { pending, .. } | SessionPhase::Landing { pending, .. } => Some(pending.clone()),
        _ => None,
    }
}

/// The result of the pending request goes to its call (`reply`) and to its `Mutate` author.
fn finish(fx: &mut Vec<SessionEffect>, pending: &PendingCall, result: SessionAnswer) { finish_retained(fx, pending, result, false); }

fn finish_retained(fx: &mut Vec<SessionEffect>, pending: &PendingCall, result: SessionAnswer, pending_retained: bool) {
    if let Some(id) = pending.reply { reply(fx, id, result.clone()); }
    if let Some(author) = pending.author {
        let outcome = match result {
            Ok(body) => AuthorOutcome::Response(body),
            Err(error) => AuthorOutcome::Error { class: ErrorClass::Permanent, error, pending_retained },
        };
        fx.push(SessionEffect::Answer { author, req_id: pending.request_id, outcome });
    }
}

fn refuse(fx: &mut Vec<SessionEffect>, queued: Queued, error: SessionError) {
    match queued {
        Queued::Call { reply: id, .. } => reply(fx, id, Err(error)),
        Queued::Mutate { author, req_id, .. } => author_error(fx, author, req_id, ErrorClass::Permanent, error),
    }
}

/// The only retry timer of Session: a new `Schedule` replaces the previous one.
const RETRY_TICK: ScheduleId = ScheduleId(1);

/// Backoff of Http for the attempt number `attempts` (>= 1).
fn http_delay(attempts: u64) -> Duration {
    crate::runtime::RetryPolicy::Http.backoff().delay(u32::try_from(attempts.saturating_sub(1)).unwrap_or(u32::MAX))
}

pub struct SessionCalls;

impl StateOwner for SessionCalls {
    const EPOCHED: bool = false;

    type State = SessionState;
    type Input = SessionInput;
    type Effect = SessionEffect;

    fn step(state: &mut SessionState, input: SessionInput) -> Effects<Self> {
        let mut fx = Vec::new();
        let interest = if let SessionInput::InterestPlus { stream_id } = &input { (!matches!(state.phase, SessionPhase::Sealing { .. })).then_some(*stream_id) } else { None };
        state.run(input, &mut fx);
        let view = state.view();
        if view != state.reported {
            state.reported = view;
            for stream_id in &state.subscribers { fx.push(SessionEffect::RetryState { stream_id: *stream_id, attempts: view.0, phase: view.1 }); }
        }
        if let Some(stream_id) = interest {
            if !matches!(state.phase, SessionPhase::Closed) { fx.push(SessionEffect::RetryState { stream_id, attempts: view.0, phase: view.1 }); }
        }
        fx.into_iter().map(|effect| match effect {
            SessionEffect::ScheduleTick { attempts } => Effect::Schedule { id: RETRY_TICK, after: http_delay(attempts), input: SessionInput::Tick(Time(0)) },
            other => Effect::Run(other),
        }).collect()
    }

    fn tick(_now: Time) -> SessionInput { SessionInput::Idle }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> SessionInput { SessionInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> SessionInput { SessionInput::Closing }

    fn effect_id(effect: &SessionEffect) -> Option<EffectId> { effect.io_id() }

    fn defer_executor_failure(pending: &[EffectId]) -> bool { pending.iter().any(|id| id & HTTP_ID != 0) }

    fn executor_failed(_executor: &str, pending: Vec<EffectId>) -> Vec<SessionInput> { pending.into_iter().filter_map(SessionInput::io_failed).collect() }
}

const HTTP_ID: EffectId = 1 << 63;
const SECRET_ID: EffectId = 1 << 62;
const SEAL_ID: EffectId = 1 << 61;

pub fn seal_id(id: u64) -> EffectId { SEAL_ID | id }

/// The id of the HTTP exchange of the attempt.
pub fn http_id(attempt_id: AttemptId) -> EffectId { HTTP_ID | attempt_id }

/// The id of a secret operation: `kind` 0 generate, 1 store, 2 clear, 3 read.
pub fn secret_id(kind: u64, secret_op_id: SecretOpId) -> EffectId { SECRET_ID | (kind << 56) | secret_op_id }

impl SessionEffect {
    /// The id of an effect that does IO: a batch, an HTTP attempt or a secret operation. The other effects answer or notify and need no executor.
    pub fn io_id(&self) -> Option<EffectId> {
        match self {
            Self::SealProbe { seal_id: id, .. } => Some(seal_id(*id)),
            Self::CommitPending { batch, .. } | Self::CommitLanding { batch, .. } => Some(*batch),
            Self::HttpRequest { attempt_id, .. } => Some(http_id(*attempt_id)),
            Self::GenerateCredential { secret_op_id } => Some(secret_id(0, *secret_op_id)),
            Self::StoreSecret { secret_op_id, .. } => Some(secret_id(1, *secret_op_id)),
            Self::ClearSecret { secret_op_id } => Some(secret_id(2, *secret_op_id)),
            Self::ReadSecret { secret_op_id } => Some(secret_id(3, *secret_op_id)),
            _ => None,
        }
    }
}

impl SessionInput {
    /// The failure cell of the IO effect `id`.
    fn io_failed(id: EffectId) -> Option<SessionInput> {
        let detail = "the executor of Session failed".to_string();
        Some(match id {
            id if id & HTTP_ID != 0 => SessionInput::HttpFailed { attempt_id: id & !HTTP_ID },
            id if id & SEAL_ID != 0 => SessionInput::SealFailed { seal_id: id & !SEAL_ID, error: SessionError::Storage(detail) },
            id if id & SECRET_ID != 0 => {
                let secret_op_id = id & ((1 << 56) - 1);
                match (id >> 56) & 0x3 {
                    2 => SessionInput::SecretCleared { secret_op_id, ok: false },
                    3 => SessionInput::SecretRead { secret_op_id, read: SecretState::Error(detail) },
                    _ => SessionInput::SecretStored { secret_op_id, ok: false },
                }
            }
            batch => SessionInput::Rejected(Rejected { batch, reason: RejectReason::Storage(detail) }),
        })
    }
}

#[path = "session_flow.rs"]
mod flow;
#[cfg(test)]
#[path = "session_sealing_tests.rs"]
mod sealing_tests;
#[path = "session_events.rs"]
mod events;
