//! Vocabulary of the Session owner tests: the inputs the stand feeds and the effects it reads back from the owner.

pub type RequestId = u64;
pub type BatchId = u64;
pub type ReplyId = u64;
pub type AuthorId = u64;
pub type Digest = u64;
pub type AttemptId = u64;
pub type SecretOpId = u64;
pub type Owner = crate::transport::RequestOwner;

pub const OUTBOX_AUTHOR: AuthorId = 1;
pub const TRANSFER_AUTHOR: AuthorId = 2;

pub fn author_of(owner: &Owner) -> Option<AuthorId> {
    match owner {
        Owner::Operation { .. } | Owner::Unmatched { .. } => Some(OUTBOX_AUTHOR),
        Owner::Transfer { .. } | Owner::Orphan { .. } => Some(TRANSFER_AUTHOR),
        Owner::Detached => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndCause {
    SessionReset,
    ServerEnded,
    Close,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    NoSession,
    Storage,
    Invalid,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fail {
    ResponseTooLarge { limit: u64 },
    TooLarge { size: u64, limit: u64 },
    Invalid,
    Session(crate::transport::SessionCode),
    Graphql(String),
    Storage,
    SessionExists,
    NoCredential,
    Revoked,
    StateLost,
    SequenceConflict,
    Unauthorized,
    Security { code: crate::transport::SecurityCode, detail: String },
    Http,
    Network,
    Interrupted,
    NotSent,
    NoSession,
    Closed,
}

pub type Answer = Result<String, Fail>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Credential {
    pub digest: Digest,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    Login,
    Authenticated,
    Sessionless,
    Logout,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    pub name: String,
    pub path: String,
    pub class: Class,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    pub request_id: RequestId,
    pub sequence: u64,
    pub class: Class,
    pub operation: String,
    pub path: String,
    pub body: String,
    pub document: Option<String>,
    pub credential: Option<Digest>,
    pub purpose: u8,
    pub author: Option<AuthorId>,
    pub owner: Option<Owner>,
    pub epoch: u64,
    pub reply: Option<ReplyId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PubState {
    NoSession,
    Session,
    Ending,
    EndingFailed,
    StorageFailed,
    Closing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Secret {
    Present(Digest),
    Missing,
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    Sealed { seal_id: u64, size: u64 },
    SealFailed { seal_id: u64, error: Fail },
    Open { credential: Option<Credential>, pending: Option<Pending>, dropped_pending: bool, secret: Secret },
    SessionCall { operation: String, body: String, reply: ReplyId },
    Mutate { author: AuthorId, owner: Option<Owner>, req_id: RequestId, epoch: u64, path: String, body: String, purpose: u8 },
    Cancel { req_id: RequestId },
    Release { req_id: RequestId },
    CredentialGenerated { op: SecretOpId, credential: Digest },
    SecretStored { op: SecretOpId, ok: bool },
    SecretCleared { op: SecretOpId, ok: bool },
    SecretRead { op: SecretOpId, read: Secret },
    SecretMissing { request_id: RequestId, attempt_id: AttemptId, lost: bool },
    GateClosed { request_id: RequestId, attempt_id: AttemptId },
    Foreground,
    RestoreReady { epoch: u64 },
    RestoreFailed { epoch: u64, error: String },
    Restored { epoch: u64, pending: Option<Pending> },
    Response { request_id: RequestId, attempt_id: AttemptId, body: String, sequence: Option<u64>, refusal: Option<crate::transport::SessionCode> },
    Revoked { request_id: RequestId, attempt_id: AttemptId, body: String, author_ref: Option<AuthorId> },
    HttpError { request_id: RequestId, attempt_id: AttemptId, temporary: bool, auth: bool, security: Option<(crate::transport::SecurityCode, String)>, body: Option<String> },
    AnswerTooLarge { request_id: RequestId, attempt_id: AttemptId, limit: u64 },
    Dropped { request_id: RequestId, attempt_id: AttemptId },
    Unknown { request_id: RequestId, attempt_id: AttemptId },
    Committed(BatchId),
    Rejected(BatchId, Reason),
    Tick,
    Online(bool),
    Snapshot { epoch: u64, online: bool },
    Published { epoch: u64, identity: Option<Digest>, state: PubState },
    EpochEnded { epoch: u64, cause: EndCause },
    Closing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Permanent,
    Temporary,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Response(String, bool),
    Error(Kind, Fail, bool),
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    SealProbe { seal_id: u64, pending: Pending, credential: Option<Digest> },
    /// Store `Commit{encrypted_pending = pending}`, with `Session{digest, 0}` at the first sign-in.
    CommitPending { batch: BatchId, epoch: u64, pending: Pending, session: Option<Digest> },
    /// Store `Commit{Session{digest or the previous one, sequence}, RemovePending, Answer}`.
    CommitLanding { batch: BatchId, epoch: u64, digest: Option<Digest>, sequence: Option<u64>, answer: Option<(RequestId, Answer)>, resume_answer: Option<Answer>, erase_session: bool, end_journal: bool },
    HttpRequest { pending: Pending, attempt_id: AttemptId },
    HttpCancel { attempt_id: AttemptId },
    ScheduleTick,
    GenerateCredential { op: SecretOpId },
    StoreSecret { credential: Digest, op: SecretOpId },
    ClearSecret { op: SecretOpId },
    ReadSecret { op: SecretOpId },
    CredentialCommitted(Digest),
    ServerEnded { epoch: u64 },
    SessionReset,
    RestoreClassified { epoch: u64 },
    SnapshotRequest { epoch: u64 },
    Answer { author: AuthorId, req_id: RequestId, outcome: Outcome },
    Drained,
}

