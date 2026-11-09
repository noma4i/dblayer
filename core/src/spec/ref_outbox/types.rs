//! Types of the Outbox reference: owner state, inputs, effects. Written from specs/machines/outbox.md only.

use std::collections::{BTreeMap, BTreeSet};

pub type OpId = u64;
pub type Epoch = u64;
pub const MAX_ATTEMPTS: u32 = 40;
pub const DRAIN_LIMIT: usize = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stage { WaitingDeclaration, Submitting, Queued, PersistingAttempt, Sending, Landing, RollingBack, MarkingFailed, MarkingRetry, Failed, Done }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase { NoOwner, Restoring, Classified, Ready, Ended }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpOrigin { Api, Transfer(u64), Group(u64) }

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Addr { Call, Transfer(u64) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrKind { Exhausted, Server, RestoreFailed, StateLost, Internal }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause { Discard, Refusal, Expired, DiscardRestoreFailed }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndCause { SessionEnded, Closed }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome { Confirmed, Rejected, Failed, Discarded, Ended(EndCause) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind { Submit, Attempt, Land, Rollback, MarkFailed, Retry }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reason { StaleGeneration(BTreeMap<String, u64>), NoSession, Closed, Storage, Invalid }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Batch { Op(OpId), Local(u64) }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct LandFacts { pub root_other_row: bool, pub root_present: bool, pub returns: bool, pub swap: bool, pub refetch: bool }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resp {
    Data(LandFacts),
    /// errors non-empty, temporary class; `has_sequence` = the answer spent the sequence number (wire.md Answered).
    Temporary { has_sequence: bool },
    Permanent,
    /// destroy action, errors = only the code NOT_FOUND.
    NotFound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendKind { NotSend, Declared, NoCreateDeclared, UploadActionUndeclared }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpSpec {
    pub op_id: Option<OpId>,
    pub model: String,
    pub row_id: String,
    pub action: String,
    pub expires_after: Option<u64>,
    pub created_at: u64,
    pub keep: bool,
    pub effects_empty: bool,
    pub destroy: bool,
    pub response_ignore: bool,
    pub batch: bool,
    pub origin: OpOrigin,
    /// attempts already durable (Transfer writes the intent itself).
    pub attempts: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmitFlags {
    pub valid: bool,
    pub send: SendKind,
    pub removes_receiver: bool,
    pub leaves_scope: bool,
    pub join_missing_row: bool,
    pub intent_written: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Saved { Ok, Refused }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreItem {
    pub op_id: OpId,
    pub spec: OpSpec,
    pub cycle_id: u64,
    pub request_id: u64,
    pub attempts: u32,
    pub sent: bool,
    pub restore_failed: bool,
    pub has_error_mark: bool,
    pub answer: Option<Saved>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Sel { Row { model: String, row_id: String, action: Option<String> }, Op(OpId) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelKind { Retry, Discard }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Receipt { pub epoch: Epoch, pub op_id: OpId, pub cycle_id: u64 }

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LKind { SetLocal, SetEphemeral, Attach, Detach, ModelWrite, Noop }

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct LFlags {
    pub row_exists: bool,
    pub path_is_local: bool,
    pub same_value: bool,
    pub memory_model: bool,
    pub memory_same_values: bool,
    pub source_outside_files_dir: bool,
    pub expiry: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    EpochStarted { epoch: Epoch, identity: bool, generations: BTreeMap<String, u64> },
    Submit { epoch: Epoch, op: OpSpec, flags: SubmitFlags },
    ReserveOp { epoch: Epoch, target: u64 },
    Restored { epoch: Epoch, next_seq: OpId, queue: Result<Vec<RestoreItem>, ()>, pending: Vec<u64>, pending_lost: bool },
    RegisterOperations { declarations: Vec<(String, String)> },
    RestoreReady { epoch: Epoch },
    Committed { epoch: Epoch, batch: Batch, kind: Kind, revision: u64 },
    Rejected { epoch: Epoch, batch: Batch, kind: Kind, reason: Reason },
    Response { op_id: OpId, cycle_id: u64, resp: Resp, swap: Option<(String, String)>, pending_retained: Option<bool> },
    RowRemoved { model: String, row_id: String, revision: u64 },
    TransportError { op_id: OpId, cycle_id: u64, temporary: bool, pending_retained: Option<bool> },
    Dropped { op_id: OpId, cycle_id: u64, pending_retained: Option<bool> },
    Retry { epoch: Epoch, sel: Sel, reply_to: Addr },
    Discard { epoch: Epoch, sel: Sel, reply_to: Addr },
    Outcome { receipt: Receipt },
    SelectedOutbox { reply: u64, action: String },
    SelectedNone { reply: u64 },
    Online(bool),
    EpochEnded(Epoch),
    ModelsReset { models: Vec<String>, generations: BTreeMap<String, u64> },
    Closing,
    Tick { op_id: OpId, cycle_id: u64 },
    Now(u64),
    LocalWrite { epoch: Epoch, id: u64, kind: LKind, flags: LFlags },
    CopyDone { id: u64 },
    CopyFailed { id: u64 },
    ExpireRow { model: String, row_id: String, write_revision: u64 },
    ResetEphemeral { model: String, row_id: String, field: String, position: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// The test id of a fired row, rule or entry.
    Row(&'static str),
    Undefined { row: String, reason: String },
    Store { op: u64, what: &'static str },
    Session { op: u64, what: &'static str },
    Schedule { op: u64, what: &'static str },
    Transfer { op: u64, what: &'static str },
    Reply { to: Addr, op: u64, what: &'static str },
    Lifecycle(&'static str),
    Network(&'static str),
    Drained(&'static str),
}

#[derive(Clone, Debug)]
pub struct Op {
    pub id: OpId,
    pub model: String,
    pub row_id: String,
    pub action: String,
    pub expires_after: Option<u64>,
    pub created_at: u64,
    pub attempts: u32,
    pub cycle_id: u64,
    pub request_id: u64,
    pub origin: OpOrigin,
    pub stage: Stage,
    pub waiting_retry: bool,
    pub keep: bool,
    pub effects_empty: bool,
    pub destroy: bool,
    pub response_ignore: bool,
    pub batch: bool,
    pub error: Option<ErrKind>,
    pub cause: Cause,
    pub deferred: bool,
    pub restored: bool,
    pub ever_sent: bool,
    pub cycle_sent: bool,
    pub pending_owned: bool,
    pub release_sent: bool,
    pub submit_reply: bool,
    pub retry_waiter: u32,
    pub swap: Option<(String, String)>,
    pub discard_waiters: Vec<Addr>,
    pub outcome_waiters: u32,
    pub item: Option<RestoreItem>,
    pub land: LandFacts,
    pub outcome: Option<Outcome>,
    pub cancels: Option<OpId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LStage { Copying, Writing }

#[derive(Clone, Debug)]
pub struct State {
    pub phase: Phase,
    pub epoch: Option<Epoch>,
    pub epoch_live: bool,
    pub queue_failed: bool,
    pub online: Option<bool>,
    pub now: u64,
    pub generations: BTreeMap<String, u64>,
    pub declared: BTreeSet<(String, String)>,
    pub ops: BTreeMap<OpId, Op>,
    pub next_seq: OpId,
    pub next_request: u64,
    pub held: Vec<Input>,
    pub aliases: BTreeMap<(String, String), String>,
    pub closing: bool,
    pub drained: bool,
    pub pending_requests: BTreeSet<u64>,
    pub selects: BTreeMap<u64, (SelKind, Sel, Addr, Epoch)>,
    pub pending_lost: bool,
    pub local: BTreeMap<u64, LStage>,
    pub local_expiry: BTreeSet<u64>,
    pub local_closing: bool,
    pub local_drained: bool,
}

impl State {
    pub fn new() -> Self {
        State {
            phase: Phase::NoOwner, epoch: None, epoch_live: false, queue_failed: false, online: None, now: 0,
            generations: BTreeMap::new(), declared: BTreeSet::new(), ops: BTreeMap::new(), next_seq: 1, next_request: 1,
            held: Vec::new(), aliases: BTreeMap::new(), closing: false, drained: false, pending_requests: BTreeSet::new(),
            selects: BTreeMap::new(), pending_lost: false, local: BTreeMap::new(), local_expiry: BTreeSet::new(), local_closing: false, local_drained: false,
        }
    }
    pub fn epoch_is(&self, e: Epoch) -> bool { self.epoch_live && self.epoch == Some(e) }
    pub fn online_now(&self) -> bool { self.online == Some(true) }
}

impl Default for State { fn default() -> Self { Self::new() } }

pub fn row(e: &mut Vec<Effect>, id: &'static str) { e.push(Effect::Row(id)); }
pub fn undefined(e: &mut Vec<Effect>, row: &str, reason: &str) { e.push(Effect::Undefined { row: row.to_string(), reason: reason.to_string() }); }
pub fn reply(e: &mut Vec<Effect>, to: Addr, op: u64, what: &'static str) { e.push(Effect::Reply { to, op, what }); }
pub fn outcome_name(o: Outcome) -> &'static str {
    match o { Outcome::Confirmed => "Confirmed", Outcome::Rejected => "Rejected", Outcome::Failed => "Failed", Outcome::Discarded => "Discarded", Outcome::Ended(EndCause::SessionEnded) => "Ended(SessionEnded)", Outcome::Ended(EndCause::Closed) => "Ended(Closed)" }
}
