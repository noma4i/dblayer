//! Owner `Lifecycle`: the epoch, the identity of the session, the model generation and the close.
//! Every output is an effect for the executor: it publishes, broadcasts the control inputs and sends the Store its requests.

use std::collections::BTreeSet;

use super::contract::{CredentialCommitted, Drained, Published, PublishedState, ReplyId, ServerEnded};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

pub type Identity = u64;
pub type EraseId = u64;
pub type PublicationId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Peer {
    Network,
    Session,
    Windows,
    Feed,
    Deliveries,
    Outbox,
    Transfer,
    Calls,
    LocalWrites,
    Rounds,
}

/// `all owners` of the table.
pub const ALL_OWNERS: [Peer; 10] = [Peer::Network, Peer::Session, Peer::Windows, Peer::Feed, Peer::Deliveries, Peer::Outbox, Peer::Transfer, Peer::Calls, Peer::LocalWrites, Peer::Rounds];

impl Peer {
    pub fn name(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Session => "session",
            Self::Windows => "windows",
            Self::Feed => "feed",
            Self::Deliveries => "deliveries",
            Self::Outbox => "outbox",
            Self::Transfer => "transfer",
            Self::Calls => "calls",
            Self::LocalWrites => "local_writes",
            Self::Rounds => "rounds",
        }
    }

    fn named(name: &str) -> Option<Self> { ALL_OWNERS.into_iter().find(|peer| peer.name() == name) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EraseKind {
    Account,
    Models,
}

/// An erase in the Store without an answer, with the waits bound to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Erase {
    pub id: EraseId,
    pub kind: EraseKind,
    /// The generation reserved for this erase (`issued_generation` + 1 at the time of issue).
    pub target: u64,
    pub replies: Vec<ReplyId>,
}

/// The restore of the intents of an epoch with an identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Restoring {
    pub epoch: Epoch,
    pub start_waiting: BTreeSet<Peer>,
    pub restore_waiting: BTreeSet<Peer>,
    pub open_reply: Option<ReplyId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecyclePhase {
    NoSession { epoch: Epoch },
    Session { epoch: Epoch, identity: Identity },
    Ending { epoch_old: Epoch, cause: Cause, next: Option<Identity>, failed: bool, erase_credential: bool, erases: Vec<Erase> },
    Resetting { epoch: Epoch, identity: Option<Identity>, models: BTreeSet<String>, queued_models: BTreeSet<String>, queued_replies: Vec<ReplyId>, pending_credential: Option<Identity>, erases: Vec<Erase> },
    Closing { waiting: BTreeSet<Peer>, erases: Vec<Erase>, close_replies: Vec<ReplyId>, store_closing: bool, store_closed: bool, observers_stopped: bool },
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    Storage,
    Closed,
    Runtime,
    Interrupted,
}

/// The `EpochStarted` that waits for `Saved` of its publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Started {
    pub epoch: Epoch,
    pub identity: Option<Identity>,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    pub id: PublicationId,
    pub published: Published,
    pub started: Option<Started>,
    pub replies: Vec<ReplyId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleEffect {
    /// The cell at once, the Store saves the value as `publication_id`.
    Publish { publication_id: PublicationId, published: Published },
    /// The cell only: the Store did not save the value.
    PublishCell(Published),
    EpochStarted { epoch: Epoch, identity: Option<Identity>, generation: u64 },
    EpochEnded { epoch: Epoch, cause: Cause },
    ModelsReset { epoch: Epoch, generation: u64, models: BTreeSet<String> },
    Closing,
    StopObserving,
    ClearAccount { epoch_old: Epoch, cause: Cause, generation_new: u64, erase_id: EraseId },
    ClearModels { epoch: Epoch, generation_new: u64, models: BTreeSet<String>, erase_id: EraseId },
    ReadIntents { epoch: Epoch },
    RestoreReady { epoch: Epoch },
    StoreClose,
    Reply { id: ReplyId, result: Result<(), LifecycleError> },
}

#[derive(Clone, Debug)]
pub enum LifecycleInput {
    Open { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, saved_unowned_pending: bool, reply: Option<ReplyId> },
    CredentialCommitted(CredentialCommitted),
    SessionReset { reply: Option<ReplyId> },
    ServerEnded(ServerEnded),
    ResetModels { models: BTreeSet<String>, reply: Option<ReplyId> },
    Cleared { erase_id: EraseId },
    ModelsCleared { generation_new: u64, erase_id: EraseId },
    ClearFailed { erase_id: EraseId, error: String },
    Saved { publication_id: PublicationId },
    SaveFailed { publication_id: PublicationId, error: String },
    StoreClosed,
    ObserversStopped,
    ExecutorFailed { executor: String },
    Close { reply: ReplyId },
    Drained(Drained),
    Started { owner: Peer, epoch: Epoch },
    RestoreClassified { owner: Peer, epoch: Epoch },
    /// `Tick`, `EpochEnded` and `Closing` are not inputs of Lifecycle.
    Idle,
}

pub struct LifecycleState {
    phase: LifecyclePhase,
    published: Published,
    publishing: Vec<Publication>,
    /// The value of `PublishedCell`: restricting publications are written at once, active ones at `Saved`.
    cell: Published,
    cell_publication: PublicationId,
    ended_up_to: Option<Epoch>,
    restoring: Option<Restoring>,
    next_publication: PublicationId,
    next_erase: EraseId,
    /// `issued_generation`: the highest generation reserved for an erase.
    allocated: u64,
}

fn reply_all(replies: Vec<ReplyId>, result: Result<(), LifecycleError>, fx: &mut Vec<LifecycleEffect>) {
    fx.extend(replies.into_iter().map(|id| LifecycleEffect::Reply { id, result }));
}

fn restricting(state: PublishedState) -> bool { matches!(state, PublishedState::Ending | PublishedState::EndingFailed | PublishedState::Closing | PublishedState::StorageFailed) }

fn settled_state(identity: Option<Identity>) -> PublishedState { if identity.is_some() { PublishedState::Session } else { PublishedState::NoSession } }

fn settled(epoch: Epoch, identity: Option<Identity>) -> LifecyclePhase {
    match identity {
        Some(identity) => LifecyclePhase::Session { epoch, identity },
        None => LifecyclePhase::NoSession { epoch },
    }
}

fn take_erase(erases: &mut Vec<Erase>, id: EraseId) -> Option<Erase> {
    let at = erases.iter().position(|erase| erase.id == id)?;
    Some(erases.remove(at))
}

impl Default for LifecycleState {
    fn default() -> Self { Self::new() }
}

impl LifecycleState {
    /// Before `Open`: no session, epoch 0.
    pub fn new() -> Self {
        Self { phase: LifecyclePhase::NoSession { epoch: 0 }, published: Published::initial(), publishing: Vec::new(), cell: Published::initial(), cell_publication: 0, ended_up_to: None, restoring: None, next_publication: 1, next_erase: 1, allocated: 0 }
    }

    pub fn phase(&self) -> &LifecyclePhase { &self.phase }

    pub fn published(&self) -> &Published { &self.published }

    pub fn publishing(&self) -> &[Publication] { &self.publishing }

    pub fn cell(&self) -> &Published { &self.cell }

    pub fn restoring(&self) -> Option<&Restoring> { self.restoring.as_ref() }

    fn publish(&mut self, started: Option<Started>, replies: Vec<ReplyId>, fx: &mut Vec<LifecycleEffect>) {
        let id = self.next_publication;
        self.next_publication += 1;
        fx.push(LifecycleEffect::Publish { publication_id: id, published: self.published.clone() });
        if restricting(self.published.state) {
            self.cell = self.published.clone();
            self.cell_publication = id;
        }
        self.publishing.push(Publication { id, published: self.published.clone(), started, replies });
    }

    fn set_published(&mut self, epoch: Epoch, identity: Option<Identity>, generation: u64, state: PublishedState) {
        self.published = Published { epoch, identity, generation, state, closed: false };
    }

    fn start(&mut self, epoch: Epoch, identity: Option<Identity>, generation: u64, replies: Vec<ReplyId>, fx: &mut Vec<LifecycleEffect>) {
        self.set_published(epoch, identity, generation, settled_state(identity));
        if identity.is_some() {
            self.restoring = Some(self.new_restoring(epoch, None));
        }
        self.publish(Some(Started { epoch, identity, generation }), replies, fx);
    }

    fn new_restoring(&self, epoch: Epoch, open_reply: Option<ReplyId>) -> Restoring {
        Restoring { epoch, start_waiting: [Peer::Outbox, Peer::Transfer].into(), restore_waiting: [Peer::Outbox, Peer::Transfer, Peer::Session].into(), open_reply }
    }

    fn restore_progress(&mut self, epoch: Epoch, fx: &mut Vec<LifecycleEffect>) {
        let Some(restoring) = self.restoring.as_ref().filter(|restoring| restoring.epoch == epoch) else { return };
        if restoring.start_waiting.is_empty() {
            fx.push(LifecycleEffect::ReadIntents { epoch });
        }
        if restoring.restore_waiting.is_empty() {
            self.finish_restore(epoch, fx);
        }
    }

    fn finish_restore(&mut self, epoch: Epoch, fx: &mut Vec<LifecycleEffect>) {
        let Some(restoring) = self.restoring.take() else { return };
        reply_all(restoring.open_reply.into_iter().collect(), Ok(()), fx);
        fx.push(LifecycleEffect::RestoreReady { epoch });
    }

    /// The restore of an epoch that ended, or of a close, ends with the wait of `open`.
    fn drop_restoring(&mut self, result: Result<(), LifecycleError>, fx: &mut Vec<LifecycleEffect>) {
        if let Some(restoring) = self.restoring.take() {
            reply_all(restoring.open_reply.into_iter().collect(), result, fx);
        }
    }

    /// The epoch of a restore ended by a new identity or a reset: `open` already passed `Saved` -> `Ok`, its publication still waits -> `Interrupted`.
    fn end_restoring(&mut self, fx: &mut Vec<LifecycleEffect>) {
        let saved = self.restoring.as_ref().is_some_and(|restoring| !self.publishing.iter().any(|publication| publication.started.as_ref().is_some_and(|started| started.epoch == restoring.epoch)));
        self.drop_restoring(if saved { Ok(()) } else { Err(LifecycleError::Interrupted) }, fx);
    }

    fn ended(&mut self, epoch: Epoch) { self.ended_up_to = Some(self.ended_up_to.map_or(epoch, |ended| ended.max(epoch))); }

    fn new_erase(&mut self, kind: EraseKind, replies: Vec<ReplyId>) -> Erase {
        let id = self.next_erase;
        self.next_erase += 1;
        self.allocated = self.allocated.max(self.published.generation) + 1;
        Erase { id, kind, target: self.allocated, replies }
    }

    fn closing(&mut self, erases: Vec<Erase>, close_replies: Vec<ReplyId>, end: Option<Epoch>, fx: &mut Vec<LifecycleEffect>) -> LifecyclePhase {
        self.published.state = PublishedState::Closing;
        self.published.closed = true;
        self.publish(None, Vec::new(), fx);
        self.end_restoring(fx);
        if let Some(epoch) = end {
            self.ended(epoch);
            fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::Close });
        }
        fx.push(LifecycleEffect::Closing);
        fx.push(LifecycleEffect::StopObserving);
        LifecyclePhase::Closing { waiting: ALL_OWNERS.into_iter().collect(), erases, close_replies, store_closing: false, store_closed: false, observers_stopped: false }
    }

    /// `Ending` entered from a state with a live epoch: `EpochEnded` · `Publish` · `ClearAccount`.
    fn ending(&mut self, epoch: Epoch, cause: Cause, next: Option<Identity>, mut erases: Vec<Erase>, replies: Vec<ReplyId>, fx: &mut Vec<LifecycleEffect>) {
        self.ended(epoch);
        self.end_restoring(fx);
        fx.push(LifecycleEffect::EpochEnded { epoch, cause });
        self.published.state = PublishedState::Ending;
        self.publish(None, Vec::new(), fx);
        let erase = self.new_erase(EraseKind::Account, replies);
        fx.push(LifecycleEffect::ClearAccount { epoch_old: epoch, cause, generation_new: erase.target, erase_id: erase.id });
        erases.push(erase);
        self.phase = LifecyclePhase::Ending { epoch_old: epoch, cause, next, failed: false, erase_credential: false, erases };
    }

    fn resetting(&mut self, epoch: Epoch, identity: Option<Identity>, models: BTreeSet<String>, reply: Option<ReplyId>, fx: &mut Vec<LifecycleEffect>) -> LifecyclePhase {
        self.published.state = PublishedState::Resetting;
        self.publish(None, Vec::new(), fx);
        let erase = self.new_erase(EraseKind::Models, reply.into_iter().collect());
        fx.push(LifecycleEffect::ClearModels { epoch, generation_new: erase.target, models: models.clone(), erase_id: erase.id });
        LifecyclePhase::Resetting { epoch, identity, models, queued_models: BTreeSet::new(), queued_replies: Vec::new(), pending_credential: None, erases: vec![erase] }
    }
}

pub struct Lifecycle;

impl StateOwner for Lifecycle {
    const EPOCHED: bool = false;

    type State = LifecycleState;
    type Input = LifecycleInput;
    type Effect = LifecycleEffect;

    fn step(state: &mut LifecycleState, input: LifecycleInput) -> Effects<Self> {
        let mut fx = Vec::new();
        state.run(input, &mut fx);
        fx.into_iter().map(Effect::Run).collect()
    }

    fn tick(_now: Time) -> LifecycleInput { LifecycleInput::Idle }

    fn epoch_ended(_epoch: Epoch, _cause: Cause) -> LifecycleInput { LifecycleInput::Idle }

    fn closing() -> LifecycleInput { LifecycleInput::Idle }

    fn executor_failed(executor: &str, _pending: Vec<crate::runtime::EffectId>) -> Vec<LifecycleInput> { vec![LifecycleInput::ExecutorFailed { executor: executor.to_string() }] }
}

#[path = "lifecycle_flow.rs"]
mod flow;
