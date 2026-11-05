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
    Open { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, reply: Option<ReplyId> },
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
    restore_participants: BTreeSet<Peer>,
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

impl LifecycleState {
    /// Before `Open`: no session, epoch 0.
    pub fn new() -> Self {
        Self { phase: LifecyclePhase::NoSession { epoch: 0 }, published: Published::initial(), publishing: Vec::new(), cell: Published::initial(), cell_publication: 0, ended_up_to: None, restoring: None, next_publication: 1, next_erase: 1, restore_participants: [Peer::Outbox, Peer::Transfer, Peer::Session].into(), allocated: 0 }
    }

    pub fn with_restore_participants(mut self, participants: impl IntoIterator<Item = Peer>) -> Self {
        self.restore_participants = participants.into_iter().collect();
        self
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
        let taking_part = |owners: &[Peer]| owners.iter().copied().filter(|owner| self.restore_participants.contains(owner)).collect();
        Restoring { epoch, start_waiting: taking_part(&[Peer::Outbox, Peer::Transfer]), restore_waiting: taking_part(&[Peer::Outbox, Peer::Transfer, Peer::Session]), open_reply }
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
        self.drop_restoring(Err(LifecycleError::Interrupted), fx);
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

impl LifecycleState {
    /// One row of `lifecycle.md` per match arm, in table order.
    fn run(&mut self, input: LifecycleInput, fx: &mut Vec<LifecycleEffect>) {
        match input {
            LifecycleInput::Open { saved_epoch, saved_generation, saved_identity, reply } => {
                self.allocated = saved_generation;
                let epoch = saved_epoch + 1;
                self.phase = settled(epoch, saved_identity);
                self.set_published(epoch, saved_identity, saved_generation, settled_state(saved_identity));
                if saved_identity.is_some() {
                    self.restoring = Some(self.new_restoring(epoch, reply));
                    self.publish(Some(Started { epoch, identity: saved_identity, generation: saved_generation }), Vec::new(), fx);
                } else {
                    self.publish(Some(Started { epoch, identity: saved_identity, generation: saved_generation }), reply.into_iter().collect(), fx);
                }
                return;
            }
            LifecycleInput::Saved { publication_id } => return self.saved(publication_id, true, fx),
            LifecycleInput::SaveFailed { publication_id, .. } => return self.saved(publication_id, false, fx),
            LifecycleInput::ExecutorFailed { .. } => return self.executor_failed(fx),
            LifecycleInput::Started { owner, epoch } => return self.restore(owner, epoch, true, fx),
            LifecycleInput::RestoreClassified { owner, epoch } => return self.restore(owner, epoch, false, fx),
            _ => {}
        }
        let generation = self.published.generation;
        let phase = std::mem::replace(&mut self.phase, LifecyclePhase::Closed);
        self.phase = match (phase, input) {
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity })) => {
                self.ended(epoch);
                fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                self.start(epoch + 1, Some(identity), generation, Vec::new(), fx);
                LifecyclePhase::Session { epoch: epoch + 1, identity }
            }
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::SessionReset { reply }) => {
                self.ending(epoch, Cause::SessionReset, None, Vec::new(), reply.into_iter().collect(), fx);
                return;
            }
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::ResetModels { models, reply }) => self.resetting(epoch, None, models, reply, fx),
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::Close { reply }) => self.closing(Vec::new(), vec![reply], Some(epoch), fx),
            (phase @ LifecyclePhase::NoSession { .. }, _) => phase,

            (LifecyclePhase::Session { epoch, identity }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) if other == identity => LifecyclePhase::Session { epoch, identity },
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) => {
                self.ending(epoch, Cause::IdentityChanged, Some(other), Vec::new(), Vec::new(), fx);
                return;
            }
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::SessionReset { reply }) => {
                self.ending(epoch, Cause::SessionReset, None, Vec::new(), reply.into_iter().collect(), fx);
                return;
            }
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::ServerEnded(ServerEnded { epoch: request_epoch })) if request_epoch == epoch => {
                self.ending(epoch, Cause::ServerEnded, None, Vec::new(), Vec::new(), fx);
                return;
            }
            (LifecyclePhase::Session { epoch, identity }, LifecycleInput::ResetModels { models, reply }) => self.resetting(epoch, Some(identity), models, reply, fx),
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::Close { reply }) => self.closing(Vec::new(), vec![reply], Some(epoch), fx),
            (phase @ LifecyclePhase::Session { .. }, _) => phase,

            (LifecyclePhase::Ending { epoch_old, cause, failed, erase_credential, mut erases, .. }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity })) => {
                if failed {
                    self.published.state = PublishedState::Ending;
                    self.publish(None, Vec::new(), fx);
                    let erase = self.new_erase(EraseKind::Account, Vec::new());
                    fx.push(LifecycleEffect::ClearAccount { epoch_old, cause, generation_new: erase.target, erase_id: erase.id });
                    erases.push(erase);
                }
                LifecyclePhase::Ending { epoch_old, cause, next: Some(identity), failed: false, erase_credential, erases }
            }
            (LifecyclePhase::Ending { epoch_old, cause, failed, erase_credential, mut erases, .. }, LifecycleInput::SessionReset { reply }) => {
                if failed {
                    self.published.state = PublishedState::Ending;
                    self.publish(None, Vec::new(), fx);
                    let erase = self.new_erase(EraseKind::Account, reply.into_iter().collect());
                    fx.push(LifecycleEffect::ClearAccount { epoch_old, cause: Cause::SessionReset, generation_new: erase.target, erase_id: erase.id });
                    erases.push(erase);
                    LifecyclePhase::Ending { epoch_old, cause: Cause::SessionReset, next: None, failed: false, erase_credential: false, erases }
                } else {
                    let credential = erase_credential || cause == Cause::IdentityChanged;
                    if let Some(account) = erases.iter_mut().rev().find(|erase| erase.kind == EraseKind::Account) {
                        account.replies.extend(reply);
                    }
                    LifecyclePhase::Ending { epoch_old, cause, next: None, failed: false, erase_credential: credential, erases }
                }
            }
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::ResetModels { reply, .. }) => {
                if failed {
                    reply_all(reply.into_iter().collect(), Err(LifecycleError::Storage), fx);
                } else if let Some(account) = erases.iter_mut().rev().find(|erase| erase.kind == EraseKind::Account) {
                    account.replies.extend(reply);
                }
                LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases }
            }
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::Cleared { erase_id }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) if erase_credential => {
                    self.published.generation = self.published.generation.max(erase.target);
                    let second = self.new_erase(EraseKind::Account, erase.replies);
                    fx.push(LifecycleEffect::ClearAccount { epoch_old, cause: Cause::SessionReset, generation_new: second.target, erase_id: second.id });
                    erases.push(second);
                    LifecyclePhase::Ending { epoch_old, cause: Cause::SessionReset, next, failed, erase_credential: false, erases }
                }
                Some(erase) => {
                    let generation = self.published.generation.max(erase.target);
                    match next {
                        None => {
                            self.start(epoch_old + 1, None, generation, erase.replies, fx);
                            LifecyclePhase::NoSession { epoch: epoch_old + 1 }
                        }
                        Some(identity) => {
                            self.start(epoch_old + 2, Some(identity), generation, erase.replies, fx);
                            LifecyclePhase::Session { epoch: epoch_old + 2, identity }
                        }
                    }
                }
            },
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::ClearFailed { erase_id, .. }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) => {
                    reply_all(erase.replies, Err(LifecycleError::Storage), fx);
                    if erase.kind == EraseKind::Account {
                        self.published.state = PublishedState::EndingFailed;
                        self.publish(None, Vec::new(), fx);
                        LifecyclePhase::Ending { epoch_old, cause, next, failed: true, erase_credential, erases }
                    } else {
                        LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases }
                    }
                }
            },
            (LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, LifecycleInput::ModelsCleared { erase_id, .. }) => {
                if let Some(erase) = take_erase(&mut erases, erase_id) {
                    reply_all(erase.replies, Ok(()), fx);
                }
                LifecyclePhase::Ending { epoch_old, cause, next, failed, erase_credential, erases }
            }
            (LifecyclePhase::Ending { erases, .. }, LifecycleInput::Close { reply }) => self.closing(erases, vec![reply], None, fx),
            (phase @ LifecyclePhase::Ending { .. }, _) => phase,

            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) => match identity {
                None => LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential: Some(other), erases },
                Some(current) if current == other => LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(_) => {
                    self.ending(epoch, Cause::IdentityChanged, Some(other), erases, queued_replies, fx);
                    return;
                }
            },
            (LifecyclePhase::Resetting { epoch, mut queued_replies, erases, .. }, LifecycleInput::SessionReset { reply }) => {
                queued_replies.extend(reply);
                self.ending(epoch, Cause::SessionReset, None, erases, queued_replies, fx);
                return;
            }
            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }, LifecycleInput::ServerEnded(ServerEnded { epoch: request_epoch })) => {
                if request_epoch == epoch && identity.is_some() {
                    self.ending(epoch, Cause::ServerEnded, None, erases, queued_replies, fx);
                    return;
                }
                LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }
            }
            (LifecyclePhase::Resetting { epoch, identity, models, mut queued_models, mut queued_replies, pending_credential, erases }, LifecycleInput::ResetModels { models: more, reply }) => {
                queued_models.extend(more);
                queued_replies.extend(reply);
                LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }
            }
            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, mut erases }, LifecycleInput::ModelsCleared { generation_new, erase_id }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(erase) => {
                    let _ = generation_new;
                    let generation_new = self.published.generation.max(erase.target);
                    self.published.generation = generation_new;
                    if !queued_models.is_empty() {
                        self.published.state = PublishedState::Resetting;
                        self.publish(None, erase.replies, fx);
                        fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                        let next = self.new_erase(EraseKind::Models, queued_replies);
                        fx.push(LifecycleEffect::ClearModels { epoch, generation_new: next.target, models: queued_models.clone(), erase_id: next.id });
                        erases.push(next);
                        self.phase = LifecyclePhase::Resetting { epoch, identity, models: queued_models, queued_models: BTreeSet::new(), queued_replies: Vec::new(), pending_credential, erases };
                        return;
                    }
                    match pending_credential {
                        Some(next) => {
                            fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                            self.ended(epoch);
                            fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                            self.start(epoch + 1, Some(next), generation_new, erase.replies, fx);
                            LifecyclePhase::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.set_published(epoch, identity, generation_new, settled_state(identity));
                            self.publish(None, erase.replies, fx);
                            fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                            settled(epoch, identity)
                        }
                    }
                }
            },
            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, mut erases }, LifecycleInput::ClearFailed { erase_id, .. }) => match take_erase(&mut erases, erase_id) {
                None => LifecyclePhase::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(erase) => {
                    reply_all(erase.replies, Err(LifecycleError::Storage), fx);
                    reply_all(queued_replies, Err(LifecycleError::Storage), fx);
                    match pending_credential {
                        Some(next) => {
                            self.ended(epoch);
                            fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                            self.start(epoch + 1, Some(next), generation, Vec::new(), fx);
                            LifecyclePhase::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.set_published(epoch, identity, generation, settled_state(identity));
                            self.publish(None, Vec::new(), fx);
                            settled(epoch, identity)
                        }
                    }
                }
            },
            (LifecyclePhase::Resetting { epoch, queued_replies, erases, .. }, LifecycleInput::Close { reply }) => {
                reply_all(queued_replies, Err(LifecycleError::Interrupted), fx);
                self.closing(erases, vec![reply], Some(epoch), fx)
            }
            (phase @ LifecyclePhase::Resetting { .. }, _) => phase,

            (LifecyclePhase::Closing { mut waiting, erases, close_replies, store_closing, store_closed, observers_stopped }, LifecycleInput::Drained(Drained { owner })) => {
                if let Some(peer) = Peer::named(owner) {
                    waiting.remove(&peer);
                }
                let last = waiting.is_empty() && erases.is_empty() && !store_closing;
                if last {
                    fx.push(LifecycleEffect::StoreClose);
                }
                LifecyclePhase::Closing { waiting, erases, close_replies, store_closing: store_closing || last, store_closed, observers_stopped }
            }
            (LifecyclePhase::Closing { waiting, mut erases, close_replies, mut store_closing, store_closed, observers_stopped }, input @ (LifecycleInput::Cleared { .. } | LifecycleInput::ModelsCleared { .. } | LifecycleInput::ClearFailed { .. })) => {
                let (erase_id, result) = match input {
                    LifecycleInput::Cleared { erase_id } | LifecycleInput::ModelsCleared { erase_id, .. } => (erase_id, Ok(())),
                    LifecycleInput::ClearFailed { erase_id, .. } => (erase_id, Err(LifecycleError::Storage)),
                    _ => (u64::MAX, Ok(())),
                };
                if let Some(erase) = take_erase(&mut erases, erase_id) {
                    reply_all(erase.replies, result, fx);
                    if waiting.is_empty() && erases.is_empty() {
                        store_closing = true;
                        fx.push(LifecycleEffect::StoreClose);
                    }
                }
                LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
            }
            (LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }, LifecycleInput::StoreClosed) => {
                if store_closing && observers_stopped {
                    reply_all(close_replies, Ok(()), fx);
                    LifecyclePhase::Closed
                } else {
                    LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed: store_closed || store_closing, observers_stopped }
                }
            }
            (LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, .. }, LifecycleInput::ObserversStopped) => {
                if store_closed {
                    reply_all(close_replies, Ok(()), fx);
                    LifecyclePhase::Closed
                } else {
                    LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped: true }
                }
            }
            (LifecyclePhase::Closing { waiting, erases, mut close_replies, store_closing, store_closed, observers_stopped }, LifecycleInput::Close { reply }) => {
                close_replies.push(reply);
                LifecyclePhase::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
            }
            (phase @ LifecyclePhase::Closing { .. }, LifecycleInput::SessionReset { reply } | LifecycleInput::ResetModels { reply, .. }) => {
                reply_all(reply.into_iter().collect(), Err(LifecycleError::Closed), fx);
                phase
            }
            (phase @ LifecyclePhase::Closing { .. }, _) => phase,

            (LifecyclePhase::Closed, LifecycleInput::SessionReset { reply } | LifecycleInput::ResetModels { reply, .. }) => {
                reply_all(reply.into_iter().collect(), Err(LifecycleError::Closed), fx);
                LifecyclePhase::Closed
            }
            (LifecyclePhase::Closed, LifecycleInput::Close { reply }) => {
                reply_all(vec![reply], Err(LifecycleError::Closed), fx);
                LifecyclePhase::Closed
            }
            (LifecyclePhase::Closed, _) => LifecyclePhase::Closed,
        };
    }

    /// `life_owner_started`, `life_restore_classified` and their stray row.
    fn restore(&mut self, owner: Peer, epoch: Epoch, started: bool, fx: &mut Vec<LifecycleEffect>) {
        if matches!(self.phase, LifecyclePhase::Closing { .. } | LifecyclePhase::Closed) {
            return;
        }
        let Some(restoring) = &mut self.restoring else { return };
        if restoring.epoch != epoch {
            return;
        }
        if started {
            if restoring.start_waiting.remove(&owner) && restoring.start_waiting.is_empty() {
                fx.push(LifecycleEffect::ReadIntents { epoch });
            }
            return;
        }
        if !restoring.restore_waiting.remove(&owner) || !restoring.restore_waiting.is_empty() {
            return;
        }
        self.finish_restore(epoch, fx);
    }

    /// `life_saved`, `life_saved_stale`, `life_save_failed`, `life_save_failed_stale`, `life_closing_saved` and the stray rows.
    fn saved(&mut self, id: PublicationId, ok: bool, fx: &mut Vec<LifecycleEffect>) {
        let Some(at) = self.publishing.iter().position(|publication| publication.id == id) else { return };
        if matches!(self.phase, LifecyclePhase::Closed) {
            return;
        }
        let publication = self.publishing.remove(at);
        if matches!(self.phase, LifecyclePhase::Closing { .. }) {
            reply_all(publication.replies, if ok { Ok(()) } else { Err(LifecycleError::Storage) }, fx);
            return;
        }
        let newer = self.cell_publication > id;
        let restricting = restricting(publication.published.state);
        if ok {
            let ended = self.ended_up_to.is_some_and(|ended| publication.published.epoch <= ended);
            if !restricting && (ended || newer) {
                reply_all(publication.replies, Err(LifecycleError::Interrupted), fx);
                return;
            }
            if !restricting {
                self.cell = publication.published.clone();
                self.cell_publication = id;
                fx.push(LifecycleEffect::PublishCell(publication.published.clone()));
            }
            reply_all(publication.replies, Ok(()), fx);
            if let Some(Started { epoch, identity, generation }) = publication.started {
                fx.push(LifecycleEffect::EpochStarted { epoch, identity, generation });
                self.restore_progress(epoch, fx);
            }
            return;
        }
        reply_all(publication.replies, Err(LifecycleError::Storage), fx);
        let opening = publication.started.as_ref().map(|started| started.epoch);
        if let Some(restoring) = self.restoring.take_if(|restoring| opening == Some(restoring.epoch)) {
            reply_all(restoring.open_reply.into_iter().collect(), Err(LifecycleError::Storage), fx);
        }
        if !newer {
            self.cell.state = PublishedState::StorageFailed;
            fx.push(LifecycleEffect::PublishCell(self.cell.clone()));
        }
    }

    /// `life_executor_failed`.
    fn executor_failed(&mut self, fx: &mut Vec<LifecycleEffect>) {
        let phase = std::mem::replace(&mut self.phase, LifecyclePhase::Closed);
        self.phase = match phase {
            LifecyclePhase::Closed => LifecyclePhase::Closed,
            phase @ LifecyclePhase::Closing { .. } => phase,
            phase => {
                let (end, erases, mut open) = match phase {
                    LifecyclePhase::NoSession { epoch } | LifecyclePhase::Session { epoch, .. } => (Some(epoch), Vec::new(), Vec::new()),
                    LifecyclePhase::Resetting { epoch, erases, queued_replies, .. } => (Some(epoch), erases, queued_replies),
                    LifecyclePhase::Ending { erases, .. } => (None, erases, Vec::new()),
                    _ => (None, Vec::new(), Vec::new()),
                };
                for erase in &erases {
                    open.extend(erase.replies.iter().copied());
                }
                for publication in &mut self.publishing {
                    open.append(&mut publication.replies);
                }
                self.drop_restoring(Err(LifecycleError::Runtime), fx);
                reply_all(open, Err(LifecycleError::Runtime), fx);
                let erases: Vec<Erase> = erases.into_iter().map(|erase| Erase { replies: Vec::new(), ..erase }).collect();
                self.closing(erases, Vec::new(), end, fx)
            }
        };
    }
}
