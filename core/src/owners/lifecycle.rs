//! Owner `Lifecycle` (`specs/machines/lifecycle.md`): the epoch, the identity of the session, the model generation and the close.
//! Every output is an effect for the executor: it publishes, broadcasts the control inputs and sends the Store its requests.

use std::collections::BTreeSet;

use super::contract::{CredentialCommitted, Drained, Published, ServerEnded};
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

pub type Identity = u64;
pub type ReplyId = u64;

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
}

/// `all owners` of the table.
pub const ALL_OWNERS: [Peer; 8] = [Peer::Network, Peer::Session, Peer::Windows, Peer::Feed, Peer::Deliveries, Peer::Outbox, Peer::Transfer, Peer::Calls];

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
        }
    }

    fn named(name: &str) -> Option<Self> { ALL_OWNERS.into_iter().find(|peer| peer.name() == name) }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecyclePhase {
    NoSession { epoch: Epoch },
    Session { epoch: Epoch, identity: Identity },
    Ending { epoch_old: Epoch, cause: Cause, next: Option<Identity>, replies: Vec<ReplyId>, failed: bool },
    Resetting { epoch: Epoch, identity: Option<Identity>, models: BTreeSet<String>, queued_models: BTreeSet<String>, replies: Vec<ReplyId>, pending_credential: Option<Identity> },
    Closing { waiting: BTreeSet<Peer>, clearing: bool, replies: Vec<ReplyId> },
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    Storage,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleEffect {
    Publish(Published),
    EpochStarted { epoch: Epoch, identity: Option<Identity>, generation: u64 },
    EpochEnded { epoch: Epoch, cause: Cause },
    ModelsReset { epoch: Epoch, generation: u64, models: BTreeSet<String> },
    Closing,
    ClearAccount { epoch_old: Epoch, cause: Cause, generation_new: u64 },
    ClearModels { epoch: Epoch, generation_new: u64, models: BTreeSet<String> },
    StoreClose,
    /// `Self ResetModels(changed_models, None)` of `life_open_changed_models`.
    ResetModelsSelf(BTreeSet<String>),
    Reply { id: ReplyId, result: Result<(), LifecycleError> },
}

#[derive(Clone, Debug)]
pub enum LifecycleInput {
    Open { saved_epoch: Epoch, saved_generation: u64, saved_identity: Option<Identity>, changed_models: BTreeSet<String> },
    CredentialCommitted(CredentialCommitted),
    SessionReset { reply: ReplyId },
    ServerEnded(ServerEnded),
    ResetModels { models: BTreeSet<String>, reply: Option<ReplyId> },
    Cleared { epoch_old: Epoch },
    ModelsCleared { generation_new: u64 },
    ClearFailed { error: String },
    Close { reply: ReplyId },
    Drained(Drained),
    /// `Tick`, `EpochEnded` and `Closing` are not inputs of Lifecycle.
    Idle,
}

pub struct LifecycleState {
    phase: LifecyclePhase,
    published: Published,
}

impl LifecycleState {
    /// Before `Open`: no session, epoch 0.
    pub fn new() -> Self { Self { phase: LifecyclePhase::NoSession { epoch: 0 }, published: Published { epoch: 0, identity: None, generation: 0, closed: false } } }

    pub fn phase(&self) -> &LifecyclePhase { &self.phase }

    pub fn published(&self) -> &Published { &self.published }

    fn publish(&self, fx: &mut Vec<LifecycleEffect>) { fx.push(LifecycleEffect::Publish(self.published.clone())); }

    fn started(&mut self, epoch: Epoch, identity: Option<Identity>, generation: u64, fx: &mut Vec<LifecycleEffect>) {
        self.published = Published { epoch, identity, generation, closed: false };
        self.publish(fx);
        fx.push(LifecycleEffect::EpochStarted { epoch, identity, generation });
    }

    fn closing(&mut self, replies: Vec<ReplyId>, clearing: bool, end: Option<Epoch>, fx: &mut Vec<LifecycleEffect>) -> LifecyclePhase {
        self.published.closed = true;
        self.publish(fx);
        if let Some(epoch) = end {
            fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::Close });
        }
        fx.push(LifecycleEffect::Closing);
        LifecyclePhase::Closing { waiting: ALL_OWNERS.into_iter().collect(), clearing, replies }
    }

    fn ending(&mut self, epoch: Epoch, cause: Cause, next: Option<Identity>, replies: Vec<ReplyId>, fx: &mut Vec<LifecycleEffect>) {
        fx.push(LifecycleEffect::EpochEnded { epoch, cause });
        fx.push(LifecycleEffect::ClearAccount { epoch_old: epoch, cause, generation_new: self.published.generation + 1 });
        self.phase = LifecyclePhase::Ending { epoch_old: epoch, cause, next, replies, failed: false };
    }
}

fn reply_all(replies: Vec<ReplyId>, result: Result<(), LifecycleError>, fx: &mut Vec<LifecycleEffect>) {
    fx.extend(replies.into_iter().map(|id| LifecycleEffect::Reply { id, result }));
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
}

impl LifecycleState {
    /// One row of `lifecycle.md` per match arm, in table order.
    fn run(&mut self, input: LifecycleInput, fx: &mut Vec<LifecycleEffect>) {
        let generation = self.published.generation;
        let phase = std::mem::replace(&mut self.phase, LifecyclePhase::Closed);
        if let LifecycleInput::Open { saved_epoch, saved_generation, saved_identity, changed_models } = input {
            self.phase = match saved_identity {
                Some(identity) => LifecyclePhase::Session { epoch: saved_epoch + 1, identity },
                None => LifecyclePhase::NoSession { epoch: saved_epoch + 1 },
            };
            self.started(saved_epoch + 1, saved_identity, saved_generation, fx);
            if !changed_models.is_empty() {
                fx.push(LifecycleEffect::ResetModelsSelf(changed_models));
            }
            return;
        }
        self.phase = match (phase, input) {
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity })) => {
                fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                self.started(epoch + 1, Some(identity), generation, fx);
                LifecyclePhase::Session { epoch: epoch + 1, identity }
            }
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::SessionReset { reply }) => {
                self.ending(epoch, Cause::SessionReset, None, vec![reply], fx);
                return;
            }
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::ResetModels { models, reply }) => {
                fx.push(LifecycleEffect::ClearModels { epoch, generation_new: generation + 1, models: models.clone() });
                LifecyclePhase::Resetting { epoch, identity: None, models, queued_models: BTreeSet::new(), replies: reply.into_iter().collect(), pending_credential: None }
            }
            (LifecyclePhase::NoSession { epoch }, LifecycleInput::Close { reply }) => self.closing(vec![reply], false, Some(epoch), fx),
            (phase @ LifecyclePhase::NoSession { .. }, _) => phase,

            (LifecyclePhase::Session { epoch, identity }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) if other == identity => LifecyclePhase::Session { epoch, identity },
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) => {
                self.ending(epoch, Cause::IdentityChanged, Some(other), vec![], fx);
                return;
            }
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::SessionReset { reply }) => {
                self.ending(epoch, Cause::SessionReset, None, vec![reply], fx);
                return;
            }
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::ServerEnded(ServerEnded { epoch: request_epoch })) if request_epoch == epoch => {
                self.ending(epoch, Cause::ServerEnded, None, vec![], fx);
                return;
            }
            (LifecyclePhase::Session { epoch, identity }, LifecycleInput::ResetModels { models, reply }) => {
                fx.push(LifecycleEffect::ClearModels { epoch, generation_new: generation + 1, models: models.clone() });
                LifecyclePhase::Resetting { epoch, identity: Some(identity), models, queued_models: BTreeSet::new(), replies: reply.into_iter().collect(), pending_credential: None }
            }
            (LifecyclePhase::Session { epoch, .. }, LifecycleInput::Close { reply }) => self.closing(vec![reply], false, Some(epoch), fx),
            (phase @ LifecyclePhase::Session { .. }, _) => phase,

            (LifecyclePhase::Ending { epoch_old, cause, replies, failed, .. }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity })) => {
                if failed {
                    fx.push(LifecycleEffect::ClearAccount { epoch_old, cause, generation_new: generation + 1 });
                }
                LifecyclePhase::Ending { epoch_old, cause, next: Some(identity), replies, failed: false }
            }
            (LifecyclePhase::Ending { epoch_old, cause, mut replies, failed, .. }, LifecycleInput::SessionReset { reply }) => {
                let mut cause = cause;
                if failed {
                    replies = vec![reply];
                    cause = Cause::SessionReset;
                    fx.push(LifecycleEffect::ClearAccount { epoch_old, cause, generation_new: generation + 1 });
                } else {
                    replies.push(reply);
                }
                LifecyclePhase::Ending { epoch_old, cause, next: None, replies, failed: false }
            }
            (LifecyclePhase::Ending { epoch_old, cause, next, mut replies, failed }, LifecycleInput::ResetModels { reply, .. }) => {
                replies.extend(reply);
                LifecyclePhase::Ending { epoch_old, cause, next, replies, failed }
            }
            (LifecyclePhase::Ending { epoch_old, next, replies, .. }, LifecycleInput::Cleared { .. }) => {
                reply_all(replies, Ok(()), fx);
                match next {
                    None => {
                        self.started(epoch_old + 1, None, generation + 1, fx);
                        LifecyclePhase::NoSession { epoch: epoch_old + 1 }
                    }
                    Some(identity) => {
                        self.started(epoch_old + 2, Some(identity), generation + 1, fx);
                        LifecyclePhase::Session { epoch: epoch_old + 2, identity }
                    }
                }
            }
            (LifecyclePhase::Ending { epoch_old, cause, next, replies, .. }, LifecycleInput::ClearFailed { .. }) => {
                reply_all(replies, Err(LifecycleError::Storage), fx);
                LifecyclePhase::Ending { epoch_old, cause, next, replies: Vec::new(), failed: true }
            }
            (LifecyclePhase::Ending { mut replies, .. }, LifecycleInput::Close { reply }) => {
                replies.push(reply);
                self.closing(replies, true, None, fx)
            }
            (phase @ LifecyclePhase::Ending { .. }, _) => phase,

            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, replies, pending_credential }, LifecycleInput::CredentialCommitted(CredentialCommitted { identity: other })) => match identity {
                None => LifecyclePhase::Resetting { epoch, identity, models, queued_models, replies, pending_credential: Some(other) },
                Some(current) if current == other => LifecyclePhase::Resetting { epoch, identity, models, queued_models, replies, pending_credential },
                Some(_) => {
                    self.ending(epoch, Cause::IdentityChanged, Some(other), replies, fx);
                    return;
                }
            },
            (LifecyclePhase::Resetting { epoch, mut replies, .. }, LifecycleInput::SessionReset { reply }) => {
                replies.push(reply);
                self.ending(epoch, Cause::SessionReset, None, replies, fx);
                return;
            }
            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, replies, pending_credential }, LifecycleInput::ServerEnded(ServerEnded { epoch: request_epoch })) => {
                if request_epoch == epoch && identity.is_some() {
                    self.ending(epoch, Cause::ServerEnded, None, replies, fx);
                    return;
                }
                LifecyclePhase::Resetting { epoch, identity, models, queued_models, replies, pending_credential }
            }
            (LifecyclePhase::Resetting { epoch, identity, models, mut queued_models, mut replies, pending_credential }, LifecycleInput::ResetModels { models: more, reply }) => {
                queued_models.extend(more);
                replies.extend(reply);
                LifecyclePhase::Resetting { epoch, identity, models, queued_models, replies, pending_credential }
            }
            (LifecyclePhase::Resetting { epoch, identity, models, queued_models, replies, pending_credential }, LifecycleInput::ModelsCleared { generation_new }) => {
                self.published.generation = generation_new;
                if !queued_models.is_empty() {
                    self.publish(fx);
                    fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                    fx.push(LifecycleEffect::ClearModels { epoch, generation_new: generation_new + 1, models: queued_models.clone() });
                    return self.set(LifecyclePhase::Resetting { epoch, identity, models: queued_models, queued_models: BTreeSet::new(), replies, pending_credential });
                }
                reply_all(replies, Ok(()), fx);
                match pending_credential {
                    Some(next) => {
                        self.published.epoch = epoch + 1;
                        self.published.identity = Some(next);
                        self.publish(fx);
                        fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                        fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                        fx.push(LifecycleEffect::EpochStarted { epoch: epoch + 1, identity: Some(next), generation: generation_new });
                        LifecyclePhase::Session { epoch: epoch + 1, identity: next }
                    }
                    None => {
                        self.publish(fx);
                        fx.push(LifecycleEffect::ModelsReset { epoch, generation: generation_new, models });
                        match identity {
                            Some(identity) => LifecyclePhase::Session { epoch, identity },
                            None => LifecyclePhase::NoSession { epoch },
                        }
                    }
                }
            }
            (LifecyclePhase::Resetting { epoch, identity, replies, pending_credential, .. }, LifecycleInput::ClearFailed { .. }) => {
                reply_all(replies, Err(LifecycleError::Storage), fx);
                match pending_credential {
                    Some(next) => {
                        self.published.epoch = epoch + 1;
                        self.published.identity = Some(next);
                        self.publish(fx);
                        fx.push(LifecycleEffect::EpochEnded { epoch, cause: Cause::SessionStarted });
                        fx.push(LifecycleEffect::EpochStarted { epoch: epoch + 1, identity: Some(next), generation });
                        LifecyclePhase::Session { epoch: epoch + 1, identity: next }
                    }
                    None => match identity {
                        Some(identity) => LifecyclePhase::Session { epoch, identity },
                        None => LifecyclePhase::NoSession { epoch },
                    },
                }
            }
            (LifecyclePhase::Resetting { epoch, mut replies, .. }, LifecycleInput::Close { reply }) => {
                replies.push(reply);
                self.closing(replies, true, Some(epoch), fx)
            }
            (phase @ LifecyclePhase::Resetting { .. }, _) => phase,

            (LifecyclePhase::Closing { mut waiting, clearing, replies }, LifecycleInput::Drained(Drained { owner })) => {
                if let Some(peer) = Peer::named(owner) {
                    waiting.remove(&peer);
                }
                if waiting.is_empty() && !clearing {
                    reply_all(replies, Ok(()), fx);
                    fx.push(LifecycleEffect::StoreClose);
                    LifecyclePhase::Closed
                } else {
                    LifecyclePhase::Closing { waiting, clearing, replies }
                }
            }
            (LifecyclePhase::Closing { waiting, replies, .. }, LifecycleInput::Cleared { .. } | LifecycleInput::ModelsCleared { .. } | LifecycleInput::ClearFailed { .. }) => {
                if waiting.is_empty() {
                    reply_all(replies, Ok(()), fx);
                    fx.push(LifecycleEffect::StoreClose);
                    LifecyclePhase::Closed
                } else {
                    LifecyclePhase::Closing { waiting, clearing: false, replies }
                }
            }
            (LifecyclePhase::Closing { waiting, clearing, mut replies }, LifecycleInput::Close { reply }) => {
                replies.push(reply);
                LifecyclePhase::Closing { waiting, clearing, replies }
            }
            (phase @ LifecyclePhase::Closing { .. }, LifecycleInput::SessionReset { reply }) => {
                fx.push(LifecycleEffect::Reply { id: reply, result: Err(LifecycleError::Closed) });
                phase
            }
            (phase @ LifecyclePhase::Closing { .. }, LifecycleInput::ResetModels { reply, .. }) => {
                refuse(reply, fx);
                phase
            }
            (phase @ LifecyclePhase::Closing { .. }, _) => phase,

            (LifecyclePhase::Closed, LifecycleInput::SessionReset { reply } | LifecycleInput::Close { reply }) => {
                fx.push(LifecycleEffect::Reply { id: reply, result: Err(LifecycleError::Closed) });
                LifecyclePhase::Closed
            }
            (LifecyclePhase::Closed, LifecycleInput::ResetModels { reply, .. }) => {
                refuse(reply, fx);
                LifecyclePhase::Closed
            }
            (LifecyclePhase::Closed, _) => LifecyclePhase::Closed,
        };
    }

    fn set(&mut self, phase: LifecyclePhase) { self.phase = phase; }
}

fn refuse(reply: Option<ReplyId>, fx: &mut Vec<LifecycleEffect>) {
    fx.extend(reply.map(|id| LifecycleEffect::Reply { id, result: Err(LifecycleError::Closed) }));
}
