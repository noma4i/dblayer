use std::collections::BTreeSet;

use serde_json::json;

use crate::check::model::RefModel;
use crate::check::value::{Input, Observation};

pub type Identity = u64;
pub type Id = u64;
pub type EraseId = u64;
pub type PublicationId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Owner {
    Network,
    Session,
    Window,
    Feed,
    Delivery,
    Outbox,
    Transfer,
    Calls,
    LocalWrites,
    Rounds,
}

/// All owners.
pub const OWNERS: [Owner; 10] = [Owner::Network, Owner::Session, Owner::Window, Owner::Feed, Owner::Delivery, Owner::Outbox, Owner::Transfer, Owner::Calls, Owner::LocalWrites, Owner::Rounds];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    SessionReset,
    ServerEnded,
    IdentityChanged,
    SessionStarted,
    Close,
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
    pub replies: Vec<Id>,
}

/// The restore of the intents of an epoch with an identity.
#[derive(Clone, Debug, PartialEq)]
pub struct Restoring {
    pub epoch: u64,
    pub start_waiting: BTreeSet<Owner>,
    pub restore_waiting: BTreeSet<Owner>,
    pub open_reply: Option<Id>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PubState {
    NoSession,
    Session,
    Ending,
    EndingFailed,
    Resetting,
    StorageFailed,
    Closing,
}

#[derive(Clone, Debug, PartialEq)]
pub enum State {
    NoSession { epoch: u64 },
    Session { epoch: u64, identity: Identity },
    Ending { epoch_old: u64, cause: Cause, next: Option<Identity>, failed: bool, erase_credential: bool, erases: Vec<Erase> },
    Resetting { epoch: u64, identity: Option<Identity>, models: BTreeSet<String>, queued_models: BTreeSet<String>, queued_replies: Vec<Id>, pending_credential: Option<Identity>, erases: Vec<Erase> },
    Closing { waiting: BTreeSet<Owner>, erases: Vec<Erase>, close_replies: Vec<Id>, store_closing: bool, store_closed: bool, observers_stopped: bool },
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Credential(Identity),
    SessionReset(Option<Id>),
    ServerEnded(u64),
    ResetModels(BTreeSet<String>, Option<Id>),
    Cleared(EraseId),
    ModelsCleared(u64, EraseId),
    ClearFailed(EraseId),
    Saved(PublicationId),
    SaveFailed(PublicationId),
    StoreClosed,
    ObserversStopped,
    ExecutorFailed,
    Started(Owner, u64),
    RestoreClassified(Owner, u64),
    Close(Id),
    Drained(Owner),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    /// `Publish`: the cell at once, the Store saves it as `publication_id`.
    Publish(PublicationId, Published),
    /// The cell only: the Store did not save the value (`life_save_failed`).
    PublishCell(Published),
    EpochStarted(u64, Option<Identity>, u64),
    EpochEnded(u64, Cause),
    ModelsReset(u64, u64, BTreeSet<String>),
    Closing,
    StopObserving,
    ClearAccount { epoch_old: u64, cause: Cause, generation_new: u64, erase_id: EraseId },
    ClearModels { epoch: u64, generation_new: u64, models: BTreeSet<String>, erase_id: EraseId },
    ReadIntents(u64),
    RestoreReady(u64),
    StoreClose,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Answer {
    Ok,
    Storage,
    Closed,
    Runtime,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub epoch: u64,
    pub identity: Option<Identity>,
    pub generation: u64,
    pub state: PubState,
    pub closed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Publication {
    pub id: PublicationId,
    pub published: Published,
    pub epoch_started: Option<(u64, Option<Identity>, u64)>,
    pub replies: Vec<Id>,
}

pub struct Lifecycle {
    pub state: State,
    pub published: Published,
    pub publishing: Vec<Publication>,
    /// The value of `PublishedCell`: restricting publications are written at once, active ones at `Saved`.
    pub cell: Published,
    pub restoring: Option<Restoring>,
    cell_publication: PublicationId,
    ended_up_to: Option<u64>,
    next_publication: PublicationId,
    next_erase: EraseId,
    /// `issued_generation`: the highest generation reserved for an erase.
    allocated: u64,
}

#[derive(Default)]
pub struct Out {
    pub effects: Vec<Effect>,
    pub replies: Vec<(Id, Answer)>,
}

fn answer_all(out: &mut Out, replies: Vec<Id>, answer: Answer) { out.replies.extend(replies.into_iter().map(|id| (id, answer))); }

fn all_owners() -> BTreeSet<Owner> { OWNERS.into_iter().collect() }

impl Lifecycle {
    fn blank() -> Self {
        Self {
            state: State::NoSession { epoch: 0 },
            published: Published { epoch: 0, identity: None, generation: 0, state: PubState::NoSession, closed: false },
            publishing: Vec::new(),
            cell: Published { epoch: 0, identity: None, generation: 0, state: PubState::NoSession, closed: false },
            restoring: None,
            cell_publication: 0,
            ended_up_to: None,
            next_publication: 1,
            next_erase: 1,
            allocated: 0,
        }
    }

    /// `Open` rows: the initial state, the first publication, `EpochStarted` and the wait of `open` held for `Saved`.
    pub fn open(saved_epoch: u64, saved_generation: u64, saved_identity: Option<Identity>, reply: Option<Id>) -> (Self, Out) {
        let mut life = Self::blank();
        life.allocated = saved_generation;
        let epoch = saved_epoch + 1;
        let (state, pub_state) = match saved_identity {
            Some(identity) => (State::Session { epoch, identity }, PubState::Session),
            None => (State::NoSession { epoch }, PubState::NoSession),
        };
        life.state = state;
        life.published = Published { epoch, identity: saved_identity, generation: saved_generation, state: pub_state, closed: false };
        let mut out = Out::default();
        if saved_identity.is_some() {
            life.restoring = Some(Restoring { epoch, start_waiting: [Owner::Outbox, Owner::Transfer].into(), restore_waiting: [Owner::Outbox, Owner::Transfer, Owner::Session].into(), open_reply: reply });
            life.publish(&mut out, Some((epoch, saved_identity, saved_generation)), Vec::new());
        } else {
            life.publish(&mut out, Some((epoch, saved_identity, saved_generation)), reply.into_iter().collect());
        }
        (life, out)
    }

    /// `Publish` of the current `published`. `started` and `replies` wait for `Saved` of this publication.
    fn publish(&mut self, out: &mut Out, started: Option<(u64, Option<Identity>, u64)>, replies: Vec<Id>) {
        let id = self.next_publication;
        self.next_publication += 1;
        out.effects.push(Effect::Publish(id, self.published.clone()));
        if matches!(self.published.state, PubState::Ending | PubState::EndingFailed | PubState::Closing | PubState::StorageFailed) {
            self.cell = self.published.clone();
            self.cell_publication = id;
        }
        self.publishing.push(Publication { id, published: self.published.clone(), epoch_started: started, replies });
    }

    fn set_published(&mut self, epoch: u64, identity: Option<Identity>, generation: u64, state: PubState) {
        self.published = Published { epoch, identity, generation, state, closed: false };
    }

    fn erase(&mut self, kind: EraseKind, replies: Vec<Id>) -> Erase {
        let id = self.next_erase;
        self.next_erase += 1;
        self.allocated = self.allocated.max(self.published.generation) + 1;
        Erase { id, kind, target: self.allocated, replies }
    }

    /// The restore of an epoch that ended, or of a close, ends with the wait of `open`.
    fn drop_restoring(&mut self, out: &mut Out, answer: Answer) {
        if let Some(restoring) = self.restoring.take() { answer_all(out, restoring.open_reply.into_iter().collect(), answer); }
    }

    fn end_restoring(&mut self, out: &mut Out) {
        let saved = self.restoring.as_ref().is_some_and(|restoring| !self.publishing.iter().any(|publication| publication.epoch_started.is_some_and(|(epoch, ..)| epoch == restoring.epoch)));
        self.drop_restoring(out, if saved { Answer::Ok } else { Answer::Interrupted });
    }

    fn ended(&mut self, epoch: u64) { self.ended_up_to = Some(self.ended_up_to.map_or(epoch, |ended| ended.max(epoch))); }

    fn closing(&mut self, out: &mut Out, erases: Vec<Erase>, close_replies: Vec<Id>, end: Option<u64>) -> State {
        self.published.state = PubState::Closing;
        self.published.closed = true;
        self.publish(out, None, Vec::new());
        self.drop_restoring(out, Answer::Interrupted);
        if let Some(epoch) = end {
            self.ended(epoch);
            out.effects.push(Effect::EpochEnded(epoch, Cause::Close));
        }
        out.effects.push(Effect::Closing);
        out.effects.push(Effect::StopObserving);
        State::Closing { waiting: all_owners(), erases, close_replies, store_closing: false, store_closed: false, observers_stopped: false }
    }

    /// `Ending` entered from a state with a live epoch: `EpochEnded` · `Publish` · `ClearAccount`.
    fn ending(&mut self, epoch: u64, cause: Cause, next: Option<Identity>, mut erases: Vec<Erase>, replies: Vec<Id>, out: &mut Out) {
        self.ended(epoch);
        self.end_restoring(out);
        out.effects.push(Effect::EpochEnded(epoch, cause));
        self.published.state = PubState::Ending;
        self.publish(out, None, Vec::new());
        let erase = self.erase(EraseKind::Account, replies);
        out.effects.push(Effect::ClearAccount { epoch_old: epoch, cause, generation_new: erase.target, erase_id: erase.id });
        erases.push(erase);
        self.state = State::Ending { epoch_old: epoch, cause, next, failed: false, erase_credential: false, erases };
    }

    fn start(&mut self, epoch: u64, identity: Option<Identity>, generation: u64, replies: Vec<Id>, out: &mut Out) {
        self.set_published(epoch, identity, generation, if identity.is_some() { PubState::Session } else { PubState::NoSession });
        if identity.is_some() {
            self.restoring = Some(Restoring { epoch, start_waiting: [Owner::Outbox, Owner::Transfer].into(), restore_waiting: [Owner::Outbox, Owner::Transfer, Owner::Session].into(), open_reply: None });
        }
        self.publish(out, Some((epoch, identity, generation)), replies);
    }

    /// The state a reset returns to.
    fn settled(epoch: u64, identity: Option<Identity>) -> State {
        match identity {
            Some(identity) => State::Session { epoch, identity },
            None => State::NoSession { epoch },
        }
    }

    fn take_erase(erases: &mut Vec<Erase>, id: EraseId) -> Option<Erase> {
        let at = erases.iter().position(|erase| erase.id == id)?;
        Some(erases.remove(at))
    }

    /// One row of the `lifecycle` machine per match arm, in table order.
    pub fn step(&mut self, event: Event) -> Out {
        let mut out = Out::default();
        if let Event::Saved(id) | Event::SaveFailed(id) = event {
            self.saved(id, matches!(event, Event::Saved(_)), &mut out);
            return out;
        }
        if matches!(event, Event::ExecutorFailed) {
            self.executor_failed(&mut out);
            return out;
        }
        if let Event::Started(owner, epoch) | Event::RestoreClassified(owner, epoch) = event {
            self.restore(owner, epoch, matches!(event, Event::Started(..)), &mut out);
            return out;
        }
        let state = std::mem::replace(&mut self.state, State::Closed);
        self.state = match (state, event) {
            (State::NoSession { epoch }, Event::Credential(identity)) => {
                self.ended(epoch);
                out.effects.push(Effect::EpochEnded(epoch, Cause::SessionStarted));
                self.start(epoch + 1, Some(identity), self.published.generation, Vec::new(), &mut out);
                State::Session { epoch: epoch + 1, identity }
            }
            (State::NoSession { epoch }, Event::SessionReset(reply)) => {
                self.ending(epoch, Cause::SessionReset, None, Vec::new(), reply.into_iter().collect(), &mut out);
                return out;
            }
            (State::NoSession { epoch }, Event::ResetModels(models, reply)) => self.resetting(epoch, None, models, Vec::new(), reply, &mut out),
            (State::NoSession { epoch }, Event::Close(reply)) => self.closing(&mut out, Vec::new(), vec![reply], Some(epoch)),
            (state @ State::NoSession { .. }, _) => state,

            (State::Session { epoch, identity }, Event::Credential(other)) if other == identity => State::Session { epoch, identity },
            (State::Session { epoch, .. }, Event::Credential(other)) => {
                self.ending(epoch, Cause::IdentityChanged, Some(other), Vec::new(), Vec::new(), &mut out);
                return out;
            }
            (State::Session { epoch, .. }, Event::SessionReset(reply)) => {
                self.ending(epoch, Cause::SessionReset, None, Vec::new(), reply.into_iter().collect(), &mut out);
                return out;
            }
            (State::Session { epoch, .. }, Event::ServerEnded(request_epoch)) if request_epoch == epoch => {
                self.ending(epoch, Cause::ServerEnded, None, Vec::new(), Vec::new(), &mut out);
                return out;
            }
            (State::Session { epoch, identity }, Event::ResetModels(models, reply)) => self.resetting(epoch, Some(identity), models, Vec::new(), reply, &mut out),
            (State::Session { epoch, .. }, Event::Close(reply)) => self.closing(&mut out, Vec::new(), vec![reply], Some(epoch)),
            (state @ State::Session { .. }, _) => state,

            (State::Ending { epoch_old, cause, failed, mut erases, erase_credential, .. }, Event::Credential(identity)) => {
                if failed {
                    self.published.state = PubState::Ending;
                    self.publish(&mut out, None, Vec::new());
                    let erase = self.erase(EraseKind::Account, Vec::new());
                    out.effects.push(Effect::ClearAccount { epoch_old, cause, generation_new: erase.target, erase_id: erase.id });
                    erases.push(erase);
                }
                State::Ending { epoch_old, cause, next: Some(identity), failed: false, erase_credential, erases }
            }
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::SessionReset(reply)) => {
                if failed {
                    self.published.state = PubState::Ending;
                    self.publish(&mut out, None, Vec::new());
                    let erase = self.erase(EraseKind::Account, reply.into_iter().collect());
                    out.effects.push(Effect::ClearAccount { epoch_old, cause: Cause::SessionReset, generation_new: erase.target, erase_id: erase.id });
                    erases.push(erase);
                    let _ = next;
                    State::Ending { epoch_old, cause: Cause::SessionReset, next: None, failed: false, erase_credential: false, erases }
                } else {
                    let credential = erase_credential || cause == Cause::IdentityChanged;
                    if let Some(account) = erases.iter_mut().rev().find(|erase| erase.kind == EraseKind::Account) { account.replies.extend(reply); }
                    State::Ending { epoch_old, cause, next: None, failed: false, erase_credential: credential, erases }
                }
            }
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::ResetModels(_, reply)) => {
                if failed {
                    answer_all(&mut out, reply.into_iter().collect(), Answer::Storage);
                } else if let Some(account) = erases.iter_mut().rev().find(|erase| erase.kind == EraseKind::Account) {
                    account.replies.extend(reply);
                }
                State::Ending { epoch_old, cause, next, failed, erase_credential, erases }
            }
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::Cleared(id)) => match Self::take_erase(&mut erases, id) {
                None => State::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) if erase_credential => {
                    self.published.generation = self.published.generation.max(erase.target);
                    let second = self.erase(EraseKind::Account, erase.replies);
                    out.effects.push(Effect::ClearAccount { epoch_old, cause: Cause::SessionReset, generation_new: second.target, erase_id: second.id });
                    erases.push(second);
                    State::Ending { epoch_old, cause: Cause::SessionReset, next, failed, erase_credential: false, erases }
                }
                Some(erase) => {
                    let generation = self.published.generation.max(erase.target);
                    match next {
                        None => {
                            self.start(epoch_old + 1, None, generation, erase.replies, &mut out);
                            State::NoSession { epoch: epoch_old + 1 }
                        }
                        Some(identity) => {
                            self.start(epoch_old + 2, Some(identity), generation, erase.replies, &mut out);
                            State::Session { epoch: epoch_old + 2, identity }
                        }
                    }
                }
            },
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::ClearFailed(id)) => match Self::take_erase(&mut erases, id) {
                None => State::Ending { epoch_old, cause, next, failed, erase_credential, erases },
                Some(erase) => {
                    answer_all(&mut out, erase.replies, Answer::Storage);
                    if erase.kind == EraseKind::Account {
                        self.published.state = PubState::EndingFailed;
                        self.publish(&mut out, None, Vec::new());
                        State::Ending { epoch_old, cause, next, failed: true, erase_credential, erases }
                    } else {
                        State::Ending { epoch_old, cause, next, failed, erase_credential, erases }
                    }
                }
            },
            (State::Ending { epoch_old, cause, next, failed, erase_credential, mut erases }, Event::ModelsCleared(_, id)) => {
                if let Some(erase) = Self::take_erase(&mut erases, id) { answer_all(&mut out, erase.replies, Answer::Ok); }
                State::Ending { epoch_old, cause, next, failed, erase_credential, erases }
            }
            (State::Ending { erases, .. }, Event::Close(reply)) => self.closing(&mut out, erases, vec![reply], None),
            (state @ State::Ending { .. }, _) => state,

            (State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }, Event::Credential(other)) => match identity {
                None => State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential: Some(other), erases },
                Some(current) if current == other => State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(_) => {
                    self.ending(epoch, Cause::IdentityChanged, Some(other), erases, queued_replies, &mut out);
                    return out;
                }
            },
            (State::Resetting { epoch, mut queued_replies, erases, .. }, Event::SessionReset(reply)) => {
                queued_replies.extend(reply);
                self.ending(epoch, Cause::SessionReset, None, erases, queued_replies, &mut out);
                return out;
            }
            (State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }, Event::ServerEnded(request_epoch)) => {
                if request_epoch == epoch && identity.is_some() {
                    self.ending(epoch, Cause::ServerEnded, None, erases, queued_replies, &mut out);
                    return out;
                }
                State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }
            }
            (State::Resetting { epoch, identity, models, mut queued_models, mut queued_replies, pending_credential, erases }, Event::ResetModels(more, reply)) => {
                queued_models.extend(more);
                queued_replies.extend(reply);
                State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases }
            }
            (State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, mut erases }, Event::ModelsCleared(generation_new, id)) => match Self::take_erase(&mut erases, id) {
                None => State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(erase) => {
                    let _ = generation_new;
                    let generation_new = self.published.generation.max(erase.target);
                    self.published.generation = generation_new;
                    if !queued_models.is_empty() {
                        self.published.state = PubState::Resetting;
                        self.publish(&mut out, None, erase.replies);
                        out.effects.push(Effect::ModelsReset(epoch, generation_new, models));
                        let next = self.erase(EraseKind::Models, queued_replies);
                        out.effects.push(Effect::ClearModels { epoch, generation_new: next.target, models: queued_models.clone(), erase_id: next.id });
                        erases.push(next);
                        self.state = State::Resetting { epoch, identity, models: queued_models, queued_models: BTreeSet::new(), queued_replies: Vec::new(), pending_credential, erases };
                        return out;
                    }
                    match pending_credential {
                        Some(next) => {
                            out.effects.push(Effect::ModelsReset(epoch, generation_new, models));
                            self.ended(epoch);
                            out.effects.push(Effect::EpochEnded(epoch, Cause::SessionStarted));
                            self.start(epoch + 1, Some(next), generation_new, erase.replies, &mut out);
                            State::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.set_published(epoch, identity, generation_new, if identity.is_some() { PubState::Session } else { PubState::NoSession });
                            self.publish(&mut out, None, erase.replies);
                            out.effects.push(Effect::ModelsReset(epoch, generation_new, models));
                            Self::settled(epoch, identity)
                        }
                    }
                }
            },
            (State::Resetting { epoch, identity, queued_replies, pending_credential, mut erases, models, queued_models }, Event::ClearFailed(id)) => match Self::take_erase(&mut erases, id) {
                None => State::Resetting { epoch, identity, models, queued_models, queued_replies, pending_credential, erases },
                Some(erase) => {
                    answer_all(&mut out, erase.replies, Answer::Storage);
                    answer_all(&mut out, queued_replies, Answer::Storage);
                    match pending_credential {
                        Some(next) => {
                            self.ended(epoch);
                            out.effects.push(Effect::EpochEnded(epoch, Cause::SessionStarted));
                            let generation = self.published.generation;
                            self.start(epoch + 1, Some(next), generation, Vec::new(), &mut out);
                            State::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.set_published(epoch, identity, self.published.generation, if identity.is_some() { PubState::Session } else { PubState::NoSession });
                            self.publish(&mut out, None, Vec::new());
                            Self::settled(epoch, identity)
                        }
                    }
                }
            },
            (State::Resetting { epoch, queued_replies, erases, .. }, Event::Close(reply)) => {
                answer_all(&mut out, queued_replies, Answer::Interrupted);
                self.closing(&mut out, erases, vec![reply], Some(epoch))
            }
            (state @ State::Resetting { .. }, _) => state,

            (State::Closing { mut waiting, erases, close_replies, store_closing, store_closed, observers_stopped }, Event::Drained(owner)) => {
                waiting.remove(&owner);
                let last = waiting.is_empty() && erases.is_empty() && !store_closing;
                if last { out.effects.push(Effect::StoreClose); }
                State::Closing { waiting, erases, close_replies, store_closing: store_closing || last, store_closed, observers_stopped }
            }
            (State::Closing { waiting, mut erases, close_replies, mut store_closing, store_closed, observers_stopped }, ev @ (Event::Cleared(_) | Event::ModelsCleared(..) | Event::ClearFailed(_))) => {
                let (Event::Cleared(id) | Event::ModelsCleared(_, id) | Event::ClearFailed(id)) = ev.clone() else { unreachable!() };
                if let Some(erase) = Self::take_erase(&mut erases, id) {
                    answer_all(&mut out, erase.replies, if matches!(ev, Event::ClearFailed(_)) { Answer::Storage } else { Answer::Ok });
                    if waiting.is_empty() && erases.is_empty() {
                        store_closing = true;
                        out.effects.push(Effect::StoreClose);
                    }
                }
                State::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
            }
            (State::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }, Event::StoreClosed) => {
                if store_closing && observers_stopped {
                    answer_all(&mut out, close_replies, Answer::Ok);
                    State::Closed
                } else {
                    State::Closing { waiting, erases, close_replies, store_closing, store_closed: store_closed || store_closing, observers_stopped }
                }
            }
            (State::Closing { waiting, erases, close_replies, store_closing, store_closed, .. }, Event::ObserversStopped) => {
                if store_closed {
                    answer_all(&mut out, close_replies, Answer::Ok);
                    State::Closed
                } else {
                    State::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped: true }
                }
            }
            (State::Closing { waiting, erases, mut close_replies, store_closing, store_closed, observers_stopped }, Event::Close(reply)) => {
                close_replies.push(reply);
                State::Closing { waiting, erases, close_replies, store_closing, store_closed, observers_stopped }
            }
            (state @ State::Closing { .. }, Event::SessionReset(reply)) => {
                answer_all(&mut out, reply.into_iter().collect(), Answer::Closed);
                state
            }
            (state @ State::Closing { .. }, Event::ResetModels(_, reply)) => {
                answer_all(&mut out, reply.into_iter().collect(), Answer::Closed);
                state
            }
            (state @ State::Closing { .. }, _) => state,

            (State::Closed, Event::SessionReset(reply)) => {
                answer_all(&mut out, reply.into_iter().collect(), Answer::Closed);
                State::Closed
            }
            (State::Closed, Event::ResetModels(_, reply)) => {
                answer_all(&mut out, reply.into_iter().collect(), Answer::Closed);
                State::Closed
            }
            (State::Closed, Event::Close(reply)) => {
                answer_all(&mut out, vec![reply], Answer::Closed);
                State::Closed
            }
            (State::Closed, _) => State::Closed,
        };
        out
    }

    fn resetting(&mut self, epoch: u64, identity: Option<Identity>, models: BTreeSet<String>, mut erases: Vec<Erase>, reply: Option<Id>, out: &mut Out) -> State {
        self.published.state = PubState::Resetting;
        self.publish(out, None, Vec::new());
        let erase = self.erase(EraseKind::Models, reply.into_iter().collect());
        out.effects.push(Effect::ClearModels { epoch, generation_new: erase.target, models: models.clone(), erase_id: erase.id });
        erases.push(erase);
        State::Resetting { epoch, identity, models, queued_models: BTreeSet::new(), queued_replies: Vec::new(), pending_credential: None, erases }
    }

    /// `life_owner_started`, `life_restore_classified` and their stray row.
    fn restore(&mut self, owner: Owner, epoch: u64, started: bool, out: &mut Out) {
        if matches!(self.state, State::Closing { .. } | State::Closed) { return; }
        let Some(restoring) = &mut self.restoring else { return };
        if restoring.epoch != epoch { return; }
        if started {
            if restoring.start_waiting.remove(&owner) && restoring.start_waiting.is_empty() { out.effects.push(Effect::ReadIntents(epoch)); }
            return;
        }
        if !restoring.restore_waiting.remove(&owner) || !restoring.restore_waiting.is_empty() { return; }
        let reply = restoring.open_reply;
        self.restoring = None;
        answer_all(out, reply.into_iter().collect(), Answer::Ok);
        out.effects.push(Effect::RestoreReady(epoch));
    }

    fn restricting(state: PubState) -> bool { matches!(state, PubState::Ending | PubState::EndingFailed | PubState::Closing | PubState::StorageFailed) }

    /// `Saved` and `SaveFailed` of the rows `life_saved`, `life_saved_stale`, `life_save_failed`, `life_save_failed_stale`, `life_closing_saved` and the stray rows.
    fn saved(&mut self, id: PublicationId, ok: bool, out: &mut Out) {
        let Some(at) = self.publishing.iter().position(|publication| publication.id == id) else { return };
        if matches!(self.state, State::Closed) { return; }
        let publication = self.publishing.remove(at);
        let closing = matches!(self.state, State::Closing { .. });
        if closing {
            answer_all(out, publication.replies, if ok { Answer::Ok } else { Answer::Storage });
            return;
        }
        let newer = self.cell_publication > id;
        let restricting = Self::restricting(publication.published.state);
        if ok {
            let ended = self.ended_up_to.is_some_and(|ended| publication.published.epoch <= ended);
            if !restricting && (ended || newer) {
                answer_all(out, publication.replies, Answer::Interrupted);
                return;
            }
            if !restricting {
                self.cell = publication.published.clone();
                self.cell_publication = id;
                out.effects.push(Effect::PublishCell(publication.published.clone()));
            }
            answer_all(out, publication.replies, Answer::Ok);
            if let Some((epoch, identity, generation)) = publication.epoch_started {
                out.effects.push(Effect::EpochStarted(epoch, identity, generation));
            }
            return;
        }
        answer_all(out, publication.replies, Answer::Storage);
        let opening = publication.epoch_started.map(|(epoch, ..)| epoch);
        if let Some(restoring) = self.restoring.take_if(|restoring| opening == Some(restoring.epoch)) {
            answer_all(out, restoring.open_reply.into_iter().collect(), Answer::Storage);
        }
        if !newer {
            self.cell.state = PubState::StorageFailed;
            out.effects.push(Effect::PublishCell(self.cell.clone()));
        }
    }

    /// `life_executor_failed`.
    fn executor_failed(&mut self, out: &mut Out) {
        let state = std::mem::replace(&mut self.state, State::Closed);
        self.state = match state {
            State::Closed => State::Closed,
            state @ State::Closing { .. } => state,
            state => {
                let (end, erases, open) = match state {
                    State::NoSession { epoch } | State::Session { epoch, .. } => (Some(epoch), Vec::new(), Vec::new()),
                    State::Resetting { epoch, erases, queued_replies, .. } => (Some(epoch), erases, queued_replies),
                    State::Ending { erases, .. } => (None, erases, Vec::new()),
                    _ => (None, Vec::new(), Vec::new()),
                };
                self.drop_restoring(out, Answer::Runtime);
                let mut waiting_replies: Vec<Id> = open;
                for erase in &erases { waiting_replies.extend(erase.replies.iter().copied()); }
                for publication in &mut self.publishing { waiting_replies.append(&mut publication.replies); }
                answer_all(out, waiting_replies, Answer::Runtime);
                let erases: Vec<Erase> = erases.into_iter().map(|erase| Erase { replies: Vec::new(), ..erase }).collect();
                self.closing(out, erases, Vec::new(), end)
            }
        };
    }
}

fn names(input: &Input, key: &str) -> BTreeSet<String> {
    input.args.get(key).and_then(|value| value.as_array()).map(|items| items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

fn answer_kind(answer: Answer) -> Option<&'static str> {
    match answer {
        Answer::Ok => None,
        Answer::Storage => Some("database"),
        Answer::Closed => Some("closed"),
        Answer::Runtime => Some("runtime"),
        Answer::Interrupted => Some("interrupted"),
    }
}

fn reply(id: Id, kind: Option<&str>) -> Observation {
    match kind {
        None => Observation::new("reply", json!({ "id": id, "ok": true })),
        Some(kind) => Observation::new("reply", json!({ "id": id, "error": kind })),
    }
}

/// What the Store and the platform answer to the lifecycle effects: nothing is held, or the Store holds its answers (`store_hold`).
enum Pending {
    ClearAccount(EraseId),
    ClearModels(EraseId, u64),
    Save(PublicationId),
}

/// The table behind the public vocabulary of the lifecycle checker. The store and the owners answer at once unless the test holds them.
/// With `full` the model also reports `generation` and `closed` (the values `Published` carries that the current API does not show).
pub struct LifecycleRef {
    machine: Lifecycle,
    full: bool,
    held: bool,
    store: std::collections::VecDeque<Pending>,
    store_close: bool,
    /// The inputs the owners answer to the effects: `Started` after `EpochStarted`, `RestoreClassified` after `ReadIntents`.
    owners: std::collections::VecDeque<Event>,
    rows: BTreeSet<String>,
    step: u64,
}

impl LifecycleRef {
    pub fn new(full: bool) -> Self {
        let (machine, out) = Lifecycle::open(0, 0, None, None);
        let mut model = Self { machine, full, held: false, store: Default::default(), store_close: false, owners: Default::default(), rows: BTreeSet::new(), step: 0 };
        model.absorb(out);
        model.pump(&mut Vec::new());
        model
    }

    fn absorb(&mut self, out: Out) -> Vec<(Id, Answer)> {
        for effect in out.effects {
            match effect {
                Effect::Publish(id, _) => self.store.push_back(Pending::Save(id)),
                Effect::ClearAccount { erase_id, .. } => self.store.push_back(Pending::ClearAccount(erase_id)),
                Effect::ClearModels { erase_id, generation_new, .. } => self.store.push_back(Pending::ClearModels(erase_id, generation_new)),
                Effect::StoreClose => self.store_close = true,
                Effect::EpochStarted(epoch, Some(_), _) => self.owners.extend([Event::Started(Owner::Outbox, epoch), Event::Started(Owner::Transfer, epoch)]),
                Effect::ReadIntents(epoch) => self.owners.extend([Event::RestoreClassified(Owner::Outbox, epoch), Event::RestoreClassified(Owner::Transfer, epoch), Event::RestoreClassified(Owner::Session, epoch)]),
                _ => {}
            }
        }
        out.replies
    }

    fn gate(&self, epoch_current: bool) -> Result<(), &'static str> {
        match (&self.machine.state, self.machine.cell.state) {
            (State::Closing { .. } | State::Closed, _) => Err("closed"),
            (_, PubState::StorageFailed) => Err("database"),
            (State::Ending { .. } | State::NoSession { .. }, _) => Err("no_session"),
            (State::Resetting { identity: None, .. }, _) => Err("no_session"),
            (State::Session { .. } | State::Resetting { .. }, _) if !epoch_current => Err("no_session"),
            _ => Ok(()),
        }
    }

    fn feed(&mut self, event: Event, replies: &mut Vec<(Id, Answer)>) {
        let out = self.machine.step(event);
        replies.extend(self.absorb(out));
    }

    fn deliver_store(&mut self, ok: bool, replies: &mut Vec<(Id, Answer)>) {
        let Some(request) = self.store.pop_front() else { return };
        let event = match request {
            Pending::ClearAccount(id) if ok => { self.rows.clear(); Event::Cleared(id) }
            Pending::ClearAccount(id) => Event::ClearFailed(id),
            Pending::ClearModels(id, generation) if ok => { self.rows.clear(); Event::ModelsCleared(generation, id) }
            Pending::ClearModels(id, _) => Event::ClearFailed(id),
            Pending::Save(id) if ok => Event::Saved(id),
            Pending::Save(id) => Event::SaveFailed(id),
        };
        self.feed(event, replies);
    }

    fn pump(&mut self, replies: &mut Vec<(Id, Answer)>) {
        while !self.held {
            if let State::Closing { waiting, .. } = &self.machine.state {
                if let Some(owner) = waiting.iter().next().copied() {
                    self.feed(Event::Drained(owner), replies);
                    continue;
                }
            }
            if let Some(event) = self.owners.pop_front() {
                self.feed(event, replies);
                continue;
            }
            if !self.store.is_empty() {
                self.deliver_store(true, replies);
                continue;
            }
            if std::mem::take(&mut self.store_close) {
                self.feed(Event::StoreClosed, replies);
                self.feed(Event::ObserversStopped, replies);
                continue;
            }
            break;
        }
    }

    fn state_observations(&self) -> Vec<Observation> {
        let published = &self.machine.cell;
        let mut out = vec![Observation::new("epoch", json!(published.epoch)), Observation::new("identity", json!({ "set": published.identity.is_some() }))];
        if self.full {
            out.push(Observation::new("generation", json!(published.generation)));
            out.push(Observation::new("closed", json!(published.closed)));
        }
        out
    }
}

impl RefModel for LifecycleRef {
    fn step(&mut self, input: &Input) -> Vec<Observation> {
        self.step += 1;
        let id = self.step;
        let mut replies: Vec<(Id, Answer)> = Vec::new();
        let mut observed: Vec<Observation> = Vec::new();
        let published_epoch = self.machine.cell.epoch;
        let is_closed = matches!(self.machine.state, State::Closing { .. } | State::Closed);
        match input.name.as_str() {
            "open" => {}
            "login" | "credential_other" => {
                let identity = if input.name == "login" { 1 } else { 2 };
                if is_closed {
                    observed.push(reply(id, Some("closed")));
                } else {
                    self.feed(Event::Credential(identity), &mut replies);
                    observed.push(reply(id, None));
                }
            }
            "logout" => match &self.machine.state {
                State::Closing { .. } | State::Closed => observed.push(reply(id, Some("closed"))),
                State::Session { .. } | State::Resetting { identity: Some(_), .. } => {
                    self.feed(Event::ServerEnded(published_epoch), &mut replies);
                    observed.push(reply(id, None));
                }
                State::Ending { .. } => observed.push(reply(id, Some("no_session"))),
                _ => observed.push(reply(id, Some("session"))),
            },
            "server_ended" => self.feed(Event::ServerEnded(published_epoch), &mut replies),
            "stray_cleared" => self.feed(Event::Cleared(u64::MAX), &mut replies),
            "stray_models_cleared" => self.feed(Event::ModelsCleared(self.machine.cell.generation + 1, u64::MAX), &mut replies),
            "stray_clear_failed" => self.feed(Event::ClearFailed(u64::MAX), &mut replies),
            "stray_drained" => self.feed(Event::Drained(Owner::Feed), &mut replies),
            "server_ended_stale" => self.feed(Event::ServerEnded(published_epoch.saturating_sub(1)), &mut replies),
            "session_reset" => self.feed(Event::SessionReset(Some(id)), &mut replies),
            "reset_models" => self.feed(Event::ResetModels(names(input, "models"), Some(id)), &mut replies),
            "close" => self.feed(Event::Close(id), &mut replies),
            "store_hold" => self.held = true,
            "store_release" => self.held = false,
            "store_ok" | "store_fail" => {
                let ok = input.name == "store_ok";
                self.deliver_store(ok, &mut replies);
            }
            "drain" => {
                let owner = OWNERS.into_iter().find(|owner| format!("{owner:?}") == input.text("owner").unwrap_or_default());
                if let Some(owner) = owner { self.feed(Event::Drained(owner), &mut replies); }
            }
            "call_data" | "create" | "rows" => {
                let current = input.text("epoch") != Some("other");
                match self.gate(current) {
                    Err(kind) => observed.push(Observation::new(if input.name == "call_data" { "call" } else if input.name == "create" { "created" } else { "rows" }, json!({ "error": kind }))),
                    Ok(()) => match input.name.as_str() {
                        "call_data" => observed.push(Observation::new("call", json!({ "ok": true }))),
                        "create" => {
                            let name = input.text("as").unwrap_or("a").to_string();
                            self.rows.insert(name.clone());
                            observed.push(Observation::new("created", json!({ "row": name, "epoch": published_epoch })));
                        }
                        _ => observed.push(Observation::new("rows", json!({ "epoch": published_epoch, "present": self.rows.iter().collect::<Vec<_>>() }))),
                    },
                }
            }
            "idle" => return Vec::new(),
            _ => return vec![Observation::new("unsupported", json!(input.name))],
        }
        self.pump(&mut replies);
        let mut all: Vec<Observation> = replies.into_iter().map(|(reply_id, answer)| reply(reply_id, answer_kind(answer))).collect();
        all.extend(observed);
        all.extend(self.state_observations());
        all
    }
}
