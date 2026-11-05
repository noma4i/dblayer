use std::collections::{BTreeSet, VecDeque};

use serde_json::json;

use crate::check::model::RefModel;
use crate::check::value::{Input, Observation};

pub type Identity = u64;
pub type Id = u64;

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
}

/// `all owners` of `lifecycle.md`.
pub const OWNERS: [Owner; 8] = [Owner::Network, Owner::Session, Owner::Window, Owner::Feed, Owner::Delivery, Owner::Outbox, Owner::Transfer, Owner::Calls];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    SessionReset,
    ServerEnded,
    IdentityChanged,
    SessionStarted,
    Close,
}

#[derive(Clone, Debug, PartialEq)]
pub enum State {
    NoSession { epoch: u64 },
    Session { epoch: u64, identity: Identity },
    Ending { epoch_old: u64, cause: Cause, next: Option<Identity>, replies: Vec<Id>, failed: bool },
    Resetting { epoch: u64, identity: Option<Identity>, models: BTreeSet<String>, queued: BTreeSet<String>, replies: Vec<Id>, pending: Option<Identity> },
    Closing { waiting: BTreeSet<Owner>, clearing: bool, replies: Vec<Id> },
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Credential(Identity),
    SessionReset(Id),
    ServerEnded(u64),
    ResetModels(BTreeSet<String>, Option<Id>),
    Cleared,
    ModelsCleared(u64),
    ClearFailed,
    Close(Id),
    Drained(Owner),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    Publish(Published),
    EpochStarted(u64, Option<Identity>, u64),
    EpochEnded(u64, Cause),
    ModelsReset(u64, u64, BTreeSet<String>),
    Closing,
    ClearAccount(u64, Cause, u64),
    ClearModels(u64, u64, BTreeSet<String>),
    StoreClose,
    /// `Self ResetModels(changed_models, None)` of `life_open_changed_models`.
    ResetModelsSelf(BTreeSet<String>),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Answer {
    Ok,
    Storage,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub epoch: u64,
    pub identity: Option<Identity>,
    pub generation: u64,
    pub closed: bool,
}

pub struct Lifecycle {
    pub state: State,
    pub published: Published,
}

#[derive(Default)]
pub struct Out {
    pub effects: Vec<Effect>,
    pub replies: Vec<(Id, Answer)>,
}

impl Lifecycle {
    pub fn new() -> Self {
        Self { state: State::NoSession { epoch: 0 }, published: Published { epoch: 0, identity: None, generation: 0, closed: false } }
    }

    fn publish(&self, out: &mut Out) { out.effects.push(Effect::Publish(self.published.clone())); }

    fn closing(&mut self, replies: Vec<Id>, clearing: bool, out: &mut Out, with_end: Option<u64>) -> State {
        self.published.closed = true;
        self.publish(out);
        if let Some(epoch) = with_end { out.effects.push(Effect::EpochEnded(epoch, Cause::Close)); }
        out.effects.push(Effect::Closing);
        State::Closing { waiting: OWNERS.into_iter().collect(), clearing, replies }
    }

    fn ending(&mut self, epoch: u64, cause: Cause, next: Option<Identity>, replies: Vec<Id>, out: &mut Out) {
        out.effects.push(Effect::EpochEnded(epoch, cause));
        out.effects.push(Effect::ClearAccount(epoch, cause, self.published.generation + 1));
        self.state = State::Ending { epoch_old: epoch, cause, next, replies, failed: false };
    }

    fn started(&mut self, epoch: u64, identity: Option<Identity>, generation: u64, out: &mut Out) {
        self.published.epoch = epoch;
        self.published.identity = identity;
        self.published.generation = generation;
        self.publish(out);
        out.effects.push(Effect::EpochStarted(epoch, identity, generation));
    }

    /// One row of the `lifecycle` machine per match arm, in table order.
    pub fn step(&mut self, event: Event) -> Out {
        let mut out = Out::default();
        let state = std::mem::replace(&mut self.state, State::Closed);
        let generation = self.published.generation;
        self.state = match (state, event) {
            (State::NoSession { epoch }, Event::Credential(identity)) => {
                out.effects.push(Effect::EpochEnded(epoch, Cause::SessionStarted));
                self.started(epoch + 1, Some(identity), generation, &mut out);
                State::Session { epoch: epoch + 1, identity }
            }
            (State::NoSession { epoch }, Event::SessionReset(reply)) => {
                self.ending(epoch, Cause::SessionReset, None, vec![reply], &mut out);
                return out;
            }
            (State::NoSession { epoch }, Event::ResetModels(models, reply)) => {
                out.effects.push(Effect::ClearModels(epoch, generation + 1, models.clone()));
                State::Resetting { epoch, identity: None, models, queued: BTreeSet::new(), replies: reply.into_iter().collect(), pending: None }
            }
            (State::NoSession { epoch }, Event::Close(reply)) => self.closing(vec![reply], false, &mut out, Some(epoch)),
            (state @ State::NoSession { .. }, _) => state,

            (State::Session { epoch, identity }, Event::Credential(other)) if other == identity => State::Session { epoch, identity },
            (State::Session { epoch, .. }, Event::Credential(other)) => {
                self.ending(epoch, Cause::IdentityChanged, Some(other), vec![], &mut out);
                return out;
            }
            (State::Session { epoch, .. }, Event::SessionReset(reply)) => {
                self.ending(epoch, Cause::SessionReset, None, vec![reply], &mut out);
                return out;
            }
            (State::Session { epoch, .. }, Event::ServerEnded(request_epoch)) if request_epoch == epoch => {
                self.ending(epoch, Cause::ServerEnded, None, vec![], &mut out);
                return out;
            }
            (State::Session { epoch, identity }, Event::ResetModels(models, reply)) => {
                out.effects.push(Effect::ClearModels(epoch, generation + 1, models.clone()));
                State::Resetting { epoch, identity: Some(identity), models, queued: BTreeSet::new(), replies: reply.into_iter().collect(), pending: None }
            }
            (State::Session { epoch, .. }, Event::Close(reply)) => self.closing(vec![reply], false, &mut out, Some(epoch)),
            (state @ State::Session { .. }, _) => state,

            (State::Ending { epoch_old, cause, replies, failed, .. }, Event::Credential(identity)) => {
                if failed { out.effects.push(Effect::ClearAccount(epoch_old, cause, generation + 1)); }
                State::Ending { epoch_old, cause, next: Some(identity), replies, failed: false }
            }
            (State::Ending { epoch_old, mut cause, mut replies, failed, .. }, Event::SessionReset(reply)) => {
                if failed {
                    replies = vec![reply];
                    cause = Cause::SessionReset;
                    out.effects.push(Effect::ClearAccount(epoch_old, cause, generation + 1));
                } else {
                    replies.push(reply);
                }
                State::Ending { epoch_old, cause, next: None, replies, failed: false }
            }
            (State::Ending { epoch_old, cause, next, mut replies, failed }, Event::ResetModels(_, reply)) => {
                replies.extend(reply);
                State::Ending { epoch_old, cause, next, replies, failed }
            }
            (State::Ending { epoch_old, next, replies, .. }, Event::Cleared) => {
                out.replies.extend(replies.into_iter().map(|id| (id, Answer::Ok)));
                match next {
                    None => {
                        self.started(epoch_old + 1, None, generation + 1, &mut out);
                        State::NoSession { epoch: epoch_old + 1 }
                    }
                    Some(identity) => {
                        self.started(epoch_old + 2, Some(identity), generation + 1, &mut out);
                        State::Session { epoch: epoch_old + 2, identity }
                    }
                }
            }
            (State::Ending { epoch_old, cause, next, replies, .. }, Event::ClearFailed) => {
                out.replies.extend(replies.into_iter().map(|id| (id, Answer::Storage)));
                State::Ending { epoch_old, cause, next, replies: vec![], failed: true }
            }
            (State::Ending { replies, .. }, Event::Close(reply)) => {
                let mut all = replies;
                all.push(reply);
                self.closing(all, true, &mut out, None)
            }
            (state @ State::Ending { .. }, _) => state,

            (State::Resetting { epoch, identity, models, queued, mut replies, pending }, Event::Credential(other)) => match identity {
                None => State::Resetting { epoch, identity, models, queued, replies, pending: Some(other) },
                Some(current) if current == other => State::Resetting { epoch, identity, models, queued, replies, pending },
                Some(_) => {
                    self.ending(epoch, Cause::IdentityChanged, Some(other), std::mem::take(&mut replies), &mut out);
                    return out;
                }
            },
            (State::Resetting { epoch, mut replies, .. }, Event::SessionReset(reply)) => {
                replies.push(reply);
                self.ending(epoch, Cause::SessionReset, None, replies, &mut out);
                return out;
            }
            (State::Resetting { epoch, identity, models, queued, replies, pending }, Event::ServerEnded(request_epoch)) => {
                if request_epoch == epoch && identity.is_some() {
                    self.ending(epoch, Cause::ServerEnded, None, replies, &mut out);
                    return out;
                }
                State::Resetting { epoch, identity, models, queued, replies, pending }
            }
            (State::Resetting { epoch, identity, models, mut queued, mut replies, pending }, Event::ResetModels(more, reply)) => {
                queued.extend(more);
                replies.extend(reply);
                State::Resetting { epoch, identity, models, queued, replies, pending }
            }
            (State::Resetting { epoch, identity, models, queued, replies, pending }, Event::ModelsCleared(generation_new)) => {
                self.published.generation = generation_new;
                if !queued.is_empty() {
                    self.publish(&mut out);
                    out.effects.push(Effect::ModelsReset(epoch, generation_new, models));
                    out.effects.push(Effect::ClearModels(epoch, generation_new + 1, queued.clone()));
                    State::Resetting { epoch, identity, models: queued, queued: BTreeSet::new(), replies, pending }
                } else {
                    out.replies.extend(replies.into_iter().map(|id| (id, Answer::Ok)));
                    match pending {
                        Some(next) => {
                            self.published.epoch = epoch + 1;
                            self.published.identity = Some(next);
                            self.publish(&mut out);
                            out.effects.extend([Effect::ModelsReset(epoch, generation_new, models), Effect::EpochEnded(epoch, Cause::SessionStarted), Effect::EpochStarted(epoch + 1, Some(next), generation_new)]);
                            State::Session { epoch: epoch + 1, identity: next }
                        }
                        None => {
                            self.publish(&mut out);
                            out.effects.push(Effect::ModelsReset(epoch, generation_new, models));
                            match identity {
                                Some(identity) => State::Session { epoch, identity },
                                None => State::NoSession { epoch },
                            }
                        }
                    }
                }
            }
            (State::Resetting { epoch, identity, replies, pending, .. }, Event::ClearFailed) => {
                out.replies.extend(replies.into_iter().map(|id| (id, Answer::Storage)));
                match pending {
                    Some(next) => {
                        out.effects.push(Effect::EpochEnded(epoch, Cause::SessionStarted));
                        self.started(epoch + 1, Some(next), generation, &mut out);
                        // `Publish` precedes `EpochEnded` in the table.
                        out.effects.swap(0, 1);
                        State::Session { epoch: epoch + 1, identity: next }
                    }
                    None => match identity {
                        Some(identity) => State::Session { epoch, identity },
                        None => State::NoSession { epoch },
                    },
                }
            }
            (State::Resetting { epoch, replies, .. }, Event::Close(reply)) => {
                let mut all = replies;
                all.push(reply);
                self.closing(all, true, &mut out, Some(epoch))
            }
            (state @ State::Resetting { .. }, _) => state,

            (State::Closing { mut waiting, clearing, replies }, Event::Drained(owner)) => {
                waiting.remove(&owner);
                if waiting.is_empty() && !clearing {
                    out.replies.extend(replies.into_iter().map(|id| (id, Answer::Ok)));
                    out.effects.push(Effect::StoreClose);
                    State::Closed
                } else {
                    State::Closing { waiting, clearing, replies }
                }
            }
            (State::Closing { waiting, replies, .. }, Event::Cleared | Event::ModelsCleared(_) | Event::ClearFailed) => {
                if waiting.is_empty() {
                    out.replies.extend(replies.into_iter().map(|id| (id, Answer::Ok)));
                    out.effects.push(Effect::StoreClose);
                    State::Closed
                } else {
                    State::Closing { waiting, clearing: false, replies }
                }
            }
            (State::Closing { waiting, clearing, mut replies }, Event::Close(reply)) => {
                replies.push(reply);
                State::Closing { waiting, clearing, replies }
            }
            (State::Closing { waiting, clearing, replies }, Event::SessionReset(reply)) => {
                out.replies.push((reply, Answer::Closed));
                State::Closing { waiting, clearing, replies }
            }
            (State::Closing { waiting, clearing, replies }, Event::ResetModels(_, reply)) => {
                out.replies.extend(reply.map(|id| (id, Answer::Closed)));
                State::Closing { waiting, clearing, replies }
            }
            (state @ State::Closing { .. }, _) => state,

            (State::Closed, Event::SessionReset(reply) | Event::Close(reply)) => {
                out.replies.push((reply, Answer::Closed));
                State::Closed
            }
            (State::Closed, Event::ResetModels(_, reply)) => {
                out.replies.extend(reply.map(|id| (id, Answer::Closed)));
                State::Closed
            }
            (State::Closed, _) => State::Closed,
        };
        out
    }

    /// `Open` rows of the table: the initial state, `Publish`, `EpochStarted`, and the self-sent `ResetModels` for changed models.
    pub fn open(saved_epoch: u64, saved_generation: u64, saved_identity: Option<Identity>, changed_models: BTreeSet<String>) -> (Self, Out) {
        let mut life = Self { state: State::NoSession { epoch: saved_epoch + 1 }, published: Published { epoch: saved_epoch + 1, identity: saved_identity, generation: saved_generation, closed: false } };
        if let Some(identity) = saved_identity { life.state = State::Session { epoch: saved_epoch + 1, identity }; }
        let mut out = Out::default();
        life.publish(&mut out);
        out.effects.push(Effect::EpochStarted(saved_epoch + 1, saved_identity, saved_generation));
        if !changed_models.is_empty() { out.effects.push(Effect::ResetModelsSelf(changed_models)); }
        (life, out)
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
    }
}

fn reply(id: Id, kind: Option<&str>) -> Observation {
    match kind {
        None => Observation::new("reply", json!({ "id": id, "ok": true })),
        Some(kind) => Observation::new("reply", json!({ "id": id, "error": kind })),
    }
}

/// The table behind the public vocabulary of the lifecycle checker. The store and the owners answer at once unless the test holds them.
/// With `full` the model also reports `generation` and `closed` (the values `Published` carries that the current API does not show).
pub struct LifecycleRef {
    machine: Lifecycle,
    full: bool,
    held: bool,
    store: VecDeque<Effect>,
    rows: BTreeSet<String>,
    step: u64,
}

impl LifecycleRef {
    pub fn new(full: bool) -> Self {
        Self { machine: Lifecycle::open(0, 0, None, Default::default()).0, full, held: false, store: VecDeque::new(), rows: BTreeSet::new(), step: 0 }
    }

    fn gate(&self, epoch_current: bool) -> Result<(), &'static str> {
        match &self.machine.state {
            State::Closing { .. } | State::Closed => Err("closed"),
            State::Ending { .. } => Err("no_session"),
            State::NoSession { .. } => Err("no_session"),
            State::Resetting { identity: None, .. } => Err("no_session"),
            State::Session { .. } | State::Resetting { .. } if !epoch_current => Err("no_session"),
            _ => Ok(()),
        }
    }

    fn feed(&mut self, event: Event, replies: &mut Vec<(Id, Answer)>) {
        let out = self.machine.step(event);
        replies.extend(out.replies);
        for effect in out.effects {
            match effect {
                Effect::ClearAccount(..) | Effect::ClearModels(..) | Effect::StoreClose => self.store.push_back(effect),
                _ => {}
            }
        }
    }

    fn deliver_store(&mut self, ok: bool, replies: &mut Vec<(Id, Answer)>) {
        let Some(request) = self.store.pop_front() else { return };
        match request {
            Effect::ClearAccount(..) => {
                if ok {
                    self.rows.clear();
                    self.feed(Event::Cleared, replies);
                } else {
                    self.feed(Event::ClearFailed, replies);
                }
            }
            Effect::ClearModels(_, generation, _) => {
                if ok {
                    self.rows.clear();
                    self.feed(Event::ModelsCleared(generation), replies);
                } else {
                    self.feed(Event::ClearFailed, replies);
                }
            }
            _ => {}
        }
    }

    fn pump(&mut self, replies: &mut Vec<(Id, Answer)>) {
        while !self.held {
            if let State::Closing { waiting, .. } = &self.machine.state {
                if let Some(owner) = waiting.iter().next().copied() {
                    self.feed(Event::Drained(owner), replies);
                    continue;
                }
            }
            if self.store.is_empty() { break; }
            self.deliver_store(true, replies);
        }
    }

    fn state_observations(&self) -> Vec<Observation> {
        let published = &self.machine.published;
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
        let published_epoch = self.machine.published.epoch;
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
            "stray_cleared" => self.feed(Event::Cleared, &mut replies),
            "stray_models_cleared" => self.feed(Event::ModelsCleared(self.machine.published.generation + 1), &mut replies),
            "stray_clear_failed" => self.feed(Event::ClearFailed, &mut replies),
            "stray_drained" => self.feed(Event::Drained(Owner::Feed), &mut replies),
            "server_ended_stale" => self.feed(Event::ServerEnded(published_epoch.saturating_sub(1)), &mut replies),
            "session_reset" => {
                self.feed(Event::SessionReset(id), &mut replies);
            }
            "reset_models" => {
                self.feed(Event::ResetModels(names(input, "models"), Some(id)), &mut replies);
            }
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
