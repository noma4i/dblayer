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
    StorageFailed,
    Closing,
}

#[derive(Clone, Debug, PartialEq)]
pub enum State {
    NoSession { epoch: u64 },
    Session { epoch: u64, identity: Identity },
    Ending { epoch_old: u64, cause: Cause, next: Option<Identity>, failed: bool, erase_credential: bool, erases: Vec<Erase> },
    Closing { waiting: BTreeSet<Owner>, erases: Vec<Erase>, close_replies: Vec<Id>, store_closing: bool, store_closed: bool, observers_stopped: bool },
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Credential(Identity),
    SessionReset(Option<Id>),
    ServerEnded(u64),
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
    Closing,
    StopObserving,
    ClearAccount { epoch_old: u64, cause: Cause, generation_new: u64, erase_id: EraseId },
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
    /// The `open` reply held by the initial publication (no identity, no pending): answered `Closed` by Close before `Saved` (D-271).
    open_reply: Option<Id>,
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

mod flow;

fn answer_kind(answer: Answer) -> Option<&'static str> {
    match answer {
        Answer::Ok => None,
        Answer::Storage => Some("storage"),
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
        let (machine, out) = Lifecycle::open(0, 0, None, false, None);
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
            (_, PubState::StorageFailed) => Err("storage"),
            (State::Ending { .. } | State::NoSession { .. }, _) => Err("no_session"),
            (State::Session { .. }, _) if !epoch_current => Err("no_session"),
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
                State::Session { .. } => {
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
