use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde_json::json;

use super::ref_lifecycle::{Cause, Effect as LifeEffect, Event, Lifecycle, State as LifeState};
use crate::check::model::RefModel;
use crate::check::value::{Input, Observation};

#[derive(Clone, Debug, PartialEq)]
pub struct Batch {
    pub epoch: u64,
    pub basis: BTreeMap<String, u64>,
    pub rows: Vec<(String, String)>,
    /// Operations of the queue, `encrypted_*`: number of intents the batch writes.
    pub intents: u64,
    /// The batch writes the `encrypted_session` row with this identity.
    pub credential: Option<u64>,
}

impl Batch {
    pub fn rows(epoch: u64, rows: Vec<(String, String)>) -> Self { Self { epoch, basis: BTreeMap::new(), rows, intents: 0, credential: None } }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    Commit { id: u64, batch: Batch },
    ClearAccount { epoch_old: u64, cause: Cause, generation_new: u64, erase_id: u64 },
    ClearModels { epoch: u64, generation_new: u64, models: BTreeSet<String>, erase_id: u64 },
    /// `active`: the state of the publication is NoSession, Session or Resetting.
    Published { publication_id: u64, epoch: u64, generation: u64, active: bool },
    EpochEnded { epoch: u64 },
    ReadIntents { epoch: u64 },
    Close,
    Written { id: u64, result: Result<u64, ()> },
    Erased { io_id: u64, result: Result<u64, ()> },
    ReadsClosed(Vec<u64>),
    ReadFinished(u64),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Reason {
    NoSession,
    StaleGeneration(Vec<String>),
    Storage,
    Closed,
    Invalid,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Open {
    Open,
    Closed,
}

/// What the Store does in a step, in the order of the column "Effects".
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Committed { id: u64, revision: u64 },
    Rejected { id: u64, reason: Reason },
    Write(u64),
    EraseAccount { io_id: u64, generation_new: u64, erase_encrypted: bool },
    EraseModels { io_id: u64, models: BTreeSet<String> },
    ClearSecret { io_id: u64 },
    SavePublished { io_id: u64, publication_id: u64 },
    Cleared { epoch_old: u64, erase_id: u64 },
    ModelsCleared { generation_new: u64, erase_id: u64 },
    ClearFailed { erase_id: u64, closed: bool },
    Saved(u64),
    SaveFailed { publication_id: u64, closed: bool },
    BroadcastCommitted { revision: u64, all: bool },
    ReadIntents(u64),
    CloseReads,
    CloseConnections,
    StoreClosed,
}

/// What `Boot` finds in the database.
#[derive(Clone, Debug, PartialEq)]
pub enum Boot {
    New,
    Same,
    ChangedModels,
    /// The fingerprint of a `storage: local` model changed, the server models match.
    LocalModelChanged,
    FormatChange,
    SqliteError,
    CorruptManifest,
}

/// `Lifecycle Open{..}` that the `Boot` rows send.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenMessage {
    pub saved_epoch: u64,
    pub saved_generation: u64,
    pub saved_identity: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
enum IoKind {
    Account { epoch_old: u64, cause: Cause, generation_new: u64, erase_id: u64 },
    Models { generation_new: u64, models: BTreeSet<String>, erase_id: u64 },
    Secret { epoch_old: u64, erase_id: u64, revision: u64 },
    Save { publication_id: u64, epoch: u64, generation: u64, active: bool },
}

#[derive(Clone, Debug, PartialEq)]
struct Io {
    id: u64,
    kind: IoKind,
    started: bool,
}

/// The rows of the `store` machine: `rows` holds the committed rows (model, id) a snapshot shows.
#[derive(Clone, Debug, PartialEq)]
pub struct Store {
    pub state: Open,
    pub closing: bool,
    pub revision: u64,
    pub epoch: u64,
    pub saved_epoch: u64,
    pub saved_generation: u64,
    pub saved_revision: u64,
    pub generations: BTreeMap<String, u64>,
    /// `model_generation[*]` of the models that have no entry.
    pub base_generation: u64,
    pub rows: BTreeSet<(String, String)>,
    pub intents: u64,
    pub credential: Option<u64>,
    pub pending: bool,
    writing: Option<(u64, Batch)>,
    io: VecDeque<Io>,
    next_io: u64,
    deferred: VecDeque<Request>,
    reads: BTreeSet<u64>,
    reads_closed: bool,
}

impl Store {
    pub fn new() -> Self {
        Self {
            state: Open::Open,
            closing: false,
            revision: 0,
            epoch: 0,
            saved_epoch: 0,
            saved_generation: 0,
            saved_revision: 0,
            generations: BTreeMap::new(),
            base_generation: 0,
            rows: BTreeSet::new(),
            intents: 0,
            credential: None,
            pending: false,
            writing: None,
            io: VecDeque::new(),
            next_io: 1,
            deferred: VecDeque::new(),
            reads: BTreeSet::new(),
            reads_closed: false,
        }
    }

    pub fn generation(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base_generation) }

    fn max_generation(&self) -> u64 { self.generations.values().copied().max().unwrap_or(0).max(self.base_generation) }

    pub fn busy(&self) -> bool { self.writing.is_some() || !self.io.is_empty() }

    /// `store_boot_*`: the new state after `Boot` and the message to Lifecycle (`None` for `store_boot_error`).
    pub fn boot(&mut self, found: Boot) -> Option<OpenMessage> {
        match found {
            Boot::SqliteError | Boot::CorruptManifest => {
                self.state = Open::Closed;
                None
            }
            Boot::New => Some(OpenMessage { saved_epoch: 0, saved_generation: 0, saved_identity: None }),
            Boot::Same | Boot::LocalModelChanged => {
                self.revision = self.saved_revision;
                self.generations.clear();
                self.base_generation = self.saved_generation;
                Some(OpenMessage { saved_epoch: self.saved_epoch, saved_generation: self.saved_generation, saved_identity: self.credential })
            }
            Boot::ChangedModels | Boot::FormatChange => {
                self.revision = self.saved_revision + 1;
                self.rows.clear();
                self.generations.clear();
                self.saved_generation += 1;
                self.base_generation = self.saved_generation;
                Some(OpenMessage { saved_epoch: self.saved_epoch, saved_generation: self.saved_generation, saved_identity: self.credential })
            }
        }
    }

    /// Starts the head of `io` when it waits and nothing is being written.
    fn start_head(&mut self, out: &mut Vec<Action>) {
        if self.writing.is_some() { return; }
        let Some(head) = self.io.front_mut() else { return };
        if head.started { return; }
        head.started = true;
        if let IoKind::Save { publication_id, .. } = head.kind {
            out.push(Action::SavePublished { io_id: head.id, publication_id });
        }
    }

    /// Applies the inputs that waited, in order, while the Store is free.
    fn drain(&mut self, out: &mut Vec<Action>) {
        self.start_head(out);
        while !self.busy() {
            let Some(request) = self.deferred.pop_front() else { break };
            self.apply(request, out);
        }
    }

    fn check_close(&mut self, out: &mut Vec<Action>) {
        if self.closing && self.reads_closed && self.writing.is_none() && self.io.is_empty() && self.reads.is_empty() {
            self.state = Open::Closed;
            out.push(Action::CloseConnections);
            out.push(Action::StoreClosed);
        }
    }

    pub fn step(&mut self, request: Request) -> Vec<Action> {
        let mut out = Vec::new();
        if self.state == Open::Closed {
            match request {
                Request::Commit { id, .. } => out.push(Action::Rejected { id, reason: Reason::Closed }),
                Request::ClearAccount { erase_id, .. } | Request::ClearModels { erase_id, .. } => out.push(Action::ClearFailed { erase_id, closed: true }),
                _ => {}
            }
            return out;
        }
        match request {
            Request::Written { .. } | Request::Erased { .. } | Request::ReadsClosed(_) | Request::ReadFinished(_) => {
                self.apply(request, &mut out);
                self.check_close(&mut out);
            }
            request => self.apply(request, &mut out),
        }
        out
    }

    fn apply(&mut self, request: Request, out: &mut Vec<Action>) {
        if self.closing && matches!(request, Request::Commit { .. } | Request::ClearAccount { .. } | Request::ClearModels { .. } | Request::ReadIntents { .. } | Request::Published { .. }) {
            match request {
                Request::Commit { id, .. } => out.push(Action::Rejected { id, reason: Reason::Closed }),
                Request::ClearAccount { erase_id, .. } | Request::ClearModels { erase_id, .. } => out.push(Action::ClearFailed { erase_id, closed: true }),
                Request::Published { publication_id, .. } => out.push(Action::SaveFailed { publication_id, closed: true }),
                _ => {}
            }
            return;
        }
        match request {
            Request::Commit { id, batch } => {
                if self.busy() {
                    self.deferred.push_back(Request::Commit { id, batch });
                } else if batch.epoch != self.epoch {
                    out.push(Action::Rejected { id, reason: Reason::NoSession });
                } else if batch.rows.iter().any(|(model, _)| !batch.basis.contains_key(model)) {
                    out.push(Action::Rejected { id, reason: Reason::Invalid });
                } else if batch.basis.iter().any(|(model, basis)| *basis < self.generation(model)) {
                    let stale = batch.basis.iter().filter(|(model, basis)| **basis < self.generation(model)).map(|(model, _)| model.clone()).collect();
                    out.push(Action::Rejected { id, reason: Reason::StaleGeneration(stale) });
                } else {
                    self.writing = Some((id, batch));
                    out.push(Action::Write(id));
                }
            }
            Request::Written { id, result } => {
                let Some((writing, batch)) = self.writing.take() else { return };
                if writing != id {
                    self.writing = Some((writing, batch));
                    return;
                }
                match result {
                    Ok(revision) => {
                        self.rows.extend(batch.rows.clone());
                        self.intents += batch.intents;
                        if let Some(identity) = batch.credential { self.credential = Some(identity); }
                        self.revision = revision;
                        out.push(Action::Committed { id, revision });
                        out.push(Action::BroadcastCommitted { revision, all: false });
                    }
                    Err(()) => out.push(Action::Rejected { id, reason: Reason::Storage }),
                }
                self.drain(out);
            }
            Request::ClearAccount { epoch_old, cause, generation_new, erase_id } => {
                if self.busy() {
                    self.deferred.push_back(Request::ClearAccount { epoch_old, cause, generation_new, erase_id });
                } else if generation_new <= self.max_generation() {
                    out.push(Action::ClearFailed { erase_id, closed: false });
                } else {
                    let io_id = self.next_io;
                    self.next_io += 1;
                    self.io.push_back(Io { id: io_id, kind: IoKind::Account { epoch_old, cause, generation_new, erase_id }, started: true });
                    out.push(Action::EraseAccount { io_id, generation_new, erase_encrypted: matches!(cause, Cause::SessionReset | Cause::ServerEnded) });
                }
            }
            Request::ClearModels { epoch, generation_new, models, erase_id } => {
                if self.busy() {
                    self.deferred.push_back(Request::ClearModels { epoch, generation_new, models, erase_id });
                } else if models.iter().any(|model| generation_new <= self.generation(model)) {
                    out.push(Action::ClearFailed { erase_id, closed: false });
                } else {
                    let io_id = self.next_io;
                    self.next_io += 1;
                    self.io.push_back(Io { id: io_id, kind: IoKind::Models { generation_new, models: models.clone(), erase_id }, started: true });
                    out.push(Action::EraseModels { io_id, models });
                }
            }
            Request::Erased { io_id, result } => {
                if self.io.front().map(|head| head.id) != Some(io_id) { return; }
                let head = self.io.pop_front().expect("the head exists");
                match (head.kind, result) {
                    (IoKind::Account { erase_id, .. }, Err(())) | (IoKind::Models { erase_id, .. }, Err(())) => out.push(Action::ClearFailed { erase_id, closed: false }),
                    (IoKind::Save { publication_id, .. }, Err(())) => out.push(Action::SaveFailed { publication_id, closed: false }),
                    (IoKind::Account { epoch_old, cause, generation_new, erase_id }, Ok(revision)) => {
                        self.wipe_account(cause, generation_new);
                        if matches!(cause, Cause::SessionReset | Cause::ServerEnded) {
                            self.io.push_front(Io { id: head.id, kind: IoKind::Secret { epoch_old, erase_id, revision }, started: true });
                            out.push(Action::ClearSecret { io_id: head.id });
                            return;
                        }
                        self.revision = revision;
                        out.push(Action::Cleared { epoch_old, erase_id });
                        out.push(Action::BroadcastCommitted { revision, all: true });
                    }
                    (IoKind::Secret { epoch_old, erase_id, revision }, Ok(_) | Err(())) => {
                        self.revision = revision;
                        out.push(Action::Cleared { epoch_old, erase_id });
                        out.push(Action::BroadcastCommitted { revision, all: true });
                    }
                    (IoKind::Models { generation_new, models, erase_id }, Ok(revision)) => {
                        self.rows.retain(|(model, _)| !models.contains(model));
                        for model in models { self.generations.insert(model, generation_new); }
                        self.revision = revision;
                        out.push(Action::ModelsCleared { generation_new, erase_id });
                        out.push(Action::BroadcastCommitted { revision, all: false });
                    }
                    (IoKind::Save { publication_id, epoch, generation, active }, Ok(_)) => {
                        if active { self.epoch = self.epoch.max(epoch); }
                        self.saved_epoch = epoch;
                        self.saved_generation = generation;
                        out.push(Action::Saved(publication_id));
                    }
                }
                self.drain(out);
            }
            Request::Published { publication_id, epoch, generation, active } => {
                let id = self.next_io;
                self.next_io += 1;
                self.io.push_back(Io { id, kind: IoKind::Save { publication_id, epoch, generation, active }, started: false });
                self.start_head(out);
            }
            Request::EpochEnded { epoch } => {
                if self.busy() {
                    self.deferred.push_back(Request::EpochEnded { epoch });
                } else if epoch + 1 > self.epoch {
                    self.epoch = epoch + 1;
                }
            }
            Request::ReadIntents { epoch } => out.push(Action::ReadIntents(epoch)),
            Request::Close => {
                if self.busy() {
                    self.deferred.push_back(Request::Close);
                } else if !self.closing {
                    self.closing = true;
                    self.reads_closed = false;
                    out.push(Action::CloseReads);
                }
            }
            Request::ReadsClosed(ids) => {
                self.reads = ids.into_iter().collect();
                self.reads_closed = true;
            }
            Request::ReadFinished(id) => {
                self.reads.remove(&id);
            }
        }
    }

    fn wipe_account(&mut self, cause: Cause, generation_new: u64) {
        self.rows.clear();
        self.intents = 0;
        if matches!(cause, Cause::SessionReset | Cause::ServerEnded) {
            self.credential = None;
            self.pending = false;
        }
        self.generations.clear();
        self.base_generation = generation_new;
    }
}

/// The store table behind the public vocabulary: the lifecycle table gates the calls and sends the store its requests.
/// `fail_commit` and `fail_clear` make the next SQLite action fail. With `full` the model also reports `revision`.
/// The executor of SQLite answers at once unless the writer is held.
pub struct StoreRef {
    life: Lifecycle,
    store: Store,
    full: bool,
    fail_commit: bool,
    fail_clear: bool,
    held: bool,
    waiting: VecDeque<Request>,
    batch: u64,
}

impl StoreRef {
    pub fn new(full: bool) -> Self {
        let (life, out) = Lifecycle::open(0, 0, None, None);
        let mut model = Self { life, store: Store::new(), full, fail_commit: false, fail_clear: false, held: false, waiting: VecDeque::new(), batch: 0 };
        model.route(out.effects);
        model
    }

    /// Feeds the lifecycle effects to the Store and the answers back.
    fn route(&mut self, effects: Vec<LifeEffect>) {
        for effect in effects {
            let request = match effect {
                LifeEffect::Publish(publication_id, published) => Some(Request::Published { publication_id, epoch: published.epoch, generation: published.generation, active: matches!(published.state, super::ref_lifecycle::PubState::NoSession | super::ref_lifecycle::PubState::Session | super::ref_lifecycle::PubState::Resetting) }),
                LifeEffect::EpochEnded(epoch, _) => Some(Request::EpochEnded { epoch }),
                LifeEffect::ClearAccount { epoch_old, cause, generation_new, erase_id } => Some(Request::ClearAccount { epoch_old, cause, generation_new, erase_id }),
                LifeEffect::ClearModels { epoch, generation_new, models, erase_id } => Some(Request::ClearModels { epoch, generation_new, models, erase_id }),
                LifeEffect::StoreClose => Some(Request::Close),
                _ => None,
            };
            if let Some(request) = request { self.queue(request); }
        }
    }

    fn queue(&mut self, request: Request) {
        if self.held && matches!(request, Request::ClearAccount { .. } | Request::ClearModels { .. } | Request::Published { .. }) {
            self.waiting.push_back(request);
        } else {
            self.send(request);
        }
    }

    /// One request; the executor answers the IO effects at once.
    fn send(&mut self, request: Request) {
        let mut todo = vec![request];
        while !todo.is_empty() {
            let request = todo.remove(0);
            let actions = self.store.step(request);
            for action in actions {
                match action {
                    Action::EraseAccount { io_id, .. } | Action::EraseModels { io_id, .. } => {
                        let ok = !std::mem::take(&mut self.fail_clear);
                        todo.push(Request::Erased { io_id, result: if ok { Ok(self.store.revision + 1) } else { Err(()) } });
                    }
                    Action::ClearSecret { io_id } => todo.push(Request::Erased { io_id, result: Ok(0) }),
                    Action::SavePublished { io_id, .. } => todo.push(Request::Erased { io_id, result: Ok(self.store.revision) }),
                    Action::Cleared { erase_id, .. } => self.feed(Event::Cleared(erase_id)),
                    Action::ModelsCleared { generation_new, erase_id } => self.feed(Event::ModelsCleared(generation_new, erase_id)),
                    Action::ClearFailed { erase_id, .. } => self.feed(Event::ClearFailed(erase_id)),
                    Action::Saved(id) => self.feed(Event::Saved(id)),
                    Action::SaveFailed { publication_id, .. } => self.feed(Event::SaveFailed(publication_id)),
                    Action::CloseReads => todo.push(Request::ReadsClosed(Vec::new())),
                    Action::StoreClosed => {
                        self.feed(Event::StoreClosed);
                        self.feed(Event::ObserversStopped);
                    }
                    _ => {}
                }
            }
        }
    }

    fn feed(&mut self, event: Event) {
        let out = self.life.step(event);
        self.route(out.effects);
        if let LifeState::Closing { waiting, .. } = &self.life.state {
            if let Some(owner) = waiting.iter().next().copied() { self.feed(Event::Drained(owner)); }
        }
    }

    fn gate(&self, current: bool) -> Result<(), &'static str> {
        match &self.life.state {
            LifeState::Closing { .. } | LifeState::Closed => Err("closed"),
            LifeState::Session { .. } | LifeState::Resetting { identity: Some(_), .. } if current => Ok(()),
            _ => Err("no_session"),
        }
    }

    fn revision(&self) -> Vec<Observation> {
        if self.full { vec![Observation::new("revision", json!(self.store.revision))] } else { Vec::new() }
    }

    fn commit(&mut self, batch: Batch, sqlite_ok: bool) -> Result<(), &'static str> {
        self.batch += 1;
        let id = self.batch;
        let mut result = Err("database");
        let mut todo = vec![Request::Commit { id, batch }];
        while !todo.is_empty() {
            let request = todo.remove(0);
            for action in self.store.step(request) {
                match action {
                    Action::Write(id) => todo.push(Request::Written { id, result: if sqlite_ok { Ok(self.store.revision + 1) } else { Err(()) } }),
                    Action::Committed { .. } => result = Ok(()),
                    Action::Rejected { reason: Reason::NoSession, .. } => result = Err("no_session"),
                    Action::Rejected { reason: Reason::Closed, .. } => result = Err("closed"),
                    _ => {}
                }
            }
        }
        result
    }

}

fn model_names(input: &Input) -> BTreeSet<String> {
    input.args.get("models").and_then(|value| value.as_array()).map(|items| items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

impl RefModel for StoreRef {
    fn step(&mut self, input: &Input) -> Vec<Observation> {
        let mut observed = Vec::new();
        match input.name.as_str() {
            "login" => self.feed(Event::Credential(1)),
            "session_reset" => self.feed(Event::SessionReset(Some(1))),
            "close" => self.feed(Event::Close(1)),
            "reset_models" => self.feed(Event::ResetModels(model_names(input), Some(1))),
            "fail_commit" => self.fail_commit = true,
            "fail_clear" => self.fail_clear = true,
            "hold_writer" => self.held = true,
            "release_writer" => {
                self.held = false;
                while let Some(request) = self.waiting.pop_front() { self.send(request); }
            }
            "create" => {
                let current = input.text("epoch") != Some("other");
                let name = input.text("as").unwrap_or("a").to_string();
                let epoch = if current { self.life.cell.epoch } else { self.life.cell.epoch + 1000 };
                let result = match self.gate(current) {
                    Err(kind) => Err(kind),
                    Ok(()) => {
                        let mut batch = Batch::rows(epoch, vec![("Moment".into(), name.clone())]);
                        batch.basis.insert("Moment".into(), self.life.cell.generation);
                        let sqlite_ok = !std::mem::take(&mut self.fail_commit);
                        self.commit(batch, sqlite_ok)
                    }
                };
                observed.push(match result {
                    Ok(()) => Observation::new("created", json!({ "row": name, "epoch": epoch })),
                    Err(kind) => Observation::new("created", json!({ "error": kind })),
                });
            }
            "rows" => {
                let current = input.text("epoch") != Some("other");
                observed.push(match self.gate(current) {
                    Err(kind) => Observation::new("rows", json!({ "error": kind })),
                    Ok(()) => Observation::new("rows", json!({ "epoch": self.life.cell.epoch, "present": self.store.rows.iter().map(|(_, name)| name).collect::<Vec<_>>() })),
                });
            }
            "commit_batch" => {
                let names: Vec<String> = input.args.get("names").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|n| n.as_str().map(str::to_string)).collect()).unwrap_or_default();
                let epoch = self.life.cell.epoch;
                match self.gate(true) {
                    Err(kind) => observed.push(Observation::new("created", json!({ "error": kind }))),
                    Ok(()) => {
                        let mut batch = Batch::rows(epoch, names.iter().map(|n| ("Moment".to_string(), n.clone())).collect());
                        batch.basis.insert("Moment".into(), self.life.cell.generation);
                        let _ = self.commit(batch, true);
                        observed.extend(names.iter().map(|n| Observation::new("created", json!({ "row": n, "epoch": epoch }))));
                    }
                }
            }
            "rows_held" => {
                observed.push(Observation::new("rows", json!({ "epoch": self.life.cell.epoch, "present": self.store.rows.iter().map(|(_, name)| name).collect::<Vec<_>>() })));
            }
            _ => return vec![Observation::new("unsupported", json!(input.name))],
        }
        observed.extend(self.revision());
        observed
    }
}
