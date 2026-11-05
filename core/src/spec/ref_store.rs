use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;

use super::ref_lifecycle::{Cause, Effect, Event, Lifecycle, State as LifeState};
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
    Commit { batch: Batch, sqlite_ok: bool },
    ClearAccount { epoch_old: u64, cause: Cause, generation_new: u64, sqlite_ok: bool },
    ClearModels { generation_new: u64, models: BTreeSet<String>, sqlite_ok: bool },
    Published { epoch: u64, generation: u64 },
    EpochEnded { epoch: u64 },
    Close,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Reason {
    NoSession,
    StaleGeneration(Vec<String>),
    Storage,
    Closed,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    Committed(u64),
    Rejected(Reason),
    Cleared(u64),
    ClearFailed,
    ModelsCleared(u64),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Open {
    Open,
    Closed,
}

/// What `Boot` finds in the database.
#[derive(Clone, Debug, PartialEq)]
pub enum Boot {
    New,
    Same,
    ChangedModels(BTreeSet<String>),
    FormatChange,
    SqliteError,
}

/// `Lifecycle Open{..}` that the `Boot` rows send.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenMessage {
    pub saved_epoch: u64,
    pub saved_generation: u64,
    pub saved_identity: Option<u64>,
    pub changed_models: BTreeSet<String>,
}

/// The rows of `specs/machines/store.md`: `rows` holds the committed rows (model, id) a snapshot shows.
#[derive(Clone, Debug, PartialEq)]
pub struct Store {
    pub state: Open,
    pub revision: u64,
    pub epoch: u64,
    pub saved_epoch: u64,
    pub saved_generation: u64,
    pub generations: BTreeMap<String, u64>,
    /// `model_generation[*]` of the models that have no entry.
    pub base_generation: u64,
    pub rows: BTreeSet<(String, String)>,
    pub intents: u64,
    pub credential: Option<u64>,
    pub pending: bool,
}

#[derive(Debug, Default, PartialEq)]
pub struct Done {
    pub reply: Option<Reply>,
}

impl Store {
    pub fn new() -> Self {
        Self { state: Open::Open, revision: 0, epoch: 0, saved_epoch: 0, saved_generation: 0, generations: BTreeMap::new(), base_generation: 0, rows: BTreeSet::new(), intents: 0, credential: None, pending: false }
    }

    pub fn generation(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base_generation) }

    /// `store_boot_*`: the new state after `Boot` and the message to Lifecycle (`None` for `store_boot_error`).
    pub fn boot(&mut self, found: Boot) -> Option<OpenMessage> {
        match found {
            Boot::SqliteError => {
                self.state = Open::Closed;
                None
            }
            Boot::New => Some(OpenMessage { saved_epoch: 0, saved_generation: 0, saved_identity: None, changed_models: BTreeSet::new() }),
            Boot::Same => Some(OpenMessage { saved_epoch: self.saved_epoch, saved_generation: self.saved_generation, saved_identity: self.credential, changed_models: BTreeSet::new() }),
            Boot::ChangedModels(changed) => Some(OpenMessage { saved_epoch: self.saved_epoch, saved_generation: self.saved_generation, saved_identity: self.credential, changed_models: changed }),
            Boot::FormatChange => {
                self.revision += 1;
                self.rows.clear();
                self.generations.clear();
                self.saved_generation += 1;
                self.base_generation = self.saved_generation;
                Some(OpenMessage { saved_epoch: self.saved_epoch, saved_generation: self.saved_generation, saved_identity: self.credential, changed_models: BTreeSet::new() })
            }
        }
    }

    pub fn step(&mut self, request: Request) -> Done {
        if self.state == Open::Closed {
            return match request {
                Request::Commit { .. } => Done { reply: Some(Reply::Rejected(Reason::Closed)) },
                _ => Done::default(),
            };
        }
        match request {
            Request::Commit { batch, .. } if batch.epoch != self.epoch => Done { reply: Some(Reply::Rejected(Reason::NoSession)) },
            Request::Commit { batch, .. } if batch.basis.iter().any(|(model, basis)| *basis < self.generation(model)) => {
                let stale = batch.basis.iter().filter(|(model, basis)| **basis < self.generation(model)).map(|(model, _)| model.clone()).collect();
                Done { reply: Some(Reply::Rejected(Reason::StaleGeneration(stale))) }
            }
            Request::Commit { sqlite_ok: false, .. } => Done { reply: Some(Reply::Rejected(Reason::Storage)) },
            Request::Commit { batch, .. } => {
                self.rows.extend(batch.rows);
                self.intents += batch.intents;
                if let Some(identity) = batch.credential { self.credential = Some(identity); }
                self.revision += 1;
                Done { reply: Some(Reply::Committed(self.revision)) }
            }
            Request::ClearAccount { sqlite_ok: false, .. } | Request::ClearModels { sqlite_ok: false, .. } => Done { reply: Some(Reply::ClearFailed) },
            Request::ClearAccount { epoch_old, cause, generation_new, .. } => {
                self.rows.clear();
                self.intents = 0;
                if matches!(cause, Cause::SessionReset | Cause::ServerEnded) {
                    self.credential = None;
                    self.pending = false;
                }
                self.revision += 1;
                self.generations.clear();
                self.base_generation = generation_new;
                Done { reply: Some(Reply::Cleared(epoch_old)) }
            }
            Request::ClearModels { generation_new, models, .. } => {
                self.rows.retain(|(model, _)| !models.contains(model));
                self.revision += 1;
                for model in models { self.generations.insert(model, generation_new); }
                Done { reply: Some(Reply::ModelsCleared(generation_new)) }
            }
            Request::Published { epoch, generation } => {
                self.epoch = epoch;
                self.saved_epoch = epoch;
                self.saved_generation = generation;
                Done::default()
            }
            Request::EpochEnded { epoch } => {
                self.epoch = epoch + 1;
                Done::default()
            }
            Request::Close => {
                self.state = Open::Closed;
                Done::default()
            }
        }
    }
}

/// The store table behind the public vocabulary: the lifecycle table gates the calls and sends the store its requests.
/// `fail_commit` and `fail_clear` make the next SQLite action fail. With `full` the model also reports `revision`.
pub struct StoreRef {
    life: Lifecycle,
    store: Store,
    full: bool,
    fail_commit: bool,
    fail_clear: bool,
    held: bool,
    waiting: Vec<Request>,
}

impl StoreRef {
    pub fn new(full: bool) -> Self {
        Self { life: Lifecycle::new(), store: Store::new(), full, fail_commit: false, fail_clear: false, held: false, waiting: Vec::new() }
    }

    fn send(&mut self, request: Request) {
        let sqlite_ok = !(matches!(request, Request::ClearAccount { .. } | Request::ClearModels { .. }) && std::mem::take(&mut self.fail_clear));
        let request = match request {
            Request::ClearAccount { epoch_old, cause, generation_new, .. } => Request::ClearAccount { epoch_old, cause, generation_new, sqlite_ok },
            Request::ClearModels { generation_new, models, .. } => Request::ClearModels { generation_new, models, sqlite_ok },
            other => other,
        };
        let done = self.store.step(request);
        let event = match done.reply {
            Some(Reply::Cleared(_)) => Some(Event::Cleared),
            Some(Reply::ModelsCleared(generation)) => Some(Event::ModelsCleared(generation)),
            Some(Reply::ClearFailed) => Some(Event::ClearFailed),
            _ => None,
        };
        if let Some(event) = event { self.feed(event); }
    }

    fn feed(&mut self, event: Event) {
        let out = self.life.step(event);
        for effect in out.effects {
            match effect {
                Effect::Publish(published) => self.send(Request::Published { epoch: published.epoch, generation: published.generation }),
                Effect::EpochEnded(epoch, _) => self.send(Request::EpochEnded { epoch }),
                Effect::ClearAccount(epoch_old, cause, generation_new) => self.queue(Request::ClearAccount { epoch_old, cause, generation_new, sqlite_ok: true }),
                Effect::ClearModels(_, generation_new, models) => self.queue(Request::ClearModels { generation_new, models, sqlite_ok: true }),
                Effect::StoreClose => self.send(Request::Close),
                _ => {}
            }
        }
        if let LifeState::Closing { waiting, .. } = &self.life.state {
            if let Some(owner) = waiting.iter().next().copied() { self.feed(Event::Drained(owner)); }
        }
    }

    fn queue(&mut self, request: Request) {
        if self.held { self.waiting.push(request); } else { self.send(request); }
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
}

fn model_names(input: &Input) -> BTreeSet<String> {
    input.args.get("models").and_then(|value| value.as_array()).map(|items| items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect()).unwrap_or_default()
}

impl RefModel for StoreRef {
    fn step(&mut self, input: &Input) -> Vec<Observation> {
        let mut observed = Vec::new();
        match input.name.as_str() {
            "login" => self.feed(Event::Credential(1)),
            "session_reset" => self.feed(Event::SessionReset(1)),
            "close" => self.feed(Event::Close(1)),
            "reset_models" => self.feed(Event::ResetModels(model_names(input), Some(1))),
            "fail_commit" => self.fail_commit = true,
            "fail_clear" => self.fail_clear = true,
            "hold_writer" => self.held = true,
            "release_writer" => {
                self.held = false;
                for request in std::mem::take(&mut self.waiting) { self.send(request); }
            }
            "create" => {
                let current = input.text("epoch") != Some("other");
                let name = input.text("as").unwrap_or("a").to_string();
                let epoch = if current { self.life.published.epoch } else { self.life.published.epoch + 1000 };
                let result = match self.gate(current) {
                    Err(kind) => Err(kind),
                    Ok(()) => {
                        let batch = Batch::rows(epoch, vec![("Moment".into(), name.clone())]);
                        let sqlite_ok = !std::mem::take(&mut self.fail_commit);
                        match self.store.step(Request::Commit { batch, sqlite_ok }).reply {
                            Some(Reply::Committed(_)) => Ok(()),
                            Some(Reply::Rejected(Reason::NoSession)) => Err("no_session"),
                            Some(Reply::Rejected(Reason::Closed)) => Err("closed"),
                            _ => Err("database"),
                        }
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
                    Ok(()) => Observation::new("rows", json!({ "epoch": self.life.published.epoch, "present": self.store.rows.iter().map(|(_, name)| name).collect::<Vec<_>>() })),
                });
            }
            "commit_batch" => {
                let names: Vec<String> = input.args.get("names").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|n| n.as_str().map(str::to_string)).collect()).unwrap_or_default();
                let epoch = self.life.published.epoch;
                match self.gate(true) {
                    Err(kind) => observed.push(Observation::new("created", json!({ "error": kind }))),
                    Ok(()) => {
                        let batch = Batch::rows(epoch, names.iter().map(|n| ("Moment".to_string(), n.clone())).collect());
                        self.store.step(Request::Commit { batch, sqlite_ok: true });
                        observed.extend(names.iter().map(|n| Observation::new("created", json!({ "row": n, "epoch": epoch }))));
                    }
                }
            }
            "rows_held" => {
                observed.push(Observation::new("rows", json!({ "epoch": self.life.published.epoch, "present": self.store.rows.iter().map(|(_, name)| name).collect::<Vec<_>>() })));
            }
            _ => return vec![Observation::new("unsupported", json!(input.name))],
        }
        observed.extend(self.revision());
        observed
    }
}
