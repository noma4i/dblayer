use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;

use super::ref_lifecycle::{Effect, Event, Lifecycle, State as LifeState};
use crate::check::model::RefModel;
use crate::check::value::{Input, Observation};

#[derive(Clone, Debug, PartialEq)]
pub struct Batch {
    pub epoch: u64,
    pub basis: BTreeMap<String, u64>,
    pub rows: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    Commit { batch: Batch, sqlite_ok: bool },
    ClearAccount { sqlite_ok: bool },
    ClearModels { generation_new: u64, models: BTreeSet<String>, sqlite_ok: bool },
    Published { epoch: u64, generation: u64 },
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
    Cleared,
    ClearFailed,
    ModelsCleared,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Open {
    Open,
    Closed,
}

/// The rows of `specs/machines/store.md`: `rows` holds the committed rows (model, id) a snapshot shows.
pub struct Store {
    pub state: Open,
    pub revision: u64,
    pub epoch: u64,
    pub published_generation: u64,
    pub generations: BTreeMap<String, u64>,
    pub rows: BTreeSet<(String, String)>,
}

#[derive(Default)]
pub struct Done {
    pub reply: Option<Reply>,
}

impl Store {
    pub fn new() -> Self {
        Self { state: Open::Open, revision: 0, epoch: 0, published_generation: 0, generations: BTreeMap::new(), rows: BTreeSet::new() }
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
            Request::Commit { batch, .. } if batch.basis.iter().any(|(model, basis)| *basis < self.generations.get(model).copied().unwrap_or(0)) => {
                let stale = batch.basis.iter().filter(|(model, basis)| **basis < self.generations.get(*model).copied().unwrap_or(0)).map(|(model, _)| model.clone()).collect();
                Done { reply: Some(Reply::Rejected(Reason::StaleGeneration(stale))) }
            }
            Request::Commit { sqlite_ok: false, .. } => Done { reply: Some(Reply::Rejected(Reason::Storage)) },
            Request::Commit { batch, .. } => {
                self.rows.extend(batch.rows);
                self.revision += 1;
                Done { reply: Some(Reply::Committed(self.revision)) }
            }
            Request::ClearAccount { sqlite_ok: false } | Request::ClearModels { sqlite_ok: false, .. } => Done { reply: Some(Reply::ClearFailed) },
            Request::ClearAccount { .. } => {
                self.rows.clear();
                self.revision += 1;
                for generation in self.generations.values_mut() { *generation += 1; }
                Done { reply: Some(Reply::Cleared) }
            }
            Request::ClearModels { generation_new, models, .. } => {
                self.rows.retain(|(model, _)| !models.contains(model));
                self.revision += 1;
                for model in models { self.generations.insert(model, generation_new); }
                Done { reply: Some(Reply::ModelsCleared) }
            }
            Request::Published { epoch, generation } => {
                self.epoch = epoch;
                self.published_generation = generation;
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
            Request::ClearAccount { .. } => Request::ClearAccount { sqlite_ok },
            Request::ClearModels { generation_new, models, .. } => Request::ClearModels { generation_new, models, sqlite_ok },
            other => other,
        };
        let done = self.store.step(request);
        let event = match done.reply {
            Some(Reply::Cleared) => Some(Event::Cleared),
            Some(Reply::ModelsCleared) => Some(Event::ModelsCleared(self.store.generations.values().copied().max().unwrap_or(0))),
            Some(Reply::ClearFailed) => Some(Event::ClearFailed),
            _ => None,
        };
        if let Some(event) = event { self.feed(event); }
    }

    fn feed(&mut self, event: Event) {
        let out = self.life.step(event);
        for effect in out.effects {
            match effect {
                Effect::Publish => self.send(Request::Published { epoch: self.life.published.epoch, generation: self.life.published.generation }),
                Effect::ClearAccount(_) => self.queue(Request::ClearAccount { sqlite_ok: true }),
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
            "reset_models" => self.feed(Event::ResetModels(model_names(input), 1)),
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
                        let batch = Batch { epoch, basis: BTreeMap::new(), rows: vec![("Moment".into(), name.clone())] };
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
                        let batch = Batch { epoch, basis: BTreeMap::new(), rows: names.iter().map(|n| ("Moment".to_string(), n.clone())).collect() };
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
