//! Owner `StoreWriter`: the only writer of SQLite. SQLite runs as the effects `Write` and `Erase`;
//! the result comes back as the input `Written` or `Erased` (R2), then `revision` grows and the author gets its answer.

use std::collections::{BTreeMap, BTreeSet};

use std::collections::VecDeque;

use super::contract::{BatchAuthor, BatchId, EpochEnded, Published, PublishedState, RejectReason};
use super::session::PendingCall;
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

use super::lifecycle::Identity;

include!("store_types.rs");
include!("store_basis.rs");
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreMode {
    Open,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IoKind {
    Account { epoch_old: Epoch, cause: Cause, generation_new: u64, erase_id: u64 },
    Models { generation_new: u64, models: BTreeSet<String>, erase_id: u64 },
    Secret { epoch_old: Epoch, erase_id: u64, revision: u64 },
    Save { publication_id: u64, published: Published },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Io {
    id: u64,
    kind: IoKind,
    started: bool,
}

pub struct StoreState {
    mode: StoreMode,
    closing: bool,
    revision: u64,
    epoch: Epoch,
    saved_epoch: Epoch,
    saved_generation: u64,
    saved_identity: Option<Identity>,
    saved_unowned_pending: bool,
    generations: BTreeMap<String, u64>,
    /// `model_generation[*]` of the models without an entry.
    base_generation: u64,
    writing: Option<StoreBatch>,
    io: VecDeque<Io>,
    next_io: u64,
    deferred: VecDeque<StoreInput>,
    pins: BTreeMap<u64, (Epoch, u64)>,
    reads: BTreeSet<u64>,
    reads_closed: bool,
}

impl StoreState {
    pub fn new() -> Self {
        Self {
            mode: StoreMode::Open,
            closing: false,
            revision: 0,
            epoch: 0,
            saved_epoch: 0,
            saved_generation: 0,
            saved_identity: None,
            saved_unowned_pending: false,
            generations: BTreeMap::new(),
            base_generation: 0,
            writing: None,
            io: VecDeque::new(),
            next_io: 1,
            deferred: VecDeque::new(),
            pins: BTreeMap::new(),
            reads: BTreeSet::new(),
            reads_closed: false,
        }
    }

    pub fn mode(&self) -> StoreMode { self.mode }

    pub fn revision(&self) -> u64 { self.revision }

    pub fn epoch(&self) -> Epoch { self.epoch }

    pub fn closing(&self) -> bool { self.closing }

    pub fn generation(&self, model: &str) -> u64 { self.generations.get(model).copied().unwrap_or(self.base_generation) }

    fn max_generation(&self) -> u64 { self.generations.values().copied().max().unwrap_or(0).max(self.base_generation) }

    fn busy(&self) -> bool { self.writing.is_some() || !self.io.is_empty() }

    fn stale_models(&self, batch: &StoreBatch) -> Vec<String> {
        batch.basis.iter().filter(|(model, basis)| **basis < self.generation(model)).map(|(model, _)| model.clone()).collect()
    }
}

impl Default for StoreState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct StoreWriter;

impl StateOwner for StoreWriter {
    const EPOCHED: bool = false;

    type State = StoreState;
    type Input = StoreInput;
    type Effect = StoreEffect;

    fn step(state: &mut StoreState, input: StoreInput) -> Effects<Self> {
        let mut fx = Vec::new();
        state.run(input, &mut fx);
        fx.into_iter().map(Effect::Run).collect()
    }

    fn tick(_now: Time) -> StoreInput { StoreInput::Idle }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> StoreInput { StoreInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> StoreInput { StoreInput::Idle }
}

impl StoreState {
    /// One row of the table per match arm, in table order.
    fn run(&mut self, input: StoreInput, fx: &mut Vec<StoreEffect>) {
        if let StoreInput::Boot(found) = input {
            self.boot(found, fx);
            return;
        }
        if self.mode == StoreMode::Closed {
            match input {
                StoreInput::Commit(batch) => fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::Closed) }),
                StoreInput::ClearAccount { erase_id, .. } | StoreInput::ClearModels { erase_id, .. } => fx.push(StoreEffect::ClearFailed { erase_id, error: "closed".into() }),
                _ => {}
            }
            return;
        }
        let ends_a_step = matches!(input, StoreInput::Written { .. } | StoreInput::Erased { .. } | StoreInput::ReadsClosed { .. } | StoreInput::ReadFinished { .. });
        self.apply(input, fx);
        if ends_a_step {
            self.check_close(fx);
        }
    }

    /// Starts the head of `io` when it waits and nothing is being written.
    fn start_head(&mut self, fx: &mut Vec<StoreEffect>) {
        if self.writing.is_some() {
            return;
        }
        let Some(head) = self.io.front_mut() else { return };
        if head.started {
            return;
        }
        head.started = true;
        if let IoKind::Save { publication_id, published } = &head.kind {
            fx.push(StoreEffect::SavePublished { io_id: head.id, publication_id: *publication_id, published: published.clone() });
        }
    }

    /// Applies the inputs that waited, in order, while the Store is free.
    fn drain(&mut self, fx: &mut Vec<StoreEffect>) {
        self.start_head(fx);
        while !self.busy() {
            let Some(input) = self.deferred.pop_front() else { break };
            self.apply(input, fx);
        }
    }

    fn check_close(&mut self, fx: &mut Vec<StoreEffect>) {
        if self.closing && self.reads_closed && self.writing.is_none() && self.io.is_empty() && self.reads.is_empty() {
            self.mode = StoreMode::Closed;
            fx.push(StoreEffect::CloseConnections);
            fx.push(StoreEffect::StoreClosed);
        }
    }

    fn wipe_account(&mut self, generation_new: u64) {
        self.generations.clear();
        self.base_generation = generation_new;
    }

    fn apply(&mut self, input: StoreInput, fx: &mut Vec<StoreEffect>) {
        if self.closing {
            match &input {
                StoreInput::Commit(batch) => {
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::Closed) });
                    return;
                }
                StoreInput::ClearAccount { erase_id, .. } | StoreInput::ClearModels { erase_id, .. } => {
                    fx.push(StoreEffect::ClearFailed { erase_id: *erase_id, error: "closed".into() });
                    return;
                }
                StoreInput::Published { publication_id, .. } => {
                    fx.push(StoreEffect::SaveFailed { publication_id: *publication_id, error: "closed".into() });
                    return;
                }
                StoreInput::ReadIntents { .. } => return,
                _ => {}
            }
        }
        match input {
            StoreInput::Commit(batch) => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::Commit(batch));
                } else if batch.epoch != self.epoch {
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::NoSession) });
                } else if batch.models().iter().any(|model| !batch.basis.contains_key(model)) {
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::Invalid) });
                } else if !self.stale_models(&batch).is_empty() {
                    let models = self.stale_models(&batch);
                    let generation = models.iter().map(|model| self.generation(model)).max().unwrap_or(self.base_generation);
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, epoch: batch.epoch, reply: StoreReply::Rejected(RejectReason::StaleGeneration { models, generation }) });
                } else {
                    self.writing = Some(batch.clone());
                    fx.push(StoreEffect::Write { batch, min_pin_revision: self.pins.values().map(|(_, revision)| *revision).min() });
                }
            }
            StoreInput::Pin { epoch, request_id } => {
                if epoch != self.epoch {
                    fx.push(StoreEffect::Pinned { epoch, request_id, base_revision: self.revision });
                } else if self.busy() {
                    self.deferred.push_back(StoreInput::Pin { epoch, request_id });
                } else {
                    self.pins.insert(request_id, (epoch, self.revision));
                    fx.push(StoreEffect::Pinned { epoch, request_id, base_revision: self.revision });
                }
            }
            StoreInput::Unpin { request_id } => { self.pins.remove(&request_id); }
            StoreInput::Written { author, batch, result } => {
                if !self.writing.as_ref().is_some_and(|writing| writing.author == author && writing.id == batch) {
                    return;
                }
                let Some(written) = self.writing.take() else { return };
                match result {
                    Err(reason) => fx.push(StoreEffect::Reply { author, batch, epoch: written.epoch, reply: StoreReply::Rejected(reason) }),
                    Ok(WriteDone { revision, position, observed, expiring, removed_memory, stale_parents, removed_operations, resets, rows }) => {
                        self.revision = revision;
                        let models: Vec<String> = written.models().into_iter().collect();
                        if !observed.is_empty() { fx.push(StoreEffect::Observed { epoch: written.epoch, batch, payloads: observed }); }
                        fx.push(StoreEffect::Reply { author, batch, epoch: written.epoch, reply: StoreReply::Committed { revision, stale_parents, rows: rows.clone() } });
                        for reset in resets { fx.push(StoreEffect::ArmReset { epoch: written.epoch, reset }); }
                        if !removed_operations.is_empty() { fx.push(StoreEffect::OperationsGone { epoch: written.epoch, ops: removed_operations }); }
                        for (model, row_id) in removed_memory { fx.push(StoreEffect::DisarmExpiry { epoch: written.epoch, model, row_id }); }
                        for item in expiring { fx.push(StoreEffect::ArmExpiry { epoch: written.epoch, model: item.model, row_id: item.row_id, revision, deadline: item.written_at.saturating_add(item.expires_after) }); }
                        fx.push(StoreEffect::BroadcastCommitted { revision, position, models: Some(models), rows });
                    }
                }
                self.drain(fx);
            }
            StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id } => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id });
                } else if generation_new <= self.max_generation() {
                    fx.push(StoreEffect::ClearFailed { erase_id, error: "stale generation".into() });
                } else {
                    let io_id = self.next_io;
                    self.next_io += 1;
                    self.io.push_back(Io { id: io_id, kind: IoKind::Account { epoch_old, cause, generation_new, erase_id }, started: true });
                    fx.push(StoreEffect::EraseAccount { io_id, generation_new, erase_encrypted: matches!(cause, Cause::SessionReset | Cause::ServerEnded) });
                }
            }
            StoreInput::ClearModels { epoch, generation_new, models, erase_id } => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::ClearModels { epoch, generation_new, models, erase_id });
                } else if models.iter().any(|model| generation_new <= self.generation(model)) {
                    fx.push(StoreEffect::ClearFailed { erase_id, error: "stale generation".into() });
                } else {
                    let io_id = self.next_io;
                    self.next_io += 1;
                    self.io.push_back(Io { id: io_id, kind: IoKind::Models { generation_new, models: models.clone(), erase_id }, started: true });
                    fx.push(StoreEffect::EraseModels { io_id, generation_new, models });
                }
            }
            StoreInput::Erased { io_id, result } => {
                if self.io.front().map(|head| head.id) != Some(io_id) {
                    return;
                }
                let Some(head) = self.io.pop_front() else { return };
                match (head.kind, result) {
                    (IoKind::Account { erase_id, .. } | IoKind::Models { erase_id, .. }, Err(error)) => fx.push(StoreEffect::ClearFailed { erase_id, error }),
                    (IoKind::Save { publication_id, .. }, Err(error)) => fx.push(StoreEffect::SaveFailed { publication_id, error }),
                    (IoKind::Account { epoch_old, cause, generation_new, erase_id }, Ok(Done { revision })) => {
                        self.wipe_account(generation_new);
                        if matches!(cause, Cause::SessionReset | Cause::ServerEnded) {
                            self.io.push_front(Io { id: head.id, kind: IoKind::Secret { epoch_old, erase_id, revision }, started: true });
                            fx.push(StoreEffect::ClearSecret { io_id: head.id });
                            return;
                        }
                        self.revision = revision;
                        fx.push(StoreEffect::Cleared { epoch_old, erase_id });
                        fx.push(StoreEffect::BroadcastCommitted { revision, position: 0, models: None, rows: None });
                    }
                    (IoKind::Secret { epoch_old, erase_id, revision }, Ok(_) | Err(_)) => {
                        self.revision = revision;
                        fx.push(StoreEffect::Cleared { epoch_old, erase_id });
                        fx.push(StoreEffect::BroadcastCommitted { revision, position: 0, models: None, rows: None });
                    }
                    (IoKind::Models { generation_new, models, erase_id }, Ok(Done { revision })) => {
                        for model in &models {
                            self.generations.insert(model.clone(), generation_new);
                        }
                        self.revision = revision;
                        fx.push(StoreEffect::ModelsCleared { generation_new, erase_id });
                        fx.push(StoreEffect::BroadcastCommitted { revision, position: 0, models: Some(models.into_iter().collect()), rows: None });
                    }
                    (IoKind::Save { publication_id, published }, Ok(_)) => {
                        if matches!(published.state, PublishedState::NoSession | PublishedState::Session | PublishedState::Resetting) {
                            self.epoch = self.epoch.max(published.epoch);
                        }
                        self.saved_epoch = published.epoch;
                        self.saved_generation = published.generation;
                        self.saved_identity = published.identity;
                        self.saved_unowned_pending &= published.identity.is_none();
                        fx.push(StoreEffect::Saved { publication_id });
                    }
                }
                self.drain(fx);
            }
            StoreInput::Published { publication_id, published } => {
                let id = self.next_io;
                self.next_io += 1;
                self.io.push_back(Io { id, kind: IoKind::Save { publication_id, published }, started: false });
                self.start_head(fx);
            }
            StoreInput::EpochEnded(EpochEnded { epoch, cause }) => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::EpochEnded(EpochEnded { epoch, cause }));
                } else {
                    self.pins.retain(|_, (pin_epoch, _)| *pin_epoch != epoch);
                    if epoch + 1 > self.epoch { self.epoch = epoch + 1; }
                }
            }
            StoreInput::ReadIntents { epoch } => fx.push(StoreEffect::ReadIntents { epoch, direct: self.saved_identity.is_none() && self.saved_unowned_pending }),
            StoreInput::Close => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::Close);
                } else if !self.closing {
                    self.closing = true;
                    self.reads_closed = false;
                    fx.push(StoreEffect::CloseReads);
                }
            }
            StoreInput::ReadsClosed { ids } => {
                self.reads = ids.into_iter().collect();
                self.reads_closed = true;
            }
            StoreInput::ReadFinished { read_id } => {
                self.reads.remove(&read_id);
            }
            StoreInput::Boot(_) | StoreInput::Idle => {}
        }
    }

    fn boot(&mut self, found: BootFound, fx: &mut Vec<StoreEffect>) {
        let open = |saved_epoch, saved_generation, saved_identity, saved_unowned_pending| StoreEffect::OpenLifecycle(OpenMessage { saved_epoch, saved_generation, saved_identity, saved_unowned_pending });
        match found {
            BootFound::Fresh => {
                fx.push(StoreEffect::Prepare { format_change: false });
                fx.push(open(0, 0, None, false));
            }
            BootFound::Same { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision } | BootFound::LocalModelChanged { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision } => {
                if matches!(found, BootFound::LocalModelChanged { .. }) {
                    fx.push(StoreEffect::Prepare { format_change: false });
                }
                self.revision = revision;
                self.saved_epoch = saved_epoch;
                self.saved_generation = saved_generation;
                self.saved_identity = saved_identity;
                self.saved_unowned_pending = saved_unowned_pending;
                self.base_generation = saved_generation;
                self.generations.clear();
                fx.push(open(saved_epoch, saved_generation, saved_identity, saved_unowned_pending));
            }
            BootFound::ChangedModels { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision } | BootFound::FormatChange { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision } => {
                self.revision = revision + 1;
                self.saved_epoch = saved_epoch;
                self.saved_generation = saved_generation + 1;
                self.saved_identity = saved_identity;
                self.saved_unowned_pending = saved_unowned_pending;
                self.base_generation = saved_generation + 1;
                self.generations.clear();
                fx.push(StoreEffect::Prepare { format_change: true });
                fx.push(open(saved_epoch, saved_generation + 1, saved_identity, saved_unowned_pending));
            }
            BootFound::SqliteError(error) => {
                self.mode = StoreMode::Closed;
                fx.push(StoreEffect::OpenFailed(error));
            }
            BootFound::CorruptManifest(error) => {
                self.mode = StoreMode::Closed;
                fx.push(StoreEffect::OpenCorrupt(error));
            }
        }
    }
}
