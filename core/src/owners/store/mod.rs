//! Owner `StoreWriter`: the only writer of SQLite. SQLite runs as the effects `Write` and `Erase`;
//! the result comes back as the input `Written` or `Erased` (R2), then `revision` grows and the author gets its answer.

use std::collections::{BTreeMap, BTreeSet};

use std::collections::VecDeque;

use super::contract::{BatchAuthor, BatchId, EpochEnded, Published, PublishedState, RejectReason};
use super::session::PendingCall;
use crate::runtime::{Cause, Effect, Effects, Epoch, StateOwner, Time};

use super::lifecycle::Identity;

mod store_types;
pub use store_types::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreMode {
    Open,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IoKind {
    Account { epoch_old: Epoch, cause: Cause, generation_new: u64, erase_id: u64 },
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
    generation: u64,
    writing: Option<StoreBatch>,
    io: VecDeque<Io>,
    next_io: u64,
    deferred: VecDeque<StoreInput>,
    pins: BTreeMap<u64, (Epoch, u64)>,
    reads: BTreeSet<u64>,
    reads_closed: bool,
    /// Saved window reads and the Window and Feed batches queued before them: a read starts when none of those is left.
    read_waits: Vec<(Epoch, u64, Vec<(BatchAuthor, BatchId)>)>,
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
            generation: 0,
            writing: None,
            io: VecDeque::new(),
            next_io: 1,
            deferred: VecDeque::new(),
            pins: BTreeMap::new(),
            reads: BTreeSet::new(),
            reads_closed: false,
            read_waits: Vec::new(),
        }
    }

    pub fn mode(&self) -> StoreMode { self.mode }

    pub fn revision(&self) -> u64 { self.revision }

    pub fn epoch(&self) -> Epoch { self.epoch }

    pub fn closing(&self) -> bool { self.closing }

    pub fn generation(&self) -> u64 { self.generation }

    fn busy(&self) -> bool { self.writing.is_some() || !self.io.is_empty() }
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

    #[cfg(test)]
    fn diagnostic(state: &StoreState) -> Option<String> { Some(format!("revision={}, writing={}, closing={}", state.revision, state.writing.is_some(), state.closing)) }

    fn step(state: &mut StoreState, input: StoreInput, _now: Time) -> Effects<Self> {
        let mut fx = Vec::new();
        state.run(input, &mut fx);
        fx.into_iter().map(Effect::Run).collect()
    }

    fn epoch_ended(epoch: Epoch, cause: Cause) -> StoreInput { StoreInput::EpochEnded(EpochEnded { epoch, cause }) }

    fn closing() -> StoreInput { StoreInput::Idle }

    fn executor_failed(_executor: &str, pending: Vec<crate::runtime::EffectId>) -> Vec<StoreInput> {
        pending.into_iter().map(|io_id| StoreInput::Erased { io_id, result: Err("the erase task failed".into()) }).collect()
    }
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
                StoreInput::Commit(batch) => {
                    self.commit_left(batch.author, batch.id, fx);
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, stamp: batch.stamp, reply: StoreReply::Rejected(RejectReason::Closed) });
                }
                StoreInput::ClearAccount { erase_id, .. } => fx.push(StoreEffect::ClearFailed { erase_id, error: "closed".into() }),
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

    /// A Window or Feed batch is done or refused: the saved window reads that waited for it go on.
    fn commit_left(&mut self, author: BatchAuthor, batch: BatchId, fx: &mut Vec<StoreEffect>) {
        if !matches!(author, BatchAuthor::Window | BatchAuthor::Feed) {
            return;
        }
        for wait in &mut self.read_waits {
            wait.2.retain(|ahead| *ahead != (author, batch));
        }
        let (ready, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.read_waits).into_iter().partition(|wait| wait.2.is_empty());
        self.read_waits = waiting;
        for (epoch, read_id, _) in ready {
            fx.push(StoreEffect::ReadReady { epoch, read_id });
        }
    }

    fn wipe_account(&mut self, generation_new: u64) {
        self.generation = generation_new;
    }

    fn apply(&mut self, input: StoreInput, fx: &mut Vec<StoreEffect>) {
        if self.closing {
            match &input {
                StoreInput::Commit(batch) if batch.stamp != crate::runtime::EpochStamp::Control => {
                    self.commit_left(batch.author, batch.id, fx);
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, stamp: batch.stamp, reply: StoreReply::Rejected(RejectReason::Closed) });
                    return;
                }
                StoreInput::ClearAccount { erase_id, .. } => {
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
                } else if batch.stamp == crate::runtime::EpochStamp::Control {
                    if batch.rows.is_empty() && batch.ops.iter().all(|op| matches!(op, StoreOp::UploadOrphan { .. } | StoreOp::RemoveOrphan { .. })) {
                        self.writing = Some(batch.clone());
                        fx.push(StoreEffect::Write { batch, min_pin_revision: self.pins.values().map(|(_, revision)| *revision).min() });
                    } else {
                        fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, stamp: batch.stamp, reply: StoreReply::Rejected(RejectReason::Invalid("the control batch carries rows or an operation of an epoch".into())) });
                    }
                } else if batch.stamp != crate::runtime::EpochStamp::Epoch(self.epoch) {
                    self.commit_left(batch.author, batch.id, fx);
                    fx.push(StoreEffect::Reply { author: batch.author, batch: batch.id, stamp: batch.stamp, reply: StoreReply::Rejected(RejectReason::NoSession) });
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
            StoreInput::ReadAfter { epoch, read_id } => {
                let ahead: Vec<(BatchAuthor, BatchId)> = self.writing.iter().map(|batch| (batch.author, batch.id)).chain(self.deferred.iter().filter_map(|input| match input { StoreInput::Commit(batch) => Some((batch.author, batch.id)), _ => None })).filter(|(author, _)| matches!(author, BatchAuthor::Window | BatchAuthor::Feed)).collect();
                if epoch != self.epoch || ahead.is_empty() {
                    fx.push(StoreEffect::ReadReady { epoch, read_id });
                } else {
                    self.read_waits.push((epoch, read_id, ahead));
                }
            }
            StoreInput::Written { author, batch, result } => {
                if !self.writing.as_ref().is_some_and(|writing| writing.author == author && writing.id == batch) {
                    return;
                }
                let Some(written) = self.writing.take() else { return };
                self.commit_left(author, batch, fx);
                let written_epoch = match written.stamp { crate::runtime::EpochStamp::Epoch(epoch) => epoch, crate::runtime::EpochStamp::Control => self.epoch };
                match result {
                    Err(reason) => fx.push(StoreEffect::Reply { author, batch, stamp: written.stamp, reply: StoreReply::Rejected(reason) }),
                    Ok(WriteDone { revision, observed, expiring, removed_memory, stale_parents, removed_operations, removed_rows, ended_temps, swaps, resets, notice, shown, files_removed, refused }) => {
                        self.revision = revision;
                        if !observed.is_empty() { fx.push(StoreEffect::Observed { epoch: written_epoch, batch, payloads: observed }); }
                        fx.push(StoreEffect::Reply { author, batch, stamp: written.stamp, reply: StoreReply::Committed { revision, stale_parents, rows: notice.as_ref().map_or(Some(Default::default()), |notice| notice.rows.clone()), shown, files_removed, refused } });
                        for reset in resets { fx.push(StoreEffect::ArmReset { epoch: written_epoch, reset }); }
                        if !removed_operations.is_empty() { fx.push(StoreEffect::OperationsGone { epoch: written_epoch, ops: removed_operations }); }
                        if !removed_rows.is_empty() { fx.push(StoreEffect::RowsRemoved { epoch: written_epoch, rows: removed_rows, revision }); }
                        if !swaps.is_empty() || !ended_temps.is_empty() { fx.push(StoreEffect::AliasFacts { epoch: written_epoch, swaps, ended: ended_temps }); }
                        for (model, row_id) in removed_memory { fx.push(StoreEffect::DisarmExpiry { epoch: written_epoch, model, row_id }); }
                        for item in expiring { fx.push(StoreEffect::ArmExpiry { epoch: written_epoch, model: item.model, row_id: item.row_id, revision, deadline: item.written_at.saturating_add(item.expires_after) }); }
                    }
                }
                self.drain(fx);
            }
            StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id } => {
                if self.busy() {
                    self.deferred.push_back(StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id });
                } else if generation_new <= self.generation {
                    fx.push(StoreEffect::ClearFailed { erase_id, error: "stale generation".into() });
                } else {
                    let io_id = self.next_io;
                    self.next_io += 1;
                    self.io.push_back(Io { id: io_id, kind: IoKind::Account { epoch_old, cause, generation_new, erase_id }, started: true });
                    fx.push(StoreEffect::EraseAccount { io_id, generation_new, erase_encrypted: matches!(cause, Cause::SessionReset | Cause::ServerEnded) });
                }
            }
            StoreInput::Erased { io_id, result } => {
                if self.io.front().map(|head| head.id) != Some(io_id) {
                    return;
                }
                let Some(head) = self.io.pop_front() else { return };
                match (head.kind, result) {
                    (IoKind::Account { erase_id, .. }, Err(error)) => fx.push(StoreEffect::ClearFailed { erase_id, error }),
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
                    }
                    (IoKind::Secret { epoch_old, erase_id, revision }, Ok(_) | Err(_)) => {
                        self.revision = revision;
                        fx.push(StoreEffect::Cleared { epoch_old, erase_id });
                    }
                    (IoKind::Save { publication_id, published }, Ok(_)) => {
                        if matches!(published.state, PublishedState::NoSession | PublishedState::Session) {
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
                self.generation = saved_generation;
                fx.push(open(saved_epoch, saved_generation, saved_identity, saved_unowned_pending));
            }
            BootFound::ChangedModels { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision } | BootFound::FormatChange { saved_epoch, saved_generation, saved_identity, saved_unowned_pending, revision } => {
                self.revision = revision + 1;
                self.saved_epoch = saved_epoch;
                self.saved_generation = saved_generation + 1;
                self.saved_identity = saved_identity;
                self.saved_unowned_pending = saved_unowned_pending;
                self.generation = saved_generation + 1;
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
