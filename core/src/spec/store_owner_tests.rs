//! Owner-level tests of the `store` machine: the owner `Store` on `DetScheduler` against `ref_store.rs`. SQLite is the effect `Write` or `Erase*`,
//! the test answers it with `Written` or `Erased`. Names are the `Test` column.

use std::collections::{BTreeMap, BTreeSet};

use super::ref_lifecycle::Cause as RefCause;
use super::ref_store::{Action, Batch, Boot, Open, Reason, Request, Store as Ref};
use crate::owners::contract::{BatchAuthor, EpochEnded, Models, Published, PublishedState, RejectReason};
use crate::owners::store::{BootFound, Done, StoreBatch, StoreEffect, StoreInput, StoreMode, StoreReply, StoreState, StoreWriter, WriteDone};
use crate::spec::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

const DISK: &str = "disk";

fn cause(cause: RefCause) -> Cause {
    match cause {
        RefCause::SessionReset => Cause::SessionReset,
        RefCause::ServerEnded => Cause::ServerEnded,
        RefCause::IdentityChanged => Cause::IdentityChanged,
        RefCause::SessionStarted => Cause::SessionStarted,
        RefCause::Close => Cause::Close,
    }
}

fn action_of(effect: &StoreEffect) -> Option<Action> {
    Some(match effect {
        StoreEffect::Reply { batch, reply: StoreReply::Committed { revision, rows, .. }, .. } => Action::Committed { id: *batch, revision: *revision, rows: rows.clone() },
        StoreEffect::Reply { batch, reply: StoreReply::Rejected(reason), .. } => Action::Rejected {
            id: *batch,
            reason: match reason {
                RejectReason::NoSession => Reason::NoSession,
                RejectReason::StaleGeneration { models, .. } => Reason::StaleGeneration(models.clone()),
                RejectReason::Storage(_) => Reason::Storage,
                RejectReason::Closed => Reason::Closed,
                RejectReason::Invalid => Reason::Invalid,
            },
        },
        StoreEffect::Write { batch, .. } => Action::Write(batch.id),
        StoreEffect::Pinned { .. } => return None,
        StoreEffect::EraseAccount { io_id, generation_new, erase_encrypted } => Action::EraseAccount { io_id: *io_id, generation_new: *generation_new, erase_encrypted: *erase_encrypted },
        StoreEffect::EraseModels { io_id, models, .. } => Action::EraseModels { io_id: *io_id, models: models.clone() },
        StoreEffect::ClearSecret { io_id } => Action::ClearSecret { io_id: *io_id },
        StoreEffect::SavePublished { io_id, publication_id, .. } => Action::SavePublished { io_id: *io_id, publication_id: *publication_id },
        StoreEffect::Cleared { epoch_old, erase_id } => Action::Cleared { epoch_old: *epoch_old, erase_id: *erase_id },
        StoreEffect::ModelsCleared { generation_new, erase_id } => Action::ModelsCleared { generation_new: *generation_new, erase_id: *erase_id },
        StoreEffect::ClearFailed { erase_id, error } => Action::ClearFailed { erase_id: *erase_id, closed: error == "closed" },
        StoreEffect::Saved { publication_id } => Action::Saved(*publication_id),
        StoreEffect::SaveFailed { publication_id, error } => Action::SaveFailed { publication_id: *publication_id, closed: error == "closed" },
        StoreEffect::BroadcastCommitted(notice) => Action::BroadcastCommitted { revision: notice.revision, all: notice.models == Models::All, tables: notice.tables.iter().cloned().collect(), models: match &notice.models { Models::All => BTreeSet::new(), Models::Set(models) => models.clone() }, rows: notice.rows.clone() },
        StoreEffect::ReadIntents { epoch, direct } => Action::ReadIntents { epoch: *epoch, direct: *direct },
        StoreEffect::CloseReads => Action::CloseReads,
        StoreEffect::CloseConnections => Action::CloseConnections,
        StoreEffect::StoreClosed => Action::StoreClosed,
        StoreEffect::Prepare { .. } | StoreEffect::OpenLifecycle(_) | StoreEffect::OpenFailed(_) | StoreEffect::OpenCorrupt(_) | StoreEffect::ArmExpiry { .. } | StoreEffect::DisarmExpiry { .. } | StoreEffect::OperationsGone { .. } | StoreEffect::ArmReset { .. } | StoreEffect::Observed { .. } => return None,
    })
}

struct Twin {
    det: DetScheduler,
    addr: Address<StoreWriter>,
    log: Log<StoreEffect>,
    reference: Ref,
    history: Vec<StoreEffect>,
    /// (epoch of the written batch, epoch of the Store) at each `Write`.
    written: Vec<(u64, u64)>,
    /// A `Write` of a batch whose basis is older than the generation of a model, at the time of the write.
    stale_writes: u64,
    /// `model_generation[Moment]` after each request.
    generations: Vec<u64>,
    /// Each request with the actions the reference gave.
    steps: Vec<(Request, Vec<Action>)>,
    batch: u64,
    /// The rows of each batch handed to the writer: the answer of SQLite names them.
    committing: std::collections::BTreeMap<u64, BTreeSet<(String, String)>>,
    publication: u64,
    erase: u64,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(9);
        let log = new_log();
        let addr = det.add::<StoreWriter>("store", StoreState::new(), Recorder::<StoreWriter>::new(&log));
        Self { det, addr, log, reference: Ref::new(), history: Vec::new(), written: Vec::new(), stale_writes: 0, generations: Vec::new(), steps: Vec::new(), batch: 0, committing: Default::default(), publication: 0, erase: 0 }
    }

    /// An open store in the epoch 1 (`Published` saved).
    fn live() -> Self {
        let mut twin = Self::new();
        twin.publish(1, 0);
        twin
    }

    /// `Published`, then the answer of SQLite that saves it.
    fn publish(&mut self, epoch: u64, generation: u64) {
        self.publication += 1;
        let publication_id = self.publication;
        self.feed(Request::Published { publication_id, epoch, generation, active: true });
        if let Some(io_id) = self.head_io() { self.feed(Request::Erased { io_id, result: Ok(self.reference.revision) }); }
    }

    fn head_io(&self) -> Option<u64> {
        self.history.iter().rev().find_map(|effect| match effect {
            StoreEffect::SavePublished { io_id, .. } | StoreEffect::EraseAccount { io_id, .. } | StoreEffect::EraseModels { io_id, .. } | StoreEffect::ClearSecret { io_id } => Some(*io_id),
            _ => None,
        })
    }

    fn send(&mut self, input: StoreInput) -> Vec<StoreEffect> {
        self.det.post(self.addr, EpochStamp::Control, input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let raw = take(&self.log);
        let epoch = self.state().epoch();
        self.written.extend(raw.iter().filter_map(|effect| match effect {
            StoreEffect::Write { batch, .. } => Some((batch.epoch, epoch)),
            _ => None,
        }));
        let stale = raw.iter().filter(|effect| matches!(effect, StoreEffect::Write { batch, .. } if batch.basis.iter().any(|(model, basis)| *basis < self.state().generation(model)))).count() as u64;
        self.stale_writes += stale;
        self.history.extend(raw.iter().cloned());
        raw
    }

    /// What SQLite reports for the batch `id`: the tables and rows the batch wrote.
    fn notice_of(&self, id: u64, revision: u64) -> crate::owners::contract::CommittedBroadcast {
        let rows = self.committing.get(&id).cloned().unwrap_or_default();
        let models: BTreeSet<String> = rows.iter().map(|(model, _)| model.clone()).collect();
        crate::owners::contract::CommittedBroadcast { revision, tables: models.iter().map(|model| format!("row_{model}")).collect(), rows: Some(rows.into_iter().map(|(model, row)| (format!("row_{model}"), row)).collect()), models: Models::Set(models), position: 0 }
    }

    fn state(&self) -> &StoreState { self.det.state(self.addr).expect("the owner state") }

    fn input_of(&self, request: &Request) -> StoreInput {
        match request.clone() {
            Request::Commit { id, batch } => StoreInput::Commit(StoreBatch { author: BatchAuthor::Outbox, id, epoch: batch.epoch, basis: batch.basis, rows: batch.rows, intents: batch.intents, credential: batch.credential, ops: Vec::new() }),
            Request::ClearAccount { epoch_old, cause: c, generation_new, erase_id } => StoreInput::ClearAccount { epoch_old, cause: cause(c), generation_new, erase_id },
            Request::ClearModels { epoch, generation_new, models, erase_id } => StoreInput::ClearModels { epoch, generation_new, models, erase_id },
            Request::Published { publication_id, epoch, generation, active } => StoreInput::Published { publication_id, published: Published { epoch, identity: None, generation, state: if active { PublishedState::Session } else { PublishedState::Ending }, closed: false } },
            Request::EpochEnded { epoch } => StoreInput::EpochEnded(EpochEnded { epoch, cause: Cause::SessionReset }),
            Request::ReadIntents { epoch } => StoreInput::ReadIntents { epoch },
            Request::Close => StoreInput::Close,
            Request::Written { id, result } => StoreInput::Written { author: BatchAuthor::Outbox, batch: id, result: result.map(|revision| WriteDone { revision, notice: self.notice_of(id, revision), ..WriteDone::default() }).map_err(|()| crate::owners::contract::RejectReason::Storage(DISK.into())) },
            Request::Erased { io_id, result } => StoreInput::Erased { io_id, result: result.map(|revision| Done { revision }).map_err(|()| DISK.into()) },
            Request::ReadsClosed(ids) => StoreInput::ReadsClosed { ids },
            Request::ReadFinished(read_id) => StoreInput::ReadFinished { read_id },
        }
    }

    /// Feeds one request of the table to both sides and compares the actions, `revision`, `epoch`, the mode and the generations.
    fn feed(&mut self, request: Request) -> Vec<Action> {
        if let Request::Commit { id, batch } = &request { self.committing.insert(*id, batch.rows.iter().cloned().collect()); }
        let raw = self.send(self.input_of(&request));
        let got: Vec<Action> = raw.iter().filter_map(action_of).collect();
        let want = self.reference.step(request.clone());
        assert_eq!(got, want, "the same actions for {request:?}");
        self.steps.push((request, want.clone()));
        let generation = self.state().generation("Moment");
        self.generations.push(generation);
        let state = self.state();
        assert_eq!(state.revision(), self.reference.revision, "the same revision");
        assert_eq!(state.epoch(), self.reference.epoch, "the same epoch");
        assert_eq!(state.mode() == StoreMode::Closed, self.reference.state == Open::Closed, "the same mode");
        assert_eq!(state.closing(), self.reference.closing, "the same closing");
        for model in ["Moment", "Other"] {
            assert_eq!(state.generation(model), self.reference.generation(model), "the same generation of {model}");
        }
        want
    }

    /// `Commit` with the answer of SQLite at once.
    fn commit(&mut self, epoch: u64, rows: &[&str]) -> Vec<Action> {
        let mut batch = Batch::rows(epoch, rows.iter().map(|id| ("Moment".to_string(), id.to_string())).collect());
        batch.basis.insert("Moment".into(), 0);
        self.commit_batch(batch, true)
    }

    fn commit_batch(&mut self, batch: Batch, sqlite_ok: bool) -> Vec<Action> {
        self.batch += 1;
        let id = self.batch;
        let mut actions = self.feed(Request::Commit { id, batch });
        if actions.iter().any(|action| matches!(action, Action::Write(_))) {
            let revision = self.reference.revision + 1;
            actions.extend(self.feed(Request::Written { id, result: if sqlite_ok { Ok(revision) } else { Err(()) } }));
        }
        actions
    }

    /// `ClearAccount`, then the answer of SQLite.
    fn clear_account(&mut self, cause: RefCause, generation_new: u64, sqlite_ok: bool) -> Vec<Action> {
        self.erase += 1;
        let erase_id = self.erase;
        let mut actions = self.feed(Request::ClearAccount { epoch_old: 1, cause, generation_new, erase_id });
        while let Some(io_id) = actions.iter().rev().find_map(|action| match action {
            Action::EraseAccount { io_id, .. } | Action::ClearSecret { io_id } => Some(*io_id),
            _ => None,
        }) {
            let done = self.feed(Request::Erased { io_id, result: if sqlite_ok { Ok(self.reference.revision + 1) } else { Err(()) } });
            let next = done.iter().any(|action| matches!(action, Action::ClearSecret { .. }));
            actions.extend(done);
            if !next { break; }
            actions.retain(|action| !matches!(action, Action::EraseAccount { .. }));
        }
        actions
    }

    fn clear_models(&mut self, generation_new: u64, models: &[&str], sqlite_ok: bool) -> Vec<Action> {
        self.erase += 1;
        let erase_id = self.erase;
        let mut actions = self.feed(Request::ClearModels { epoch: 1, generation_new, models: names(models), erase_id });
        if let Some(io_id) = actions.iter().find_map(|action| match action { Action::EraseModels { io_id, .. } => Some(*io_id), _ => None }) {
            actions.extend(self.feed(Request::Erased { io_id, result: if sqlite_ok { Ok(self.reference.revision + 1) } else { Err(()) } }));
        }
        actions
    }

    fn boot(&mut self, found: Boot, saved: (u64, u64, Option<u64>)) -> Option<StoreEffect> {
        self.reference.saved_epoch = saved.0;
        self.reference.saved_generation = saved.1;
        self.reference.credential = saved.2;
        let want = self.reference.boot(found.clone());
        let input = match found {
            Boot::New => BootFound::Fresh,
            Boot::Same => BootFound::Same { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2, saved_unowned_pending: self.reference.pending, revision: self.reference.saved_revision },
            Boot::ChangedModels => BootFound::ChangedModels { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2, saved_unowned_pending: self.reference.pending, revision: self.reference.saved_revision },
            Boot::LocalModelChanged => BootFound::LocalModelChanged { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2, saved_unowned_pending: self.reference.pending, revision: self.reference.saved_revision },
            Boot::CorruptManifest => BootFound::CorruptManifest(DISK.into()),
            Boot::FormatChange => BootFound::FormatChange { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2, saved_unowned_pending: self.reference.pending, revision: self.reference.saved_revision },
            Boot::SqliteError => BootFound::SqliteError(DISK.into()),
        };
        let raw = self.send(StoreInput::Boot(input));
        let opened = raw.iter().find(|effect| matches!(effect, StoreEffect::OpenLifecycle(_) | StoreEffect::OpenFailed(_) | StoreEffect::OpenCorrupt(_))).cloned();
        match (&opened, want) {
            (Some(StoreEffect::OpenLifecycle(message)), Some(want)) => {
                assert_eq!((message.saved_epoch, message.saved_generation, message.saved_identity), (want.saved_epoch, want.saved_generation, want.saved_identity));
            }
            (Some(StoreEffect::OpenFailed(_) | StoreEffect::OpenCorrupt(_)), None) => {}
            (got, want) => panic!("the boot rows differ: {got:?} against {want:?}"),
        }
        assert_eq!(self.state().revision(), self.reference.revision);
        for model in ["Moment", "Other"] {
            assert_eq!(self.state().generation(model), self.reference.generation(model));
        }
        opened
    }

    fn committed(actions: &[Action]) -> Option<u64> {
        actions.iter().find_map(|action| match action { Action::Committed { revision, .. } => Some(*revision), _ => None })
    }

    fn rejected(actions: &[Action]) -> Option<Reason> {
        actions.iter().find_map(|action| match action { Action::Rejected { reason, .. } => Some(reason.clone()), _ => None })
    }
}

fn names(items: &[&str]) -> BTreeSet<String> { items.iter().map(|item| item.to_string()).collect() }

#[cfg(test)]
mod store_owner_cells_tests;
#[cfg(test)]
mod store_owner_trace_tests;
