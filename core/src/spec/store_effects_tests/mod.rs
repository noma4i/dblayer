//! Owner-level tests of the `store` machine: the owner `Store` on `DetScheduler`. SQLite is the effect `Write` or `Erase*`,
//! the test answers it with `Written` or `Erased`, then reads the effects (`Action`) and the state of the owner.

use std::collections::{BTreeMap, BTreeSet};

use crate::owners::contract::{BatchAuthor, EpochEnded, Models, Published, PublishedState, RejectReason};
use crate::owners::store::{BootFound, Done, StoreBatch, StoreEffect, StoreInput, StoreMode, StoreReply, StoreState, StoreWriter, WriteDone};
use crate::spec::support::{new_log, take, Log, Recorder};
use crate::runtime::{Address, Cause, DetScheduler, EpochStamp};

const DISK: &str = "disk";

#[derive(Clone, Debug, PartialEq)]
struct Batch {
    epoch: u64,
    rows: Vec<(String, String)>,
    intents: u64,
}

impl Batch {
    fn rows(epoch: u64, rows: Vec<(String, String)>) -> Self { Self { epoch, rows, intents: 0 } }
}

#[derive(Clone, Debug, PartialEq)]
enum Request {
    Commit { id: u64, batch: Batch },
    ClearAccount { epoch_old: u64, cause: Cause, generation_new: u64, erase_id: u64 },
    /// `active`: the state of the publication is NoSession or Session.
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
enum Reason {
    NoSession,
    Storage,
    Closed,
    Invalid,
}

/// What the Store does in a step, in the order of its effects.
#[derive(Clone, Debug, PartialEq)]
enum Action {
    Committed { id: u64, revision: u64, rows: Option<BTreeSet<(String, String)>> },
    Rejected { id: u64, reason: Reason },
    Write(u64),
    EraseAccount { io_id: u64, generation_new: u64, erase_encrypted: bool },
    ClearSecret { io_id: u64 },
    SavePublished { io_id: u64, publication_id: u64 },
    Cleared { epoch_old: u64, erase_id: u64 },
    ClearFailed { erase_id: u64, closed: bool },
    Saved(u64),
    SaveFailed { publication_id: u64, closed: bool },
    BroadcastCommitted { revision: u64, all: bool, tables: BTreeSet<String>, models: BTreeSet<String>, rows: Option<BTreeSet<(String, String)>> },
    ReadIntents { epoch: u64, direct: bool },
    CloseReads,
    CloseConnections,
    StoreClosed,
}

/// What `Boot` finds in the database.
#[derive(Clone, Debug, PartialEq)]
enum Boot {
    New,
    Same,
    ChangedModels,
    /// The fingerprint of a `storage: local` model changed, the server models match.
    LocalModelChanged,
    FormatChange,
    SqliteError,
    CorruptManifest,
}

fn action_of(effect: &StoreEffect) -> Option<Action> {
    Some(match effect {
        StoreEffect::Reply { batch, reply: StoreReply::Committed { revision, rows, .. }, .. } => Action::Committed { id: *batch, revision: *revision, rows: rows.clone() },
        StoreEffect::Reply { batch, reply: StoreReply::Rejected(reason), .. } => Action::Rejected {
            id: *batch,
            reason: match reason {
                RejectReason::NoSession => Reason::NoSession,
                RejectReason::Storage(_) => Reason::Storage,
                RejectReason::Closed => Reason::Closed,
                RejectReason::Invalid(_) => Reason::Invalid,
            },
        },
        StoreEffect::Write { batch, .. } => Action::Write(batch.id),
        StoreEffect::Pinned { .. } => return None,
        StoreEffect::EraseAccount { io_id, generation_new, erase_encrypted } => Action::EraseAccount { io_id: *io_id, generation_new: *generation_new, erase_encrypted: *erase_encrypted },
        StoreEffect::ClearSecret { io_id } => Action::ClearSecret { io_id: *io_id },
        StoreEffect::SavePublished { io_id, publication_id, .. } => Action::SavePublished { io_id: *io_id, publication_id: *publication_id },
        StoreEffect::Cleared { epoch_old, erase_id } => Action::Cleared { epoch_old: *epoch_old, erase_id: *erase_id },
        StoreEffect::ClearFailed { erase_id, error } => Action::ClearFailed { erase_id: *erase_id, closed: error == "closed" },
        StoreEffect::Saved { publication_id } => Action::Saved(*publication_id),
        StoreEffect::SaveFailed { publication_id, error } => Action::SaveFailed { publication_id: *publication_id, closed: error == "closed" },
        StoreEffect::BroadcastCommitted(notice) => Action::BroadcastCommitted { revision: notice.revision, all: notice.models == Models::All, tables: notice.tables.iter().cloned().collect(), models: match &notice.models { Models::All => BTreeSet::new(), Models::Set(models) => models.clone() }, rows: notice.rows.clone() },
        StoreEffect::ReadIntents { epoch, direct } => Action::ReadIntents { epoch: *epoch, direct: *direct },
        StoreEffect::CloseReads => Action::CloseReads,
        StoreEffect::CloseConnections => Action::CloseConnections,
        StoreEffect::StoreClosed => Action::StoreClosed,
        StoreEffect::Prepare { .. } | StoreEffect::OpenLifecycle(_) | StoreEffect::OpenFailed(_) | StoreEffect::OpenCorrupt(_) | StoreEffect::RowsRemoved { .. } | StoreEffect::AliasFacts { .. } | StoreEffect::ArmExpiry { .. } | StoreEffect::DisarmExpiry { .. } | StoreEffect::OperationsGone { .. } | StoreEffect::ArmReset { .. } | StoreEffect::Observed { .. } => return None,
    })
}

struct Twin {
    det: DetScheduler,
    addr: Address<StoreWriter>,
    log: Log<StoreEffect>,
    history: Vec<StoreEffect>,
    /// (epoch of the written batch, epoch of the Store) at each `Write`.
    written: Vec<(u64, u64)>,
    /// The generation after each request.
    generation_history: Vec<u64>,
    /// Each request with the actions the owner gave.
    steps: Vec<(Request, Vec<Action>)>,
    batch: u64,
    /// The rows of each batch handed to the writer: the answer of SQLite names them.
    committing: BTreeMap<u64, BTreeSet<(String, String)>>,
    publication: u64,
    erase: u64,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(9);
        let log = new_log();
        let addr = det.add::<StoreWriter>("store", StoreState::new(), Recorder::<StoreWriter>::new(&log));
        Self { det, addr, log, history: Vec::new(), written: Vec::new(), generation_history: Vec::new(), steps: Vec::new(), batch: 0, committing: Default::default(), publication: 0, erase: 0 }
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
        if let Some(io_id) = self.head_io() { self.feed(Request::Erased { io_id, result: Ok(self.state().revision()) }); }
    }

    fn head_io(&self) -> Option<u64> {
        self.history.iter().rev().find_map(|effect| match effect {
            StoreEffect::SavePublished { io_id, .. } | StoreEffect::EraseAccount { io_id, .. } | StoreEffect::ClearSecret { io_id } => Some(*io_id),
            _ => None,
        })
    }

    fn send(&mut self, input: StoreInput) -> Vec<StoreEffect> {
        self.det.post(self.addr, EpochStamp::Control, input);
        self.det.run_until_quiet(100).expect("the owner is quiet");
        let raw = take(&self.log);
        let epoch = self.state().epoch();
        self.written.extend(raw.iter().filter_map(|effect| match effect {
            StoreEffect::Write { batch, .. } => Some((match batch.stamp { EpochStamp::Epoch(stamped) => stamped, EpochStamp::Control => epoch }, epoch)),
            _ => None,
        }));
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
            Request::Commit { id, batch } => StoreInput::Commit(StoreBatch { author: BatchAuthor::Outbox, id, stamp: EpochStamp::Epoch(batch.epoch), rows: batch.rows, intents: batch.intents, credential: None, ops: Vec::new() }),
            Request::ClearAccount { epoch_old, cause, generation_new, erase_id } => StoreInput::ClearAccount { epoch_old, cause, generation_new, erase_id },
            Request::Published { publication_id, epoch, generation, active } => StoreInput::Published { publication_id, published: Published { epoch, identity: None, generation, state: if active { PublishedState::Session } else { PublishedState::Ending }, closed: false } },
            Request::EpochEnded { epoch } => StoreInput::EpochEnded(EpochEnded { epoch, cause: Cause::SessionReset }),
            Request::ReadIntents { epoch } => StoreInput::ReadIntents { epoch },
            Request::Close => StoreInput::Close,
            Request::Written { id, result } => StoreInput::Written { author: BatchAuthor::Outbox, batch: id, result: result.map(|revision| WriteDone { revision, notice: self.notice_of(id, revision), ..WriteDone::default() }).map_err(|()| RejectReason::Storage(DISK.into())) },
            Request::Erased { io_id, result } => StoreInput::Erased { io_id, result: result.map(|revision| Done { revision }).map_err(|()| DISK.into()) },
            Request::ReadsClosed(ids) => StoreInput::ReadsClosed { ids },
            Request::ReadFinished(read_id) => StoreInput::ReadFinished { read_id },
        }
    }

    /// Feeds one request to the owner, returns the actions of its effects.
    fn feed(&mut self, request: Request) -> Vec<Action> {
        if let Request::Commit { id, batch } = &request { self.committing.insert(*id, batch.rows.iter().cloned().collect()); }
        let raw = self.send(self.input_of(&request));
        let actions: Vec<Action> = raw.iter().filter_map(action_of).collect();
        self.steps.push((request, actions.clone()));
        let generation = self.state().generation();
        self.generation_history.push(generation);
        actions
    }

    /// `Commit` with the answer of SQLite at once.
    fn commit(&mut self, epoch: u64, rows: &[&str]) -> Vec<Action> {
        self.commit_batch(Batch::rows(epoch, rows.iter().map(|id| ("Moment".to_string(), id.to_string())).collect()), true)
    }

    fn commit_batch(&mut self, batch: Batch, sqlite_ok: bool) -> Vec<Action> {
        self.batch += 1;
        let id = self.batch;
        let mut actions = self.feed(Request::Commit { id, batch });
        if actions.iter().any(|action| matches!(action, Action::Write(_))) {
            let revision = self.state().revision() + 1;
            actions.extend(self.feed(Request::Written { id, result: if sqlite_ok { Ok(revision) } else { Err(()) } }));
        }
        actions
    }

    /// `ClearAccount`, then the answer of SQLite.
    fn clear_account(&mut self, cause: Cause, generation_new: u64, sqlite_ok: bool) -> Vec<Action> {
        self.erase += 1;
        let erase_id = self.erase;
        let mut actions = self.feed(Request::ClearAccount { epoch_old: 1, cause, generation_new, erase_id });
        while let Some(io_id) = actions.iter().rev().find_map(|action| match action {
            Action::EraseAccount { io_id, .. } | Action::ClearSecret { io_id } => Some(*io_id),
            _ => None,
        }) {
            let done = self.feed(Request::Erased { io_id, result: if sqlite_ok { Ok(self.state().revision() + 1) } else { Err(()) } });
            let next = done.iter().any(|action| matches!(action, Action::ClearSecret { .. }));
            actions.extend(done);
            if !next { break; }
            actions.retain(|action| !matches!(action, Action::EraseAccount { .. }));
        }
        actions
    }

    /// `Boot` finds `found` in the database that saved `saved` (epoch, generation, identity), `pending` unowned intents and `revision`.
    fn boot(&mut self, found: Boot, saved: (u64, u64, Option<u64>), pending: bool, revision: u64) -> Option<StoreEffect> {
        let (saved_epoch, saved_generation, saved_identity) = saved;
        let input = match found {
            Boot::New => BootFound::Fresh,
            Boot::Same => BootFound::Same { saved_epoch, saved_generation, saved_identity, saved_unowned_pending: pending, revision },
            Boot::ChangedModels => BootFound::ChangedModels { saved_epoch, saved_generation, saved_identity, saved_unowned_pending: pending, revision },
            Boot::LocalModelChanged => BootFound::LocalModelChanged { saved_epoch, saved_generation, saved_identity, saved_unowned_pending: pending, revision },
            Boot::CorruptManifest => BootFound::CorruptManifest(DISK.into()),
            Boot::FormatChange => BootFound::FormatChange { saved_epoch, saved_generation, saved_identity, saved_unowned_pending: pending, revision },
            Boot::SqliteError => BootFound::SqliteError(DISK.into()),
        };
        let raw = self.send(StoreInput::Boot(input));
        raw.iter().find(|effect| matches!(effect, StoreEffect::OpenLifecycle(_) | StoreEffect::OpenFailed(_) | StoreEffect::OpenCorrupt(_))).cloned()
    }

    fn committed(actions: &[Action]) -> Option<u64> {
        actions.iter().find_map(|action| match action { Action::Committed { revision, .. } => Some(*revision), _ => None })
    }

    fn rejected(actions: &[Action]) -> Option<Reason> {
        actions.iter().find_map(|action| match action { Action::Rejected { reason, .. } => Some(reason.clone()), _ => None })
    }

    /// A one-row batch of `Moment` for the epoch 1.
    fn moment_batch() -> Batch { Batch::rows(1, vec![("Moment".into(), "a".into())]) }
}

mod store_boot_tests;
mod store_commit_tests;
mod store_clear_tests;
mod store_close_tests;
mod store_trace_tests;
