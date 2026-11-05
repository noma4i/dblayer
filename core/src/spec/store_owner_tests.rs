//! Owner-level tests of the `store` machine: the owner `Store` on `DetScheduler` against `ref_store.rs`. SQLite is the effect `Write` or `Erase*`,
//! the test answers it with `Written` or `Erased`. Names are the `Test` column.

use std::collections::{BTreeMap, BTreeSet};

use super::ref_lifecycle::Cause as RefCause;
use super::ref_store::{Action, Batch, Boot, Open, Reason, Request, Store as Ref};
use crate::owners::contract::{BatchAuthor, EpochEnded, Published, PublishedState, RejectReason};
use crate::owners::store::{BootFound, Done, StoreBatch, StoreEffect, StoreInput, StoreMode, StoreReply, StoreState, StoreWriter};
use crate::owners::support::{new_log, take, Log, Recorder};
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
        StoreEffect::Reply { batch, reply: StoreReply::Committed { revision }, .. } => Action::Committed { id: *batch, revision: *revision },
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
        StoreEffect::Write { batch } => Action::Write(batch.id),
        StoreEffect::EraseAccount { io_id, generation_new, erase_encrypted } => Action::EraseAccount { io_id: *io_id, generation_new: *generation_new, erase_encrypted: *erase_encrypted },
        StoreEffect::EraseModels { io_id, models, .. } => Action::EraseModels { io_id: *io_id, models: models.clone() },
        StoreEffect::ClearSecret { io_id } => Action::ClearSecret { io_id: *io_id },
        StoreEffect::SavePublished { io_id, publication_id, .. } => Action::SavePublished { io_id: *io_id, publication_id: *publication_id },
        StoreEffect::Cleared { epoch_old, erase_id } => Action::Cleared { epoch_old: *epoch_old, erase_id: *erase_id },
        StoreEffect::ModelsCleared { generation_new, erase_id } => Action::ModelsCleared { generation_new: *generation_new, erase_id: *erase_id },
        StoreEffect::ClearFailed { erase_id, error } => Action::ClearFailed { erase_id: *erase_id, closed: error == "closed" },
        StoreEffect::Saved { publication_id } => Action::Saved(*publication_id),
        StoreEffect::SaveFailed { publication_id, error } => Action::SaveFailed { publication_id: *publication_id, closed: error == "closed" },
        StoreEffect::BroadcastCommitted { revision, models } => Action::BroadcastCommitted { revision: *revision, all: models.is_none() },
        StoreEffect::ReadIntents { epoch } => Action::ReadIntents(*epoch),
        StoreEffect::CloseReads => Action::CloseReads,
        StoreEffect::CloseConnections => Action::CloseConnections,
        StoreEffect::StoreClosed => Action::StoreClosed,
        StoreEffect::Prepare { .. } | StoreEffect::OpenLifecycle(_) | StoreEffect::OpenFailed(_) | StoreEffect::OpenCorrupt(_) => return None,
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
    publication: u64,
    erase: u64,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(9);
        let log = new_log();
        let addr = det.add::<StoreWriter>("store", StoreState::new(), Recorder::<StoreWriter>::new(&log));
        Self { det, addr, log, reference: Ref::new(), history: Vec::new(), written: Vec::new(), stale_writes: 0, generations: Vec::new(), steps: Vec::new(), batch: 0, publication: 0, erase: 0 }
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
            StoreEffect::Write { batch } => Some((batch.epoch, epoch)),
            _ => None,
        }));
        let stale = raw.iter().filter(|effect| matches!(effect, StoreEffect::Write { batch } if batch.basis.iter().any(|(model, basis)| *basis < self.state().generation(model)))).count() as u64;
        self.stale_writes += stale;
        self.history.extend(raw.iter().cloned());
        raw
    }

    fn state(&self) -> &StoreState { self.det.state(self.addr).expect("the owner state") }

    fn input_of(request: &Request) -> StoreInput {
        match request.clone() {
            Request::Commit { id, batch } => StoreInput::Commit(StoreBatch { author: BatchAuthor::Outbox, id, epoch: batch.epoch, basis: batch.basis, rows: batch.rows, intents: batch.intents, credential: batch.credential, ops: Vec::new() }),
            Request::ClearAccount { epoch_old, cause: c, generation_new, erase_id } => StoreInput::ClearAccount { epoch_old, cause: cause(c), generation_new, erase_id },
            Request::ClearModels { epoch, generation_new, models, erase_id } => StoreInput::ClearModels { epoch, generation_new, models, erase_id },
            Request::Published { publication_id, epoch, generation, active } => StoreInput::Published { publication_id, published: Published { epoch, identity: None, generation, state: if active { PublishedState::Session } else { PublishedState::Ending }, closed: false } },
            Request::EpochEnded { epoch } => StoreInput::EpochEnded(EpochEnded { epoch, cause: Cause::SessionReset }),
            Request::ReadIntents { epoch } => StoreInput::ReadIntents { epoch },
            Request::Close => StoreInput::Close,
            Request::Written { id, result } => StoreInput::Written { author: BatchAuthor::Outbox, batch: id, result: result.map(|revision| Done { revision }).map_err(|()| DISK.into()) },
            Request::Erased { io_id, result } => StoreInput::Erased { io_id, result: result.map(|revision| Done { revision }).map_err(|()| DISK.into()) },
            Request::ReadsClosed(ids) => StoreInput::ReadsClosed { ids },
            Request::ReadFinished(read_id) => StoreInput::ReadFinished { read_id },
        }
    }

    /// Feeds one request of the table to both sides and compares the actions, `revision`, `epoch`, the mode and the generations.
    fn feed(&mut self, request: Request) -> Vec<Action> {
        let raw = self.send(Self::input_of(&request));
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
            Boot::Same => BootFound::Same { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2 },
            Boot::ChangedModels => BootFound::ChangedModels { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2 },
            Boot::LocalModelChanged => BootFound::LocalModelChanged { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2 },
            Boot::CorruptManifest => BootFound::CorruptManifest(DISK.into()),
            Boot::FormatChange => BootFound::FormatChange { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2 },
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

#[test]
fn store_boot_new() {
    let mut twin = Twin::new();
    let opened = twin.boot(Boot::New, (0, 0, None));
    assert!(matches!(opened, Some(StoreEffect::OpenLifecycle(_))));
}

#[test]
fn store_boot_same() {
    let mut twin = Twin::new();
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::Same, (4, 2, Some(7))) else { panic!("the boot opens Lifecycle") };
    assert_eq!((message.saved_epoch, message.saved_generation, message.saved_identity), (4, 2, Some(7)));
}

#[test]
fn store_boot_changed_models() {
    let mut twin = Twin::new();
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::ChangedModels, (4, 2, Some(7))) else { panic!("the boot opens Lifecycle") };
    assert_eq!(message.saved_generation, 3, "the cache is erased in the Boot transaction: the generation grows");
    assert_eq!(twin.state().generation("Moment"), 3);
    assert_eq!(twin.state().revision(), 1);
}

#[test]
fn store_boot_format_change() {
    let mut twin = Twin::new();
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::FormatChange, (4, 2, None)) else { panic!("the boot opens Lifecycle") };
    assert_eq!(message.saved_generation, 3);
    assert_eq!(twin.state().revision(), 1);
}

#[test]
fn store_boot_corrupt_manifest() {
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::CorruptManifest, (0, 0, None)), Some(StoreEffect::OpenCorrupt(_))));
    assert_eq!(twin.state().mode(), StoreMode::Closed);
    assert_eq!(twin.state().revision(), 0, "the database is not changed");
}

#[test]
fn store_boot_local_model_changed() {
    let mut twin = Twin::new();
    let Some(StoreEffect::OpenLifecycle(message)) = twin.boot(Boot::LocalModelChanged, (4, 2, Some(7))) else { panic!("the boot opens Lifecycle") };
    assert_eq!((message.saved_generation, twin.state().revision()), (2, 0), "the server cache and the generation stay");
}

#[test]
fn store_boot_error() {
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::SqliteError, (0, 0, None)), Some(StoreEffect::OpenFailed(_))));
    assert_eq!(twin.state().mode(), StoreMode::Closed);
}

#[test]
fn store_commit_while_writing() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    let first = twin.feed(Request::Commit { id: 1, batch: batch.clone() });
    assert_eq!(first, vec![Action::Write(1)]);
    let second = twin.feed(Request::Commit { id: 2, batch });
    assert!(second.is_empty(), "the second batch waits for the first");
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.contains(&Action::Write(2)), "the deferred batch starts after Written");
    twin.feed(Request::Written { id: 2, result: Ok(2) });
}

#[test]
fn store_commit_stale_epoch() { assert_eq!(Twin::live().commit(7, &["a"]), vec![Action::Rejected { id: 1, reason: Reason::NoSession }]); }

#[test]
fn store_commit_no_basis() {
    let mut twin = Twin::live();
    let batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    let actions = twin.commit_batch(batch, true);
    assert_eq!(Twin::rejected(&actions), Some(Reason::Invalid));
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_commit_stale_generation() {
    let mut twin = Twin::live();
    twin.clear_models(3, &["Moment"], true);
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 2);
    let actions = twin.commit_batch(batch, true);
    assert_eq!(Twin::rejected(&actions), Some(Reason::StaleGeneration(vec!["Moment".into()])));
    assert_eq!(twin.stale_writes, 0, "a stale batch never reaches SQLite");
}

#[test]
fn store_commit_write() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.batch += 1;
    assert_eq!(twin.feed(Request::Commit { id: 1, batch }), vec![Action::Write(1)]);
}

#[test]
fn store_commit_ok() {
    let mut twin = Twin::live();
    let actions = twin.commit(1, &["a"]);
    assert_eq!(Twin::committed(&actions), Some(1));
    assert!(actions.contains(&Action::BroadcastCommitted { revision: 1, all: false }));
    assert_eq!(twin.commit(1, &["b"]).len(), 3);
    assert_eq!(twin.state().revision(), 2);
}

#[test]
fn store_commit_sqlite_error() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    let actions = twin.commit_batch(batch, false);
    assert_eq!(Twin::rejected(&actions), Some(Reason::Storage));
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_written_stray() {
    let mut twin = Twin::live();
    assert!(twin.feed(Request::Written { id: 77, result: Ok(5) }).is_empty());
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_clear_while_writing() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.feed(Request::Commit { id: 1, batch });
    assert!(twin.feed(Request::ClearAccount { epoch_old: 1, cause: RefCause::IdentityChanged, generation_new: 1, erase_id: 1 }).is_empty());
    assert!(twin.feed(Request::ClearModels { epoch: 1, generation_new: 1, models: names(&["Moment"]), erase_id: 2 }).is_empty());
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.iter().any(|action| matches!(action, Action::EraseAccount { .. })), "the account erase starts after Written");
}

#[test]
fn store_clear_account_stale() {
    let mut twin = Twin::live();
    twin.clear_account(RefCause::IdentityChanged, 4, true);
    let actions = twin.clear_account(RefCause::IdentityChanged, 4, true);
    assert_eq!(actions, vec![Action::ClearFailed { erase_id: 2, closed: false }]);
}

#[test]
fn store_clear_account_erase() {
    let mut twin = Twin::live();
    let actions = twin.feed(Request::ClearAccount { epoch_old: 1, cause: RefCause::SessionReset, generation_new: 1, erase_id: 1 });
    assert_eq!(actions, vec![Action::EraseAccount { io_id: 2, generation_new: 1, erase_encrypted: true }]);
}

#[test]
fn store_clear_account() {
    let mut twin = Twin::live();
    let actions = twin.clear_account(RefCause::IdentityChanged, 4, true);
    assert!(actions.contains(&Action::Cleared { epoch_old: 1, erase_id: 1 }));
    assert!(actions.contains(&Action::BroadcastCommitted { revision: 1, all: true }));
    assert_eq!(twin.state().generation("Moment"), 4);
}

#[test]
fn store_clear_account_then_secret() {
    let mut twin = Twin::live();
    let actions = twin.clear_account(RefCause::SessionReset, 4, true);
    let secret = actions.iter().position(|action| matches!(action, Action::ClearSecret { .. })).expect("the secret is cleared after SQLite");
    let cleared = actions.iter().position(|action| matches!(action, Action::Cleared { .. })).expect("Cleared follows");
    assert!(secret < cleared, "Cleared waits for the secret");
}

#[test]
fn store_clear_secret_ok() { Twin::live().clear_account(RefCause::ServerEnded, 2, true); }

#[test]
fn store_clear_secret_error() {
    let mut twin = Twin::live();
    let first = twin.feed(Request::ClearAccount { epoch_old: 1, cause: RefCause::SessionReset, generation_new: 1, erase_id: 1 });
    let Some(Action::EraseAccount { io_id, .. }) = first.first().cloned() else { panic!("erase starts") };
    twin.feed(Request::Erased { io_id, result: Ok(1) });
    let done = twin.feed(Request::Erased { io_id, result: Err(()) });
    assert!(done.iter().any(|action| matches!(action, Action::Cleared { .. })), "the orphan secret does not hold Cleared back");
}

#[test]
fn store_clear_account_error() {
    let mut twin = Twin::live();
    let actions = twin.clear_account(RefCause::SessionReset, 2, false);
    assert!(actions.contains(&Action::ClearFailed { erase_id: 1, closed: false }));
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_clear_models_stale() {
    let mut twin = Twin::live();
    twin.clear_models(2, &["Moment"], true);
    assert_eq!(twin.clear_models(2, &["Moment"], true), vec![Action::ClearFailed { erase_id: 2, closed: false }]);
}

#[test]
fn store_clear_models_erase() {
    let mut twin = Twin::live();
    let actions = twin.feed(Request::ClearModels { epoch: 1, generation_new: 1, models: names(&["Moment"]), erase_id: 1 });
    assert!(matches!(actions.as_slice(), [Action::EraseModels { .. }]));
}

#[test]
fn store_clear_models() {
    let mut twin = Twin::live();
    twin.commit(1, &["a"]);
    let actions = twin.clear_models(2, &["Moment"], true);
    assert!(actions.contains(&Action::ModelsCleared { generation_new: 2, erase_id: 1 }));
    assert_eq!(twin.state().generation("Moment"), 2);
    assert_eq!(twin.state().generation("Other"), 0, "other models keep their generation");
}

#[test]
fn store_clear_models_error() {
    let mut twin = Twin::live();
    let actions = twin.clear_models(2, &["Moment"], false);
    assert!(actions.contains(&Action::ClearFailed { erase_id: 1, closed: false }));
    assert_eq!(twin.state().generation("Moment"), 0);
}

#[test]
fn store_erased_stray() {
    let mut twin = Twin::live();
    assert!(twin.feed(Request::Erased { io_id: 99, result: Ok(5) }).is_empty());
}

#[test]
fn store_publish_queued() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.feed(Request::Commit { id: 1, batch });
    let queued = twin.feed(Request::Published { publication_id: 9, epoch: 2, generation: 0, active: true });
    assert!(queued.is_empty(), "the save waits for the write");
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.iter().any(|action| matches!(action, Action::SavePublished { publication_id: 9, .. })), "the save starts after Written");
}

#[test]
fn store_epoch() {
    let mut twin = Twin::new();
    twin.publish(5, 3);
    assert_eq!(twin.state().epoch(), 5);
    assert!(twin.history.iter().any(|effect| matches!(effect, StoreEffect::Saved { .. })));
}

#[test]
fn store_epoch_restricting_kept() {
    let mut twin = Twin::live();
    twin.feed(Request::EpochEnded { epoch: 1 });
    twin.feed(Request::Published { publication_id: 8, epoch: 1, generation: 0, active: false });
    let saved = twin.feed(Request::Erased { io_id: 2, result: Ok(0) });
    assert_eq!(saved, vec![Action::Saved(8)]);
    assert_eq!(twin.state().epoch(), 2, "a restricting publication does not bring the ended epoch back");
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::NoSession));
}

#[test]
fn store_epoch_save_failed() {
    let mut twin = Twin::new();
    twin.feed(Request::Published { publication_id: 1, epoch: 5, generation: 3, active: true });
    let actions = twin.feed(Request::Erased { io_id: 1, result: Err(()) });
    assert_eq!(actions, vec![Action::SaveFailed { publication_id: 1, closed: false }]);
    assert_eq!(twin.state().epoch(), 0, "a failed save leaves the epoch of the Store");
}

#[test]
fn store_epoch_ended_deferred() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.feed(Request::Commit { id: 1, batch });
    twin.feed(Request::EpochEnded { epoch: 1 });
    assert_eq!(twin.state().epoch(), 1, "EpochEnded waits for Written");
    twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert_eq!(twin.state().epoch(), 2);
}

#[test]
fn store_epoch_ended() {
    let mut twin = Twin::live();
    twin.feed(Request::EpochEnded { epoch: 1 });
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::NoSession), "the commits of the ended epoch are refused");
}

#[test]
fn store_epoch_ended_late() {
    let mut twin = Twin::live();
    twin.feed(Request::EpochEnded { epoch: 1 });
    twin.feed(Request::EpochEnded { epoch: 0 });
    assert_eq!(twin.state().epoch(), 2);
}

#[test]
fn store_read_intents() {
    let mut twin = Twin::live();
    assert_eq!(twin.feed(Request::ReadIntents { epoch: 1 }), vec![Action::ReadIntents(1)]);
}

#[test]
fn store_reads_counted() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![4, 5]));
    twin.feed(Request::ReadFinished(4));
    assert_eq!(twin.state().mode(), StoreMode::Open, "a read is still open");
    let done = twin.feed(Request::ReadFinished(5));
    assert!(done.contains(&Action::StoreClosed));
}

#[test]
fn store_read_finished_stray() {
    let mut twin = Twin::live();
    assert!(twin.feed(Request::ReadFinished(3)).is_empty());
}

#[test]
fn store_close_waits() {
    let mut twin = Twin::live();
    assert_eq!(twin.feed(Request::Close), vec![Action::CloseReads]);
    assert_eq!(twin.state().mode(), StoreMode::Open);
}

#[test]
fn store_close() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    let actions = twin.feed(Request::ReadsClosed(vec![]));
    assert_eq!(actions, vec![Action::CloseConnections, Action::StoreClosed]);
    assert_eq!(twin.state().mode(), StoreMode::Closed);
}

#[test]
fn store_close_after_io() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.feed(Request::Commit { id: 1, batch });
    assert!(twin.feed(Request::Close).is_empty(), "Close waits for the write");
    let done = twin.feed(Request::Written { id: 1, result: Ok(1) });
    assert!(done.contains(&Action::CloseReads));
    let end = twin.feed(Request::ReadsClosed(vec![]));
    assert!(end.contains(&Action::StoreClosed));
}

#[test]
fn store_closing_refuses() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::Closed));
    assert_eq!(twin.feed(Request::ClearAccount { epoch_old: 1, cause: RefCause::SessionReset, generation_new: 1, erase_id: 1 }), vec![Action::ClearFailed { erase_id: 1, closed: true }]);
    assert_eq!(twin.feed(Request::Published { publication_id: 5, epoch: 2, generation: 0, active: true }), vec![Action::SaveFailed { publication_id: 5, closed: true }]);
}

#[test]
fn store_closed() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![]));
    assert_eq!(Twin::rejected(&twin.commit(1, &["a"])), Some(Reason::Closed));
}

#[test]
fn store_closed_clear() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![]));
    assert_eq!(twin.feed(Request::ClearModels { epoch: 1, generation_new: 1, models: names(&["Moment"]), erase_id: 3 }), vec![Action::ClearFailed { erase_id: 3, closed: true }]);
}

#[test]
fn store_closed_other() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![]));
    for request in [Request::Published { publication_id: 4, epoch: 3, generation: 0, active: true }, Request::EpochEnded { epoch: 1 }, Request::Written { id: 5, result: Ok(1) }, Request::Erased { io_id: 5, result: Ok(1) }, Request::ReadIntents { epoch: 1 }, Request::ReadsClosed(vec![]), Request::ReadFinished(2)] {
        assert!(twin.feed(request).is_empty());
    }
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn random_trace(seed: u64) -> Twin {
    let mut seed = seed;
    let mut twin = Twin::live();
    for step in 0..40u64 {
        match next(&mut seed) % 9 {
            0..=2 => {
                let epoch = if next(&mut seed) % 5 == 0 { 7 } else { twin.reference.epoch };
                let mut batch = Batch::rows(epoch, vec![("Moment".into(), format!("r{step}"))]);
                batch.basis.insert("Moment".into(), next(&mut seed) % 3);
                twin.commit_batch(batch, next(&mut seed) % 6 != 0);
            }
            3 => { twin.clear_account(RefCause::IdentityChanged, twin.reference.generation("Moment") + 1 + next(&mut seed) % 2, next(&mut seed) % 4 != 0); }
            4 => { twin.clear_models(twin.reference.generation("Moment") + next(&mut seed) % 3, &["Moment"], next(&mut seed) % 4 != 0); }
            5 => { let epoch = twin.reference.epoch; twin.publish(epoch + 1, twin.reference.saved_generation); }
            6 => { let epoch = twin.reference.epoch; twin.feed(Request::EpochEnded { epoch }); }
            7 => { twin.feed(Request::ReadIntents { epoch: twin.reference.epoch }); }
            _ => { twin.feed(Request::ReadFinished(next(&mut seed) % 3)); }
        }
    }
    twin
}

#[test]
fn store_prop_revision() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut last = 0;
        for (_, actions) in &twin.steps {
            for action in actions {
                if let Action::BroadcastCommitted { revision, .. } = action {
                    assert_eq!(*revision, last + 1, "seed {seed}: the revision grows by 1 per commit");
                    last = *revision;
                }
            }
        }
    }
}

#[test]
fn store_prop_no_stale_rows() {
    for seed in 1..=40 {
        assert_eq!(random_trace(seed).stale_writes, 0, "a row with a basis below the generation reached SQLite");
    }
}

#[test]
fn store_prop_no_stale_epoch() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        assert!(twin.written.iter().all(|(batch, store)| batch == store), "a batch of another epoch reached SQLite: {:?}", twin.written);
    }
}

#[test]
fn store_prop_atomic() {
    let mut twin = Twin::live();
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into()), ("Moment".into(), "b".into())]);
    batch.basis.insert("Moment".into(), 0);
    twin.commit_batch(batch, true);
    assert_eq!(twin.state().revision(), 1, "two rows, one transaction, one revision");
}

/// Every batch id is answered once.
#[test]
fn store_prop_reply_once() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut seen = BTreeSet::new();
        for effect in &twin.history {
            if let StoreEffect::Reply { batch, .. } = effect {
                assert!(seen.insert(*batch), "seed {seed}: batch {batch} is answered twice");
            }
        }
    }
}

/// `store.md` batch: the id is unique within the author, the answer goes to the pair `(author, id)`.
#[test]
fn store_commit_author_pair() {
    let mut twin = Twin::live();
    let batch = |author| StoreBatch { author, id: 1, epoch: 1, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: Vec::new() };
    twin.send(StoreInput::Commit(batch(BatchAuthor::Session)));
    twin.send(StoreInput::Commit(batch(BatchAuthor::Outbox)));
    let written = twin.send(StoreInput::Written { author: BatchAuthor::Session, batch: 1, result: Ok(Done { revision: 1 }) });
    assert!(written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Session, batch: 1, reply: StoreReply::Committed { .. } })), "the Session batch is answered");
    assert!(!written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Outbox, .. })), "the Outbox batch with the same id still waits");
    let written = twin.send(StoreInput::Written { author: BatchAuthor::Outbox, batch: 1, result: Ok(Done { revision: 2 }) });
    assert!(written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Outbox, batch: 1, reply: StoreReply::Committed { .. } })), "the Outbox batch is answered");
}

#[test]
fn store_prop_reads_not_blocked() {
    let mut twin = Twin::live();
    let before = twin.history.len();
    twin.send(StoreInput::Idle);
    assert_eq!(twin.history.len(), before, "a read gives no input and no effect of the writer");
    assert_eq!(twin.state().revision(), 0);
}

#[test]
fn store_prop_intents_survive_format() {
    let mut twin = Twin::new();
    let mut batch = Batch::rows(0, vec![]);
    batch.intents = 3;
    twin.commit_batch(batch, true);
    let before = twin.reference.intents;
    twin.boot(Boot::FormatChange, (0, 0, None));
    twin.clear_models(1, &["Moment"], true);
    assert_eq!(twin.reference.intents, before, "the intents survive a format change and ClearModels");
    assert!(!twin.history.iter().any(|effect| matches!(effect, StoreEffect::EraseAccount { .. })), "no account erase touched them");
}

#[test]
fn store_prop_generation_monotonic() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        assert!(twin.generations.windows(2).all(|pair| pair[0] <= pair[1]), "model_generation went down: {:?}", twin.generations);
    }
    let mut twin = Twin::live();
    twin.clear_account(RefCause::SessionReset, 4, true);
    assert_eq!(twin.state().generation("Moment"), 4);
    assert_eq!(twin.state().generation("Other"), 4, "after ClearAccount every model has generation_new");
}

/// P-STORE-10: between `Write` and `Written` the Store takes no step by `Commit`, `ClearAccount`, `ClearModels`, `EpochEnded`, `Close`.
#[test]
fn store_prop_write_atomic_step() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut writing = false;
        for (request, actions) in &twin.steps {
            if matches!(request, Request::Written { .. }) { writing = false; }
            if writing && matches!(request, Request::Commit { .. } | Request::ClearAccount { .. } | Request::ClearModels { .. } | Request::Close) {
                assert!(!actions.iter().any(|action| matches!(action, Action::Write(_) | Action::EraseAccount { .. } | Action::EraseModels { .. } | Action::CloseReads)), "seed {seed}: a step by {request:?} while writing");
            }
            if actions.iter().any(|action| matches!(action, Action::Write(_))) { writing = true; }
        }
    }
}

/// P-STORE-15: `Saved` only after the SQLite answer, `SaveFailed` only without it.
#[test]
fn store_prop_publish_saved() {
    let mut twin = Twin::new();
    twin.feed(Request::Published { publication_id: 1, epoch: 3, generation: 0, active: true });
    twin.feed(Request::Published { publication_id: 2, epoch: 4, generation: 0, active: true });
    assert_eq!(twin.state().epoch(), 0, "nothing is saved before the answer");
    twin.feed(Request::Erased { io_id: 1, result: Ok(0) });
    twin.feed(Request::Erased { io_id: 2, result: Err(()) });
    assert_eq!(twin.state().epoch(), 3, "the failed save leaves the previous epoch");
}

/// P-STORE-17: a `Published` taken during `Save(p1)` is answered once, the heads start in order.
#[test]
fn store_prop_io_head_once() {
    let mut twin = Twin::new();
    twin.feed(Request::Published { publication_id: 1, epoch: 3, generation: 0, active: true });
    twin.feed(Request::Published { publication_id: 2, epoch: 4, generation: 0, active: true });
    twin.feed(Request::Published { publication_id: 3, epoch: 5, generation: 0, active: true });
    twin.feed(Request::Erased { io_id: 1, result: Ok(0) });
    twin.feed(Request::Erased { io_id: 2, result: Err(()) });
    twin.feed(Request::Erased { io_id: 3, result: Ok(0) });
    let starts = twin.history.iter().filter(|e| matches!(e, StoreEffect::SavePublished { .. })).count();
    let answers = twin.history.iter().filter(|e| matches!(e, StoreEffect::Saved { .. } | StoreEffect::SaveFailed { .. })).count();
    assert_eq!((starts, answers), (3, 3));
}

/// P-STORE-16: `StoreClosed` once, after the reads, the write and the IO.
#[test]
fn store_prop_close_once() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    twin.feed(Request::ReadsClosed(vec![1]));
    twin.feed(Request::ReadFinished(1));
    twin.feed(Request::ReadFinished(1));
    twin.feed(Request::Close);
    assert_eq!(twin.history.iter().filter(|effect| matches!(effect, StoreEffect::StoreClosed)).count(), 1);
}
