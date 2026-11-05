//! Owner-level tests of `specs/machines/store.md`: the owner `Store` on `DetScheduler` against `ref_store.rs`. SQLite is the effect `Write` or `Erase*`,
//! the test answers it with `Written` or `Erased` (`sqlite_ok`). Names are the `Test` column.

use std::collections::{BTreeMap, BTreeSet};

use super::ref_lifecycle::Cause as RefCause;
use super::ref_store::{Batch, Boot, Open, Reason, Reply, Request, Store as Ref};
use crate::owners::contract::{EpochEnded, Published, RejectReason};
use crate::owners::contract::BatchAuthor;
use crate::owners::store::{BootFound, StoreWriter, StoreBatch, StoreEffect, StoreInput, StoreMode, StoreReply, StoreState};
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

struct Twin {
    det: DetScheduler,
    addr: Address<StoreWriter>,
    log: Log<StoreEffect>,
    reference: Ref,
    history: Vec<StoreEffect>,
    batch: u64,
    /// (epoch of the written batch, epoch of the Store) at each `Write`.
    written: Vec<(u64, u64)>,
    /// A `Write` of a batch whose basis is older than the generation of a model, at the time of the write.
    stale_writes: u64,
    /// `model_generation[Moment]` after each request.
    generations: Vec<u64>,
}

impl Twin {
    fn new() -> Self {
        let mut det = DetScheduler::new(9);
        let log = new_log();
        let addr = det.add::<StoreWriter>("store", StoreState::new(), Recorder::<StoreWriter>::new(&log));
        Self { det, addr, log, reference: Ref::new(), history: Vec::new(), batch: 0, written: Vec::new(), stale_writes: 0, generations: Vec::new() }
    }

    /// An open store in the epoch 1 (`Published` answered).
    fn live() -> Self {
        let mut twin = Self::new();
        twin.feed(Request::Published { epoch: 1, generation: 0 });
        twin
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

    /// Feeds one request of the table to both sides, answers the SQLite effect, and compares the answer, `revision`, `epoch` and the generations.
    fn feed(&mut self, request: Request) -> Option<Reply> {
        let mut raw;
        match request.clone() {
            Request::Commit { batch, sqlite_ok } => {
                self.batch += 1;
                let id = self.batch;
                raw = self.send(StoreInput::Commit(StoreBatch { author: BatchAuthor::Outbox, id, epoch: batch.epoch, basis: batch.basis, rows: batch.rows, intents: batch.intents, credential: batch.credential, ops: Vec::new() }));
                if raw.iter().any(|effect| matches!(effect, StoreEffect::Write { .. })) {
                    raw.extend(self.send(StoreInput::Written { author: BatchAuthor::Outbox, batch: id, result: if sqlite_ok { Ok(()) } else { Err(DISK.into()) } }));
                }
            }
            Request::ClearAccount { epoch_old, cause: c, generation_new, sqlite_ok } => {
                raw = self.send(StoreInput::ClearAccount { epoch_old, cause: cause(c), generation_new });
                raw.extend(self.send(StoreInput::Erased { result: if sqlite_ok { Ok(()) } else { Err(DISK.into()) } }));
            }
            Request::ClearModels { generation_new, models, sqlite_ok } => {
                raw = self.send(StoreInput::ClearModels { epoch: self.reference.epoch, generation_new, models });
                raw.extend(self.send(StoreInput::Erased { result: if sqlite_ok { Ok(()) } else { Err(DISK.into()) } }));
            }
            Request::Published { epoch, generation } => raw = self.send(StoreInput::Published(Published { epoch, identity: None, generation, closed: false })),
            Request::EpochEnded { epoch } => raw = self.send(StoreInput::EpochEnded(EpochEnded { epoch, cause: Cause::SessionReset })),
            Request::Close => raw = self.send(StoreInput::Close),
        }
        let want = self.reference.step(request).reply;
        let got = raw.iter().find_map(|effect| match effect {
            StoreEffect::Reply { reply: StoreReply::Committed { revision }, .. } => Some(Reply::Committed(*revision)),
            StoreEffect::Reply { reply: StoreReply::Rejected(reason), .. } => Some(Reply::Rejected(match reason {
                RejectReason::NoSession => Reason::NoSession,
                RejectReason::StaleGeneration { models } => Reason::StaleGeneration(models.clone()),
                RejectReason::Storage(_) => Reason::Storage,
                RejectReason::Closed => Reason::Closed,
            })),
            StoreEffect::Cleared { epoch_old } => Some(Reply::Cleared(*epoch_old)),
            StoreEffect::ClearFailed(_) => Some(Reply::ClearFailed),
            StoreEffect::ModelsCleared { generation_new } => Some(Reply::ModelsCleared(*generation_new)),
            _ => None,
        });
        assert_eq!(got, want, "the same answer");
        let generation = self.state().generation("Moment");
        self.generations.push(generation);
        let state = self.state();
        assert_eq!(state.revision(), self.reference.revision, "the same revision");
        assert_eq!(state.epoch(), self.reference.epoch, "the same epoch");
        assert_eq!(state.mode() == StoreMode::Closed, self.reference.state == Open::Closed, "the same mode");
        for model in ["Moment", "Other"] {
            assert_eq!(state.generation(model), self.reference.generation(model), "the same generation of {model}");
        }
        got
    }

    fn commit(&mut self, epoch: u64, rows: &[&str]) -> Option<Reply> {
        self.feed(Request::Commit { batch: Batch::rows(epoch, rows.iter().map(|id| ("Moment".to_string(), id.to_string())).collect()), sqlite_ok: true })
    }

    fn boot(&mut self, found: Boot, saved: (u64, u64, Option<u64>)) -> Option<StoreEffect> {
        self.reference.saved_epoch = saved.0;
        self.reference.saved_generation = saved.1;
        self.reference.credential = saved.2;
        let want = self.reference.boot(found.clone());
        let input = match found {
            Boot::New => BootFound::Fresh,
            Boot::Same => BootFound::Same { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2 },
            Boot::ChangedModels(changed) => BootFound::ChangedModels { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2, changed_models: changed },
            Boot::FormatChange => BootFound::FormatChange { saved_epoch: saved.0, saved_generation: saved.1, saved_identity: saved.2 },
            Boot::SqliteError => BootFound::SqliteError(DISK.into()),
        };
        let raw = self.send(StoreInput::Boot(input));
        let opened = raw.iter().find(|effect| matches!(effect, StoreEffect::OpenLifecycle(_) | StoreEffect::OpenFailed(_))).cloned();
        match (&opened, want) {
            (Some(StoreEffect::OpenLifecycle(message)), Some(want)) => {
                assert_eq!((message.saved_epoch, message.saved_generation, message.saved_identity, &message.changed_models), (want.saved_epoch, want.saved_generation, want.saved_identity, &want.changed_models));
            }
            (Some(StoreEffect::OpenFailed(_)), None) => {}
            (got, want) => panic!("the boot rows differ: {got:?} against {want:?}"),
        }
        assert_eq!(self.state().revision(), self.reference.revision);
        for model in ["Moment", "Other"] {
            assert_eq!(self.state().generation(model), self.reference.generation(model));
        }
        opened
    }
}

fn names(items: &[&str]) -> BTreeSet<String> { items.iter().map(|item| item.to_string()).collect() }

fn clear_account(cause: RefCause, generation_new: u64, sqlite_ok: bool) -> Request { Request::ClearAccount { epoch_old: 1, cause, generation_new, sqlite_ok } }

#[test]
fn store_boot_new() {
    let mut twin = Twin::new();
    let opened = twin.boot(Boot::New, (0, 0, None));
    assert!(twin.history.contains(&StoreEffect::Prepare { format_change: false }));
    assert!(matches!(opened, Some(StoreEffect::OpenLifecycle(m)) if m.saved_epoch == 0 && m.saved_identity.is_none()));
}

#[test]
fn store_boot_same() {
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::Same, (4, 2, Some(9))), Some(StoreEffect::OpenLifecycle(m)) if m.saved_epoch == 4 && m.saved_generation == 2 && m.saved_identity == Some(9)));
}

#[test]
fn store_boot_changed_models() {
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::ChangedModels(names(&["Moment"])), (4, 2, Some(9))), Some(StoreEffect::OpenLifecycle(m)) if m.changed_models == names(&["Moment"])));
}

#[test]
fn store_boot_format_change() {
    let mut twin = Twin::new();
    let opened = twin.boot(Boot::FormatChange, (4, 2, Some(9)));
    assert!(matches!(opened, Some(StoreEffect::OpenLifecycle(m)) if m.saved_generation == 3 && m.saved_identity == Some(9)), "the generation grows, the identity is kept");
    assert_eq!(twin.state().revision(), 1, "the revision grows by one");
}

#[test]
fn store_boot_error() {
    let mut twin = Twin::new();
    assert!(matches!(twin.boot(Boot::SqliteError, (0, 0, None)), Some(StoreEffect::OpenFailed(_))));
    assert_eq!(twin.state().mode(), StoreMode::Closed);
}

#[test]
fn store_commit_stale_epoch_owner() { assert_eq!(Twin::live().commit(7, &["a"]), Some(Reply::Rejected(Reason::NoSession))); }

#[test]
fn store_commit_stale_generation() {
    let mut twin = Twin::live();
    twin.feed(Request::ClearModels { generation_new: 2, models: names(&["Moment"]), sqlite_ok: true });
    let mut batch = Batch::rows(1, vec![("Moment".into(), "a".into())]);
    batch.basis = BTreeMap::from([("Moment".to_string(), 1)]);
    assert_eq!(twin.feed(Request::Commit { batch, sqlite_ok: true }), Some(Reply::Rejected(Reason::StaleGeneration(vec!["Moment".into()]))));
    assert!(!twin.history.iter().any(|effect| matches!(effect, StoreEffect::Write { .. })), "nothing is written, intents too");
}

#[test]
fn store_commit_ok_owner() {
    let mut twin = Twin::live();
    assert_eq!(twin.commit(1, &["a"]), Some(Reply::Committed(1)));
    assert!(twin.history.iter().any(|effect| matches!(effect, StoreEffect::BroadcastCommitted { revision: 1, .. })), "Committed goes to every reader");
}

#[test]
fn store_commit_sqlite_error() {
    let mut twin = Twin::live();
    assert_eq!(twin.feed(Request::Commit { batch: Batch::rows(1, vec![("Moment".into(), "a".into())]), sqlite_ok: false }), Some(Reply::Rejected(Reason::Storage)));
    assert_eq!(twin.commit(1, &["b"]), Some(Reply::Committed(1)), "the revision did not move");
}

#[test]
fn store_clear_account_owner() {
    let mut twin = Twin::live();
    twin.commit(1, &["a"]);
    assert_eq!(twin.feed(clear_account(RefCause::SessionReset, 1, true)), Some(Reply::Cleared(1)));
    assert!(twin.history.iter().any(|effect| matches!(effect, StoreEffect::BroadcastCommitted { models: None, .. })));
}

#[test]
fn store_clear_account_error() {
    let mut twin = Twin::live();
    assert_eq!(twin.feed(clear_account(RefCause::SessionReset, 1, false)), Some(Reply::ClearFailed));
}

#[test]
fn store_clear_models() {
    let mut twin = Twin::live();
    twin.commit(1, &["a"]);
    assert_eq!(twin.feed(Request::ClearModels { generation_new: 1, models: names(&["Moment"]), sqlite_ok: true }), Some(Reply::ModelsCleared(1)));
}

#[test]
fn store_clear_models_error() {
    let mut twin = Twin::live();
    assert_eq!(twin.feed(Request::ClearModels { generation_new: 1, models: names(&["Moment"]), sqlite_ok: false }), Some(Reply::ClearFailed));
}

#[test]
fn store_epoch_owner() {
    let mut twin = Twin::live();
    twin.feed(Request::Published { epoch: 5, generation: 3 });
    assert_eq!(twin.commit(5, &["a"]), Some(Reply::Committed(1)));
    assert!(twin.history.contains(&StoreEffect::SavePublished { epoch: 5, generation: 3 }));
}

#[test]
fn store_epoch_ended() {
    let mut twin = Twin::live();
    twin.feed(Request::EpochEnded { epoch: 1 });
    assert_eq!(twin.commit(1, &["a"]), Some(Reply::Rejected(Reason::NoSession)));
    twin.feed(Request::Published { epoch: 3, generation: 1 });
    assert_eq!(twin.commit(3, &["a"]), Some(Reply::Committed(1)));
}

#[test]
fn store_close_owner() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    assert!(twin.history.contains(&StoreEffect::CloseConnections));
}

#[test]
fn store_closed_owner() {
    let mut twin = Twin::live();
    twin.feed(Request::Close);
    assert_eq!(twin.commit(1, &["a"]), Some(Reply::Rejected(Reason::Closed)));
    twin.feed(clear_account(RefCause::SessionReset, 1, true));
    twin.feed(Request::Close);
}

// The reads of a snapshot are no input of the owner: they go through `Database` (API level).

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_read() {
    use super::harness_life::{bounded, Rig};
    let rig = Rig::open().await;
    rig.login().await.expect("login");
    let epoch = rig.database.session_epoch();
    let id = rig.create(epoch, "a").await.expect("create");
    let rows = bounded(rig.rows(epoch)).await.expect("the read answers").expect("rows");
    assert!(rows.contains(&id), "the snapshot shows the committed row");
}

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_peek_row() {
    use super::harness_life::Rig;
    let rig = Rig::open().await;
    rig.login().await.expect("login");
    let epoch = rig.database.session_epoch();
    let id = rig.create(epoch, "a").await.expect("create");
    rig.database.peek_row(epoch, "Moment".into(), id.clone()).expect("the current epoch is answered with the row or None");
    assert!(rig.database.peek_row(epoch + 1000, "Moment".into(), id.clone()).is_err(), "a stale epoch is refused");
    assert!(rig.database.peek_row(epoch, "Nope".into(), id).is_err(), "an unknown model is refused");
}

#[tokio::test(flavor = "multi_thread")]
async fn store_wait_edge_values() {
    use super::harness_life::{bounded, Rig};
    let rig = Rig::open().await;
    rig.login().await.expect("login");
    let epoch = rig.database.session_epoch();
    assert!(bounded(rig.database.edge_values(epoch + 1000, "Moment".into(), "none".into(), "{}".into(), vec![])).await.expect("answers").is_err(), "a stale epoch is refused");
    assert!(bounded(rig.database.edge_values(epoch, "Moment".into(), "none".into(), "{}".into(), vec![])).await.expect("answers").is_err(), "an unknown relation is refused");
}

fn next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn random_trace(seed: u64) -> Twin {
    let mut s = seed;
    let mut twin = Twin::live();
    let mut epoch = 1;
    let mut generation = 0;
    for step in 0..30 {
        let ok = next(&mut s) % 5 != 0;
        let basis = if next(&mut s) % 3 == 0 { BTreeMap::from([("Moment".to_string(), next(&mut s) % 3)]) } else { BTreeMap::new() };
        let request = match next(&mut s) % 9 {
            0..=3 => Request::Commit { batch: Batch { epoch: if next(&mut s) % 4 == 0 { epoch + 1 } else { epoch }, basis, rows: vec![("Moment".into(), format!("r{step}"))], intents: next(&mut s) % 3, credential: None }, sqlite_ok: ok },
            4 => { generation += 1; clear_account([RefCause::SessionReset, RefCause::IdentityChanged, RefCause::ServerEnded][(next(&mut s) % 3) as usize], generation, ok) }
            5 => { generation += 1; Request::ClearModels { generation_new: generation, models: names(&["Moment"]), sqlite_ok: ok } }
            6 => { epoch += 2; Request::EpochEnded { epoch: epoch - 2 } }
            7 => Request::Published { epoch, generation: next(&mut s) % 4 },
            _ => if next(&mut s) % 7 == 0 { Request::Close } else { Request::Published { epoch, generation: 1 } },
        };
        twin.feed(request);
    }
    twin
}

#[test]
fn store_prop_revision() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let grown = twin.history.iter().filter(|effect| matches!(effect, StoreEffect::BroadcastCommitted { .. })).count() as u64;
        assert_eq!(twin.state().revision(), grown, "the revision grows by one per commit, clearing and models clearing");
    }
}

#[test]
fn store_prop_no_stale_rows() {
    for seed in 1..=40 {
        assert_eq!(random_trace(seed).stale_writes, 0, "a row of an older generation reached SQLite");
    }
}

#[test]
fn store_prop_no_stale_epoch_owner() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        assert!(twin.written.iter().all(|(batch, store)| batch == store), "a batch of another epoch reached SQLite");
    }
    let mut live = Twin::live();
    live.feed(Request::EpochEnded { epoch: 1 });
    assert_eq!(live.commit(1, &["x"]), Some(Reply::Rejected(Reason::NoSession)), "after EpochEnded(e) a commit of e is refused");
}

#[test]
fn store_prop_atomic() {
    let mut twin = Twin::live();
    twin.feed(Request::Commit { batch: Batch::rows(1, vec![("Moment".into(), "x".into()), ("Moment".into(), "y".into())]), sqlite_ok: true });
    let writes: Vec<&StoreEffect> = twin.history.iter().filter(|effect| matches!(effect, StoreEffect::Write { .. })).collect();
    assert_eq!(writes.len(), 1, "a batch is one transaction");
    assert!(matches!(writes[0], StoreEffect::Write { batch } if batch.rows.len() == 2));
    assert_eq!(twin.state().revision(), 1, "and one revision");
}

#[test]
fn store_prop_reply_once_owner() {
    for seed in 1..=40 {
        let twin = random_trace(seed);
        let mut seen = Vec::new();
        for effect in &twin.history {
            if let StoreEffect::Reply { author, batch, .. } = effect {
                assert!(!seen.contains(&(*author, *batch)), "batch {batch} of {author:?} is answered twice");
                seen.push((*author, *batch));
            }
        }
        let sent = twin.batch;
        assert_eq!(seen.len() as u64, sent, "every batch is answered, Closed too");
    }
}

/// `store.md` batch: the id is unique within the author, the answer goes to the pair `(author, id)`.
#[test]
fn store_commit_author_pair() {
    let mut twin = Twin::live();
    let batch = |author| StoreBatch { author, id: 1, epoch: 1, basis: BTreeMap::new(), rows: Vec::new(), intents: 0, credential: None, ops: Vec::new() };
    twin.send(StoreInput::Commit(batch(BatchAuthor::Session)));
    twin.send(StoreInput::Commit(batch(BatchAuthor::Outbox)));
    let written = twin.send(StoreInput::Written { author: BatchAuthor::Session, batch: 1, result: Ok(()) });
    assert!(written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Session, batch: 1, reply: StoreReply::Committed { .. } })), "the Session batch is answered");
    assert!(!written.iter().any(|effect| matches!(effect, StoreEffect::Reply { author: BatchAuthor::Outbox, .. })), "the Outbox batch with the same id still waits");
    let written = twin.send(StoreInput::Written { author: BatchAuthor::Outbox, batch: 1, result: Ok(()) });
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
    twin.feed(Request::Commit { batch, sqlite_ok: true });
    let before = twin.reference.intents;
    twin.boot(Boot::FormatChange, (0, 0, None));
    twin.feed(Request::ClearModels { generation_new: 1, models: names(&["Moment"]), sqlite_ok: true });
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
    twin.feed(clear_account(RefCause::SessionReset, 4, true));
    assert_eq!(twin.state().generation("Moment"), 4);
    assert_eq!(twin.state().generation("Other"), 4, "after ClearAccount every model has generation_new");
}
