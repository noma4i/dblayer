//! Owner-level tests of the owner `Transfer`: the opening: `Restored`, `RegisterOperations`, `RestoreReady` and the class of every restored row.

use crate::test_wait::permutations;
use std::collections::BTreeMap;

use super::transfer_harness::*;
use crate::owners::contract::Online;
use crate::owners::store::UploadKind;
use crate::owners::write::outbox::{Fault, OutboxOutcome};
use crate::owners::write::transfer::{NextStep, Shown, StoredTransfer, TransferAnswer, TransferInput, TransferStage};
use crate::runtime::Time;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";

fn row(id: u64, key: &str) -> StoredTransfer { stored(id, key, 2) }

fn partly(id: u64, key: &str) -> StoredTransfer {
    let mut found = row(id, key);
    found.confirmed_parts = 1;
    found.upload_id = Some("u1".into());
    found
}

fn completed(id: u64, key: &str) -> StoredTransfer {
    let mut found = row(id, key);
    found.confirmed_parts = 2;
    found.completed = true;
    found.signed_id = Some("signed".into());
    found
}

fn outcomes(id: u64, outcome: OutboxOutcome) -> BTreeMap<u64, OutboxOutcome> { BTreeMap::from([(id, outcome)]) }

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xfer_prop_saved_outcome_reopens_sqlite() {
    use crate::ffi::upload_wire::WireUploadRequest;
    use crate::schema::OnFailure;
    use crate::owners::write::upload::UploadFile;
    use super::rig::{Kind, Rig, Server, within};

    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("outcome.sqlite");
    let path = database_path.to_str().unwrap().to_string();
    let source = directory.path().join("upload.bin");
    std::fs::write(&source, b"bytes").unwrap();
    let server = Server::new();
    server.hold(Kind::Put, true);
    let rig = Rig::open_at(OnFailure::Keep, Some(path.clone()), server.clone()).await;
    let seeded = rig.seed_row("42").await;
    let request = WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap()));
    within(rig.db.upload(rig.epoch(), "Moment".into(), seeded.clone(), "upload".into(), request)).await.unwrap();
    server.wait_held(Kind::Put, 1).await;
    let sqlite = rusqlite::Connection::open(&database_path).unwrap();
    sqlite.execute_batch("CREATE TRIGGER hold_upload_removal BEFORE DELETE ON sync_upload BEGIN SELECT RAISE(ABORT, 'hold removal'); END;").unwrap();
    server.answer(Kind::Put, super::rig::Reply::Ok);
    crate::test_wait::settled(&rig, "the confirmed outcome is durable", || sqlite.query_row("SELECT COUNT(*) FROM transfer_outcome WHERE kind = 'confirmed'", [], |row| row.get::<_, i64>(0)).unwrap_or(0) == 1).await;
    assert_eq!(server.count("MakeMoment"), 1);
    assert_eq!(sqlite.query_row("SELECT COUNT(*) FROM sync_upload", [], |row| row.get::<_, i64>(0)).unwrap(), 1);
    within(rig.db.close_database()).await.unwrap();
    drop(rig);
    sqlite.execute_batch("DROP TRIGGER hold_upload_removal").unwrap();
    sqlite.execute_batch("UPDATE transfer_outcome SET terminal_at = CAST(strftime('%s', 'now') AS INTEGER) * 1000").unwrap();
    let reopened = Rig::open_at(OnFailure::Keep, Some(path), server.clone()).await;
    crate::test_wait::settled(&reopened, "the restored upload is removed", || sqlite.query_row("SELECT COUNT(*) FROM sync_upload", [], |row| row.get::<_, i64>(0)).unwrap_or(1) == 0).await;
    assert_eq!(server.count("MakeMoment"), 1);
    assert_eq!(sqlite.query_row("SELECT COUNT(*) FROM transfer_outcome", [], |row| row.get::<_, i64>(0)).unwrap(), 1, "RemoveUpload keeps the outcome of the cycle (D-237)");
    within(reopened.db.close_database()).await.unwrap();
}

#[tokio::test]
async fn xfer_prop_cancelled_outcome_reopens_sqlite() {
    use crate::ffi::upload_wire::WireUploadRequest;
    use crate::schema::OnFailure;
    use crate::owners::write::upload::UploadFile;
    use super::rig::{Kind, Rig, Server, within};

    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("cancelled.sqlite");
    let path = database_path.to_str().unwrap().to_string();
    let source = directory.path().join("upload.bin");
    std::fs::write(&source, b"bytes").unwrap();
    let server = Server::new();
    server.hold(Kind::Put, true);
    let rig = Rig::open_at(OnFailure::Keep, Some(path.clone()), server.clone()).await;
    let seeded = rig.seed_row("42").await;
    let request = WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap()));
    within(rig.db.upload(rig.epoch(), "Moment".into(), seeded.clone(), "upload".into(), request)).await.unwrap();
    server.wait_held(Kind::Put, 1).await;
    assert!(within(rig.db.cancel_upload(rig.epoch(), "Moment".into(), seeded, "upload".into())).await.unwrap());
    let sqlite = rusqlite::Connection::open(&database_path).unwrap();
    within(rig.db.close_database()).await.unwrap();
    drop(rig);
    sqlite.execute_batch("UPDATE transfer_outcome SET terminal_at = CAST(strftime('%s', 'now') AS INTEGER) * 1000").unwrap();
    let reopened = Rig::open_at(OnFailure::Keep, Some(path), server.clone()).await;
    assert_eq!(sqlite.query_row("SELECT COUNT(*) FROM transfer_outcome WHERE kind = 'discarded'", [], |row| row.get::<_, i64>(0)).unwrap(), 1, "the cancelled cycle keeps its outcome after close and open (D-237)");
    within(reopened.db.close_database()).await.unwrap();
}

/// The owner after `Restored` of `rows` with no declaration: every row waits for it.
fn undeclared(rows: Vec<StoredTransfer>) -> (Twin, Out) {
    let mut twin = Twin::bare(1);
    let out = twin.post(TransferInput::Restored { uploads: rows, outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
    (twin, out)
}

#[test]
fn xfer_restored() {
    let mut failed = row(2, OTHER);
    failed.restore_failed = Some("moved".into());
    let (mut twin, out) = Twin::opened(vec![partly(1, KEY), failed], Default::default());
    assert_eq!((twin.stage(1), twin.stage(2), out.classified()), (TransferStage::Ready, TransferStage::Failed, 1), "every row has its class and Lifecycle hears it");
    twin.online(true);
    assert_eq!(twin.stage(1), TransferStage::Ready, "the network start of a restored transfer waits for RestoreReady");
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!((twin.stage(1), out.steps().len()), (TransferStage::PersistingAttempt, 0));
    assert_eq!(twin.commit().steps().len(), 1);
}

#[test]
fn xfer_restoring_register() {
    let mut twin = Twin::bare(1);
    let out = twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
    assert!(Twin::quiet(&out), "{out:?}");
    twin.post(TransferInput::Restored { uploads: vec![partly(1, KEY)], outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
    assert_eq!(twin.stage(1), TransferStage::Ready, "the declaration of the Restoring phase counts");
}

/// xfer_calls_held_while_restoring: a call that arrives before RestoreReady is held and replayed on RestoreReady against the row the transfer was classified into.
#[test]
fn xfer_calls_held_while_restoring() {
    let mut twin = Twin::bare(1);
    twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
    let reply = twin.reply();
    let held = twin.post(TransferInput::Cancel { key: KEY.into(), reply });
    assert!(Twin::quiet(&held), "a call is held during Restoring: {held:?}");
    let restored = twin.post(TransferInput::Restored { uploads: vec![partly(1, KEY)], outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
    assert!(restored.removed().is_empty(), "the held call waits for RestoreReady, not classification: {restored:?}");
    let ready = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(ready.removed(), vec![KEY.to_string()], "on RestoreReady the held cancel replays against the classified transfer");
}

#[test]
fn xfer_prop_online_before_declaration() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Event { Online, Offline, Restored, RestoreReady, Register }
    use Event::*;
    let orders: Vec<_> = permutations(&[Online, Offline, Restored, RestoreReady, Register]).into_iter().filter(|order| order.iter().position(|e| *e == Restored) < order.iter().position(|e| *e == RestoreReady)).collect();
    assert_eq!(orders.len(), 60);
    for order in orders {
        let mut twin = Twin::bare(1);
        let mut steps = 0;
        for event in order.iter().copied().chain([Online]) {
            let out = match event {
                Online => twin.online(true),
                Offline => twin.online(false),
                Restored => twin.post(TransferInput::Restored { uploads: vec![partly(1, KEY)], outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None }),
                RestoreReady => twin.post(TransferInput::RestoreReady { epoch: EPOCH }),
                Register => twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] }),
            };
            steps += out.steps().len();
        }
        steps += twin.commit().steps().len();
        assert_eq!(steps, 1, "{order:?}: the saved step of the restored transfer goes out once");
    }
}

#[test]
fn xfer_restore_ready() {
    let (mut twin, _) = Twin::opened(vec![partly(1, KEY), partly(2, OTHER)], Default::default());
    twin.online(true);
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(out.steps().len(), 0);
    assert_eq!(twin.stage(1), TransferStage::PersistingAttempt);
    assert_eq!(twin.stage(2), TransferStage::Ready);
    assert_eq!(twin.commit().steps().len(), 1);
}

#[test]
fn xfer_ready_attempts_exhausted() {
    let mut row = partly(1, KEY);
    row.attempts = 50;
    let (mut twin, _) = Twin::opened(vec![row], Default::default());
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    let out = twin.online(true);
    assert_eq!(twin.stage(1), TransferStage::PersistingFailure);
    assert!(out.steps().is_empty());
    assert_eq!(out.orphans_written(), vec![("u1".into(), KEY.into())]);
    assert_eq!(out.rows()[0].0.confirmed_parts, 0);
    assert!(out.orphan_aborts().is_empty());
    assert_eq!(twin.commit().orphan_aborts(), vec!["u1".to_string()]);
}

#[test]
fn xfer_last_attempt_is_persisted_once() {
    for complete in [false, true] {
        let mut row = partly(1, KEY);
        row.attempts = 49;
        if complete {
            row.confirmed_parts = row.source.as_ref().unwrap().parts;
        }
        let (mut twin, _) = Twin::opened(vec![row], Default::default());
        twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        let out = twin.online(true);
        assert_eq!(out.rows()[0].0.attempts, 50);
        assert!(out.steps().is_empty() && out.completes() == 0);
        twin.commit();
        let out = if complete { twin.complete_error(1, true) } else { twin.step_error(1, true) };
        assert_eq!(twin.stage(1), TransferStage::PersistingFailure);
        assert_eq!(out.rows()[0].0.attempts, 50);
        assert_eq!(out.timers, 0);
        assert_eq!(out.orphans_written().len(), usize::from(!complete));
    }
}

#[test]
fn xfer_part_commit_resets_attempts() {
    let mut row = partly(1, KEY);
    row.attempts = 49;
    let (mut twin, _) = Twin::opened(vec![row], Default::default());
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    twin.online(true);
    twin.commit();
    let out = twin.step_ok(1, 2, None);
    assert_eq!((out.rows()[0].0.confirmed_parts, out.rows()[0].0.attempts), (2, 0));
    twin.commit();
    let out = twin.commit();
    assert_eq!(out.completes(), 1);
    assert_eq!(twin.t(1).attempts, 1);
}

#[test]
fn xfer_attempt_ended() {
    for reason in [crate::owners::contract::RejectReason::NoSession, crate::owners::contract::RejectReason::Closed] {
        let (mut twin, _) = Twin::opened(vec![partly(1, KEY)], Default::default());
        twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        twin.online(true);
        let out = twin.reject(reason);
        assert_eq!(twin.stage(1), TransferStage::Done);
        assert!(out.steps().is_empty());
        assert_eq!(twin.state().active(), 0);
    }
}

#[test]
fn xfer_attempt_close() {
    let (mut twin, _) = Twin::opened(vec![partly(1, KEY)], Default::default());
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    twin.online(true);
    let out = twin.close();
    assert_eq!(twin.stage(1), TransferStage::Done);
    assert_eq!((out.steps().len(), out.cancel_steps(), out.drained()), (0, 0, 1));
}

#[test]
fn xfer_step_temp_offline() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 2);
    let attempt_id = twin.attempt(id);
    twin.online(false);
    let out = twin.post(TransferInput::StepError { transfer_id: id, attempt_id, temporary: true, error: super::transfer_harness::refusal() });
    assert_eq!(twin.stage(id), TransferStage::Ready);
    assert!(Twin::quiet(&out));
    let out = twin.online(true);
    assert!(out.steps().is_empty());
    assert_eq!(twin.commit().steps().len(), 1);
}

#[test]
fn xfer_complete_temp_offline() {
    let mut twin = Twin::new();
    let id = twin.completing(KEY);
    let attempt_id = twin.attempt(id);
    twin.online(false);
    let out = twin.post(TransferInput::CompleteError { transfer_id: id, attempt_id, temporary: true, error: super::transfer_harness::refusal() });
    assert_eq!(twin.stage(id), TransferStage::Ready);
    assert!(Twin::quiet(&out));
    let out = twin.online(true);
    assert_eq!(out.completes(), 0);
    assert_eq!(twin.commit().completes(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_attempt_reopen_file() {
    use crate::ffi::upload_wire::WireUploadRequest;
    use crate::schema::OnFailure;
    use crate::owners::write::upload::UploadFile;
    use super::rig::{Kind, Rig, Server, within};

    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("database.sqlite");
    let source = directory.path().join("upload.bin");
    std::fs::write(&source, b"bytes").unwrap();
    let path = database_path.to_str().unwrap().to_string();
    let server = Server::new();
    server.hold(Kind::Put, true);
    let rig = Rig::open_at(OnFailure::Keep, Some(path.clone()), server.clone()).await;
    let seeded = rig.seed_row("42").await;
    let request = WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap()));
    within(rig.db.upload(rig.epoch(), "Moment".into(), seeded.clone(), "upload".into(), request)).await.unwrap();
    server.wait_held(Kind::Put, 1).await;
    let attempts = || {
        rusqlite::Connection::open(&database_path).unwrap().query_row("SELECT attempts FROM sync_upload WHERE model = 'Moment' AND row_id = ?1 AND action = 'upload'", [seeded.as_str()], |row| row.get::<_, u32>(0)).unwrap()
    };
    assert_eq!(attempts(), 1);
    within(rig.db.close_database()).await.unwrap();
    drop(rig);
    server.hold(Kind::Put, true);
    let reopened = Rig::open_at(OnFailure::Keep, Some(path), server.clone()).await;
    server.wait_held(Kind::Put, 1).await;
    assert_eq!(attempts(), 2);
    server.release_all();
    drop(reopened);
}

#[tokio::test(flavor = "multi_thread")]
async fn xfer_upload_survives_model_fingerprint_change() {
    use crate::ffi::upload_wire::WireUploadRequest;
    use crate::schema::OnFailure;
    use crate::owners::write::upload::UploadFile;
    use super::rig::{moment_models, Kind, Rig, Server, within};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("database.sqlite").to_str().unwrap().to_string();
    let source = directory.path().join("upload.bin");
    std::fs::write(&source, b"bytes").unwrap();
    let server = Server::new();
    server.hold(Kind::Put, true);
    let models = moment_models(OnFailure::Keep);
    let rig = Rig::open_models(models.clone(), Some(path.clone()), server.clone()).await;
    let seeded = rig.seed_row("42").await;
    let request = WireUploadRequest::of(&UploadFile::new(source.to_str().unwrap()));
    within(rig.db.upload(rig.epoch(), "Moment".into(), seeded.clone(), "upload".into(), request)).await.unwrap();
    server.wait_held(Kind::Put, 1).await;
    within(rig.db.close_database()).await.unwrap();
    drop(rig);
    let mut changed: Vec<crate::schema::ModelSpec> = serde_json::from_str(&models).unwrap();
    changed[0].fields.push(crate::schema::FieldSpec::scalar("added", crate::value::ScalarKind::Str).optional());
    server.hold(Kind::Put, true);
    let reopened = Rig::open_models(serde_json::to_string(&changed).unwrap(), Some(path), server.clone()).await;
    server.wait_held(Kind::Put, 1).await;
    server.answer(Kind::Put, super::rig::Reply::Ok);
    server.wait_seen("MakeMoment", 1).await;
    assert_eq!(server.count("MakeMoment"), 1);
    drop(reopened);
}

#[test]
fn xfer_restore_ready_other() {
    let (mut twin, _) = Twin::opened(vec![partly(1, KEY)], Default::default());
    twin.online(true);
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH + 1 });
    assert!(Twin::quiet(&out), "{out:?}");
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert!(Twin::quiet(&out), "a second RestoreReady changes nothing: {out:?}");
    let mut restoring = Twin::bare(1);
    restoring.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
    restoring.online(true);
    assert!(Twin::quiet(&restoring.post(TransferInput::RestoreReady { epoch: EPOCH })), "no rows have been read yet");
    let out = restoring.post(TransferInput::Restored { uploads: vec![partly(1, KEY)], outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
    assert_eq!(out.steps().len(), 0, "the early RestoreReady did not open the start");
}

#[test]
fn xfer_register_rechecks_waiting() {
    let (mut twin, _) = undeclared(vec![partly(1, KEY)]);
    assert_eq!(twin.stage(1), TransferStage::WaitingDeclaration);
    let out = twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
    assert_eq!((twin.stage(1), out.shows(), out.round_needed()), (TransferStage::Ready, vec![Shown::Queued], vec![vec![1]]), "the row is classified again, only this one");
}

#[test]
fn xfer_register_still_waiting() {
    let (mut twin, _) = undeclared(vec![partly(1, KEY)]);
    let out = twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "other".into())] });
    assert!(Twin::quiet(&out), "{out:?}");
    assert_eq!(twin.stage(1), TransferStage::WaitingDeclaration);
}

#[test]
fn xfer_register_other() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.ignores(id, vec![TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] }]);
}

#[test]
fn xfer_waiting_declaration_refuses() {
    let (mut twin, _) = undeclared(vec![partly(1, KEY)]);
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    for (name, (reply, out)) in [("cancel", twin.cancel(KEY)), ("discard", twin.discard(KEY)), ("retry", twin.retry(KEY, None))] {
        assert_eq!(Twin::fault(&out, reply), Some(Fault::InFlight), "{name}");
    }
    assert_eq!(twin.stage(1), TransferStage::WaitingDeclaration);
}

#[test]
fn xfer_waiting_declaration_other() {
    let (mut twin, _) = undeclared(vec![partly(1, KEY)]);
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    let mut inputs = twin.stray_results(1);
    inputs.extend([
        TransferInput::Online(Online(true)),
        TransferInput::PowerSaving(true),
        TransferInput::SlotFreed,
        TransferInput::MutationOutcome { transfer_id: 1, cycle_id: 1, intent_removed: true, outcome: OutboxOutcome::Confirmed { revision: 1, result: None, row_id: None } },
        TransferInput::Tick { now: Time(1), kind: crate::owners::write::transfer::TransferTick::Expire },
    ]);
    twin.ignores(1, inputs);
}

#[test]
fn xfer_open_restore_failed() {
    let mut broken = row(1, KEY);
    broken.restore_failed = Some("moved".into());
    let (mut twin, out) = Twin::opened(vec![broken], Default::default());
    assert_eq!((twin.stage(1), out.shows(), out.timers), (TransferStage::Failed, vec![Shown::Failed { error: "RestoreFailed".into() }], 1));
    let (reply, out) = twin.outcome(1, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Failed { error: "RestoreFailed".into() }, None)], "the outcome is Failed, not NotFound");
}

#[test]
fn xfer_open_waiting_declaration() {
    let (twin, out) = undeclared(vec![partly(1, KEY)]);
    assert_eq!((twin.stage(1), out.shows().len(), out.writes(), out.copies()), (TransferStage::WaitingDeclaration, 0, 0, 0));
    assert_eq!(out.classified(), 1);
}

#[test]
fn xfer_open_copying() {
    let mut found = row(1, KEY);
    found.copying = true;
    found.size = 0;
    let (twin, out) = Twin::opened(vec![found], Default::default());
    assert_eq!((twin.stage(1), out.copies(), out.shows(), out.round_needed()), (TransferStage::Copying, 1, vec![Shown::Copying], vec![vec![1]]));
    assert!(twin.t(1).accept_reply.is_none(), "no call waits for a restored copy");
}

#[test]
fn xfer_open_ready() {
    let (mut twin, out) = Twin::opened(vec![partly(1, KEY)], Default::default());
    assert_eq!((twin.stage(1), twin.next_step(1), out.shows(), out.round_needed(), out.timers), (TransferStage::Ready, NextStep::Part(2), vec![Shown::Queued], vec![vec![1]], 1));
    twin.online(true);
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert!(out.steps().is_empty());
    assert_eq!(twin.commit().steps().iter().map(|step| step.0).collect::<Vec<_>>(), vec![Some(2)]);
}

#[test]
fn xfer_open_pending_step() {
    let (mut twin, out) = Twin::opened(vec![row(1, KEY)], Default::default());
    assert_eq!((twin.stage(1), twin.next_step(1), out.shows(), out.round_needed(), out.steps().len()), (TransferStage::Ready, NextStep::Part(1), vec![Shown::Queued], vec![vec![1]], 0), "the opening asks for no step: the step of the saved pair runs once, after the round");
    twin.online(true);
    assert!(twin.post(TransferInput::RestoreReady { epoch: EPOCH }).steps().is_empty());
    assert_eq!(twin.commit().steps().iter().map(|step| step.0).collect::<Vec<_>>(), vec![Some(1)], "one step request, which carries the owner of its saved pair");
}

#[test]
fn xfer_open_pending_step_session_drives() {
    let mut held = row(1, KEY);
    held.pending_step = Some(55);
    let (mut twin, out) = Twin::opened(vec![held], Default::default());
    assert_eq!((twin.stage(1), out.steps().len(), out.releases().len()), (TransferStage::Ready, 0, 0), "the opening of a held pair asks for no step and releases nothing");
    twin.online(true);
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!((twin.stage(1), out.steps().len(), out.releases().len()), (TransferStage::Ready, 0, 0), "the Session drives the held pair: the Transfer starts no attempt and sends no step of its own");
    let again = twin.online(true);
    let repeated = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!((again.steps().len(), repeated.steps().len(), twin.stage(1)), (0, 0, TransferStage::Ready), "no second request while the held pair is outstanding");
}

#[test]
fn xfer_open_completing() {
    let mut found = row(1, KEY);
    found.confirmed_parts = 2;
    let (mut twin, out) = Twin::opened(vec![found], Default::default());
    assert_eq!((twin.stage(1), twin.next_step(1), out.shows(), out.round_needed()), (TransferStage::Ready, NextStep::Complete, vec![Shown::Uploading { progress: 100 }], vec![vec![1]]));
    twin.online(true);
    assert_eq!(twin.post(TransferInput::RestoreReady { epoch: EPOCH }).completes(), 0);
    assert_eq!(twin.commit().completes(), 1);
}

#[test]
fn xfer_open_saved_outcome() {
    let (mut twin, out) = Twin::opened(vec![completed(1, KEY)], outcomes(1, OutboxOutcome::Confirmed { revision: 5, result: None, row_id: None }));
    assert_eq!((twin.stage(1), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    twin.commit();
    let (reply, out) = twin.outcome(1, 1);
    assert_eq!((twin.stage(1), out.replies(reply)), (TransferStage::Done, vec![TransferAnswer::Outcome(OutboxOutcome::Confirmed { revision: 5, result: None, row_id: None }, None)]));
}

#[test]
fn xfer_open_saved_outcome_discarded() {
    let (mut twin, out) = Twin::opened(vec![completed(1, KEY)], outcomes(1, OutboxOutcome::Discarded));
    assert_eq!((twin.stage(1), out.removed()), (TransferStage::Removing, vec![KEY.to_string()]));
    twin.commit();
    let (reply, out) = twin.outcome(1, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Discarded, None)]);
}

/// xfer_open_retry_after_crash: a completed row of a later cycle with no intent and a saved outcome of a prior cycle (a crash after the Committed of a retry) reopens as PersistingBatchIntent and writes the intent of the final mutation on RestoreReady.
#[test]
fn xfer_open_retry_after_crash() {
    let mut found = completed(1, KEY);
    found.cycle_id = 2;
    let (mut twin, out) = Twin::opened(vec![found], outcomes(1, OutboxOutcome::Confirmed { revision: 5, result: None, row_id: None }));
    assert_eq!((twin.stage(1), out.shows()), (TransferStage::PersistingBatchIntent, vec![Shown::Uploading { progress: 100 }]));
    let ready = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!((ready.writes(), ready.batches.first().map(|batch| batch.intents)), (1, Some(1)), "RestoreReady writes the intent of the final mutation: {ready:?}");
}

/// xfer_open_saved_interrupted: a completed row whose saved outcome of the current cycle is Ended restores no live transfer; the Outcome call returns that interrupted outcome with no revision.
#[test]
fn xfer_open_saved_interrupted() {
    let cause = crate::owners::read::deliveries::EndCause::SessionEnded;
    let (mut twin, out) = Twin::opened(vec![completed(1, KEY)], outcomes(1, OutboxOutcome::Ended(cause.clone())));
    assert!(out.batches.is_empty() && out.shows().is_empty() && out.outbox.is_empty() && out.timers == 0, "a saved interrupted outcome restores no live transfer: no batch, no status, no network, only RestoreClassified: {out:?}");
    assert_eq!(twin.stage(1), TransferStage::Done, "the cycle already ended");
    let (reply, out) = twin.outcome(1, 1);
    assert_eq!(out.replies(reply), vec![TransferAnswer::Outcome(OutboxOutcome::Ended(cause), None)], "the Outcome call gets the saved interrupted outcome, no revision");
}

#[test]
fn xfer_open_saved_outcome_rejected() {
    let (mut twin, out) = Twin::opened(vec![completed(1, KEY)], outcomes(1, OutboxOutcome::Rejected { error: "refused".into() }));
    assert_eq!((twin.stage(1), out.kinds()), (TransferStage::PersistingFailure, vec![UploadKind::Failed]));
    let out = twin.commit();
    assert_eq!((twin.stage(1), out.shows()), (TransferStage::Failed, vec![Shown::Failed { error: "refused".into() }]));
}

#[test]
fn xfer_open_mutating() {
    let mut found = completed(1, KEY);
    found.has_intent = true;
    let (twin, out) = Twin::opened(vec![found], Default::default());
    assert_eq!((twin.stage(1), out.submits(), out.writes(), out.shows()), (TransferStage::Mutating, 0, 0, vec![Shown::Uploading { progress: 100 }]), "the outcome comes from Outbox");
}

/// xfer_calls_held_cancel_wins (K1, D-221 F16, transfer.md:467): a Cancel held during restore replays on
/// RestoreReady against the state the transfer was classified into (Ready). With the network up, the Cancel must
/// be applied before any attempt starts: the transfer goes to Removing, no network request ever starts and the
/// cancel reply is Ok. Prod runs start_in_order before replay_held, so the Ready transfer moves to
/// PersistingAttempt first and the held Cancel then meets it with Err(InFlight), while the committed attempt
/// starts the network the caller cancelled before the open finished.
#[test]
fn xfer_calls_held_cancel_wins() {
    let mut twin = Twin::bare(1);
    twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
    let reply = twin.reply();
    let held = twin.post(TransferInput::Cancel { key: KEY.into(), reply });
    assert!(Twin::quiet(&held), "a call during Restoring is held: {held:?}");
    twin.post(TransferInput::Restored { uploads: vec![partly(1, KEY)], outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
    twin.online(true);
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert_eq!(
        (twin.stage(1), out.removed(), Twin::fault(&out, reply)),
        (TransferStage::Removing, vec![KEY.to_string()], None),
        "the held cancel wins over the restore start: Removing, no attempt, no InFlight: {out:?}",
    );
}

#[test]
fn xfer_open_failed() {
    let mut found = partly(1, KEY);
    found.failed = Some("e".into());
    let (twin, out) = Twin::opened(vec![found], Default::default());
    assert_eq!((twin.stage(1), out.shows(), out.timers), (TransferStage::Failed, vec![Shown::Failed { error: "e".into() }], 1));
}

#[test]
fn xfer_open_missing_file() {
    let mut found = partly(1, KEY);
    found.file_present = false;
    let (mut twin, out) = Twin::opened(vec![found], Default::default());
    assert_eq!((twin.stage(1), out.kinds(), out.shows().len()), (TransferStage::PersistingFailure, vec![UploadKind::Failed], 0));
    let out = twin.commit();
    assert_eq!(out.shows(), vec![Shown::Failed { error: "missing_file".into() }]);
}

#[test]
fn xfer_open_completed_no_intent() {
    let (twin, out) = Twin::opened(vec![completed(1, KEY)], Default::default());
    assert_eq!((twin.stage(1), out.submits(), out.shows()), (TransferStage::Failed, 0, vec![Shown::Failed { error: "RestoreFailed".into() }]));
}

#[test]
fn xfer_open_saved_outcome_failed() {
    let (mut twin, out) = Twin::opened(vec![completed(1, KEY)], outcomes(1, OutboxOutcome::Failed { error: "server".into() }));
    assert_eq!((twin.stage(1), out.submits(), out.shows()), (TransferStage::PersistingFailure, 0, Vec::new()));
    assert_eq!(out.rows()[0].0.failed.as_deref(), Some("server"));
    assert_eq!(twin.commit().shows(), vec![Shown::Failed { error: "server".into() }]);
}

#[test]
fn xfer_open_saved_outcome_of_a_removed_transfer_is_readable() {
    let (mut twin, _) = Twin::opened(Vec::new(), BTreeMap::from([(7, OutboxOutcome::Discarded)]));
    let (reply, out) = twin.outcome(7, 1);
    assert!(matches!(out.replies(reply).as_slice(), [TransferAnswer::Outcome(OutboxOutcome::Discarded, _)]), "the receipt of a cycle reads its outcome after the removal and the restart (D-237): {:?}", out.replies(reply));
}
