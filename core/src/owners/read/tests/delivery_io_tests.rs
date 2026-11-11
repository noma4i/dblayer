//! `DeliveryIo` on `DetScheduler` with a real SQLite file in WAL mode. Requirement -> observed value -> assert:
//! delivery_io_read_sees_commit: the read of revision N sees the rows of the commit N, a waiting `Pull` settles its outcome with the first `Snapshot`.
//! delivery_io_read_older_snapshot: a read of a revision the file does not hold yet gives `ReadFailed`, not rows.

use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};

use super::deliveries::{self as d, Deliveries, DeliveryInput, Kind, PullAnswer, Value, View};
use super::rig::{collector, pump, runtime};
use super::feed::Feed;
use super::windows::{WindowKey, Windows};
use super::{DeliveryIo, Snapshots};
use crate::store::ddl::{create_model_tables, manifest_read, manifest_write, migrate_core, REVISION_SEQ_KEY};
use crate::owners::contract::{WindowPhase, WindowState};
use crate::store::query::{compile_keys, fetch_keys, Query};
use crate::spec::support::{new_log, Discard, Log, Sink};
use crate::runtime::{Address, DetScheduler, EpochStamp, Outcome, Outcomes, Waiter};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

const EPOCH: u64 = 1;
const STREAM: u64 = 7;

fn people() -> ModelSpec { ModelSpec::new("people", vec![FieldSpec::scalar("id", ScalarKind::Id)]) }

fn view() -> View { View { kind: Kind::List, model: "people".into(), tables: vec!["row_people".into()], local: false, remote: true, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: "people".into(), name: "list".into() }, subject: String::new(), deferred_rows: Vec::new(), query: None } }

struct SqliteSnapshots {
    path: std::path::PathBuf,
}

impl Snapshots for SqliteSnapshots {
    fn read(&self, _: &View, revision: i64, _: i64, mode: d::ReadMode) -> Result<(d::ReadVersion, d::ReadData), String> {
        if !matches!(mode, d::ReadMode::Snapshot { .. }) { return Err("the rig keeps no journal of changes".into()); }
        let statement = compile_keys(&Query::new("people"), &people()).map_err(|error| error.to_string())?;
        let connection = Connection::open_with_flags(&self.path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|error| error.to_string())?;
        let snapshot = connection.unchecked_transaction().map_err(|error| error.to_string())?;
        let stored = manifest_read(&connection, REVISION_SEQ_KEY).map_err(|error| error.to_string())?;
        let seen = stored.map_or(Ok(0), |text| text.parse::<i64>().map_err(|error| error.to_string()))?;
        if seen < revision {
            return Err(format!("the snapshot is at revision {seen}, older than {revision}"));
        }
        let rows = fetch_keys(&connection, &statement).map_err(|error| error.to_string())?.into_iter().map(|(id, stamp)| d::StreamRow { id, stamp, row: None }).collect();
        drop(snapshot);
        Ok((d::ReadVersion { revision: seen, position: 0, relevant: Some((seen, 0)) }, d::ReadData::Rows { rows, replaced: Vec::new(), total: None }))
    }
}

/// Commit `revision`: the row and the revision of the manifest in one transaction, like the writer of the Store.
fn commit(connection: &Connection, id: &str, revision: i64) {
    let transaction = connection.unchecked_transaction().unwrap();
    connection.execute("INSERT INTO row_people (id, revision, born_rev) VALUES (?1, ?2, ?2)", params![id, revision]).unwrap();
    manifest_write(connection, REVISION_SEQ_KEY, &revision.to_string()).unwrap();
    transaction.commit().unwrap();
}

struct Rig {
    det: DetScheduler,
    owner: Address<Deliveries>,
    answers: Outcomes<PullAnswer>,
    windows: Log<String>,
    rest: Arc<Mutex<Vec<d::DeliveryEffect>>>,
    writer: Connection,
    _dir: tempfile::TempDir,
    _runtime: tokio::runtime::Runtime,
}

fn rig() -> Rig { rig_with(|path| Arc::new(SqliteSnapshots { path })) }

fn rig_with(snapshots: impl FnOnce(std::path::PathBuf) -> Arc<dyn Snapshots>) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.db");
    let writer = Connection::open(&path).unwrap();
    migrate_core(&writer).unwrap();
    create_model_tables(&writer, &[people()]).unwrap();
    let runtime = runtime();
    let answers = Outcomes::new();
    let (rest, take_rest) = collector();
    let io = DeliveryIo::new(snapshots(path), answers.clone(), runtime.handle().clone(), take_rest);
    let mut det = DetScheduler::new(1);
    let windows = new_log();
    let windows_slot = det.add::<Sink<Windows>>("windows", windows.clone(), Discard);
    let feed_slot = det.add::<Sink<Feed>>("feed", new_log(), Discard);
    let peers = d::DeliveryPeers { windows: Address::<Windows>::new(windows_slot.index()), feed: Address::<Feed>::new(feed_slot.index()) };
    let owner = det.add::<Deliveries>("deliveries", d::DeliveriesState::new(EPOCH, peers), io);
    det.start_epoch(EPOCH);
    Rig { det, owner, answers, windows, rest, writer, _dir: dir, _runtime: runtime }
}

fn open(rig: &mut Rig, revision: i64) {
    rig.det.post(rig.owner, EpochStamp::Epoch(EPOCH), DeliveryInput::Open { stream: STREAM, view: view(), epoch: EPOCH, identity: true, valid: true, revision, deferred: None });
    rig.det.post(rig.owner, EpochStamp::Epoch(EPOCH), DeliveryInput::Committed(crate::owners::contract::Committed { batch: 0, revision, tables: vec!["row_people".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    let loaded = WindowState { phase: WindowPhase::Loaded, error: None, loaded: Some(revision), has_more: false, settled: true, total: None };
    rig.det.post(rig.owner, EpochStamp::Epoch(EPOCH), DeliveryInput::WindowState { stream: STREAM, state: loaded });
}

fn pull(rig: &mut Rig) -> Waiter<PullAnswer> {
    let waiter = rig.answers.register(EPOCH);
    rig.det.post(rig.owner, EpochStamp::Control, DeliveryInput::Pull { stream: STREAM, outcome: waiter.id(), reader: 1 });
    waiter
}

#[test]
fn delivery_io_read_sees_commit() {
    let mut rig = rig();
    commit(&rig.writer, "p1", 1);
    commit(&rig.writer, "p2", 2);
    open(&mut rig, 2);
    let mut waiter = pull(&mut rig);
    let mut taken = None;
    pump(&mut rig.det, |_| {
        taken = taken.take().or_else(|| waiter.try_take());
        taken.is_some()
    });
    let Some(Outcome::Value(PullAnswer::Value(first))) = taken else { panic!("the first value is a value: {taken:?}") };
    let Value::Snapshot { revision, rows, .. } = *first else { panic!("the first value is a Snapshot") };
    assert_eq!(revision, 2);
    assert_eq!(rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), ["p1", "p2"]);
    assert!(rig.windows.lock().unwrap().iter().any(|input| input.starts_with("InterestPlus")), "the window interest of a List is an input of the Windows");
    assert!(rig.rest.lock().unwrap().is_empty(), "nothing but IO leaves the runner");
}

#[test]
fn delivery_io_read_older_snapshot() {
    let mut rig = rig();
    commit(&rig.writer, "p1", 1);
    open(&mut rig, 2);
    let owner = rig.owner;
    pump(&mut rig.det, |det| det.state(owner).is_some_and(|state| !state.journal(STREAM).is_empty()));
    let journal = rig.det.state(owner).unwrap().journal(STREAM);
    let Some(Value::Snapshot { rows, state, .. }) = journal.first() else { panic!("a failed first read gives a Snapshot without rows: {journal:?}") };
    assert!(rows.is_empty(), "no rows of a snapshot older than the revision");
    assert_eq!(state.phase, WindowPhase::Failed);
    assert!(state.error.as_ref().is_some_and(|error| error.to_string().contains("older than 2")), "the error of the read reaches the stream: {state:?}");
}

struct Panicking;

impl Snapshots for Panicking {
    fn read(&self, _: &View, _: i64, _: i64, _: d::ReadMode) -> Result<(d::ReadVersion, d::ReadData), String> { panic!("the snapshot read failed") }
}

#[test]
fn rt_prop_task_panic_fails() {
    use super::window_io_tests::{panicked_read, panicked_request, read_id, request_id};
    let requested = panicked_request();
    assert!((0..4).any(|attempt| requested == [request_id(1, attempt)]), "the query task reports its request: {requested:?}");
    assert_eq!(panicked_read(), [read_id(1)], "the read task reports its read");
    let mut rig = rig_with(|_| Arc::new(Panicking));
    open(&mut rig, 1);
    let owner = rig.owner;
    pump(&mut rig.det, |det| det.trace().iter().filter(|entry| entry.input.starts_with("TaskFailed")).count() == 2 && det.state(owner).is_some_and(|state| !state.journal(STREAM).is_empty()));
    let failed: Vec<String> = rig.det.trace().iter().filter(|entry| entry.input.starts_with("TaskFailed")).map(|entry| entry.input.clone()).collect();
    let inputs: Vec<&str> = rig.det.trace().iter().map(|entry| entry.input.as_str()).collect();
    let mut failed = failed;
    failed.sort();
    assert_eq!(failed, [1, 2].map(|read| format!("TaskFailed {{ id: {} }}", d::read_task(STREAM, read))), "each panicked read reports its stream and read, inputs {inputs:?}");
    let journal = rig.det.state(owner).unwrap().journal(STREAM);
    let Some(Value::Snapshot { rows, state, .. }) = journal.first() else { panic!("a panicked first read gives a Snapshot without rows: {journal:?}") };
    assert!(rows.is_empty() && state.phase == WindowPhase::Failed, "the stream ends its wait with an error: {state:?}");
}
