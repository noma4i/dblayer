//! `DeliveryIo` on `DetScheduler` with a real SQLite file in WAL mode. Requirement -> observed value -> assert:
//! delivery_io_read_sees_commit: the read of revision N sees the rows of the commit N, a waiting `Pull` settles its outcome with the first `Snapshot`.
//! delivery_io_read_older_snapshot: a read of a revision the file does not hold yet gives `ReadFailed`, not rows.

use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};

use super::deliveries::{self as d, Deliveries, DeliveryInput, Kind, PullAnswer, Value, View};
use super::rig::{collector, pump, runtime, saw_input};
use super::feed::Feed;
use super::windows::{WindowKey, Windows};
use super::{DeliveryIo, SnapshotStatements, SqliteSnapshots};
use crate::ddl::{create_model_tables, manifest_write, migrate_core, REVISION_SEQ_KEY};
use crate::owners::contract::{WindowPhase, WindowState};
use crate::query::{compile_keys, CompiledStatement, Query};
use crate::owners::support::{new_log, Discard, Log, Sink};
use crate::runtime::{Address, DetScheduler, EpochStamp, Outcome, Outcomes, Waiter};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

const EPOCH: u64 = 1;
const STREAM: u64 = 7;

fn people() -> ModelSpec { ModelSpec::new("people", vec![FieldSpec::scalar("id", ScalarKind::Id)]) }

fn view() -> View { View { kind: Kind::List, model: "people".into(), tables: vec!["row_people".into()], local: false, remote: true, live: false, reload: false, window: WindowKey { model: "people".into(), name: "list".into() }, subject: String::new() } }

struct People;

impl SnapshotStatements for People {
    fn statement(&self, _view: &View) -> Result<CompiledStatement, String> { compile_keys(&Query::new("people"), &people()).map_err(|error| error.to_string()) }
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

fn rig() -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.db");
    let writer = Connection::open(&path).unwrap();
    migrate_core(&writer).unwrap();
    create_model_tables(&writer, &[people()]).unwrap();
    let runtime = runtime();
    let answers = Outcomes::new();
    let (rest, take_rest) = collector();
    let io = DeliveryIo::new(Arc::new(SqliteSnapshots::new(path, Arc::new(People))), answers.clone(), runtime.handle().clone(), take_rest);
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
    rig.det.post(rig.owner, EpochStamp::Epoch(EPOCH), DeliveryInput::Open { stream: STREAM, view: view(), epoch: EPOCH, identity: true, valid: true, now: 1_000_000, revision });
    rig.det.post(rig.owner, EpochStamp::Epoch(EPOCH), DeliveryInput::Committed(crate::owners::contract::Committed { batch: 0, revision, tables: vec!["row_people".into()], models: Default::default(), stale_parents: Vec::new(), position: 0 }));
    let loaded = WindowState { phase: WindowPhase::Loaded, error: None, loaded: Some(revision), has_more: false, settled: true, total: None };
    rig.det.post(rig.owner, EpochStamp::Epoch(EPOCH), DeliveryInput::WindowState { stream: STREAM, state: loaded });
}

fn pull(rig: &mut Rig) -> Waiter<PullAnswer> {
    let waiter = rig.answers.register(EPOCH);
    rig.det.post(rig.owner, EpochStamp::Control, DeliveryInput::Pull { stream: STREAM, outcome: waiter.id() });
    waiter
}

#[test]
fn delivery_io_read_sees_commit() {
    let mut rig = rig();
    commit(&rig.writer, "p1", 1);
    commit(&rig.writer, "p2", 2);
    open(&mut rig, 2);
    let mut waiter = pull(&mut rig);
    let owner = rig.owner;
    let mut taken = None;
    pump(&mut rig.det, owner, |_| {
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
    pump(&mut rig.det, owner, |det| saw_input(det, "ReadFailed") > 0);
    let journal = rig.det.state(owner).unwrap().journal(STREAM);
    let Some(Value::Snapshot { rows, state, .. }) = journal.first() else { panic!("a failed first read gives a Snapshot without rows: {journal:?}") };
    assert!(rows.is_empty(), "no rows of a snapshot older than the revision");
    assert_eq!(state.phase, WindowPhase::Failed);
    assert!(state.error.as_ref().is_some_and(|error| error.to_string().contains("older than 2")), "the error of the read reaches the stream: {state:?}");
}
