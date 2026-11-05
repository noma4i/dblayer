//! `DeliveryIo` on `DetScheduler` with a real SQLite file in WAL mode. Requirement -> observed value -> assert:
//! delivery_io_read_sees_commit: the read of revision N sees the rows of the commit N, the first `Snapshot` has them and wakes the sink.
//! delivery_io_read_older_snapshot: a read of a revision the file does not hold yet gives `ReadFailed`, not rows.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection};

use super::deliveries::{self as d, Deliveries, DeliveryInput, Kind, Value, View};
use super::rig::{collector, pump, runtime, saw_input};
use super::{DeliveryIo, SnapshotStatements, SqliteSnapshots, StreamTable};
use crate::ddl::{create_model_tables, manifest_write, migrate_core, REVISION_SEQ_KEY};
use crate::ffi::ValueSink;
use crate::owners::contract::{WindowPhase, WindowState};
use crate::query::{compile_keys, CompiledStatement, Query};
use crate::runtime::{Address, DetScheduler, EpochStamp};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

const EPOCH: u64 = 1;
const STREAM: u64 = 7;

fn people() -> ModelSpec { ModelSpec::new("people", vec![FieldSpec::scalar("id", ScalarKind::Id)]) }

fn view() -> View { View { kind: Kind::List, model: "people".into(), tables: vec!["row_people".into()], local: false } }

struct People;

impl SnapshotStatements for People {
    fn statement(&self, _view: &View) -> Result<CompiledStatement, String> { compile_keys(&Query::new("people"), &people()).map_err(|error| error.to_string()) }
}

#[derive(Default)]
struct Heard(AtomicUsize);

impl ValueSink for Heard {
    fn available(&self) { self.0.fetch_add(1, Ordering::SeqCst); }
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
    heard: Arc<Heard>,
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
    let heard = Arc::new(Heard::default());
    let table = StreamTable::default();
    let sink: Arc<dyn ValueSink> = heard.clone();
    table.register(STREAM, view(), sink);
    let (rest, take_rest) = collector();
    let io = DeliveryIo::new(Arc::new(SqliteSnapshots::new(path, Arc::new(People))), table, runtime.handle().clone(), take_rest);
    let mut det = DetScheduler::new(1);
    let owner = det.add::<Deliveries>("deliveries", d::DeliveriesState::new(EPOCH), io);
    det.start_epoch(EPOCH);
    Rig { det, owner, heard, rest, writer, _dir: dir, _runtime: runtime }
}

fn open(rig: &mut Rig, revision: i64) {
    rig.det.post(rig.owner, EpochStamp::Epoch(EPOCH), DeliveryInput::Open { stream: STREAM, view: view(), epoch: EPOCH, revision, identity: true, valid: true });
    let loaded = WindowState { phase: WindowPhase::Loaded, error: None, loaded: Some(revision) };
    rig.det.post(rig.owner, EpochStamp::Epoch(EPOCH), DeliveryInput::WindowState { stream: STREAM, state: loaded });
}

#[test]
fn delivery_io_read_sees_commit() {
    let mut rig = rig();
    commit(&rig.writer, "p1", 1);
    commit(&rig.writer, "p2", 2);
    open(&mut rig, 2);
    let owner = rig.owner;
    pump(&mut rig.det, owner, |det| det.state(owner).is_some_and(|state| state.journal_len(STREAM) > 0));
    let journal = rig.det.state(owner).unwrap().journal(STREAM);
    let Some(Value::Snapshot { revision, rows, .. }) = journal.first() else { panic!("the first value is a Snapshot: {journal:?}") };
    assert_eq!(*revision, 2);
    assert_eq!(rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), ["p1", "p2"]);
    assert_eq!(rig.heard.0.load(Ordering::SeqCst), 1, "the sink heard that a value waits");
    assert!(!rig.rest.lock().unwrap().is_empty(), "the window interest of a List goes to the effects that are no IO");
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
    assert!(state.error.as_deref().is_some_and(|error| error.contains("older than 2")), "the error of the read reaches the stream: {state:?}");
    assert_eq!(rig.heard.0.load(Ordering::SeqCst), 0);
}
