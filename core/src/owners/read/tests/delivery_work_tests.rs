//! P-DLV-23 on a real Store: the owner `Deliveries` and the reads of `DbSnapshots`, stepped in one thread.
//! dlv_prop_work_by_affected: a commit that changed 1 row of 1 stream costs the same streams examined and rows read at N and 2N open streams and at windows of M and 2M rows.

use std::sync::{Arc, Mutex};

use serde_json::json;

use super::deliveries::{self as d, Deliveries, DeliveriesState, DeliveryEffect, DeliveryInput, DeliveryPeers, Kind, StreamEffect, StreamId, View};
use super::windows::{WindowKey, Windows};
use super::feed::Feed;
use super::{DbSnapshots, Snapshots};
use crate::store::ddl::table_name;
use crate::owners::contract::{ChangedRows, Committed, CommittedBroadcast};
use crate::store::query::Query;
use crate::runtime::{Address, Effect, Outcomes, StateOwner};
use crate::schema::{FieldSpec, ModelSpec};
use crate::store::fixtures::put;
use crate::store::Store;
use crate::value::ScalarKind;

const EPOCH: u64 = 1;

type Notice = CommittedBroadcast;

fn model(index: usize) -> String { format!("t{index}") }

fn spec(index: usize) -> ModelSpec { ModelSpec::new(&model(index), vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]) }

fn view(index: usize) -> View { view_of(&Query::new(&model(index))) }

fn view_of(query: &Query) -> View {
    let name = query.model.as_str().to_string();
    View { kind: Kind::List, tables: vec![table_name(&name)], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: name.clone(), name: "list".into() }, subject: String::new(), node: None, wants: Vec::new(), deferred_rows: Vec::new(), query: Some(std::sync::Arc::new(query.clone())), model: name }
}

struct PendingRead {
    stream: StreamId,
    read_id: u64,
    revision: i64,
    after: i64,
    view: View,
    mode: d::ReadMode,
}

struct Work {
    db: Arc<Store>,
    snapshots: DbSnapshots,
    state: DeliveriesState,
    notices: Arc<Mutex<Vec<Notice>>>,
}

impl Work {
    fn new(models: usize) -> Self {
        let db = Arc::new(Store::open_models(None, (0..models).map(spec).collect(), std::sync::Arc::new(crate::runtime::SystemClock)).unwrap());
        let notices = Arc::new(Mutex::new(Vec::new()));
        let sink = notices.clone();
        db.on_commit(move |notice| sink.lock().unwrap().push(notice)).unwrap();
        let peers = DeliveryPeers { windows: Address::<Windows>::new(0), feed: Address::<Feed>::new(1) };
        Self { snapshots: DbSnapshots::new(db.clone()), db, state: DeliveriesState::new(EPOCH, peers), notices }
    }

    fn feed(&mut self, input: DeliveryInput) {
        let mut queue = vec![input];
        while let Some(input) = queue.pop() {
            queue.extend(self.step(input).into_iter().map(|read| self.answer(read)));
        }
    }

    fn step(&mut self, input: DeliveryInput) -> Vec<PendingRead> {
        Deliveries::step(&mut self.state, input, crate::runtime::Time(0)).into_iter().filter_map(|effect| match effect {
            Effect::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::Read { read_id, revision, after, view, mode } }) => Some(PendingRead { stream, read_id, revision, after, view, mode }),
            _ => None,
        }).collect()
    }

    fn answer(&self, read: PendingRead) -> DeliveryInput {
        let (version, data) = self.snapshots.read(&read.view, read.revision, read.after, read.mode).unwrap();
        DeliveryInput::ReadDone { stream: read.stream, read_id: read.read_id, revision: version.revision, position: version.position, relevant: version.relevant, touching: version.touching, data }
    }

    fn put(&self, index: usize, id: &str, text: &str) { put(&self.db, &model(index), json!({"id": id, "text": text})); }

    fn commit(&mut self) {
        let notice = self.notices.lock().unwrap().pop().expect("the write announced its commit");
        self.feed(DeliveryInput::Committed(notice.for_deliveries()));
    }

    fn pull(&mut self, stream: StreamId) { self.feed(DeliveryInput::Pull { stream, outcome: Outcomes::<d::PullAnswer>::new().register(EPOCH).id(), reader: 1 }); }
}

fn entity_view(index: usize, row: &str) -> View {
    let name = model(index);
    View { kind: Kind::Entity { fetch: d::Fetch::Never }, tables: vec![table_name(&name)], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: name.clone(), name: "entity".into() }, subject: row.into(), node: None, wants: Vec::new(), deferred_rows: Vec::new(), query: None, model: name }
}

fn work_of_one_commit(streams: usize, window: usize, others: usize) -> (u64, usize) {
    let mut work = Work::new(streams);
    for row in 0..window {
        work.put(0, &format!("r{row}"), "old");
    }
    for index in 1..streams {
        work.put(index, "r0", "old");
    }
    for index in 0..streams {
        work.feed(DeliveryInput::Open { stream: index as StreamId + 1, view: view(index), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    }
    work.feed(DeliveryInput::Open { stream: 999, view: entity_view(0, "r0"), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    for other in 0..others {
        work.feed(DeliveryInput::Open { stream: 1000 + other as StreamId, view: entity_view(0, &format!("x{other}")), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    }
    work.pull(1);
    let (examined, read) = (work.state.streams_examined(), work.db.counters().rows_read);

    work.put(0, "r0", "new");
    work.commit();
    let delta = match work.state.journal(1).last() {
        Some(d::Value::Delta(delta)) => delta.clone(),
        other => panic!("the commit delivered no Delta to the stream of the row: {other:?}"),
    };
    assert_eq!((delta.edits.iter().filter_map(|edit| match edit { d::Edit::Rewrite { row, .. } => Some(row.id.as_str()), _ => None }).collect::<Vec<_>>(), delta.size as usize), (vec!["r0"], window), "the Delta rewrites the changed row and gives the size of the window");
    (work.state.streams_examined() - examined, work.db.counters().rows_read - read)
}

#[test]
fn dlv_pull_unknown_stream() {
    let mut work = Work::new(1);
    let outcome = Outcomes::<d::PullAnswer>::new().register(EPOCH).id();
    let effects = Deliveries::step(&mut work.state, DeliveryInput::Pull { stream: 77, outcome, reader: 1 }, crate::runtime::Time(0));
    let answer = d::PullAnswer::Value(Box::new(d::Value::Ended(d::EndCause::Cancelled)));
    assert!(matches!(effects.as_slice(), [Effect::Run(DeliveryEffect::Stream { stream: 77, effect: StreamEffect::Answer { answer: got, .. } })] if *got == answer), "a Pull of a stream that is gone ends as Cancelled at once: {effects:?}");
}

#[test]
fn dlv_cancel_unknown_stream() {
    let mut work = Work::new(1);
    let effects = Deliveries::step(&mut work.state, DeliveryInput::Release { stream: 77, reader: None }, crate::runtime::Time(0));
    assert!(effects.is_empty(), "a Release of a stream that is gone does nothing: {effects:?}");
    let effects = Deliveries::step(&mut work.state, DeliveryInput::PullCancelled { stream: 77, outcome: Outcomes::<d::PullAnswer>::new().register(EPOCH).id() }, crate::runtime::Time(0));
    assert!(effects.is_empty(), "a PullCancelled of a stream that is gone does nothing: {effects:?}");
}

#[test]
fn dlv_prop_work_by_affected() {
    let (examined, read) = work_of_one_commit(2, 20, 4);
    assert_eq!(examined, 2, "the list stream and the Entity stream of the changed row are examined");
    assert_eq!(work_of_one_commit(4, 20, 8), (examined, read), "2N open streams and 2N Entity streams of other rows of the same table");
    assert_eq!(work_of_one_commit(2, 40, 4), (examined, read), "a window of 2M rows");
}

#[test]
fn store_read_view_relevant_version() {
    let mut work = Work::new(2);
    work.put(0, "r0", "before");
    work.notices.lock().unwrap().clear();
    work.feed(DeliveryInput::Open { stream: 1, view: view(0), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    work.pull(1);
    work.put(0, "r0", "five");
    let first = work.notices.lock().unwrap().pop().unwrap();
    let read = work.step(DeliveryInput::Committed(first.for_deliveries())).pop().expect("the first commit starts a read");
    work.put(0, "r0", "six");
    let relevant = work.notices.lock().unwrap().pop().unwrap();
    work.put(1, "other", "unrelated");
    let unrelated = work.notices.lock().unwrap().pop().unwrap();
    assert!(unrelated.revision > relevant.revision);
    let done = work.answer(read);
    work.feed(done);
    let journal = work.state.journal(1);
    let delta = match journal.last() { Some(d::Value::Delta(delta)) => delta, other => panic!("expected a delta, got {other:?}") };
    assert!(delta.edits.iter().any(|edit| match edit {
        d::Edit::Rewrite { row, .. } => row.row.as_ref().is_some_and(|wire| wire.cells.iter().any(|cell| matches!(cell, crate::ffi::wire::Cell::Text { value } if value == "six"))),
        _ => false,
    }), "the snapshot contains the second commit");
    assert_eq!(delta.revision, i64::try_from(relevant.revision).unwrap(), "an unrelated commit must not change the revision of the view");
    assert!(work.step(DeliveryInput::Committed(relevant.for_deliveries())).is_empty(), "a late notice already in the snapshot starts no read");
}

#[test]
fn store_relevant_version_includes_deferred_parent() {
    let mut work = Work::new(2);
    work.put(0, "r0", "root");
    work.notices.lock().unwrap().clear();
    let deferred = d::Deferred { temps: vec![(model(1), "temp-00000000-0000-0000-0000-310000000000".into())], rebind: d::Rebind::new(|| None) };
    let read = work.step(DeliveryInput::Open { stream: 1, view: view(0), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: Some(deferred) }).pop().unwrap();
    let done = work.answer(read);
    work.feed(done);
    work.pull(1);
    work.put(1, "temp-00000000-0000-0000-0000-310000000000", "parent");
    let notice = work.notices.lock().unwrap().pop().unwrap();
    let revision = i64::try_from(notice.revision).unwrap();
    let read = work.step(DeliveryInput::Committed(notice.for_deliveries())).pop().expect("the deferred parent wakes the stream");
    let (version, _) = work.snapshots.read(&read.view, read.revision, read.after, read.mode).unwrap();
    assert_eq!(version.relevant, Some((revision, 0)), "the same parent change is relevant in the journal");
}

#[test]
fn inv_observe_16_a_row_that_cannot_enter_the_list_costs_no_read_and_no_delivery() {
    let mut work = Work::new(1);
    work.put(0, "a", "keep");
    let query = Query::new(&model(0)).filtered(crate::store::query::Where::equal("text", json!("keep")));
    work.feed(DeliveryInput::Open { stream: 1, view: view_of(&query), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    work.pull(1);
    let cost = |work: &Work| (work.db.counters().rows_read, work.state.journal_len(1));

    let before = cost(&work);
    work.put(0, "b", "skip");
    work.commit();
    assert_eq!(cost(&work), before, "a row outside the filter is not read and not delivered");

    work.put(0, "c", "keep");
    work.commit();
    assert_eq!(cost(&work), (before.0 + 1, before.1 + 1), "a row inside the filter is read once and delivered once");
}

#[test]
fn dlv_streams_of_one_commit_deliver_independently() {
    let mut work = Work::new(1);
    work.put(0, "r0", "old");
    let marked = Query::new(&model(0)).filtered(crate::store::query::Where::equal("text", json!("new")));
    work.feed(DeliveryInput::Open { stream: 1, view: view(0), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    work.feed(DeliveryInput::Open { stream: 2, view: view_of(&marked), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
    work.pull(1);
    work.pull(2);
    let (all_before, marked_before) = (work.state.journal_len(1), work.state.journal_len(2));

    work.put(0, "r0", "new");
    let notice = work.notices.lock().unwrap().pop().expect("the write announced its commit");
    let reads = work.step(DeliveryInput::Committed(notice.for_deliveries()));
    assert_eq!(reads.iter().map(|read| read.stream).collect::<Vec<_>>(), [1, 2], "one commit asks both streams to read");
    let mut reads = reads.into_iter();
    let (all_read, marked_read) = (reads.next().unwrap(), reads.next().unwrap());

    let answered = work.answer(marked_read);
    work.feed(answered);
    assert_eq!((work.state.journal_len(2), work.state.journal_len(1)), (marked_before + 1, all_before), "the marked stream shows the change while the list of all rows has not delivered it yet");
    let answered = work.answer(all_read);
    work.feed(answered);
    assert_eq!(work.state.journal_len(1), all_before + 1, "the list of all rows delivers it with its own read");
}

#[test]
fn dlv_committed_rows_select_the_entity_streams_of_those_rows() {
    let mut work = Work::new(1);
    work.put(0, "a", "old");
    work.put(0, "b", "old");
    for (stream, row) in [(1, "a"), (2, "b")] {
        work.feed(DeliveryInput::Open { stream, view: entity_view(0, row), epoch: EPOCH, identity: true, valid: true, revision: 0, deferred: None });
        work.pull(stream);
    }
    let table = table_name(&model(0));
    let committed = |revision: i64, rows: ChangedRows| DeliveryInput::Committed(Committed { rows, ..crate::spec::support::committed_at(0, revision, &[table.as_str()]) });
    let examined = work.state.streams_examined();

    let reads = work.step(committed(100, Some([(table.clone(), "b".to_string())].into())));
    assert_eq!(reads.iter().map(|read| read.stream).collect::<Vec<_>>(), [2], "only the Entity stream of the changed row reads");
    assert_eq!(work.state.streams_examined() - examined, 1, "the other Entity stream of the table is not examined");

    let reads = work.step(committed(101, None));
    assert_eq!(reads.iter().map(|read| read.stream).collect::<Vec<_>>(), [1, 2], "a commit without a row list reads every Entity stream of its tables");
}
