//! P-DLV-23 on a real Store: the owner `Deliveries` and the reads of `DbSnapshots`, stepped in one thread.
//! dlv_prop_work_by_affected: a commit that changed 1 row of 1 stream costs the same streams examined and rows read at N and 2N open streams and at windows of M and 2M rows.

use std::sync::{Arc, Mutex};

use serde_json::json;

use super::deliveries::{self as d, Deliveries, DeliveriesState, DeliveryEffect, DeliveryInput, DeliveryPeers, Kind, StreamEffect, StreamId, View};
use super::windows::{WindowKey, Windows};
use super::feed::Feed;
use super::{DbSnapshots, Snapshots};
use crate::store::ddl::table_name;
use crate::owners::contract::{ChangedRows, Committed, CommittedBroadcast, Models};
use crate::spec::ref_delivery_fanout::{self as r, Fanout, Stream, Watched};
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
    View { kind: Kind::List, tables: vec![table_name(&name)], local: true, remote: false, live: false, reload: false, window: WindowKey { model: name.clone(), name: "list".into() }, subject: String::new(), query: Some(std::sync::Arc::new(query.clone())), model: name }
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
        let db = Arc::new(Store::open_models(None, (0..models).map(spec).collect(), None).unwrap());
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
        Deliveries::step(&mut self.state, input).into_iter().filter_map(|effect| match effect {
            Effect::Run(DeliveryEffect::Stream { stream, effect: StreamEffect::Read { read_id, revision, after, view, mode } }) => Some(PendingRead { stream, read_id, revision, after, view, mode }),
            _ => None,
        }).collect()
    }

    fn answer(&self, read: PendingRead) -> DeliveryInput {
        let (seen, data) = self.snapshots.read(&read.view, read.revision, read.after, read.mode).unwrap();
        DeliveryInput::ReadDone { stream: read.stream, read_id: read.read_id, revision: seen, data }
    }

    fn put(&self, index: usize, id: &str, text: &str) { put(&self.db, &model(index), json!({"id": id, "text": text})); }

    fn commit(&mut self) {
        let notice = self.notices.lock().unwrap().pop().expect("the write announced its commit");
        self.feed(DeliveryInput::Committed(notice.for_deliveries()));
    }

    fn pull(&mut self, stream: StreamId) { self.feed(DeliveryInput::Pull { stream, outcome: Outcomes::<d::PullAnswer>::new().register(EPOCH).id() }); }
}

fn entity_view(index: usize, row: &str) -> View {
    let name = model(index);
    View { kind: Kind::Entity { fetch: d::Fetch::Never }, tables: vec![table_name(&name)], local: true, remote: false, live: false, reload: false, window: WindowKey { model: name.clone(), name: "entity".into() }, subject: row.into(), query: None, model: name }
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
        work.feed(DeliveryInput::Open { stream: index as StreamId + 1, view: view(index), epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
    }
    work.feed(DeliveryInput::Open { stream: 999, view: entity_view(0, "r0"), epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
    for other in 0..others {
        work.feed(DeliveryInput::Open { stream: 1000 + other as StreamId, view: entity_view(0, &format!("x{other}")), epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
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
fn dlv_prop_work_by_affected() {
    let (examined, read) = work_of_one_commit(2, 20, 4);
    assert_eq!(examined, 2, "the list stream and the Entity stream of the changed row are examined");
    assert_eq!(work_of_one_commit(4, 20, 8), (examined, read), "2N open streams and 2N Entity streams of other rows of the same table");
    assert_eq!(work_of_one_commit(2, 40, 4), (examined, read), "a window of 2M rows");
}

#[test]
fn inv_observe_16_a_row_that_cannot_enter_the_list_costs_no_read_and_no_delivery() {
    let mut work = Work::new(1);
    work.put(0, "a", "keep");
    let query = Query::new(&model(0)).filtered(crate::store::query::Where::equal("text", json!("keep")));
    work.feed(DeliveryInput::Open { stream: 1, view: view_of(&query), epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
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
    work.feed(DeliveryInput::Open { stream: 1, view: view(0), epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
    work.feed(DeliveryInput::Open { stream: 2, view: view_of(&marked), epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
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
        work.feed(DeliveryInput::Open { stream, view: entity_view(0, row), epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
        work.pull(stream);
    }
    let table = table_name(&model(0));
    let committed = |revision: i64, rows: ChangedRows| DeliveryInput::Committed(Committed { batch: 0, revision, tables: vec![table.clone()], rows, models: Models::default(), stale_parents: Vec::new(), position: 0, shown: None });
    let examined = work.state.streams_examined();

    let reads = work.step(committed(100, Some([(table.clone(), "b".to_string())].into())));
    assert_eq!(reads.iter().map(|read| read.stream).collect::<Vec<_>>(), [2], "only the Entity stream of the changed row reads");
    assert_eq!(work.state.streams_examined() - examined, 1, "the other Entity stream of the table is not examined");

    let reads = work.step(committed(101, None));
    assert_eq!(reads.iter().map(|read| read.stream).collect::<Vec<_>>(), [1, 2], "a commit without a row list reads every Entity stream of its tables");
}

fn linked_entity_view(index: usize, row: &str, target: usize) -> View {
    let mut view = entity_view(index, row);
    view.tables.push(table_name(&model(target)));
    view
}

fn prod_result(write: &crate::spec::ref_delivery_fanout::Write) -> crate::store::CommitResult {
    use crate::identity::{ModelKey, RowRef};
    use crate::store::fixtures::{id, scope};
    use crate::store::{CommitResult, RowChange, RowChangeKind};
    let reference = |(name, row): &(String, String)| RowRef::new(ModelKey::new(name), id(row));
    let mut result = CommitResult::noop(uuid::Uuid::nil());
    result.revision = Some(100);
    result.rows = write.rows.iter().map(|row| RowChange { row: reference(row), kind: RowChangeKind::Updated, fields: Default::default() }).collect();
    result.scopes = write.scopes.iter().map(|name| scope(name, "list")).collect();
    result.members = write.members.iter().map(reference).collect();
    result.renewed = write.renewed.iter().map(reference).collect();
    result.replaced = write.replaced.iter().map(|(from, to)| (reference(from), id(to))).collect();
    if write.queue { result.operations.insert(1); }
    result
}

type Change = Result<r::Write, Option<Vec<&'static str>>>;

fn write(build: fn(&mut r::Write)) -> Change {
    let mut write = r::Write::default();
    build(&mut write);
    Ok(write)
}

fn pair(model: &str, row: &str) -> (String, String) { (model.to_string(), row.to_string()) }

#[test]
fn dlv_prop_fanout_matches_reference() {
    let kinds: Vec<(&str, Change)> = vec![
        ("server row", write(|w| w.rows.push(pair("t0", "b")))),
        ("local field of the row of an Entity and a list", write(|w| w.rows.push(pair("t0", "a")))),
        ("tombstone in the other table", write(|w| w.rows.push(pair("t1", "a")))),
        ("cascade", write(|w| w.rows.extend([pair("t0", "a"), pair("t1", "a")]))),
        ("scope only", write(|w| w.scopes.push("t0".into()))),
        ("member", write(|w| w.members.push(pair("t0", "b")))),
        ("renewed row of another table", write(|w| { w.rows.push(pair("t0", "b")); w.renewed.push(pair("t1", "a")); })),
        ("replaced", write(|w| w.replaced.push((pair("t0", "a"), "b".into())))),
        ("queue", write(|w| w.queue = true)),
        ("reset of a model", Err(Some(vec!["t1"]))),
        ("reset of every model", Err(None)),
    ];
    let own = |model: &str, row: &str| Watched::Row { table: r::table_of(model), id: row.into() };
    let streams: std::collections::BTreeMap<u64, Stream> = [
        (1, Stream { tables: vec![r::table_of("t0")], watched: Watched::Tables }),
        (2, Stream { tables: vec![r::table_of("t1")], watched: Watched::Tables }),
        (3, Stream { tables: vec![r::table_of("t0")], watched: own("t0", "a") }),
        (4, Stream { tables: vec![r::table_of("t0")], watched: own("t0", "b") }),
        (5, Stream { tables: vec![r::table_of("t1"), r::table_of("t0")], watched: own("t1", "a") }),
    ].into();
    let index = Fanout::of(&streams);
    for (kind, change) in kinds {
        let mut work = Work::new(2);
        for (model, row) in [(0, "a"), (0, "b"), (1, "a")] { work.put(model, row, "old"); }
        let views = [(1, view(0)), (2, view(1)), (3, entity_view(0, "a")), (4, entity_view(0, "b")), (5, linked_entity_view(1, "a", 0))];
        for (stream, view) in views {
            work.feed(DeliveryInput::Open { stream, view, epoch: EPOCH, identity: true, valid: true, now: 0, revision: 0 });
            work.pull(stream);
        }
        let (notice, want) = match &change {
            Ok(written) => (CommittedBroadcast::of_commit(&prod_result(written)), index.affected(&r::notice_of_write(written))),
            Err(models) => {
                let owned: Option<Vec<String>> = models.as_ref().map(|models| models.iter().map(|model| model.to_string()).collect());
                (CommittedBroadcast::of_reset(100, owned.as_deref()), index.affected(&r::notice_of_reset(models.as_deref())))
            }
        };
        let reads = work.step(DeliveryInput::Committed(notice.for_deliveries()));
        let got: std::collections::BTreeSet<u64> = reads.iter().map(|read| read.stream).collect();
        assert_eq!(reads.len(), got.len(), "{kind}: one read per stream");
        assert_eq!(got, want, "{kind}: the streams the commit reaches");
        assert!(reads.iter().all(|read| read.revision == 100), "{kind}: every stream reads the revision of the commit");
    }
}
