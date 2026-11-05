use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use serde_json::json;

use super::fixtures::*;
use super::listeners::{Listener, Mailbox, Subscriber};
use super::watch::{Body, Loaded, Subject, Version};
use super::window::{Slice, WindowEdge, WindowSpec};
use super::{Change, Db, Row};
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, WriteOrigin};
use crate::identity::touches::total;
use crate::identity::ModelKey;
use crate::query::{CompareOp, Order, Query, Where};
use crate::value::SortDirection;

const SIZES: [usize; 3] = [20, 200, 2000];
const WINDOW: usize = 10;
const SEQUENCES: u64 = 1200;
const EDITS: usize = 8;

fn name_at(index: usize) -> String {
    format!("n{index:06}")
}

fn query() -> Query {
    Query::new("users").ordered(vec![Order::new("name", SortDirection::Asc)])
}

fn upsert(db: &Db, id: &str, name: &str) {
    put(db, "users", json!({"id": id, "name": name}));
}

fn seeded(size: usize) -> Db {
    let db = booted();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for index in 0..size {
        let id = format!("u{index:06}");
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("users"), id: rid(&id), row: obj(json!({"id": id, "name": name_at(index)})) });
    }
    db.land(&db.admit().unwrap(), envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    db
}

fn expected_ids(db: &Db, query: &Query, size: usize) -> Vec<String> {
    let rows = db.query(query).unwrap();
    rows[rows.len().saturating_sub(size)..].iter().map(|row| row.id.to_string()).collect()
}

#[test]
fn members_bounded_rows() {
    let db = seeded(100_000);
    let scope = crate::commit::ScopeRef::new(ModelKey::new("users"), crate::identity::ListKey::compose(&["users", "members"]));
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for n in 0..100_000 {
        envelope.scope_ops.push(crate::commit::ScopeOp::Append { scope: scope.clone(), row: rid(&format!("u{n:06}")), order_key: format!("k{n:06}") });
    }
    db.land(&db.admit().unwrap(), envelope).unwrap();
    let query = Query { stable: true, ..query().in_scope(scope.key) };
    let subject = Subject::window(query.clone(), WindowSpec::Members { size: 50, max: 200 });
    let before = total();
    let first = db.shared.evaluate(&subject).unwrap();
    assert!(total() - before <= 200, "the initial window must not read all member keys");
    let Body::Rows(body) = &first.version.body else { panic!("rows") };
    assert_eq!(first.loaded.rows.len(), 50);
    assert_eq!(body.window.as_ref().unwrap().edge.total, Some(100_000));
    let (log, callback) = recorder();
    let live = db.observe_window(query.clone(), WindowSpec::Members { size: 50, max: 200 }, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    live.grow_before(150);
    db.await_revision(db.revision_seq().unwrap());
    let widest = changes(&log).pop().unwrap();
    assert_eq!(widest.size, 200);
    assert!(widest.rows.len() <= 200);
    live.grow_before(1);
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(changes(&log).pop().unwrap().size, 50);
    assert_eq!(db.count(&query).unwrap(), 100_000);
}

#[test]
fn a_tail_window_holds_the_last_rows_and_follows_the_tail() {
    let db = seeded(12);
    let (log, callback) = recorder();
    let _window = db.observe_window(query(), WindowSpec::Tail { size: 5, max: None, paged: false }, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let first = changes(&log).pop().unwrap();
    assert_eq!(ids(&first), expected_ids(&db, &query(), 5));
    assert_eq!(first.window, Some(WindowEdge { has_before: true, has_after: false, total: Some(12), wanted_before: 0 }));
    let before = db.counters();
    upsert(&db, "tail", "zzz");
    let last = changes(&log).pop().unwrap();
    assert_eq!(ids(&last), expected_ids(&db, &query(), 5));
    assert_eq!(last.window, Some(WindowEdge { has_before: true, has_after: false, total: Some(13), wanted_before: 0 }));
    assert_eq!(db.counters().rows_delivered - before.rows_delivered, 1);
}

#[test]
fn a_change_before_the_window_delivers_no_rows_and_only_moves_the_total() {
    let db = seeded(30);
    let (log, callback) = recorder();
    let _window = db.observe_window(query(), WindowSpec::Tail { size: 5, max: None, paged: false }, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let (calls_before, counters_before) = (changes(&log).len(), db.counters());
    upsert(&db, "u000001", "n000001!");
    assert_eq!(changes(&log).len(), calls_before, "an update before the window is not delivered");
    upsert(&db, "older", "a");
    let last = changes(&log).pop().unwrap();
    assert_eq!(changes(&log).len(), calls_before + 1);
    assert_eq!(last.window.unwrap().total, Some(31));
    assert_eq!(ids(&last), expected_ids(&db, &query(), 5));
    let counters = db.counters();
    assert_eq!((counters.rows_delivered, counters.rows_loaded), (counters_before.rows_delivered, counters_before.rows_loaded));
}

#[test]
fn growing_before_adds_local_rows_and_stops_at_the_first_row() {
    let db = seeded(8);
    let (log, callback) = recorder();
    let window = db.observe_window(query(), WindowSpec::Tail { size: 3, max: None, paged: false }, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    window.grow_before(4);
    db.await_revision(db.revision_seq().unwrap());
    let wider = changes(&log).pop().unwrap();
    assert_eq!(ids(&wider), expected_ids(&db, &query(), 7));
    assert_eq!(wider.window, Some(WindowEdge { has_before: true, has_after: false, total: Some(8), wanted_before: 0 }));
    window.grow_before(10);
    db.await_revision(db.revision_seq().unwrap());
    let all = changes(&log).pop().unwrap();
    assert_eq!(ids(&all), expected_ids(&db, &query(), 8));
    assert_eq!(all.window, Some(WindowEdge { has_before: false, has_after: false, total: Some(8), wanted_before: 0 }));
    let calls = changes(&log).len();
    window.grow_before(3);
    window.grow_after(3);
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(changes(&log).len(), calls, "no local row is left to add");
}

#[test]
fn a_window_around_a_row_grows_after_and_follows_the_row() {
    let db = seeded(20);
    let (log, callback) = recorder();
    let spec = WindowSpec::Around { id: rid("u000010"), before: 2, after: 1 };
    let window = db.observe_window(query(), spec, callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let names = |change: &Change| ids(change);
    assert_eq!(names(&changes(&log).pop().unwrap()), ["u000008", "u000009", "u000010", "u000011"]);
    window.grow_after(2);
    db.await_revision(db.revision_seq().unwrap());
    let wider = changes(&log).pop().unwrap();
    assert_eq!(names(&wider), ["u000008", "u000009", "u000010", "u000011", "u000012", "u000013"]);
    assert_eq!(wider.window, Some(WindowEdge { has_before: true, has_after: true, total: Some(20), wanted_before: 0 }));
    upsert(&db, "older", "a");
    assert_eq!(names(&changes(&log).pop().unwrap()), ["u000008", "u000009", "u000010", "u000011", "u000012", "u000013"]);
    window.grow_after(50);
    db.await_revision(db.revision_seq().unwrap());
    let tail = changes(&log).pop().unwrap();
    assert_eq!(tail.window, Some(WindowEdge { has_before: true, has_after: false, total: Some(21), wanted_before: 0 }));
    assert_eq!(ids(&tail).last().unwrap(), "u000019");
}

#[test]
fn a_query_a_window_cannot_follow_is_refused() {
    let db = seeded(3);
    let refused = [Query::new("users"), query().limited(2), Query { include: vec!["moments".to_string()], ..query() }];
    for query in refused {
        assert!(db.observe_window(query, WindowSpec::Tail { size: 2, max: None, paged: false }, |_: &Change| {}).is_err());
    }
}

#[test]
fn a_slice_by_cursor_or_by_offset_equals_the_same_slice_of_the_query() {
    let db = seeded(40);
    let all = db.query(&query()).unwrap();
    let id = |at: usize| all[at].id.clone();
    let cases = [
        (Slice::Offset { offset: 5, limit: 7 }, 5..12),
        (Slice::Offset { offset: 38, limit: 7 }, 38..40),
        (Slice::Offset { offset: 90, limit: 7 }, 40..40),
        (Slice::After { id: id(10), limit: 4 }, 11..15),
        (Slice::After { id: id(38), limit: 4 }, 39..40),
        (Slice::Before { id: id(10), limit: 4 }, 6..10),
        (Slice::Before { id: id(2), limit: 4 }, 0..2),
    ];
    for (slice, range) in cases {
        let page = db.read_slice(&query(), slice.clone()).unwrap();
        assert_eq!(shown(&page.rows), shown(&all[range.clone()]), "{slice:?}");
        assert_eq!((page.has_before, page.has_after, page.total), (range.start > 0, range.end < 40, 40), "{slice:?}");
    }
    assert!(db.read_slice(&query(), Slice::After { id: rid("absent"), limit: 1 }).is_err());
}

fn step(db: &Db, previous: &Arc<Version>, touched: &[&str], grow: (usize, usize)) -> Arc<Version> {
    let touched: HashSet<String> = touched.iter().map(|id| id.to_string()).collect();
    let epoch = db.shared.ephemeral.lock().epoch;
    let horizon = (db.shared.store.revision_seq().unwrap(), epoch);
    let evaluated = db.shared.window_step(&query(), previous, &touched, horizon, grow).unwrap().expect("the window steps from ranks");
    Arc::new(evaluated.version.succeeding(previous, &[], evaluated.loaded).expect("a window version follows"))
}

fn window_version(db: &Db) -> Arc<Version> {
    Arc::new(db.shared.evaluate(&Subject::window(query(), WindowSpec::Tail { size: WINDOW, max: None, paged: false })).unwrap().version)
}

fn cost(size: usize, edit: fn(&Db, usize) -> Vec<String>, grow: (usize, usize)) -> (usize, usize) {
    let db = seeded(size);
    let previous = window_version(&db);
    let touched = edit(&db, size);
    let loaded = db.counters().rows_loaded;
    let before = total();
    step(&db, &previous, &touched.iter().map(String::as_str).collect::<Vec<_>>(), grow);
    (total() - before, db.counters().rows_loaded - loaded)
}

fn inside(db: &Db, size: usize) -> Vec<String> {
    let id = format!("u{:06}", size - 1);
    upsert(db, &id, &format!("{}!", name_at(size - 1)));
    vec![id]
}

fn outside(db: &Db, _size: usize) -> Vec<String> {
    upsert(db, "u000000", "n000000!");
    vec!["u000000".to_string()]
}

fn nothing(_db: &Db, _size: usize) -> Vec<String> {
    Vec::new()
}

#[test]
fn the_work_of_one_change_in_the_window_does_not_depend_on_the_list_size() {
    let table: Vec<_> = SIZES.iter().map(|size| (cost(*size, inside, (0, 0)), cost(*size, outside, (0, 0)), cost(*size, nothing, (5, 0)))).collect();
    println!("size | inside (touches, rows) | outside | grow\n{}", SIZES.iter().zip(&table).map(|(size, (a, b, c))| format!("{size} | {a:?} | {b:?} | {c:?}")).collect::<Vec<_>>().join("\n"));
    type Cell = (usize, usize);
    let spread = |pick: fn(&(Cell, Cell, Cell)) -> usize| table.iter().map(pick).max().unwrap() - table.iter().map(pick).min().unwrap();
    assert_eq!(spread(|row| row.0 .0) + spread(|row| row.1 .0), 0, "touches of a change inside and outside, per size 20, 200, 2000: {table:?}");
    assert!(spread(|row| row.2 .0) <= 16, "touches of an grow differ only by hash collisions: {table:?}");
    assert!(table.iter().all(|row| (row.0 .1, row.1, row.2 .1) == (table[0].0 .1, table[0].1, table[0].2 .1)), "rows loaded per size: {table:?}");
    assert_eq!(table[0].1, (0, 0), "a change outside the window touches and loads nothing");
    assert_eq!(table[0].0 .1, 1, "one row loaded for one change inside");
    assert_eq!(table[0].2 .1, 5, "an grow loads the rows it adds");
}

#[test]
fn a_commit_missing_from_the_named_rows_rebuilds_the_window_from_the_query() {
    let db = seeded(30);
    let previous = window_version(&db);
    let named = vec!["u000029".to_string()];
    upsert(&db, "u000029", "n000029!");
    let epoch = db.shared.ephemeral.lock().epoch;
    let horizon = (db.shared.store.revision_seq().unwrap(), epoch);
    upsert(&db, "late", "zzz");
    let evaluated = db.shared.window_next(&query(), &previous, Some(&named.into_iter().collect()), horizon, (0, 0)).unwrap();
    let Body::Rows(body) = &evaluated.version.body else { unreachable!() };
    let order: Vec<String> = body.entries().iter().map(|entry| entry.id.to_string()).collect();
    assert_eq!(order, expected_ids(&db, &query(), WINDOW));
}

#[test]
fn a_window_lives_as_long_as_its_handle() {
    let db = seeded(5);
    let window = db.observe_window(query(), WindowSpec::Tail { size: 2, max: None, paged: false }, |_: &Change| {}).unwrap();
    assert_eq!(db.observer_census().subscriptions, 1);
    drop(window);
    assert_eq!(db.observer_census().subscriptions, 0);
}

struct Dice(u64);

impl Dice {
    fn below(&mut self, limit: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((self.0 >> 33) % limit as u64) as usize
    }
}

const TEXTS: [&str; 8] = ["", "a", "B", "b", "é", "10", "9", "zz"];
const DATES: [&str; 3] = ["2026-01-01T00:00:00.000Z", "2026-01-03T10:00:00.000Z", "2026-02-01T00:00:00.000Z"];

fn query_of(variant: usize) -> Query {
    let moments = Query::new("moments");
    match variant % 5 {
        0 => moments.ordered(vec![Order::new("text", SortDirection::Asc)]),
        1 => moments.ordered(vec![Order::new("text", SortDirection::Desc)]),
        2 => moments.ordered(vec![Order::new("text", SortDirection::DescNullsFirst), Order::new("createdAt", SortDirection::Asc)]),
        3 => moments.ordered(vec![Order::new("id", SortDirection::Desc)]),
        _ => moments.filtered(Where::compare("createdAt", CompareOp::GreaterOrEqual, json!(DATES[1]))).ordered(vec![Order::new("createdAt", SortDirection::Desc), Order::new("text", SortDirection::AscNullsLast)]),
    }
}

fn edit(db: &Db, dice: &mut Dice, known: &mut Vec<String>, fresh: &mut usize, keep: &str) -> String {
    let moments = ModelKey::new("moments");
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    let doomed = (!known.is_empty() && dice.below(5) == 0).then(|| dice.below(known.len())).filter(|at| known[*at] != keep);
    let id = if let Some(at) = doomed {
        let id = known.swap_remove(at);
        envelope.entity_ops.push(EntityOp::Destroy { model: moments, id: rid(&id), reason: DestroyReason::Server });
        id
    } else {
        let id = if known.is_empty() || dice.below(3) == 0 {
            *fresh += 1;
            let id = format!("m{fresh:05}");
            known.push(id.clone());
            id
        } else {
            known[dice.below(known.len())].clone()
        };
        let mut row = json!({"id": id, "userId": "u1"});
        if dice.below(6) != 0 {
            row["text"] = json!(TEXTS[dice.below(TEXTS.len())]);
        }
        if dice.below(3) != 0 {
            row["createdAt"] = json!(DATES[dice.below(DATES.len())]);
        }
        envelope.entity_ops.push(EntityOp::Upsert { model: moments, id: rid(&id), row: obj(row) });
        id
    };
    db.land(&db.admit().unwrap(), envelope).unwrap();
    id
}

#[derive(Clone)]
enum Want {
    Tail(usize),
    Around { id: String, before: usize, after: usize },
}

impl Want {
    fn spec(&self) -> WindowSpec {
        match self {
            Want::Tail(size) => WindowSpec::Tail { size: *size, max: None, paged: false },
            Want::Around { id, before, after } => WindowSpec::Around { id: rid(id), before: *before, after: *after },
        }
    }

    fn bounds(&self, ids: &[String]) -> Option<(usize, usize)> {
        match self {
            Want::Tail(size) => Some((ids.len().saturating_sub(*size), ids.len())),
            Want::Around { id, before, after } => {
                let at = ids.iter().position(|found| found == id)?;
                Some((at.saturating_sub(*before), (at + after + 1).min(ids.len())))
            }
        }
    }

    fn grow(&mut self, ids: &[String], more: (usize, usize)) {
        let (start, end) = self.bounds(ids).expect("the anchor is listed");
        match self {
            Want::Tail(size) => *size += more.0.min(start),
            Want::Around { before, after, .. } => {
                *before += more.0.min(start);
                *after += more.1.min(ids.len() - end);
            }
        }
    }
}

fn shown(rows: &[Row]) -> Vec<(String, i64)> {
    rows.iter().map(|row| (row.id.to_string(), row.revision)).collect()
}

fn run_sequence(seed: u64) {
    let mut dice = Dice(seed);
    let db = booted();
    let query = query_of(seed as usize);
    let (mut known, mut fresh) = (Vec::new(), 0);
    for _ in 0..3 + dice.below(25) {
        edit(&db, &mut dice, &mut known, &mut fresh, "");
    }
    let listed: Vec<String> = db.query(&query).unwrap().iter().map(|row| row.id.to_string()).collect();
    let mut want = match listed.get(dice.below(listed.len().max(1))) {
        Some(id) if seed.is_multiple_of(3) => Want::Around { id: id.clone(), before: dice.below(4), after: dice.below(4) },
        _ => Want::Tail(1 + dice.below(6)),
    };
    let keep = if let Want::Around { id, .. } = &want { id.clone() } else { String::new() };
    let mirror: Arc<Mutex<Vec<Row>>> = Arc::default();
    let edges: Arc<Mutex<Option<WindowEdge>>> = Arc::default();
    let (sink, edge_sink) = (mirror.clone(), edges.clone());
    let subscriber = Subscriber::new(
        Listener::Rows(Box::new(move |change: &Change| {
            change.apply_to(&mut sink.lock().unwrap());
            *edge_sink.lock().unwrap() = change.window;
        })),
        Default::default(),
    );
    let first = db.shared.evaluate(&Subject::window(query.clone(), want.spec())).unwrap();
    let mut latest: Arc<Version> = Arc::new(first.version);
    let mut mailbox = Mailbox::default();
    mailbox.push(latest.clone(), &[], first.loaded);
    let mut delivered: Option<Arc<Version>> = None;
    let flush = |mailbox: &mut Mailbox, delivered: &mut Option<Arc<Version>>| {
        if let Some(mut taken) = mailbox.take() {
            taken.delivered = delivered.clone();
            subscriber.deliver(&mut taken);
            *delivered = Some(taken.latest.clone());
        }
    };
    let check = |want: &Want, label: &str| {
        let rows = db.query(&query).unwrap();
        let listed: Vec<String> = rows.iter().map(|row| row.id.to_string()).collect();
        let Some((start, end)) = want.bounds(&listed) else { return false };
        assert_eq!(shown(&mirror.lock().unwrap()), shown(&rows[start..end]), "sequence {seed} {label}: window rebuilt from deliveries");
        assert_eq!(*edges.lock().unwrap(), Some(WindowEdge { has_before: start > 0, has_after: end < rows.len(), total: Some(rows.len()), wanted_before: 0 }), "sequence {seed} {label}: edge");
        true
    };
    flush(&mut mailbox, &mut delivered);
    assert!(check(&want, "start"));
    for step in 0..EDITS {
        let grow = if dice.below(4) == 0 { (dice.below(4), dice.below(4)) } else { (0, 0) };
        let touched: HashSet<String> = if grow == (0, 0) { (0..1 + dice.below(3)).map(|_| edit(&db, &mut dice, &mut known, &mut fresh, &keep)).collect() } else { HashSet::new() };
        let epoch = db.shared.ephemeral.lock().epoch;
        let horizon = (db.shared.store.revision_seq().unwrap(), epoch);
        let evaluated = db.shared.window_next(&query, &latest, Some(&touched), horizon, grow).unwrap();
        let listed: Vec<String> = db.query(&query).unwrap().iter().map(|row| row.id.to_string()).collect();
        if want.bounds(&listed).is_none() {
            return;
        }
        want.grow(&listed, grow);
        if let Some(next) = evaluated.version.succeeding(&latest, &[], evaluated.loaded) {
            let supply = match &next.body {
                Body::Rows(body) => body.step.as_ref().map(|step| step.supply()).unwrap_or_default(),
                Body::Count(_) => Loaded::default(),
            };
            let previous = std::mem::replace(&mut latest, Arc::new(next));
            if latest.announces(&previous) {
                mailbox.push(latest.clone(), &[], supply);
            }
        }
        if dice.below(2) == 0 {
            flush(&mut mailbox, &mut delivered);
            check(&want, &format!("step {step}"));
        }
    }
    flush(&mut mailbox, &mut delivered);
    check(&want, "end");
}

#[test]
fn windows_equal_the_tail_of_a_fresh_query_over_random_edit_sequences() {
    std::thread::scope(|scope| {
        let runs: Vec<_> = (1..=SEQUENCES).map(|seed| scope.spawn(move || run_sequence(seed))).collect();
        for run in runs {
            run.join().unwrap();
        }
    });
}

fn open_cost(db: &Db) -> (usize, super::window::WindowHandle) {
    let before = total();
    let handle = db.observe_window(query(), WindowSpec::Tail { size: WINDOW, max: None, paged: false }, |_| {}).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    (total() - before, handle)
}

#[test]
fn a_second_window_over_the_same_query_reads_no_list_keys() {
    let db = seeded(2000);
    let (first, _kept) = open_cost(&db);
    let (second, _other) = open_cost(&db);
    assert!(second * 10 <= first, "{first} touches for the first window, {second} for the second");
}
