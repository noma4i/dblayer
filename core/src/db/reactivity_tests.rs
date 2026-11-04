use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, ListKey};
use crate::query::{Query, Where};
use crate::schema::Coverage;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::test_wait::join_within;

const WAIT: Duration = Duration::from_secs(10);

fn feed() -> ScopeRef {
    ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("feed"))
}

fn land_scope(db: &Db, scope_ops: Vec<ScopeOp>) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops = scope_ops;
    db.as_app().land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
}

fn append(row: &str, order_key: &str) -> ScopeOp {
    ScopeOp::Append { scope: feed(), row: rid(row), order_key: order_key.to_string() }
}

#[test]
fn inv_observe_07_changing_an_included_author_wakes_the_observer_of_moments() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").including(&["author"]), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let first = changes(&log);
    assert_eq!(first[0].related["author"][&rid("m1")][0].text("name"), Some("Ann"));
    db.as_app().update("users", &rid("u1"), obj(json!({"name": "Zed"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let all = changes(&log);
    assert_eq!(all.len(), 2);
    assert_eq!(all[1].diff.updated, vec![rid("m1")]);
    assert_eq!(all[1].related["author"][&rid("m1")][0].text("name"), Some("Zed"));
}

#[test]
fn without_include_a_change_of_the_author_is_not_a_dependency() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let reads = db.counters().observation_reads;
    put(&db, "users", json!({"id": "u1", "name": "Zed"}));
    assert_eq!(calls(&log), vec![2]);
    assert_eq!(db.counters().observation_reads, reads);
}

#[test]
fn include_of_has_many_follows_new_children() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").including(&["vibes"]), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(changes(&log)[0].related["vibes"][&rid("m1")].len(), 2);
    put(&db, "vibes", json!({"id": "v3", "momentId": "m1", "kind": "fire"}));
    let all = changes(&log);
    assert_eq!(all[1].related["vibes"][&rid("m1")].len(), 3);
    assert_eq!(all[1].diff.updated, vec![rid("m1")]);
}

#[test]
fn a_scope_query_reads_in_the_server_order_and_follows_its_scope() {
    let db = booted();
    seed_tree(&db);
    land_scope(&db, vec![append("m2", "a0"), append("m1", "a1")]);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").in_scope(feed().key), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(ids(&changes(&log)[0]), ["m2", "m1"]);
    land_scope(&db, vec![ScopeOp::Move { scope: feed(), row: rid("m1"), order_key: "Zz".to_string() }]);
    let all = changes(&log);
    assert_eq!(ids(&all[1]), ["m1", "m2"]);
    assert_eq!(all[1].diff.moved.len(), 1);
}

#[test]
fn a_scope_query_ignores_other_scopes_and_rows_outside_its_scope() {
    let db = booted();
    seed_tree(&db);
    land_scope(&db, vec![append("m1", "a0")]);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").in_scope(feed().key), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let reads = db.counters().observation_reads;
    land_scope(&db, vec![ScopeOp::Append { scope: ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("other")), row: rid("m2"), order_key: "a".to_string() }]);
    put(&db, "moments", json!({"id": "m3", "userId": "u1", "text": "outside"}));
    assert_eq!(calls(&log), vec![1]);
    assert_eq!(db.counters().observation_reads, reads);
    put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "inside changed"}));
    assert_eq!(calls(&log), vec![1, 1]);
}

#[test]
fn observe_row_follows_one_row() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe_row("moments", &rid("m1"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let reads = db.counters().observation_reads;
    put(&db, "moments", json!({"id": "m2", "userId": "u2", "text": "other row"}));
    assert_eq!(db.counters().observation_reads, reads);
    db.as_app().update("moments", &rid("m1"), obj(json!({"text": "changed"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(texts(&changes(&log)[1]), ["changed"]);
    db.as_app().destroy("moments", &rid("m1")).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let last = changes(&log).pop().unwrap();
    assert!(last.rows.is_empty());
    assert_eq!(last.diff.removed, vec![rid("m1")]);
}

#[test]
fn observe_count_delivers_only_when_the_count_changes() {
    let db = booted();
    seed_tree(&db);
    let counts = Arc::new(Mutex::new(Vec::new()));
    let sink = counts.clone();
    let query = Query::new("moments").filtered(Where::equal("text", json!("one")));
    let _subscription = db.observe_count(query, move |change| sink.lock().unwrap().push((change.revision, change.count))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    put(&db, "moments", json!({"id": "m3", "userId": "u1", "text": "three"}));
    db.as_app().update("moments", &rid("m1"), obj(json!({"userId": "u2"}))).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(counts.lock().unwrap().iter().map(|(_, count)| *count).collect::<Vec<_>>(), [1]);
    put(&db, "moments", json!({"id": "m4", "userId": "u1", "text": "one"}));
    let seen = counts.lock().unwrap().clone();
    assert_eq!(seen.iter().map(|(_, count)| *count).collect::<Vec<_>>(), [1, 2]);
    assert!(seen[0].0 < seen[1].0);
}

#[test]
fn inv_id_04_a_temp_id_swap_arrives_as_replaced() {
    let db = booted();
    let temp = db.as_app().create("moments", obj(json!({"text": "new"}))).unwrap();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Replace, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("moments"), id: temp.id.clone(), reason: DestroyReason::Replaced { by: rid("m7") } });
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m7"), row: obj(json!({"id": "m7", "text": "new"})) });
    db.as_app().land(envelope).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let all = changes(&log);
    assert_eq!(all.len(), 2);
    assert_eq!(all[1].diff.replaced, vec![(temp.id.clone(), rid("m7"))]);
    assert!(all[1].diff.inserted.is_empty() && all[1].diff.removed.is_empty());
    assert_eq!(ids(&all[1]), ["m7"]);
}

#[test]
fn inv_observe_04_destroying_a_parent_is_one_delivery_per_observer_with_one_revision() {
    let db = booted();
    seed_tree(&db);
    let observed: Vec<(ChangeLog, ObserverHandle)> = ["moments", "users", "vibes"]
        .iter()
        .map(|model| {
            let (log, callback) = recorder();
            (log, db.observe(Query::new(model), callback).unwrap())
        })
        .collect();
    db.await_revision(db.revision_seq().unwrap());
    db.as_app().destroy("users", &rid("u1")).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let revisions: Vec<i64> = observed
        .iter()
        .map(|(log, _)| {
            let all = changes(log);
            assert_eq!(all.len(), 2);
            all[1].revision
        })
        .collect();
    assert!(revisions.iter().all(|revision| *revision == revisions[0]));
    assert!(changes(&observed[0].0)[1].rows.iter().all(|row| row.id.as_str() != "m1"));
    assert!(changes(&observed[2].0)[1].rows.is_empty());
}

#[test]
fn a_thousand_subscribers_cost_at_most_one_read_per_commit() {
    let db = booted();
    let logs: Vec<(ChangeLog, ObserverHandle)> = (0..1000)
        .map(|_| {
            let (log, callback) = recorder();
            (log, db.observe(Query::new("moments"), callback).unwrap())
        })
        .collect();
    db.await_revision(db.revision_seq().unwrap());
    let reads = db.counters().observation_reads;
    for number in 0..100 {
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&format!("m{number}")), row: obj(json!({"id": format!("m{number}"), "userId": "u1", "text": "t"})) });
        db.as_app().land(envelope).unwrap();
    }
    db.await_revision(db.revision_seq().unwrap());
    assert!(db.counters().observation_reads - reads <= 100);
    assert!(logs.iter().all(|(log, _)| changes(log).last().unwrap().rows.len() == 100));
}

#[test]
fn inv_observe_03_a_slow_observer_gets_the_newest_position_after_older_ones_wait_in_its_mailbox() {
    let db = booted();
    let (entered_sender, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let revisions: Arc<Mutex<Vec<i64>>> = Arc::default();
    let sink = revisions.clone();
    let _observer = db
        .observe(Query::new("moments"), move |change| {
            let first = {
                let mut seen = sink.lock().unwrap();
                seen.push(change.revision);
                seen.len() == 1
            };
            if first {
                entered_sender.send(()).unwrap();
                released.lock().unwrap().recv_timeout(WAIT).unwrap();
            }
        })
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let (fast_sender, fast) = mpsc::channel();
    let _fast = db.observe(Query::new("moments"), move |change| fast_sender.send(change.revision).unwrap()).unwrap();
    for number in 0..4 {
        let row = obj(json!({"id": format!("m{number}"), "userId": "u1", "text": "t"}));
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&format!("m{number}")), row });
        db.as_app().land(envelope).unwrap();
    }
    let newest = db.revision_seq().unwrap();
    while fast.recv_timeout(WAIT).unwrap() < newest {}
    release.send(()).unwrap();
    let deadline = std::time::Instant::now() + WAIT;
    while revisions.lock().unwrap().last() != Some(&newest) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let seen = revisions.lock().unwrap().clone();
    assert!(seen.len() == 2 && seen[0] < seen[1] && seen[1] == newest, "older versions that waited in the mailbox must not be delivered: {seen:?}");
}

#[test]
fn inv_observe_13_commits_behind_a_busy_listener_arrive_as_one_delivery_that_holds_every_commit() {
    let db = booted();
    let (entered_sender, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let (told_sender, told) = mpsc::channel();
    let slow_whole = Whole::default();
    type Delivery = Vec<(String, String)>;
    let deliveries: Arc<Mutex<Vec<Delivery>>> = Arc::default();
    let sink = deliveries.clone();
    let _slow = db
        .observe(Query::new("moments"), move |change| {
            let rows = slow_whole.of(change);
            let first = {
                let mut seen = sink.lock().unwrap();
                seen.push(rows.iter().map(|row| (row.id.to_string(), row.text("text").unwrap_or_default().to_string())).collect());
                seen.len() == 1
            };
            if first {
                entered_sender.send(()).unwrap();
                released.lock().unwrap().recv_timeout(WAIT).unwrap();
            }
            told_sender.send(change.revision).unwrap();
        })
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    let (fast_sender, fast) = mpsc::channel();
    let _fast = db.observe(Query::new("moments"), move |change| fast_sender.send(change.revision).unwrap()).unwrap();
    let mut seed = Seed(0x5eed);
    for _ in 0..100 {
        let (id, row) = random_row(&mut seed, 14);
        if seed.below(4) == 0 {
            let _ = db.as_app().destroy("moments", &rid(&id));
        } else {
            let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
            envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&id), row });
            db.as_app().land(envelope).unwrap();
        }
    }
    let last = db.revision_seq().unwrap();
    while fast.recv_timeout(WAIT).unwrap() < last {}
    release.send(()).unwrap();
    while told.recv_timeout(WAIT).unwrap() < last {}
    let seen = deliveries.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "the blocked call and one merged delivery for the 100 commits");
    let mut expected: Vec<(String, String)> = db.query(&Query::new("moments")).unwrap().iter().map(|row| (row.id.to_string(), row.text("text").unwrap_or_default().to_string())).collect();
    let mut merged = seen[1].clone();
    expected.sort();
    merged.sort();
    assert!(!merged.is_empty());
    assert_eq!(merged, expected, "the merged delivery equals a fresh query");
}

#[test]
fn a_cancelled_subscription_gets_no_call_after_the_cancel_returns() {
    let db = booted();
    let calls_made = Arc::new(AtomicUsize::new(0));
    let (entered_sender, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let counter = calls_made.clone();
    let subscription = db
        .observe(Query::new("moments"), move |change| {
            counter.fetch_add(1, Ordering::SeqCst);
            if change.rows.len() == 1 {
                entered_sender.send(()).unwrap();
                released.lock().unwrap().recv_timeout(WAIT).unwrap();
            }
        })
        .unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m1"), row: obj(json!({"id": "m1", "userId": "u1", "text": "one"})) });
    db.as_app().land(envelope).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    drop(subscription);
    release.send(()).unwrap();
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "two"}));
    put(&db, "moments", json!({"id": "m3", "userId": "u1", "text": "three"}));
    assert_eq!(calls_made.load(Ordering::SeqCst), 2);
}

#[test]
fn inv_observe_09_a_slow_listener_does_not_hold_back_the_others_and_gets_the_latest_state() {
    let db = booted();
    let (entered_sender, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let slow_log = Arc::new(Mutex::new(Vec::new()));
    let slow_sink = slow_log.clone();
    let slow_whole = Whole::default();
    let _slow = db
        .observe(Query::new("moments"), move |change| {
            let size = slow_whole.of(change).len();
            slow_sink.lock().unwrap().push(size);
            if size == 1 {
                entered_sender.send(()).unwrap();
                released.lock().unwrap().recv_timeout(WAIT).unwrap();
            }
        })
        .unwrap();
    let (fast_sender, fast) = mpsc::channel();
    let fast_whole = Whole::default();
    let _fast = db.observe(Query::new("moments"), move |change| fast_sender.send(fast_whole.of(change).len()).unwrap()).unwrap();
    for number in 0..3 {
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&format!("m{number}")), row: obj(json!({"id": format!("m{number}"), "userId": "u1", "text": "t"})) });
        db.as_app().land(envelope).unwrap();
        if number == 0 {
            entered.recv_timeout(WAIT).unwrap();
        }
    }
    let mut last = 0;
    while last != 3 {
        last = fast.recv_timeout(WAIT).unwrap();
    }
    assert_eq!(*slow_log.lock().unwrap().last().unwrap(), 1);
    release.send(()).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let seen = slow_log.lock().unwrap().clone();
    assert_eq!(*seen.last().unwrap(), 3);
    assert!(seen.len() <= 3);
}

#[test]
fn inv_observe_11_reset_moves_every_observer_to_the_empty_state_and_keeps_the_subscriptions() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    db.reset_all().unwrap();
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log), vec![2, 0]);
    assert_eq!(changes(&log)[1].diff.removed.len(), 2);
    assert_eq!(db.observer_census().subscriptions, 1);
    assert!(db.find("moments", &rid("m1")).unwrap().is_none());
    put(&db, "moments", json!({"id": "m9", "userId": "u1", "text": "after"}));
    assert_eq!(calls(&log), vec![2, 0, 1]);
}

struct Seed(u64);

impl Seed {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

fn random_row(seed: &mut Seed, pool: u64) -> (String, serde_json::Map<String, serde_json::Value>) {
    let id = format!("m{}", seed.below(pool));
    let text = ["a", "b", "c"][seed.below(3) as usize];
    let user = format!("u{}", seed.below(3));
    (id.clone(), obj(json!({"id": id, "userId": user, "text": text})))
}

fn work(db: Db, seed_value: u64) {
    let mut seed = Seed(seed_value);
    for _ in 0..120 {
        let (id, row) = random_row(&mut seed, 14);
        match seed.below(6) {
            0 => {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
                envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&id), row });
                db.as_app().land(envelope).unwrap();
            }
            1 => {
                let _ = db.as_app().update("moments", &rid(&id), obj(json!({"text": row["text"]})));
            }
            2 => {
                let _ = db.as_app().destroy("moments", &rid(&id));
            }
            3 => {
                let rows = (0..seed.below(8)).map(|_| random_row(&mut seed, 14)).map(|(_, row)| row).collect();
                let mut request = ReplaceRequest::new(feed(), Coverage::Complete);
                request.on_absent = if seed.below(2) == 0 { OnAbsent::Destroy } else { OnAbsent::Detach };
                db.as_app().replace(&request, rows).unwrap();
            }
            4 => {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
                envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&id), row });
                db.as_app().land(envelope).unwrap();
            }
            _ => {
                let _ = db.as_app().create("moments", obj(json!({"text": "fresh"})));
            }
        }
    }
}

fn entries_of(change: &Change) -> Vec<(String, i64)> {
    change.rows.iter().map(|row| (row.id.to_string(), row.revision)).collect()
}

fn check_stream(stream: &[Change]) {
    assert!(stream.windows(2).all(|pair| pair[0].revision < pair[1].revision), "revisions must grow");
    assert_eq!(stream[0].diff.inserted.len(), stream[0].rows.len());
    for pair in stream.windows(2) {
        let (before, after) = (entries_of(&pair[0]), entries_of(&pair[1]));
        let diff = &pair[1].diff;
        let named = |id: &crate::identity::RowId| id.to_string();
        let mut expected: Vec<String> = before.iter().map(|(id, _)| id.clone()).filter(|id| !diff.removed.iter().chain(diff.replaced.iter().map(|(from, _)| from)).any(|gone| &named(gone) == id)).collect();
        expected.extend(diff.inserted.iter().map(named));
        expected.extend(diff.replaced.iter().map(|(_, to)| named(to)));
        let mut actual: Vec<String> = after.iter().map(|(id, _)| id.clone()).collect();
        expected.sort();
        actual.sort();
        assert_eq!(expected, actual, "diff must lead from the previous state to the next");
        for (id, revision) in &after {
            let was = before.iter().find(|(other, _)| other == id);
            let marked = diff.updated.iter().any(|updated| &named(updated) == id);
            if let Some((_, old_revision)) = was {
                assert!(marked || old_revision == revision, "a changed row must be marked updated: {id}");
            }
        }
    }
}

#[test]
fn inv_observe_03_inv_observe_06_model_test_with_threads_ends_every_observer_on_the_fresh_state() {
    for seed_value in [0x2545F4914F6CDD1D_u64, 0x9E3779B97F4A7C15, 0xD1B54A32D192ED03] {
        let db = booted();
        seed_tree(&db);
        land_scope(&db, vec![append("m1", "a0")]);
        let queries = [
            Query::new("moments"),
            Query::new("moments").filtered(Where::equal("text", json!("a"))),
            Query::new("moments").in_scope(feed().key),
            Query::new("moments").including(&["author"]),
        ];
        let logs: Vec<(Query, ChangeLog, ObserverHandle)> = queries
            .iter()
            .map(|query| {
                let (log, callback) = recorder();
                (query.clone(), log, db.observe(query.clone(), callback).unwrap())
            })
            .collect();
        let row_log = {
            let (log, callback) = recorder();
            (log, db.observe_row("moments", &rid("m3"), callback).unwrap())
        };
        let handles: Vec<_> = (0..4).map(|thread| {
            let db = db.clone();
            std::thread::spawn(move || work(db, seed_value + thread))
        }).collect();
        for handle in handles {
            join_within(handle);
        }
        db.await_revision(db.revision_seq().unwrap());
        for (query, log, _) in &logs {
            let stream = changes(log);
            check_stream(&stream);
            let fresh: Vec<(String, i64)> = db.query(query).unwrap().iter().map(|row| (row.id.to_string(), row.revision)).collect();
            assert_eq!(entries_of(stream.last().unwrap()), fresh);
        }
        let stream = changes(&row_log.0);
        let fresh: Vec<(String, i64)> = db.find("moments", &rid("m3")).unwrap().iter().map(|row| (row.id.to_string(), row.revision)).collect();
        assert_eq!(entries_of(stream.last().unwrap()), fresh);
    }
}
