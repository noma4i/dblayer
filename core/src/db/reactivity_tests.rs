use super::fixtures::*;
use super::*;
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, ScopeKey};
use crate::query::{Query, Where};
use crate::schema::Coverage;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(10);

fn feed() -> ScopeRef {
    ScopeRef::new(ModelKey::new("moments"), ScopeKey::from_raw("feed"))
}

fn land_scope(db: &Db, scope_ops: Vec<ScopeOp>) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.scope_ops = scope_ops;
    db.land(envelope).unwrap();
    db.settle();
}

fn append(row: &str, order_key: &str) -> ScopeOp {
    ScopeOp::Append { scope: feed(), row: rid(row), order_key: order_key.to_string() }
}

#[test]
fn changing_an_included_author_wakes_the_observer_of_moments() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").including(&["author"]), callback).unwrap();
    db.settle();
    let first = changes(&log);
    assert_eq!(first[0].related["author"][&rid("m1")][0].text("name"), Some("Ann"));
    db.update("users", &rid("u1"), obj(json!({"name": "Zed"}))).unwrap();
    db.settle();
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
    db.settle();
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
    db.settle();
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
    land_scope(&db, vec![append("m2", "a"), append("m1", "b")]);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").in_scope(feed().key), callback).unwrap();
    db.settle();
    assert_eq!(ids(&changes(&log)[0]), ["m2", "m1"]);
    land_scope(&db, vec![ScopeOp::Move { scope: feed(), row: rid("m1"), order_key: "0".to_string() }]);
    let all = changes(&log);
    assert_eq!(ids(&all[1]), ["m1", "m2"]);
    assert_eq!(all[1].diff.moved.len(), 1);
}

#[test]
fn a_scope_query_ignores_other_scopes_and_rows_outside_its_scope() {
    let db = booted();
    seed_tree(&db);
    land_scope(&db, vec![append("m1", "a")]);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments").in_scope(feed().key), callback).unwrap();
    db.settle();
    let reads = db.counters().observation_reads;
    land_scope(&db, vec![ScopeOp::Append { scope: ScopeRef::new(ModelKey::new("moments"), ScopeKey::from_raw("other")), row: rid("m2"), order_key: "a".to_string() }]);
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
    db.settle();
    let reads = db.counters().observation_reads;
    put(&db, "moments", json!({"id": "m2", "userId": "u2", "text": "other row"}));
    assert_eq!(db.counters().observation_reads, reads);
    db.update("moments", &rid("m1"), obj(json!({"text": "changed"}))).unwrap();
    db.settle();
    assert_eq!(texts(&changes(&log)[1]), ["changed"]);
    db.destroy("moments", &rid("m1")).unwrap();
    db.settle();
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
    db.settle();
    put(&db, "moments", json!({"id": "m3", "userId": "u1", "text": "three"}));
    db.update("moments", &rid("m1"), obj(json!({"userId": "u2"}))).unwrap();
    db.settle();
    assert_eq!(counts.lock().unwrap().iter().map(|(_, count)| *count).collect::<Vec<_>>(), [1]);
    put(&db, "moments", json!({"id": "m4", "userId": "u1", "text": "one"}));
    let seen = counts.lock().unwrap().clone();
    assert_eq!(seen.iter().map(|(_, count)| *count).collect::<Vec<_>>(), [1, 2]);
    assert!(seen[0].0 < seen[1].0);
}

#[test]
fn a_temp_id_swap_arrives_as_replaced() {
    let db = booted();
    let temp = db.create("moments", obj(json!({"text": "new"}))).unwrap();
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.settle();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Replace, db.generation());
    envelope.entity_ops.push(EntityOp::Destroy { model: ModelKey::new("moments"), id: temp.id.clone(), reason: DestroyReason::Replaced { by: rid("m7") } });
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m7"), row: obj(json!({"id": "m7", "text": "new"})) });
    db.land(envelope).unwrap();
    db.settle();
    let all = changes(&log);
    assert_eq!(all.len(), 2);
    assert_eq!(all[1].diff.replaced, vec![(temp.id.clone(), rid("m7"))]);
    assert!(all[1].diff.inserted.is_empty() && all[1].diff.removed.is_empty());
    assert_eq!(ids(&all[1]), ["m7"]);
}

#[test]
fn destroying_a_parent_is_one_delivery_per_observer_with_one_revision() {
    let db = booted();
    seed_tree(&db);
    let observed: Vec<(ChangeLog, ObserverHandle)> = ["moments", "users", "vibes"]
        .iter()
        .map(|model| {
            let (log, callback) = recorder();
            (log, db.observe(Query::new(model), callback).unwrap())
        })
        .collect();
    db.settle();
    db.destroy("users", &rid("u1")).unwrap();
    db.settle();
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
    db.settle();
    let reads = db.counters().observation_reads;
    for number in 0..100 {
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&format!("m{number}")), row: obj(json!({"id": format!("m{number}"), "userId": "u1", "text": "t"})) });
        db.land(envelope).unwrap();
    }
    db.settle();
    assert!(db.counters().observation_reads - reads <= 100);
    assert!(logs.iter().all(|(log, _)| changes(log).last().unwrap().rows.len() == 100));
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
    db.settle();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid("m1"), row: obj(json!({"id": "m1", "userId": "u1", "text": "one"})) });
    db.land(envelope).unwrap();
    entered.recv_timeout(WAIT).unwrap();
    drop(subscription);
    release.send(()).unwrap();
    put(&db, "moments", json!({"id": "m2", "userId": "u1", "text": "two"}));
    put(&db, "moments", json!({"id": "m3", "userId": "u1", "text": "three"}));
    assert_eq!(calls_made.load(Ordering::SeqCst), 2);
}

#[test]
fn a_slow_listener_does_not_hold_back_the_others_and_gets_the_latest_state() {
    let db = booted();
    let (entered_sender, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);
    let slow_log = Arc::new(Mutex::new(Vec::new()));
    let slow_sink = slow_log.clone();
    let _slow = db
        .observe(Query::new("moments"), move |change| {
            slow_sink.lock().unwrap().push(change.rows.len());
            if change.rows.len() == 1 {
                entered_sender.send(()).unwrap();
                released.lock().unwrap().recv_timeout(WAIT).unwrap();
            }
        })
        .unwrap();
    let (fast_sender, fast) = mpsc::channel();
    let _fast = db.observe(Query::new("moments"), move |change| fast_sender.send(change.rows.len()).unwrap()).unwrap();
    for number in 0..3 {
        let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&format!("m{number}")), row: obj(json!({"id": format!("m{number}"), "userId": "u1", "text": "t"})) });
        db.land(envelope).unwrap();
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
    db.settle();
    let seen = slow_log.lock().unwrap().clone();
    assert_eq!(*seen.last().unwrap(), 3);
    assert!(seen.len() <= 3);
}

#[test]
fn reset_moves_every_observer_to_the_empty_state_and_keeps_the_subscriptions() {
    let db = booted();
    seed_tree(&db);
    let (log, callback) = recorder();
    let _subscription = db.observe(Query::new("moments"), callback).unwrap();
    db.settle();
    db.reset_all().unwrap();
    db.settle();
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
                db.land(envelope).unwrap();
            }
            1 => {
                let _ = db.update("moments", &rid(&id), obj(json!({"text": row["text"]})));
            }
            2 => {
                let _ = db.destroy("moments", &rid(&id));
            }
            3 => {
                let rows = (0..seed.below(8)).map(|_| random_row(&mut seed, 14)).map(|(_, row)| row).collect();
                let mut request = ReplaceRequest::new(feed(), Coverage::Complete);
                request.on_absent = if seed.below(2) == 0 { OnAbsent::Destroy } else { OnAbsent::Detach };
                db.replace(&request, rows).unwrap();
            }
            4 => {
                let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
                envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(&id), row });
                db.land(envelope).unwrap();
            }
            _ => {
                let _ = db.create("moments", obj(json!({"text": "fresh"})));
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
fn model_test_with_threads_ends_every_observer_on_the_fresh_state() {
    for seed_value in [0x2545F4914F6CDD1D_u64, 0x9E3779B97F4A7C15, 0xD1B54A32D192ED03] {
        let db = booted();
        seed_tree(&db);
        land_scope(&db, vec![append("m1", "a")]);
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
            handle.join().unwrap();
        }
        db.settle();
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
