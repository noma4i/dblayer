use std::sync::{Arc, Mutex};

use serde_json::json;

use super::fixtures::*;
use super::listeners::{Listener, Mailbox, Subscriber};
use super::watch::{Body, Loaded, Subject, Version};
use super::{Change, Db, Row};
use crate::commit::{CommitEnvelope, DestroyReason, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, ListKey};
use crate::query::{CompareOp, Order, Query, Where};
use crate::value::SortDirection;

const SEQUENCES: u64 = 1200;
const EDITS: usize = 7;
const TEXTS: [&str; 8] = ["", "a", "B", "b", "é", "10", "9", "zz"];
const DATES: [&str; 4] = ["2026-01-01T00:00:00.000Z", "2026-01-02T00:00:00.000Z", "2026-01-03T10:00:00.000Z", "2026-02-01T00:00:00.000Z"];

struct Dice(u64);

impl Dice {
    fn below(&mut self, limit: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((self.0 >> 33) % limit as u64) as usize
    }
}

fn query_of(variant: usize) -> Query {
    let order = |field: &str, direction| Order::new(field, direction);
    let moments = Query::new("moments");
    let roster = || ListKey::from_raw("roster");
    match variant % 9 {
        0 => moments.ordered(vec![order("text", SortDirection::Asc)]),
        1 => moments.ordered(vec![order("text", SortDirection::Desc)]),
        2 => moments.ordered(vec![order("text", SortDirection::AscNullsLast)]),
        3 => moments.ordered(vec![order("text", SortDirection::DescNullsFirst), order("createdAt", SortDirection::Asc)]),
        4 => moments.ordered(vec![order("id", SortDirection::Desc)]),
        5 => moments.filtered(Where::equal("userId", json!("u1"))).ordered(vec![order("text", SortDirection::Asc)]),
        7 => moments.in_scope(roster()),
        8 => moments.in_scope(roster()).ordered(vec![order("text", SortDirection::Asc)]),
        _ => moments.filtered(Where::compare("createdAt", CompareOp::GreaterOrEqual, json!(DATES[2]))).ordered(vec![order("createdAt", SortDirection::Desc), order("text", SortDirection::AscNullsLast)]),
    }
}

fn random_row(dice: &mut Dice, id: &str) -> serde_json::Map<String, serde_json::Value> {
    let user = ["u1", "u2", "u3"][dice.below(3)];
    let mut row = json!({"id": id, "userId": user});
    if dice.below(6) != 0 {
        row["text"] = json!(TEXTS[dice.below(TEXTS.len())]);
    }
    if dice.below(4) != 0 {
        row["createdAt"] = json!(DATES[dice.below(DATES.len())]);
    }
    obj(row)
}

fn stage(envelope: &mut CommitEnvelope, dice: &mut Dice, op: EntityOp) {
    let scope = ScopeRef::new(ModelKey::new("moments"), ListKey::from_raw("roster"));
    let row = op.row_ref().id;
    let order_key = format!("a1{}{}", &row.as_str()[1..], 1 + dice.below(9));
    match (&op, dice.below(4)) {
        (EntityOp::Upsert { .. }, 0 | 1) => envelope.scope_ops.push(ScopeOp::Append { scope, row, order_key }),
        (EntityOp::Upsert { .. }, 2) => envelope.scope_ops.push(ScopeOp::Move { scope, row, order_key }),
        (EntityOp::Upsert { .. }, _) => envelope.scope_ops.push(ScopeOp::Detach { scope, row }),
        _ => {}
    }
    envelope.entity_ops.push(op);
}

fn edit(db: &Db, dice: &mut Dice, known: &mut Vec<String>, fresh: &mut usize) -> String {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    let id = edit_into(&mut envelope, dice, known, fresh);
    db.land(&db.admit().unwrap(), envelope).unwrap();
    id
}

fn edit_into(envelope: &mut CommitEnvelope, dice: &mut Dice, known: &mut Vec<String>, fresh: &mut usize) -> String {
    let moments = ModelKey::new("moments");
    if !known.is_empty() && dice.below(5) == 0 {
        let id = known.swap_remove(dice.below(known.len()));
        stage(envelope, dice, EntityOp::Destroy { model: moments, id: rid(&id), reason: DestroyReason::Server });
        return id;
    }
    let id = if known.is_empty() || dice.below(3) == 0 {
        *fresh += 1;
        let id = format!("m{fresh:05}");
        known.push(id.clone());
        id
    } else {
        known[dice.below(known.len())].clone()
    };
    let row = random_row(dice, &id);
    stage(envelope, dice, EntityOp::Upsert { model: moments, id: rid(&id), row });
    id
}

fn shown(rows: &[Row]) -> Vec<(String, i64)> {
    rows.iter().map(|row| (row.id.to_string(), row.revision)).collect()
}

fn run_sequence(seed: u64) {
    let mut dice = Dice(seed);
    let db = booted();
    let query = query_of(seed as usize);
    let (mut known, mut fresh) = (Vec::new(), 0);
    let mut preparation = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for _ in 0..dice.below(25) {
        edit_into(&mut preparation, &mut dice, &mut known, &mut fresh);
    }
    db.land(&db.admit().unwrap(), preparation).unwrap();
    let mirror: Arc<Mutex<Vec<Row>>> = Arc::default();
    let sink = mirror.clone();
    let subscriber = Subscriber::new(Listener::Rows(Box::new(move |change: &Change| change.apply_to(&mut sink.lock().unwrap()))), Default::default());
    let first = db.shared.evaluate(&Subject::Rows(query.clone())).unwrap();
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
    flush(&mut mailbox, &mut delivered);
    for step in 0..EDITS {
        let touched: Vec<String> = (0..1 + dice.below(3)).map(|_| edit(&db, &mut dice, &mut known, &mut fresh)).collect();
        let epoch = db.shared.ephemeral.lock().epoch;
        let horizon = (db.shared.store.revision_seq().unwrap(), epoch);
        let evaluated = db.shared.evaluate_step(&query, &latest, &touched.iter().cloned().collect(), horizon).unwrap();
        if let Some(next) = evaluated.version.succeeding(&latest, &[], evaluated.loaded) {
            let supply = match &next.body {
                Body::Rows(body) => body.step.as_ref().map(|step| step.supply()).unwrap_or_default(),
                Body::Count(_) => Loaded::default(),
            };
            latest = Arc::new(next);
            mailbox.push(latest.clone(), &[], supply);
        }
        let fresh_rows = db.query(&query).unwrap();
        let Body::Rows(body) = &latest.body else { unreachable!() };
        let order: Vec<String> = body.entries().iter().map(|entry| entry.id.to_string()).collect();
        assert_eq!(order, fresh_rows.iter().map(|row| row.id.to_string()).collect::<Vec<_>>(), "sequence {seed} step {step}: order of the stepped list");
        if dice.below(2) == 0 {
            flush(&mut mailbox, &mut delivered);
            assert_eq!(shown(&mirror.lock().unwrap()), shown(&fresh_rows), "sequence {seed} step {step}: list rebuilt from deliveries");
        }
    }
    flush(&mut mailbox, &mut delivered);
    assert_eq!(shown(&mirror.lock().unwrap()), shown(&db.query(&query).unwrap()), "sequence {seed}: list rebuilt from deliveries");
}

#[test]
fn stepped_lists_equal_a_fresh_query_over_random_edit_sequences() {
    std::thread::scope(|scope| {
        let runs: Vec<_> = (1..=SEQUENCES).map(|seed| scope.spawn(move || run_sequence(seed))).collect();
        for run in runs {
            run.join().unwrap();
        }
    });
}
