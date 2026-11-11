//! P-OUT-59: the time of a Submit and of a local write is the time of the step; the time an input carries decides nothing.

use super::outbox_harness::*;
use crate::owners::contract::{Committed, Online};
use crate::owners::write::outbox::{CommitKind, LocalWriteKind, NewOp, OutboxInput};
use crate::runtime::Time;
use std::time::Duration;

const EXPIRES_MS: u64 = 10_000;
const MEMORY_MS: u64 = 10;

#[derive(Clone, Copy, Debug)]
enum Turn {
    Submit,
    Write,
}

fn input_of(turn: Turn, row: &str, reply: u64) -> OutboxInput {
    match turn {
        Turn::Submit => OutboxInput::Submit { epoch: EPOCH, op: NewOp { expires_after: Some(Duration::from_millis(EXPIRES_MS)), ..model_op(row, Policy::Rollback, false) }, reply },
        Turn::Write => OutboxInput::LocalWrite { model: "Moment".into(), epoch: EPOCH, write: LocalWriteKind::SetEphemeral { expire_after: Some(MEMORY_MS) }, row: row.into(), values: "{}".into(), reply },
    }
}

/// A queued operation submitted at the step `at` meets its selection at the step `at + wait`: the stage after it.
fn selected(twin: &mut Twin, row: &str, reply: u64, at: u64, wait: u64) -> St {
    twin.post(OutboxInput::Online(Online(false)));
    twin.det.tick(Time(at));
    twin.post(input_of(Turn::Submit, row, reply));
    let id = *twin.state().ops.keys().max().expect("the operation exists");
    twin.committed(id);
    assert_eq!(twin.stage(id), Some(St::Queued), "{row}: offline the operation waits");
    twin.det.tick(Time(at + wait));
    twin.post(OutboxInput::Online(Online(true)));
    let stage = twin.stage(id).expect("the stage");
    match stage {
        St::Sending => {
            twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None });
            twin.committed(id);
        }
        _ => {
            twin.committed(id);
        }
    }
    stage
}

fn rule_stage(wait: u64) -> St {
    if wait > EXPIRES_MS { St::RollingBack(RWhy::Rejected) } else { St::Sending }
}

/// A memory row written at the step `at` leaves at the step `at + MEMORY_MS` and not before: the kinds of the commits an expiry gives.
fn written(at: u64, due: u64) -> Vec<CommitKind> {
    let mut twin = Twin::new();
    twin.det.tick(Time(at));
    let effects = twin.post(input_of(Turn::Write, "w", 60));
    let batch = effects.iter().find_map(|effect| match effect { OutboxEffect::Store { batch, .. } => Some(*batch), _ => None }).expect("the batch of the write");
    twin.post(OutboxInput::Committed(Committed { batch, revision: 11, tables: vec!["moments".into()], rows: None, models: Default::default(), stale_parents: Vec::new(), position: 0, shown: Vec::new(), files_removed: 0, refused: Vec::new() }));
    let expired = twin.post(OutboxInput::Expire { model: "Moment".into(), row: "w".into(), due: Time(due) });
    expired.iter().filter_map(|effect| match effect { OutboxEffect::Store { kind, .. } => Some(*kind), _ => None }).collect()
}

fn seeded(seed: u64, failures: &mut Vec<String>) {
    const WAITS: [u64; 6] = [1, 9_999, 10_000, 10_001, 12_345, 60_000];
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut roll = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let mut twin = Twin::new();
    let mut at = 1_000 + roll(1_000_000);
    for round in 0..4u64 {
        let wait = WAITS[roll(WAITS.len() as u64) as usize];
        let got = selected(&mut twin, &format!("s{round}"), 100 + round, at, wait);
        if got != rule_stage(wait) {
            failures.push(format!("seed {seed} round {round}: submitted at {at}, selected after {wait} ms: the owner gave {got:?}, the time of the step gives {:?}", rule_stage(wait)));
        }
        at += wait + 1 + roll(5_000);
    }
    let (early, on_term) = (written(at, at + MEMORY_MS - 1), written(at, at + MEMORY_MS));
    if !early.is_empty() || on_term != [CommitKind::RemoveRow] {
        failures.push(format!("seed {seed}: a memory row written at {at}: before the term {early:?}, on the term {on_term:?}"));
    }
}

#[test]
fn out_prop_expiry_for_any_submit_times() {
    let mut failures = Vec::new();
    let at = 50_000;
    let cases: [(&str, u64); 4] = [
        ("1 ms over the term", EXPIRES_MS + 1),
        ("the term is exact", EXPIRES_MS),
        ("1 ms before the term", EXPIRES_MS - 1),
        ("a long wait", 60_000),
    ];
    for (index, (name, wait)) in cases.into_iter().enumerate() {
        let got = selected(&mut Twin::new(), "r", 30 + index as u64, at, wait);
        if got != rule_stage(wait) {
            failures.push(format!("{name}: submitted at {at}, selected after {wait} ms: the owner gave {got:?}, the time of the step gives {:?}", rule_stage(wait)));
        }
    }
    let memory: [(&str, u64, u64); 2] = [
        ("memory row, the term is exact", at, at + MEMORY_MS),
        ("memory row, 1 ms before the term", at, at + MEMORY_MS - 1),
    ];
    for (name, step, due) in memory {
        let want: Vec<CommitKind> = if due >= step + MEMORY_MS { vec![CommitKind::RemoveRow] } else { Vec::new() };
        let got = written(step, due);
        if got != want {
            failures.push(format!("{name}: written at {step}, expiry due {due}: the owner gave {got:?}, the time of the step gives {want:?}"));
        }
    }
    for seed in 1..=8u64 {
        seeded(seed, &mut failures);
    }
    assert!(failures.is_empty(), "the time of the input decided: {failures:#?}");
}
