//! Owner-level tests of the queue rules of the `outbox` machine: the drain limit, the order of the slots and the expiry of a queued operation.

use std::time::Duration;

use super::outbox_cells_tests::{answers, kinds, sends, submit_op};
use super::outbox_harness::*;
use super::outbox_tests::want;
use super::ref_outbox::{Ctx, Eff, In, Policy, St, Why as RWhy};
use crate::owners::contract::Online;
use crate::owners::write::failure;
use crate::owners::write::outbox::{NewOp, OpId, OutboxAnswer, OutboxInput, OutboxOutcome, Stage, DRAIN_LIMIT};
use crate::runtime::Time;
use crate::value::sync::FailureKind;

fn respond(twin: &mut Twin, id: OpId) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None })
}

fn sent_ids(effects: &[OutboxEffect]) -> Vec<OpId> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Send { op_id, .. } => Some(*op_id), _ => None }).collect()
}

#[test]
fn out_queued_drain_limit() {
    let full = Ctx { slot_free: false, ..Ctx::ready() };
    assert_eq!((want(St::Queued, &full, In::Online(true)).next, want(St::Queued, &Ctx::ready(), In::RowFreed).next), (St::Queued, St::Sending));

    let mut twin = Twin::new();
    let ids: Vec<OpId> = (1..=DRAIN_LIMIT + 1).map(|row| twin.submit(&format!("r{row}"), Policy::Rollback, false)).collect();
    let started: usize = ids.iter().map(|id| sends(&twin.committed(*id))).sum();
    assert_eq!(started, DRAIN_LIMIT, "{DRAIN_LIMIT} Mutate go out, the next waits");
    let last = *ids.last().unwrap();
    assert_eq!((twin.stage(last), twin.state().slots_taken()), (Some(St::Queued), DRAIN_LIMIT));

    let freed = respond(&mut twin, ids[0]);
    assert_eq!(sent_ids(&freed), vec![last], "the freed slot goes to the queued operation: {freed:?}");
    assert_eq!(twin.state().slots_taken(), DRAIN_LIMIT);
}

#[test]
fn out_queued_expired() {
    let expired = Ctx { expired: true, ..Ctx::ready() };
    let cell = want(St::Queued, &expired, In::Online(true));
    assert_eq!((cell.next, cell.eff.contains(&Eff::Send)), (St::RollingBack(RWhy::Rejected), false));

    let queued_for = |seconds: u64| {
        let mut twin = Twin::new();
        twin.post(OutboxInput::Online(Online(false)));
        let op = NewOp { expires_after: Some(Duration::from_secs(10)), ..model_op("r", Policy::Rollback, false) };
        let id = submit_op(&mut twin, op, 30);
        twin.committed(id);
        twin.waiter(id);
        assert!(twin.post(OutboxInput::Tick(Time(seconds * 1000))).is_empty(), "no timer: time alone does nothing");
        (twin, id)
    };
    let (mut twin, id) = queued_for(11);
    let online = twin.post(OutboxInput::Online(Online(true)));
    assert_eq!((kinds(&online), sends(&online)), (vec![o_rollback()], 0), "{online:?}");
    let done = twin.committed(id);
    let outcome = answers(&done, WAITER).into_iter().find_map(|answer| match answer { OutboxAnswer::Outcome(OutboxOutcome::Rejected { error }) => Some(error), _ => None }).expect("the outcome is Rejected");
    assert_eq!(failure::parse(&outcome).kind, FailureKind::Expired);

    let (mut twin, id) = queued_for(9);
    assert_eq!(sends(&twin.post(OutboxInput::Online(Online(true)))), 1);
    assert_eq!(twin.stage(id), Some(St::Sending));
}

fn o_rollback() -> crate::owners::write::outbox::CommitKind { crate::owners::write::outbox::CommitKind::Rollback }

struct Dice(u64);

impl Dice {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        usize::try_from(self.0 >> 33).unwrap_or(0) % bound
    }
}

/// One random trace: a step submits, commits, answers, refuses or moves the network. `rows` = how many rows the submits spread over.
fn trace(seed: u64, limit: usize, rows: usize) -> Vec<OpId> {
    let mut twin = Twin::new_with(limit);
    let mut dice = Dice(seed);
    let mut order: Vec<OpId> = Vec::new();
    for _ in 0..60 {
        let open: Vec<(OpId, Stage)> = twin.state().ops.values().map(|operation| (operation.op_id, operation.stage)).collect();
        let pick = |stages: &[fn(Stage) -> bool], dice: &mut Dice| -> Option<OpId> {
            let found: Vec<OpId> = open.iter().filter(|(_, stage)| stages.iter().any(|test| test(*stage))).map(|(id, _)| *id).collect();
            (!found.is_empty()).then(|| found[dice.below(found.len())])
        };
        let effects = match dice.below(10) {
            0..=3 => {
                let row = if rows == 0 { open.len() } else { dice.below(rows) };
                twin.submit(&format!("r{row}"), Policy::Rollback, false);
                Vec::new()
            }
            4 | 5 => match pick(&[|stage| matches!(stage, Stage::Submitting | Stage::Landing | Stage::RollingBack(_))], &mut dice) {
                Some(id) => {
                    let oldest = open.iter().filter(|(_, stage)| *stage == Stage::Submitting).map(|(id, _)| *id).min();
                    twin.committed(if open.iter().any(|(found, stage)| *found == id && *stage == Stage::Submitting) { oldest.unwrap_or(id) } else { id })
                }
                None => Vec::new(),
            },
            6 | 7 => match pick(&[|stage| stage == Stage::Sending], &mut dice) {
                Some(id) => respond(&mut twin, id),
                None => Vec::new(),
            },
            8 => match pick(&[|stage| stage == Stage::Sending], &mut dice) {
                Some(id) => twin.post(OutboxInput::TransportError { op_id: id, temporary: false }),
                None => Vec::new(),
            },
            _ => twin.post(OutboxInput::Online(Online(dice.below(2) == 0))),
        };
        let sent = sent_ids(&effects);
        assert!(sent.windows(2).all(|pair| pair[0] < pair[1]), "seed {seed}: one step sends in seq order: {sent:?}");
        order.extend(sent);
        let state = twin.state();
        assert!(state.slots_taken() <= limit, "seed {seed}: {} slots", state.slots_taken());
        let startable = state.online && state.slots_taken() < limit && state.ops.values().any(|queued| queued.stage == Stage::Queued && !state.ops.values().any(|other| other.op_id < queued.op_id && other.op.row_id == queued.op.row_id && other.stage != Stage::Done));
        assert!(!startable, "seed {seed}: a free slot and a queued operation on a free row");
    }
    order
}

#[test]
fn out_prop_drain_order() {
    for seed in 1..=40 {
        trace(seed, DRAIN_LIMIT, 6);
    }
    for seed in 1..=40 {
        let order = trace(seed, 1, 0);
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(order, sorted, "seed {seed}: with one slot the Mutate order is the op_id order");
    }
}
