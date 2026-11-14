//! Owner-level tests of the queue rules of the `outbox` machine: the drain limit, the order of the slots and the expiry of a queued operation.

use std::time::Duration;

use super::outbox_cells_tests::{submit_op, tick};
use super::outbox_harness::*;
use crate::owners::contract::Online;
use crate::owners::store::StoreOp;
use crate::owners::write::failure;
use crate::owners::write::outbox::{NewOp, OpId, OutboxAnswer, OutboxInput, OutboxOutcome, Stage, StoredOp, DRAIN_LIMIT};
use crate::runtime::Time;
use crate::value::sync::FailureKind;

#[test]
fn out_answer_releases_slot_before_decoding() {
    let mut twin = Twin::new();
    let first = twin.submit("r1", Policy::Rollback, false);
    twin.committed(first);
    let second = twin.submit("r2", Policy::Rollback, false);
    twin.committed(second);
    assert_eq!(twin.stage(second), Some(St::Queued));
    let request_id = twin.state().ops[&first].request_id;
    let effects = twin.post(OutboxInput::Answered { req_id: request_id, outcome: crate::owners::session::AuthorOutcome::Response { body: "{}".into(), kind: crate::transport::AnswerKind::Answered { sequence: None }, pending_retained: false } });
    assert_eq!(sent_ids(&effects), vec![second], "the answer frees the slot before it is decoded");
}

fn respond(twin: &mut Twin, id: OpId) -> Vec<OutboxEffect> {
    twin.post(OutboxInput::Response { op_id: id, rows: server_rows(), swaps: Vec::new(), stale: Vec::new(), result: None, missing: Vec::new(), errors: None })
}

fn sent_ids(effects: &[OutboxEffect]) -> Vec<OpId> {
    effects.iter().filter_map(|effect| match effect { OutboxEffect::Send { op_id, .. } => Some(*op_id), _ => None }).collect()
}

#[test]
fn out_queued_drain_limit() {

    let mut twin = Twin::new();
    let ids: Vec<OpId> = (1..=DRAIN_LIMIT + 1).map(|row| twin.submit(&format!("r{row}"), Policy::Rollback, false)).collect();
    let started: usize = ids.iter().map(|id| sends(&twin.committed(*id))).sum();
    assert_eq!(started, DRAIN_LIMIT, "{DRAIN_LIMIT} Mutate go out, the next waits");
    let last = *ids.last().unwrap();

    let freed = respond(&mut twin, ids[0]);
    assert_eq!(sent_ids(&freed), vec![last], "the freed slot goes to the queued operation: {freed:?}");
}

#[test]
fn out_queued_expired() {

    let queued_for = |seconds: u64| {
        let mut twin = Twin::new();
        twin.post(OutboxInput::Online(Online(false)));
        let op = NewOp { expires_after: Some(Duration::from_secs(10)), ..model_op("r", Policy::Rollback, false) };
        let id = submit_op(&mut twin, op, 30);
        twin.committed(id);
        twin.waiter(id);
        twin.det.tick(Time(seconds * 1000));
        (twin, id)
    };
    let (mut twin, id) = queued_for(11);
    let online = twin.post(OutboxInput::Online(Online(true)));
    assert_eq!((kinds(&online), sends(&online)), (vec![o_rollback()], 0), "{online:?}");
    assert_eq!(twin.stores.len(), 1);
    assert!(twin.store_ops.contains(&StoreOp::RollBack { op_id: id }) && twin.store_ops.contains(&StoreOp::RemoveIntent { op_id: id }), "{:?}", twin.store_ops);
    let done = twin.committed(id);
    let outcome = answers(&done, WAITER).into_iter().find_map(|answer| match answer { OutboxAnswer::Outcome(OutboxOutcome::Rejected { error }) => Some(error), _ => None }).expect("the outcome is Rejected");
    assert_eq!(failure::parse(&outcome).kind, FailureKind::Expired);

    for (attempts, expect_rollback) in [(1, true), (2, false)] {
        let mut twin = Twin::bare();
        declare(&mut twin);
        let op = NewOp { expires_after: Some(Duration::from_secs(10)), ..model_op("r", Policy::Rollback, false) };
        twin.det.tick(Time(11_000));
        let restored = twin.post(OutboxInput::Restored { ops: vec![StoredOp { answered: None, op_id: 1, op, created_at: 0, error_mark: false, pending: None, attempts, cycle_id: 1, cause: None, restore_failed: None, restore_element: None, removed_rows: Vec::new() }], pending_lost: false });
        let mut ready = restored;
        ready.extend(twin.post(OutboxInput::RestoreReady { epoch: EPOCH }));
        assert_eq!((kinds(&ready) == vec![o_rollback()], sends(&ready)), (expect_rollback, usize::from(!expect_rollback)), "attempts {attempts}: the recorded attempt leaves without a new write: {ready:?}");
    }
}

fn o_rollback() -> crate::owners::write::outbox::CommitKind { crate::owners::write::outbox::CommitKind::Rollback }

struct Dice(u64);

impl Dice {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        usize::try_from(self.0 >> 33).unwrap_or(0) % bound
    }
}

/// One random trace: a step submits, commits, answers, refuses, fails on the network, fires a retry or moves the network. `rows` = how many rows the submits spread over, 0 = one row per operation.
fn trace(seed: u64, rows: usize) -> Vec<OpId> {
    let mut twin = Twin::new();
    let mut dice = Dice(seed);
    let mut order: Vec<OpId> = Vec::new();
    for _ in 0..80 {
        let open: Vec<(OpId, Stage)> = twin.state().ops.values().map(|operation| (operation.op_id, operation.stage)).collect();
        let pick = |stages: &[fn(Stage) -> bool], dice: &mut Dice| -> Option<OpId> {
            let found: Vec<OpId> = open.iter().filter(|(_, stage)| stages.iter().any(|test| test(*stage))).map(|(id, _)| *id).collect();
            (!found.is_empty()).then(|| found[dice.below(found.len())])
        };
        let effects = match dice.below(12) {
            0..=3 => {
                let row = if rows == 0 { open.len() } else { dice.below(rows) };
                twin.submit(&format!("r{row}"), Policy::Rollback, false);
                Vec::new()
            }
            4 | 5 => match pick(&[|stage| matches!(stage, Stage::Submitting | Stage::Landing | Stage::RollingBack(_) | Stage::PersistingAttempt)], &mut dice) {
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
                Some(id) => twin.post(OutboxInput::TransportError { op_id: id, temporary: dice.below(2) == 0 }),
                None => Vec::new(),
            },
            9 => match pick(&[|stage| stage == Stage::Sending], &mut dice) {
                Some(id) => tick(&mut twin, id),
                None => Vec::new(),
            },
            _ => twin.post(OutboxInput::Online(Online(dice.below(3) != 0))),
        };
        let sent = sent_ids(&effects);
        assert!(sent.windows(2).all(|pair| pair[0] < pair[1]), "seed {seed}: one step sends in seq order: {sent:?}");
        order.extend(sent);
        let state = twin.state();
        assert!(state.slots_taken() <= DRAIN_LIMIT, "seed {seed}: {} slots", state.slots_taken());
        let startable = state.online == Some(true) && state.slots_taken() < DRAIN_LIMIT && state.ops.values().any(|queued| queued.stage == Stage::Queued && !state.ops.values().any(|other| other.op_id < queued.op_id && other.op.row_id == queued.op.row_id && other.stage != Stage::Done));
        assert!(!startable, "seed {seed}: a free slot and a queued operation on a free row");
    }
    order
}

#[test]
fn out_prop_drain_order() {
    for seed in 1..=60 {
        trace(seed, 6);
    }
    for seed in 1..=60 {
        let order = trace(seed, 0);
        let mut first_sends: Vec<OpId> = Vec::new();
        for id in order { if !first_sends.contains(&id) { first_sends.push(id); } }
        let mut sorted = first_sends.clone();
        sorted.sort_unstable();
        assert_eq!(first_sends, sorted, "seed {seed}: the first Mutate of each operation follows the op_id order");
    }
}

#[test]
fn out_sending_tick_no_slot() {

    let mut twin = Twin::new();
    let first = twin.submit("r1", Policy::Rollback, false);
    twin.committed(first);
    twin.post(OutboxInput::TransportError { op_id: first, temporary: true });
    let second = twin.submit("r2", Policy::Rollback, false);
    assert_eq!(sends(&twin.committed(second)), 1, "the waiting retry holds no slot");
    let due = tick(&mut twin, first);
    assert_eq!((sends(&due), kinds(&due)), (0, vec![crate::owners::write::outbox::CommitKind::Requeue]), "the operation waits in the queue and its column says pending: {due:?}");
    let freed = respond(&mut twin, second);
    assert_eq!((kinds(&freed), sends(&freed)), (vec![crate::owners::write::outbox::CommitKind::Land, crate::owners::write::outbox::CommitKind::Attempt], 0), "the freed slot gives the retry its next attempt");
}

#[test]
fn out_expiry_clock_at_select() {
    let after_tick = |now_ms: u64| -> Vec<OutboxEffect> {
        let mut twin = Twin::new();
        let blocker = twin.submit("b", Policy::Rollback, false);
        twin.committed(blocker);
        let op = NewOp { expires_after: Some(Duration::from_secs(10)), ..model_op("r", Policy::Rollback, false) };
        let id = submit_op(&mut twin, op, 30);
        twin.committed(id);
        twin.waiter(id);
        twin.det.tick(Time(now_ms));
        let mut effects = respond(&mut twin, blocker);
        effects.extend(twin.committed(blocker));
        effects
    };
    let early = after_tick(9_000);
    assert_eq!((sends(&early), kinds(&early).contains(&o_rollback())), (1, false), "the slot-freed selection at a now below the deadline sends the operation");
    let late = after_tick(11_000);
    assert_eq!((sends(&late), kinds(&late).contains(&o_rollback())), (0, true), "the selection at a later now expires it, the earlier Tick did not");
}

#[test]
fn out_waits_for_the_first_online_value() {
    let mut twin = Twin::unannounced();
    declare(&mut twin);
    twin.post(OutboxInput::Restored { ops: Vec::new(), pending_lost: false });
    twin.post(OutboxInput::RestoreReady { epoch: EPOCH });
    let id = twin.submit("r", Policy::Rollback, false);
    let committed = twin.committed(id);
    assert_eq!((sends(&committed), twin.pauses.len()), (0, 0), "no value yet: nothing goes out and no retry is scheduled: {committed:?}");
    let online = twin.post(OutboxInput::Online(Online(true)));
    assert_eq!(sent_ids(&online), vec![id], "{online:?}");
}

#[derive(Clone, Copy, Debug)]
enum Answer {
    Commit(usize),
    Storage(usize),
}

fn orders(items: &mut Vec<usize>, from: usize, found: &mut Vec<Vec<usize>>) {
    if from == items.len() {
        found.push(items.clone());
        return;
    }
    for at in from..items.len() {
        items.swap(from, at);
        orders(items, from + 1, found);
        items.swap(from, at);
    }
}

/// P-OUT-70: the `Mutate` of the operations leave in the order of `Submit`, whatever the order of the Store answers to their intent batches.
#[test]
fn out_prop_operations_leave_in_submit_order() {
    let mut perms = Vec::new();
    orders(&mut vec![0, 1, 2], 0, &mut perms);
    let mut scripts: Vec<(Vec<Answer>, Option<usize>)> = Vec::new();
    for perm in &perms {
        scripts.push((perm.iter().map(|op| Answer::Commit(*op)).collect(), None));
        for refused in 0..3 {
            let refusing: Vec<Answer> = perm.iter().map(|op| if *op == refused { Answer::Storage(*op) } else { Answer::Commit(*op) }).collect();
            scripts.push((refusing, Some(refused)));
        }
    }
    for (script, refused) in scripts {
        let mut twin = Twin::new();
        let ids: Vec<OpId> = (0..3).map(|row| twin.submit(&format!("r{row}"), Policy::Rollback, false)).collect();
        let mut left: Vec<OpId> = Vec::new();
        for answer in &script {
            let effects = match *answer {
                Answer::Commit(op) => twin.committed(ids[op]),
                Answer::Storage(op) => twin.rejected(ids[op], RReason::Storage),
            };
            left.extend(sent_ids(&effects));
        }
        let mut answered = 0;
        while answered < left.len() {
            let effects = respond(&mut twin, left[answered]);
            answered += 1;
            left.extend(sent_ids(&effects));
        }
        let expected: Vec<OpId> = ids.iter().enumerate().filter(|(op, _)| Some(*op) != refused).map(|(_, id)| *id).collect();
        assert_eq!(left, expected, "answers {script:?}: the Mutate leave in the order of Submit");
    }

    let mut twin = Twin::new();
    let failed = twin.submit("r1", Policy::Keep, false);
    twin.committed(failed);
    twin.post(OutboxInput::TransportError { op_id: failed, temporary: false });
    twin.committed(failed);
    assert_eq!(twin.stage(failed), Some(St::Failed), "the earlier operation of r1 failed");
    let behind_failed = twin.submit("r1", Policy::Rollback, false);
    let other_row = twin.submit("r2", Policy::Rollback, false);
    assert!(sent_ids(&twin.committed(other_row)).is_empty(), "B waits for the earlier intent");
    let effects = twin.committed(behind_failed);
    assert_eq!(sent_ids(&effects), vec![other_row], "B leaves in the step the earlier intent is written, A2 waits behind the failed operation");
    assert_eq!(twin.stage(behind_failed), Some(St::Queued));

    let mut twin = Twin::new();
    twin.post(OutboxInput::Online(Online(false)));
    twin.submit_as("c", Policy::Rollback, false, "create");
    let created = *twin.state().ops.keys().max().expect("the create");
    twin.committed(created);
    twin.submit_as("c", Policy::Rollback, false, "destroy");
    let other_row = twin.submit("r2", Policy::Rollback, false);
    twin.committed(other_row);
    twin.post(OutboxInput::Online(Online(true)));
    assert!(twin.state().ops[&other_row].stage == Stage::Queued, "B waits for the cancelling destroy");
    let destroy = *twin.state().ops.keys().find(|id| twin.state().ops[id].cancels == Some(created) && **id != created).expect("the destroy");
    let effects = twin.committed(created);
    assert_eq!(twin.stage(destroy), Some(St::Done), "the destroy ended with the rollback of the create");
    assert_eq!(sent_ids(&effects), vec![other_row], "B leaves in the step the cancelled operation ends");
}
