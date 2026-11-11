//! P-XFER-47: the time of a transfer (`created_at`, `failed_at`, the term of seven days) is the time of the step; the time an input carries decides nothing.

use super::transfer_harness::*;
use crate::owners::write::transfer::{TransferId, TransferInput, TransferStage, TransferTick};
use crate::runtime::Time;
use crate::owners::store::StoreOp;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";
const WEEK: u64 = 7 * DAY;

#[derive(Clone, Copy, Debug)]
enum Turn {
    Accept,
    AcceptBatch,
    Expire,
}

fn input_of(turn: Turn, reply: u64) -> TransferInput {
    match turn {
        Turn::Accept => TransferInput::Accept { epoch: EPOCH, item: Box::new(item(KEY, 2)), reply },
        Turn::AcceptBatch => TransferInput::AcceptBatch { epoch: EPOCH, items: vec![item(KEY, 2), item(OTHER, 2)], joint: Box::new(joint()), reply },
        Turn::Expire => TransferInput::Tick { kind: TransferTick::Expire },
    }
}

/// A call of the platform at the step `at`: the owner runs on the clock of the step.
fn created(turn: Turn, at: u64) -> Vec<Time> {
    let mut twin = Twin::new();
    twin.det.tick(Time(at));
    let reply = twin.reply();
    twin.post(input_of(turn, reply));
    [KEY, OTHER].into_iter().filter_map(|key| twin.state().open_id(key)).map(|id| twin.t(id).created_at).collect()
}

/// The check of the term at the step `at`.
fn expire_at(twin: &mut Twin, at: u64) -> Vec<String> {
    twin.det.tick(Time(at));
    twin.det.post(crate::runtime::Address::<crate::owners::write::transfer::Transfers>::new(TRANSFER_AT), crate::runtime::EpochStamp::Epoch(EPOCH), input_of(Turn::Expire, 0));
    twin.det.run_until_quiet(10_000).expect("the owner is quiet");
    twin.at(Time(at)).removed()
}

/// The transfer that failed at the step `failed`: the time its failure row carries.
fn failed_at(failed: u64) -> (Twin, TransferId, Option<u64>) {
    let mut twin = Twin::new();
    twin.det.tick(Time(failed));
    let id = twin.uploading(KEY, 2);
    let out = twin.step_error(id, false);
    let rows: Vec<Option<u64>> = out.rows().iter().map(|(row, _)| row.failed_at).collect();
    twin.commit();
    assert_eq!(twin.stage(id), TransferStage::Failed, "the transfer failed");
    (twin, id, rows.into_iter().flatten().next())
}

fn seeded(seed: u64, failures: &mut Vec<String>) {
    const WAITS: [u64; 6] = [1, WEEK - 1, WEEK, WEEK + 1, 3 * WEEK, 90 * DAY];
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut roll = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let mut at = DAY + roll(1_000_000);
    for round in 0..3 {
        let turn = [Turn::Accept, Turn::AcceptBatch][roll(2) as usize];
        let found = created(turn, at);
        if found.iter().any(|time| time.0 != at) || found.is_empty() {
            failures.push(format!("seed {seed} round {round}: {turn:?} at {at}: created_at {found:?}, the time of the step is {at}"));
        }
        at += 1 + roll(DAY);
        let wait = WAITS[roll(WAITS.len() as u64) as usize];
        let (mut twin, _, failed) = failed_at(at);
        if failed != Some(at) {
            failures.push(format!("seed {seed} round {round}: failed at {at}: failed_at {failed:?}"));
        }
        let removed = expire_at(&mut twin, at + wait);
        let want: Vec<String> = if wait > WEEK { vec![KEY.to_string()] } else { Vec::new() };
        if removed != want {
            failures.push(format!("seed {seed} round {round}: failed at {at}, checked after {wait} ms: removed {removed:?}, the time of the step gives {want:?}"));
        }
        at += wait + 1 + roll(DAY);
    }
}

#[test]
fn xfer_prop_timeouts_for_any_accept_times() {
    let mut failures = Vec::new();
    let at = 10 * DAY;
    for (name, turn) in [("Accept", Turn::Accept), ("AcceptBatch", Turn::AcceptBatch)] {
        let found = created(turn, at);
        let want = if matches!(turn, Turn::Accept) { 1 } else { 2 };
        if found != vec![Time(at); want] {
            failures.push(format!("{name}: accepted at {at}: created_at {found:?}, the time of the step gives {:?}", vec![Time(at); want]));
        }
    }
    let failed = 3 * DAY;
    let (_, _, written) = failed_at(failed);
    if written != Some(failed) {
        failures.push(format!("failed at the step {failed}: failed_at {written:?}"));
    }
    for (name, wait) in [("1 ms over the week", WEEK + 1), ("exactly the week", WEEK), ("1 ms before the week", WEEK - 1)] {
        let (mut twin, id, _) = failed_at(failed);
        let removed = expire_at(&mut twin, failed + wait);
        let want: Vec<String> = if wait > WEEK { vec![KEY.to_string()] } else { Vec::new() };
        let stage = if wait > WEEK { TransferStage::Removing } else { TransferStage::Failed };
        if removed != want || twin.stage(id) != stage {
            failures.push(format!("{name}: failed at {failed}, checked after {wait} ms: removed {removed:?} in {:?}, the time of the step gives {want:?} in {stage:?}", twin.stage(id)));
        }
    }
    for (name, wait) in [("Restored one ms over the week after the failure", WEEK + 1), ("Restored exactly a week after the failure", WEEK)] {
        let mut row = stored(1, KEY, 2);
        row.restore_failed = Some("prior".into());
        row.created_at = Time(0);
        row.failed_at = Some(Time(failed));
        let mut twin = Twin::bare(1);
        twin.det.tick(Time(failed + wait));
        twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] });
        twin.post(TransferInput::Restored { uploads: vec![row], outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
        twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        let out = twin.post(TransferInput::Online(crate::owners::contract::Online(true)));
        let discarded = out.batches.iter().flat_map(|batch| batch.ops.iter()).any(|op| matches!(op, StoreOp::RemoveUpload { .. }));
        let stage = twin.stage(1);
        if wait > WEEK && stage != TransferStage::Removing || wait <= WEEK && (stage != TransferStage::Failed || discarded) {
            failures.push(format!("{name}: stage {stage:?}, discard written {discarded}"));
        }
    }
    for seed in 1..=8u64 {
        seeded(seed, &mut failures);
    }
    assert!(failures.is_empty(), "the time of the input decided: {failures:#?}");
}
