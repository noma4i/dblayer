//! Owner-level property of the owner `Transfer`: a restored row in `preparing` ends under every order of its inputs and of the tasks of its executor.

use super::transfer_harness::*;
use crate::ffi::upload_wire::WirePrepare;
use crate::owners::contract::Online;
use crate::owners::write::rounds::RoundsInput;
use crate::owners::write::transfer::{TransferInput, TransferStage};
use crate::prepare::{ImageFormat, PhotoEdit};

const KEY: &str = "Moment:r1:upload";

#[derive(Clone, Copy, Debug)]
enum Start {
    Preparing,
    FailedThenRetry,
}

fn preparing_row(start: Start) -> crate::owners::write::transfer::StoredTransfer {
    let mut found = stored(1, KEY, 1);
    found.prepare = Some(WirePrepare::Image { edit: PhotoEdit { max_size: None, enlarge: false, crop: None, output_size: None, mirror: false, rotation_degrees: 0 }, quality: None, format: ImageFormat::Jpeg, max_bytes: None });
    found.size = 10;
    if matches!(start, Start::FailedThenRetry) { found.failed = Some(crate::owners::write::failure::of_prepare(crate::value::sync::PrepareReason::Failed, "the prepared file is gone".into())); }
    found
}

fn answers_of(out: &Out, files: &mut PrepareFiles, pool: &mut Vec<Pending>) {
    for run in &out.runs { files.effect(run, pool); }
    for (_, input) in &out.schedules {
        match input.as_str() {
            "PrepareSlotFreed" => pool.push(Pending::Input(TransferInput::PrepareSlotFreed)),
            "SlotFreed" => pool.push(Pending::Input(TransferInput::SlotFreed)),
            _ => {}
        }
    }
    for input in &out.rounds {
        if let RoundsInput::Needed { ids, .. } = input { pool.push(Pending::Input(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: ids.clone(), refused: None })); }
    }
}

fn run_seed(start: Start, seed: u64) -> Result<(), String> {
    let mut twin = Twin::bare(1);
    let mut files = PrepareFiles::default();
    let mut pool: Vec<Pending> = Vec::new();
    let out = twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())], complete: false });
    answers_of(&out, &mut files, &mut pool);
    let out = twin.post(TransferInput::Restored { uploads: vec![preparing_row(start)], outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
    answers_of(&out, &mut files, &mut pool);
    let mut external = vec![TransferInput::RestoreReady { epoch: EPOCH }, TransferInput::Online(Online(true)), TransferInput::PowerSaving(true)];
    if matches!(start, Start::FailedThenRetry) {
        let reply = twin.reply();
        external.push(TransferInput::Retry { key: KEY.into(), source: None, reply });
    }
    let mut saving_off = Some(TransferInput::PowerSaving(false));
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut delivered: Vec<String> = Vec::new();
    for _ in 0..300 {
        let commits = usize::from(twin.has_pending());
        let saving_ready = saving_off.is_some() && !external.iter().any(|input| matches!(input, TransferInput::PowerSaving(true)));
        let candidates = pool.len() + external.len() + commits + usize::from(saving_ready);
        if candidates == 0 { break; }
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut at = (state % candidates as u64) as usize;
        let input = if at < pool.len() {
            let task = pool.remove(at);
            delivered.push(format!("{task:?}"));
            files.deliver(task)
        } else {
            at -= pool.len();
            if at < external.len() {
                let input = external.remove(at);
                delivered.push(format!("{input:?}"));
                Some(input)
            } else if at - external.len() < commits {
                delivered.push("Committed(oldest)".into());
                let out = twin.commit();
                answers_of(&out, &mut files, &mut pool);
                continue;
            } else {
                let input = saving_off.take().expect("the saving off");
                delivered.push(format!("{input:?}"));
                Some(input)
            }
        };
        if let Some(input) = input {
            let out = twin.post(input);
            answers_of(&out, &mut files, &mut pool);
        }
    }
    let stage = twin.stage(1);
    let ends = stage == TransferStage::Mutating && !twin.has_pending() && pool.is_empty() && external.is_empty() && saving_off.is_none();
    if ends { Ok(()) } else { Err(format!("{start:?} seed {seed}: stage {stage:?} error {:?}, unanswered commits {}, pool {pool:?}, external {external:?}, order {delivered:?}", twin.t(1).error, twin.has_pending())) }
}

#[test]
fn xfer_prop_restored_preparing_ends_under_every_order() {
    let results: Vec<_> = std::thread::scope(|scope| {
        let runs: Vec<_> = (0..64u64).flat_map(|chunk| [Start::Preparing, Start::FailedThenRetry].map(|start| scope.spawn(move || (chunk * 32..chunk * 32 + 32).filter_map(|seed| run_seed(start, seed).err()).collect::<Vec<_>>()))).collect();
        runs.into_iter().flat_map(|run| run.join().unwrap()).collect()
    });
    assert!(results.is_empty(), "{} of 4096 orders leave the restored preparing transfer without its end; first: {}", results.len(), results[0]);
}
