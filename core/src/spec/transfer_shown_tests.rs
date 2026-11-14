//! Owner-level tests of the owner `Transfer`: the shown value of a transfer reaches `sync_upload` (D-226).

use super::transfer_harness::*;
use crate::owners::write::transfer::{Shown, TransferInput, TransferStage, TransferTick};

const KEY: &str = "Moment:r1:upload";

fn live(shows: Vec<Shown>) -> Vec<Shown> { shows.into_iter().filter(|shown| *shown != Shown::Gone).collect() }

#[test]
fn xfer_every_show_is_stored() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    let steps = [
        twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: Some("denied".into()) }),
        twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: None }),
        twin.online(false),
        twin.online(true),
        twin.step_ok(id, 1, None),
    ];
    let mut shown = 0;
    for (index, out) in steps.iter().enumerate() {
        shown += out.shows().len();
        assert_eq!(out.stored_shows(), live(out.shows()), "step {index}: the value the readers get is the value of the row");
    }
    assert!(shown >= 2, "the steps show nothing: {shown}");
}

#[test]
fn xfer_show_publishes_after_committed() {
    let mut twin = Twin::new();
    twin.uploading(KEY, 3);
    twin.hold_shown = true;
    let out = twin.online(false);
    let batches = out.shown_batches();
    assert_eq!((batches.len(), out.shows()), (1, Vec::new()), "a value that waits for the Store is not published before Committed");
    let committed = twin.commit_shown(batches[0]);
    assert_eq!(committed.shows(), live(out.stored_shows()), "Committed publishes the value of its batch");
}

/// P-XFER-50: a shown value the Store fails to write is written again after the backoff, and Committed publishes it.
#[test]
fn xfer_show_storage_refusal_is_written_again() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.hold_shown = true;
    let out = twin.online(false);
    let rejected = twin.reject_shown(out.shown_batches()[0]);
    let due = TransferInput::Tick { kind: TransferTick::Shown { transfer_id: id } };
    assert!((rejected.shows(), rejected.shown.len()) == (Vec::new(), 0) && matches!(rejected.schedules.as_slice(), [(_, TransferInput::Tick { kind: TransferTick::Shown { transfer_id } })] if *transfer_id == id), "the refused value is not published and one retry of the value is scheduled: {rejected:?}");
    assert!(twin.tick_retry(id).shown.is_empty(), "the retry tick of the step does not write the value");
    let again = twin.post(due);
    assert_eq!((again.stored_shows(), again.shows()), (out.stored_shows(), Vec::new()), "the retry writes the same value, nothing is published before Committed: {again:?}");
    let committed = twin.commit_shown(again.shown_batches()[0]);
    assert_eq!(committed.shows(), live(out.stored_shows()), "Committed of the retry publishes the value");
}

/// P-XFER-50: a shown value refused as invalid data or after the end of the epoch is not written again.
#[test]
fn xfer_show_refusal_without_retry() {
    use crate::owners::contract::RejectReason;
    for reason in [RejectReason::Invalid("bad row".into()), RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        twin.uploading(KEY, 3);
        twin.hold_shown = true;
        let out = twin.online(false);
        let refused = twin.refuse_shown(out.shown_batches()[0], reason.clone());
        assert!(Twin::quiet(&refused) && refused.shown.is_empty(), "{reason:?}: nothing is written, scheduled or published: {refused:?}");
    }
}

/// P-XFER-51: a refused write of the round error changes nothing: the error stays in memory and the next round status writes it.
#[test]
fn xfer_round_error_refusal_is_quiet() {
    use crate::owners::contract::RejectReason;
    for reason in [RejectReason::Storage("disk".into()), RejectReason::Invalid("bad row".into()), RejectReason::NoSession, RejectReason::Closed] {
        let mut twin = Twin::new();
        let id = twin.uploading(KEY, 3);
        let denied = twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: Some("denied".into()) });
        assert_eq!(denied.round_errors(), vec![(KEY.to_string(), Some("denied".to_string()))], "{reason:?}");
        let refused = twin.reject(reason.clone());
        assert!(Twin::quiet(&refused) && refused.shown.is_empty(), "{reason:?}: {refused:?}");
        let cleared = twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: None });
        assert_eq!(cleared.round_errors(), vec![(KEY.to_string(), None)], "{reason:?}: the next round status is written");
    }
}

/// P-XFER-50: the retry of a refused shown value has its own timer: the timeout of the running step stays armed and no timer is cancelled.
#[test]
fn xfer_show_retry_keeps_the_step_timers() {
    let mut twin = Twin::new();
    let id = twin.uploading(KEY, 3);
    twin.online(false);
    twin.online(true);
    let attempt = twin.attempt(id);
    twin.commit();
    let started = twin.put_started(id, attempt);
    let step_timers: Vec<_> = started.schedules.iter().zip(&started.timer_ids).filter(|((_, input), _)| matches!(input, TransferInput::Tick { kind: TransferTick::PutTimeout { .. } })).map(|(_, id)| *id).collect();
    assert_eq!((twin.stage(id), step_timers.len()), (TransferStage::Uploading, 1), "the step runs with its timeout armed: {started:?}");
    twin.hold_shown = true;
    let denied = twin.post(TransferInput::RoundStatus { epoch: EPOCH, transfer_ids: vec![id], refused: Some("denied".into()) });
    let rejected = twin.reject_shown(denied.shown_batches()[0]);
    assert_eq!((rejected.timer_ids.len(), rejected.cancels), (1, 0), "one timer of the value, none cancelled: {rejected:?}");
    assert!(!step_timers.contains(&rejected.timer_ids[0]), "the timer of the value does not replace the timeout of the step");
    let again = twin.post(TransferInput::Tick { kind: TransferTick::Shown { transfer_id: id } });
    assert_eq!((again.stored_shows(), again.batches.len(), again.steps().len(), again.timers, twin.stage(id)), (denied.stored_shows(), 0, 0, 0, TransferStage::Uploading), "the tick of the value writes the value and nothing else: {again:?}");
}

/// P-XFER-50: the tick of the shown value with no value waiting for its write does nothing in any stage.
#[test]
fn xfer_show_tick_without_a_waiting_value_is_quiet() {
    let reach: [(&str, fn(&mut Twin) -> u64); 5] = [("uploading", |twin| twin.uploading(KEY, 3)), ("persisting failure", |twin| twin.persisting_failure(KEY)), ("failed", |twin| twin.failed(KEY)), ("completing", |twin| twin.completing(KEY)), ("mutating", |twin| twin.mutating(KEY))];
    for (name, arrive) in reach {
        let mut twin = Twin::new();
        let id = arrive(&mut twin);
        let stage = twin.stage(id);
        let out = twin.post(TransferInput::Tick { kind: TransferTick::Shown { transfer_id: id } });
        assert!(Twin::quiet(&out) && out.shown.is_empty() && twin.stage(id) == stage, "{name}: {out:?}");
    }
}
