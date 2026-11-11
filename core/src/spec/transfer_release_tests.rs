//! Owner-level tests of the owner `Transfer`: the saved step of a transfer that will not replay it goes back to Session.

use super::transfer_harness::*;
use crate::owners::write::transfer::{StoredTransfer, TransferInput, TransferStage};

const KEY: &str = "Moment:r1:upload";
const STEP: u64 = 7;

fn saved(attempts: u32) -> StoredTransfer {
    let mut found = stored(1, KEY, 2);
    found.attempts = attempts;
    found.pending_step = Some(STEP);
    found
}

fn opened(found: StoredTransfer, online: bool) -> (Twin, Out) {
    let (mut twin, opening) = Twin::opened(vec![found], Default::default());
    twin.online(online);
    let ready = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    (twin, Out { runs: [opening.runs, ready.runs].concat(), batches: [opening.batches, ready.batches].concat(), shown: [opening.shown, ready.shown].concat(), ..Default::default() })
}

fn all(outs: &[&Out]) -> Vec<u64> { outs.iter().flat_map(|out| out.releases()).collect() }

#[test]
fn xfer_release_pending_step() {
    let (mut twin, ready) = opened(saved(50), true);
    assert_eq!(ready.kinds(), vec![crate::owners::store::UploadKind::Failed], "exhausted attempts: the restore writes Failed before anything else");
    let written = twin.commit();
    assert_eq!((twin.stage(1), all(&[&ready, &written])), (TransferStage::Failed, vec![STEP]), "exhausted attempts: one Release of the saved step");

    for discard in [false, true] {
        let (mut twin, ready) = opened(saved(0), false);
        let (_, out) = if discard { twin.discard(KEY) } else { twin.cancel(KEY) };
        let removed = twin.commit();
        let ended = twin.end_epoch();
        assert_eq!(all(&[&ready, &out, &removed, &ended]), vec![STEP], "removed by discard {discard}: one Release, none at the end of the epoch");
    }

    let (mut twin, ready) = opened(saved(0), false);
    let ended = twin.end_epoch();
    assert_eq!((twin.stage(1), all(&[&ready, &ended])), (TransferStage::Done, vec![STEP]), "Done by EpochEnded");

    let (mut twin, ready) = opened(saved(0), true);
    assert_eq!((ready.steps().len(), twin.stage(1), all(&[&ready])), (0, TransferStage::Ready, Vec::<u64>::new()), "online, the transfer starts no step of its own and releases nothing while Session owns the saved pair (D-246)");
    let answered = twin.post(TransferInput::StepAnswer { request_id: STEP, outcome: crate::owners::session::AuthorOutcome::Dropped });
    assert_eq!((answered.steps().len(), twin.stage(1)), (0, TransferStage::PersistingAttempt), "a dropped held pair clears pending_step and writes a plain attempt first (D-269)");
    let started = twin.commit();
    assert_eq!((started.steps().len(), twin.stage(1)), (1, TransferStage::Uploading), "the committed attempt sends one step of its own");
    let failed = twin.step_error(1, false);
    let (_, discarded) = twin.discard(KEY);
    assert_eq!(all(&[&ready, &answered, &started, &failed, &discarded]), Vec::<u64>::new(), "a replayed and answered step releases nothing");
}

#[derive(Clone, Debug)]
enum Setup { RestoreReady, Online, Interest }

/// P-XFER-45 (xfer_prop_held_step_released): in every order of {Restored, RestoreReady, Online, Interest+} a restored held pair is driven by Session alone: Transfer sends no step of its own and no Release until `StepAnswer`, which then lands exactly one physical request (D-246).
#[test]
fn xfer_prop_held_step_released() {
    use crate::owners::session::AuthorOutcome;
    for order in crate::test_wait::permutations(&[Setup::RestoreReady, Setup::Online, Setup::Interest]) {
        let (mut twin, opening) = Twin::opened(vec![saved(0)], Default::default());
        let mut outs = vec![opening];
        for step in &order {
            outs.push(match step {
                Setup::RestoreReady => twin.post(TransferInput::RestoreReady { epoch: EPOCH }),
                Setup::Online => twin.online(true),
                Setup::Interest => twin.post(TransferInput::StatusInterestPlus { key: KEY.into(), stream_id: 5 }),
            });
        }
        let refs: Vec<&Out> = outs.iter().collect();
        let steps: usize = refs.iter().map(|out| out.steps().len()).sum();
        assert_eq!((steps, all(&refs)), (0, Vec::<u64>::new()), "held pair: no step of its own and no Release before the answer, order {order:?}");
        assert_eq!(twin.stage(1), TransferStage::Ready, "the restored transfer waits for the pair, order {order:?}");
        let answered = twin.post(TransferInput::StepAnswer { request_id: STEP, outcome: AuthorOutcome::Dropped });
        assert_eq!((answered.steps().len(), twin.stage(1)), (0, TransferStage::PersistingAttempt), "StepAnswer clears the pair and writes a plain attempt first, order {order:?}");
        let started = twin.commit();
        assert_eq!((started.steps().len(), twin.stage(1)), (1, TransferStage::Uploading), "exactly one physical request for the pair, order {order:?}");
        assert!(all(&[&answered, &started]).is_empty(), "the answered pair is not released, order {order:?}");
    }
}

#[test]
fn xfer_prop_held_step_answer_before_declaration() {
    use crate::owners::session::AuthorOutcome;
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Event { Answer, Register }
    for order in crate::test_wait::permutations(&[Event::Answer, Event::Register]) {
        let mut twin = Twin::bare(1);
        twin.post(TransferInput::Restored { uploads: vec![saved(0)], outcomes: Default::default(), orphans: Vec::new(), batches: Default::default(), held_orphan: None });
        twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        twin.online(true);
        for event in &order {
            match event {
                Event::Answer => twin.post(TransferInput::StepAnswer { request_id: STEP, outcome: AuthorOutcome::Dropped }),
                Event::Register => twin.post(TransferInput::RegisterOperations { declarations: vec![("Moment".into(), "upload".into())] }),
            };
        }
        assert_eq!(twin.stage(1), TransferStage::PersistingAttempt, "{order:?}: the answered pair lets the declared transfer write its attempt");
    }
}
