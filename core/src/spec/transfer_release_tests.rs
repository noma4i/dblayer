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
    (twin, Out { runs: [opening.runs, ready.runs].concat(), ..Default::default() })
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
    assert_eq!((answered.steps().len(), twin.stage(1)), (1, TransferStage::Uploading), "a dropped held pair clears pending_step and starts one plain attempt (D-269)");
    let failed = twin.step_error(1, false);
    let (_, discarded) = twin.discard(KEY);
    assert_eq!(all(&[&ready, &answered, &failed, &discarded]), Vec::<u64>::new(), "a replayed and answered step releases nothing");
}
