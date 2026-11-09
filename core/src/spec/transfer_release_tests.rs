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
    let started = twin.commit();
    assert_eq!(started.steps().len(), 0, "online, the transfer starts no step of its own and waits for the Session answer of the held pair");
    assert_eq!((twin.stage(1), all(&[&ready, &started])), (TransferStage::Ready, Vec::<u64>::new()), "while Session still owns the saved pair the transfer stays with pending_step and releases nothing");
}
