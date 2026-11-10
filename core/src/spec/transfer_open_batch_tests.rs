//! Owner-level tests of the owner `Transfer`: the opening of the elements of a batch whose files are on the server.

use super::transfer_harness::*;
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::{OpSource, OutboxOutcome};
use crate::owners::write::transfer::{Shown, StoredGroup, StoredTransfer, TransferInput, TransferStage, SIGNED_ID_SENTINEL};

const FIRST: &str = "Moment:r1:upload";
const SECOND: &str = "Moment:r2:upload";

type Case = (&'static str, Vec<StoredTransfer>, std::collections::BTreeMap<u64, StoredGroup>, TransferStage);

fn element(id: u64, key: &str, has_intent: bool) -> StoredTransfer {
    let mut row = stored(id, key, 1);
    row.batch = Some(7);
    row.fields.insert("name".into(), serde_json::Value::String(key.into()));
    row.fields.insert("signedId".into(), serde_json::Value::String(SIGNED_ID_SENTINEL.into()));
    row.completed = true;
    row.confirmed_parts = 1;
    row.signed_id = Some("signed".into());
    row.has_intent = has_intent;
    row
}

fn restored(rows: Vec<StoredTransfer>, intent_members: Vec<u64>) -> (Twin, Out) { restored_groups(rows, [(7, StoredGroup { joint: joint(), op_id: Some(1_000_007), intent_members })].into()) }

fn restored_groups(rows: Vec<StoredTransfer>, batches: std::collections::BTreeMap<u64, StoredGroup>) -> (Twin, Out) {
    let mut twin = Twin::bare(1);
    let declarations = rows.iter().map(|row| (row.mutation.model.clone(), row.mutation.action.clone())).collect();
    twin.post(TransferInput::RegisterOperations { declarations });
    let out = twin.post(TransferInput::Restored { uploads: rows, outcomes: Default::default(), orphans: Vec::new(), batches, held_orphan: None });
    (twin, out)
}

#[test]
fn xfer_open_batch_ready() {
    let (mut twin, out) = restored(vec![element(1, FIRST, false), element(2, SECOND, false)], Vec::new());
    assert_eq!((twin.stage(1), twin.stage(2)), (TransferStage::BatchReady, TransferStage::BatchReady));
    assert_eq!(out.shows(), vec![Shown::Uploading { progress: 100 }, Shown::Uploading { progress: 100 }]);
    assert!(out.submit_ops().is_empty() && out.writes() == 0, "nothing goes before the classification of every element: {out:?}");
    twin.online(true);
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert!(twin.state().transfers.values().all(|transfer| transfer.stage == TransferStage::PersistingBatchIntent));
    assert_eq!(out.writes(), 1);
    let intents: Vec<_> = out.batches[0].ops.iter().filter(|op| matches!(op, StoreOp::Intent { .. })).collect();
    assert_eq!(intents.len(), 1, "one intent for the group");
    assert!(out.submit_ops().is_empty());
    let out = twin.commit();
    assert!(twin.state().transfers.values().all(|transfer| transfer.stage == TransferStage::Mutating));
    assert_eq!(out.submit_ops().len(), 1);
    assert!(matches!(&out.submit_ops()[0].origin, OpSource::TransferGroup { batch_id: 7, members } if members.len() == 2));
}

#[test]
fn xfer_open_batch_intent() {
    let (mut twin, out) = restored(vec![element(1, FIRST, true), element(2, SECOND, true)], vec![1, 2]);
    assert_eq!((twin.stage(1), twin.stage(2)), (TransferStage::Mutating, TransferStage::Mutating));
    assert!(out.shows().iter().all(|shown| *shown == Shown::Uploading { progress: 100 }));
    assert_eq!(out.writes(), 0, "the intent is already in the base");
    twin.online(true);
    let out = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    assert!(out.writes() == 0 && out.submits() == 0 && out.steps().is_empty(), "Outbox restores the intent itself: {out:?}");
    let members = vec![(1, 1, OutboxOutcome::Confirmed { revision: 3, result: None, row_id: None }), (2, 1, OutboxOutcome::Failed { error: "boom".into() })];
    let out = twin.post(TransferInput::MutationOutcomes { batch_id: 7, members, intent_removed: false });
    assert_eq!((twin.stage(1), twin.stage(2)), (TransferStage::Removing, TransferStage::PersistingFailure), "each element gets its own outcome");
    assert_eq!(out.writes(), 1);
}

#[test]
fn xfer_prop_batch_restore_provenance() {
    let group = |op_id: Option<u64>, intent_members: Vec<u64>| [(7, StoredGroup { joint: joint(), op_id, intent_members })].into();
    let mut single = element(1, FIRST, false);
    single.batch = None;
    let cases: Vec<Case> = vec![
        ("a completed single without intent and outcome", vec![single], Default::default(), TransferStage::Failed),
        ("a collecting group with a known provenance", vec![element(1, FIRST, false), element(2, SECOND, false)], group(Some(1_000_007), Vec::new()), TransferStage::BatchReady),
        ("the group is not in the base", vec![element(1, FIRST, false), element(2, SECOND, false)], Default::default(), TransferStage::Failed),
        ("the group has no reserved op_id", vec![element(1, FIRST, false), element(2, SECOND, false)], group(None, Vec::new()), TransferStage::Failed),
        ("the group already wrote its intent but this element is not in it", vec![element(1, FIRST, false), element(2, SECOND, true)], group(Some(1_000_007), vec![2]), TransferStage::Failed),
    ];
    for (name, rows, batches, stage) in cases {
        let ids: Vec<u64> = rows.iter().map(|row| row.transfer_id).collect();
        let (mut twin, out) = restored_groups(rows, batches);
        assert_eq!(twin.stage(ids[0]), stage, "{name}");
        twin.online(true);
        let ready = twin.post(TransferInput::RestoreReady { epoch: EPOCH });
        assert_eq!(out.submits(), 0, "{name}: no resend at the opening");
        if stage == TransferStage::Failed {
            assert_eq!(ready.submits(), 0, "{name}: no resend");
        }
        if stage == TransferStage::Failed {
            assert!(out.shows().contains(&Shown::Failed { error: "RestoreFailed".into() }), "{name}: {:?}", out.shows());
            let (reply, retry) = twin.retry(if ids[0] == 1 { FIRST } else { SECOND }, None);
            assert_eq!(Twin::fault(&retry, reply), Some(crate::owners::write::outbox::Fault::RestoreFailed), "{name}");
            assert_eq!(retry.submits() + retry.writes(), 0, "{name}");
        }
    }
}
