//! Owner-level tests of the owner `Transfer`: the rule and the elements of a batch survive a restart.

use super::transfer_harness::*;
use crate::owners::store::StoreOp;
use crate::owners::write::outbox::OpSource;
use crate::owners::write::transfer::{Joint, TransferInput, TransferStage};
use crate::runtime::Time;

const FIRST: &str = "Moment:r1:upload";
const SECOND: &str = "Moment:r2:upload";

fn element(id: u64, key: &str, completed: bool) -> crate::owners::write::transfer::StoredTransfer {
    let mut row = stored(id, key, 1);
    row.batch = Some(7);
    row.fields.insert("name".into(), serde_json::Value::String(key.into()));
    row.fields.insert("signedId".into(), serde_json::Value::String(crate::owners::write::transfer::SIGNED_ID_SENTINEL.into()));
    if completed {
        row.completed = true;
        row.signed_id = Some("signed".into());
    }
    row
}

#[test]
fn xfer_batch_rule_is_written_with_the_acceptance() {
    let mut twin = Twin::new();
    let reply = twin.reply();
    let out = twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: vec![item(FIRST, 1), item(SECOND, 1)], reply, now: Time(0) });
    let rules: Vec<(u64, String)> = out.batches.iter().flat_map(|batch| batch.ops.iter()).filter_map(|op| if let StoreOp::UploadBatch { batch_id, joint } = op { Some((*batch_id, joint.clone())) } else { None }).collect();
    assert_eq!(rules.len(), 1, "one rule for the two elements");
    let back = Joint::from_text(&rules[0].1).expect("the text reads back");
    assert_eq!((back.minimum, back.maximum, back.items_key.as_str(), back.mutation.body.as_str()), (2, 10, "items", joint().mutation.body.as_str()));
}

#[test]
fn xfer_batch_restored_goes_out_as_one_mutation() {
    let rows = vec![element(1, FIRST, false), element(2, SECOND, true)];
    let (mut twin, out) = Twin::opened_with(rows, Default::default(), [(7, joint())].into());
    assert!(out.submit_ops().is_empty(), "the finished element waits for the other one: {out:?}");
    assert_eq!((twin.stage(2), twin.stage(1)), (TransferStage::BatchReady, TransferStage::Ready));
    twin.post(TransferInput::RestoreReady { epoch: EPOCH });
    twin.online(true);
    twin.step_ok(1, 1, None);
    twin.commit();
    twin.completed(1);
    let out = twin.commit();
    let ops = out.submit_ops();
    assert_eq!(ops.len(), 1, "{out:?}");
    assert_eq!(ops[0].origin, OpSource::Transfer("batch:7".into()));
    let body: serde_json::Value = serde_json::from_str(&ops[0].body).expect("a body");
    let names: Vec<&str> = body["variables"]["items"].as_array().expect("the items").iter().filter_map(|item| item["name"].as_str()).collect();
    assert_eq!(names, vec![FIRST, SECOND], "the elements keep their fields and their order");
    assert_eq!((twin.stage(1), twin.stage(2)), (TransferStage::Mutating, TransferStage::Mutating));
}
