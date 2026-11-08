//! Orders of the inputs of the owner `Transfer` for the batches: the copies, the files and the removals of the elements.

use serde_json::Value;

use super::transfer_harness::*;
use super::transfer_orders_tests::drain;
use crate::owners::write::transfer::{BatchItem, Joint, Renumber, TransferAnswer, TransferId, TransferInput, TransferStage, SIGNED_ID_SENTINEL};
use crate::owners::contract::ReplyId;
use crate::runtime::Time;

const KEY: &str = "Moment:r1:upload";
const OTHER: &str = "Moment:r2:upload";
const THIRD: &str = "Moment:r3:upload";
const FOURTH: &str = "Moment:r4:upload";

fn batch_twin(keys: &[&str]) -> (Twin, ReplyId, Vec<TransferId>) {
    let mut twin = Twin::new();
    let reply = twin.reply();
    twin.post(TransferInput::AcceptBatch { joint: Box::new(joint()), epoch: EPOCH, items: keys.iter().map(|key| item(key, 2)).collect(), reply, now: Time(0) });
    let ids: Vec<TransferId> = keys.iter().map(|key| twin.state().open_id(key).expect("an element")).collect();
    twin.reserve_batch(&ids);
    twin.commit();
    (twin, reply, ids)
}

/// P-XFER-09 and P-XFER-16: the results of the copies of a batch in any order give N receipts after one write, or none and no metadata, never a part. The cancel of an element after the start of the mutation fails in any order.
#[test]
fn xfer_orders_batch_copies() {
    let keys = [KEY, OTHER, THIRD];
    let count = |twin: &Twin| twin.state().transfers.values().filter(|transfer| transfer.batch_id.is_some() && transfer.stage != TransferStage::Done).count();
    let orders = permutations(&[0usize, 1, 2]);
    assert_eq!(orders.len(), 6);
    for order in &orders {
        let (mut twin, reply, ids) = batch_twin(&keys);
        let mut outs = Vec::new();
        for (index, at) in order.iter().enumerate() {
            let out = twin.copy_done(ids[*at]);
            assert_eq!((out.all_replies(), out.steps().len(), out.writes()), (0, 0, usize::from(index == 2)), "{order:?} at {index}: one common write after the last copy, no receipt and no request before it");
            outs.push(out);
        }
        let first = twin.commit();
        assert_eq!((first.all_replies(), first.steps().len()), (1, 0), "{order:?}: the receipts come with the write");
        assert!(matches!(first.replies(reply).as_slice(), [TransferAnswer::Receipts(receipts)] if receipts.len() == 3), "{order:?}");
        assert_eq!(count(&twin), 3, "{order:?}");
        assert_eq!(twin.commit().steps().len(), 1, "{order:?}");
        for failing in 0..3 {
            let (mut twin, reply, ids) = batch_twin(&keys);
            let mut outs = Vec::new();
            for at in order {
                outs.push(if *at == failing { twin.copy_failed(ids[*at]) } else { twin.copy_done(ids[*at]) });
            }
            outs.extend(drain(&mut twin));
            let answers: Vec<TransferAnswer> = outs.iter().flat_map(|out| out.replies(reply)).collect();
            assert!(answers.iter().all(|answer| matches!(answer, TransferAnswer::Err(_))) && answers.len() == 1, "{order:?} fails {failing}: no receipt, one refusal: {answers:?}");
            assert_eq!(outs.iter().map(|out| out.removed().len()).sum::<usize>(), 3, "{order:?} fails {failing}: the metadata of the whole batch goes");
            assert_eq!(count(&twin), 0, "{order:?} fails {failing}");
        }
    }
    let cancels = permutations(&[KEY, OTHER, THIRD]);
    for order in cancels {
        let mut running = Twin::with_slots(3);
        let ids = running.batch_mutating(&keys);
        for key in &order {
            let (reply, out) = running.cancel(key);
            assert_eq!(Twin::fault(&out, reply), Some(crate::owners::write::outbox::Fault::InFlight), "{order:?} {key}");
        }
        assert!(ids.iter().all(|id| running.stage(*id) == TransferStage::Mutating), "{order:?}");
    }
}

fn fielded(key: &str) -> BatchItem {
    let mut element = item(key, 1);
    element.fields = serde_json::json!({"name": key, "position": 0, "size": 0, "signedId": SIGNED_ID_SENTINEL}).as_object().unwrap().clone();
    element.mutation.body = serde_json::json!({"variables": {"name": key, "position": 0, "size": 0, "signedId": SIGNED_ID_SENTINEL}}).to_string();
    element
}

fn rules(renumber: bool) -> Joint {
    let renumber = renumber.then(|| Renumber { position: "position".into(), size: "size".into(), position_target: "position".into(), size_target: "size".into(), start: 1 });
    Joint { minimum: 2, renumber, ..joint() }
}

fn batch_started(keys: &[&str], renumber: bool) -> (Twin, Vec<TransferId>) {
    let mut twin = Twin::with_slots(4);
    let ids = twin.accepted_items(keys.iter().map(|key| fielded(key)).collect(), rules(renumber));
    for _ in &ids {
        assert_eq!(twin.commit().steps().len(), 1);
    }
    (twin, ids)
}

fn file_done(twin: &mut Twin, id: TransferId) -> Out {
    twin.step_ok(id, 1, None);
    twin.commit();
    twin.commit();
    twin.completed(id);
    let out = twin.commit();
    if twin.state().transfers.values().any(|transfer| transfer.stage == TransferStage::PersistingBatchIntent) { twin.commit() } else { out }
}

/// P-XFER-31: the files of a batch done in any order give one mutation, at the last file, with the elements in the original order.
#[test]
fn xfer_orders_batch_files() {
    let keys = [KEY, OTHER, THIRD];
    let orders = permutations(&[0usize, 1, 2]);
    assert_eq!(orders.len(), 6);
    for order in orders {
        let (mut twin, ids) = batch_started(&keys, false);
        let outs: Vec<Out> = order.iter().map(|at| file_done(&mut twin, ids[*at])).collect();
        assert_eq!(outs.iter().map(|out| out.submit_ops().len()).collect::<Vec<_>>(), vec![0, 0, 1], "{order:?}");
        let body: Value = serde_json::from_str(&outs[2].submit_ops()[0].body).unwrap();
        let names: Vec<Value> = body["variables"]["items"].as_array().unwrap().iter().map(|found| found["name"].clone()).collect();
        assert_eq!(names, vec![serde_json::json!(KEY), serde_json::json!(OTHER), serde_json::json!(THIRD)], "{order:?}: the original order");
    }
}

/// P-XFER-32: the removal of elements in any order renumbers the remaining ones by the original order, and stops below the minimum.
#[test]
fn xfer_orders_batch_renumber() {
    let keys = [KEY, OTHER, THIRD, FOURTH];
    let orders = permutations(&[0usize, 1, 2]);
    assert_eq!(orders.len(), 6);
    for order in orders {
        let (mut twin, _) = batch_started(&keys, true);
        let mut left: Vec<usize> = vec![0, 1, 2, 3];
        for at in &order {
            let (_, out) = twin.cancel(keys[*at]);
            twin.commit();
            left.retain(|found| found != at);
            let expected: Vec<(String, Vec<(String, i64)>)> = if left.len() >= 2 {
                left.iter().enumerate().map(|(index, found)| (format!("r{}", found + 1), vec![("position".to_string(), index as i64 + 1), ("size".to_string(), left.len() as i64)])).collect()
            } else {
                Vec::new()
            };
            assert_eq!(out.numbers(), expected, "{order:?} removes {at}");
        }
    }
}

