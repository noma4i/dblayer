//! Owner-level tests of the `outbox` machine: the properties of the offline queue, the receipt, the restored answer, the status column and the stray inputs. Names are the `Test` column.

use super::outbox_cells_tests::submit_op;
use super::outbox_harness::*;
use super::outbox_harness::{Policy, RWhy, St};
use crate::owners::contract::Committed;
use crate::owners::write::outbox::{NewOp, OutboxAnswer, OutboxInput, Receipt};
use crate::spec::support::committed_at;

fn optimistic_row() -> crate::store::Row {
    std::sync::Arc::new(crate::store::Record { id: crate::identity::RowId::new("r").expect("the row id"), revision: 11, data: serde_json::Map::new(), sync: Default::default() })
}

fn commit_shown(twin: &mut Twin, id: crate::owners::write::outbox::OpId, shown: Vec<Option<crate::store::Row>>) -> Vec<OutboxEffect> {
    let batch = twin.batches[&id];
    twin.revision += 1;
    twin.post(OutboxInput::Committed(Committed { shown, ..committed_at(batch, twin.revision, &["moments"]) }))
}

fn receipt_of(effects: &[OutboxEffect], reply: u64) -> Receipt {
    effects.iter().find_map(|effect| match effect { OutboxEffect::Reply { reply: id, answer: OutboxAnswer::Receipt(receipt) } if *id == reply => Some(receipt.clone()), _ => None }).expect("a receipt")
}

/// P-OUT-36: the receipt carries the optimistic row of its commit, fixed; a fast confirm does not rewrite it; `None` only without optimistic effects.
#[test]
fn out_prop_receipt_fast_confirm() {
    let mut twin = Twin::new();
    let id = twin.submit("r", Policy::Rollback, false);
    let row = optimistic_row();
    let committed = commit_shown(&mut twin, id, vec![Some(row.clone())]);
    let receipt = receipt_of(&committed, SUBMIT_REPLY);
    assert_eq!(receipt.optimistic, Some(row.clone()), "the receipt carries the optimistic row of the commit");
    twin.respond(id);
    twin.committed(id);
    assert_eq!(receipt.optimistic, Some(row), "the receipt is fixed, the fast confirm does not rewrite it");

    let mut plain = Twin::new();
    let op = NewOp { effects: Default::default(), ..model_op("r", Policy::Rollback, false) };
    let id = submit_op(&mut plain, op, 30);
    let committed = commit_shown(&mut plain, id, vec![None]);
    assert_eq!(receipt_of(&committed, 30).optimistic, None, "no optimistic effects means no optimistic row");
}

/// P-OUT-38: the stray pair (RestoreReady, Restored) of an owner that is ready gives no effect.
#[test]
fn out_prop_stray_input_ignored() {
    let states = [St::Submitting, St::Queued, St::PersistingAttempt, St::Sending, St::Landing, St::RollingBack(RWhy::Rejected), St::MarkingFailed, St::MarkingRetry, St::Failed, St::WaitingDeclaration];
    for state in states {
        let ctx = if state == St::Queued { offline() } else { ready() };
        for input in [OutboxInput::RestoreReady { epoch: EPOCH }, OutboxInput::Restored { ops: Vec::new(), pending_lost: false }] {
            let (mut twin, _) = reach(state, &ctx);
            let effects = twin.post(input.clone());
            assert!(effects.is_empty(), "{state:?} x {input:?}: {effects:?}");
        }
    }
}
