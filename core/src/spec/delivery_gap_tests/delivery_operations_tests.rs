use super::*;
use crate::ffi::{OperationInfo, OperationQueueStatus};

fn info(seq: i64, status: OperationQueueStatus) -> OperationInfo {
    OperationInfo { seq, model: "Moment".into(), row_id: "r".into(), action: "update".into(), status, attempts: 0, error: None, created_at: 1, blocked_by: None, upload: None }
}

fn queue_view() -> d::View {
    d::View { kind: d::Kind::Operations, model: String::new(), tables: vec!["op".into()], local: true, remote: false, live: false, scoped: Default::default(), reload: false, window: WindowKey { model: String::new(), name: String::new() }, subject: String::new(), node: None, wants: Vec::new(), deferred_rows: Vec::new(), query: None }
}

fn opened() -> Twin {
    let mut twin = Twin::new();
    twin.det.post(twin.addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: queue_view(), epoch: EPOCH, identity: false, valid: true, revision: 0, deferred: None });
    twin.run();
    twin
}

pub(super) fn live() -> Twin {
    let mut twin = opened();
    read_done(&mut twin, 1, d::ReadData::Operations(vec![info(1, OperationQueueStatus::Pending)]));
    take(&twin.log);
    twin
}

fn reads(entries: Vec<Emitted<DeliveryEffect, DeliveryInput>>) -> Vec<i64> {
    entries.into_iter().filter_map(|entry| match entry {
        Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Read { revision, .. }, .. }) => Some(revision),
        _ => None,
    }).collect()
}

#[test]
fn dlv_open_operations() {
    let mut twin = Twin::new();
    twin.det.post(twin.addr, EpochStamp::Control, d::DeliveryInput::Open { stream: STREAM, view: queue_view(), epoch: EPOCH, identity: false, valid: true, revision: 0, deferred: None });
    twin.run();
    let entries = take(&twin.log);
    assert!(entries.iter().all(|entry| matches!(entry, Emitted::Run(DeliveryEffect::Stream { effect: StreamEffect::Read { .. }, .. }))), "a read and nothing else: no window, no subscription");
    assert_eq!(reads(entries).len(), 1);
    assert_eq!(twin.state().stage(STREAM), Some(d::DeliveryStage::Opening));
}

#[test]
fn dlv_opening_operations_done() {
    let mut twin = opened();
    let items = vec![info(1, OperationQueueStatus::Pending), info(2, OperationQueueStatus::Failed)];
    read_done(&mut twin, 1, d::ReadData::Operations(items.clone()));
    assert_eq!(twin.state().journal(STREAM), [d::Value::Operations { revision: 1, items }]);
    assert_eq!(twin.state().stage(STREAM), Some(d::DeliveryStage::Live));
}

#[test]
fn dlv_live_operations_commit() {
    let mut twin = live();
    commit(&mut twin, 2, "op".into(), 0);
    assert_eq!(reads(take(&twin.log)), [2]);
    let before = twin.state().journal(STREAM).len();
    commit(&mut twin, 9, "moments".into(), 0);
    assert!(take(&twin.log).is_empty() && twin.state().journal(STREAM).len() == before, "a commit of another table starts no read");
    let sending = vec![info(1, OperationQueueStatus::Sending)];
    read_done(&mut twin, 2, d::ReadData::Operations(sending.clone()));
    assert_eq!(twin.state().journal(STREAM), [d::Value::Operations { revision: 2, items: sending.clone() }], "the unread value is replaced by the new one");
    commit(&mut twin, 3, "op".into(), 0);
    read_done(&mut twin, 3, d::ReadData::Operations(sending.clone()));
    assert_eq!(twin.state().journal(STREAM), [d::Value::Operations { revision: 2, items: sending }], "the same queue gives no value");
}

fn live_after_commit() -> Twin {
    let mut twin = live();
    commit(&mut twin, 2, "op".into(), 0);
    twin
}

#[test]
fn dlv_operations_read_failed() {
    for mut twin in [opened(), live_after_commit()] {
        let read_id = twin.read_id();
        twin.post(d::DeliveryInput::ReadFailed { stream: STREAM, read_id, revision: 2, error: "queue unreadable".into() });
        assert!(matches!(twin.state().journal(STREAM).last(), Some(d::Value::Ended(d::EndCause::Storage { error })) if error == "queue unreadable"), "{:?}", twin.state().journal(STREAM));
    }
}
