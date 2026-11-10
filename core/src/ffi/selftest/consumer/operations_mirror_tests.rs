//! P-DLV-24: the Operations value is exactly the op rows with status pending, sending or failed at its revision; an app write shows in Operations before its receipt and leaves only through committed or rolledback.
use serde_json::json;

use super::stand::{moment, Stand};
use crate::ffi::selftest::rig_tests::bound;
use crate::ffi::{OperationInfo, OperationQueueStatus, ReadRequest, ReadValue};

async fn operations(stand: &Stand) -> (i64, Vec<OperationInfo>) {
    let epoch = stand.database.session_epoch();
    let stream = stand.database.subscribe_values(epoch, ReadRequest::Operations);
    match bound(stream.pull_next()).await {
        Some(ReadValue::Operations { revision, items }) => (revision, items),
        other => panic!("the operations stream opened with {other:?}"),
    }
}

fn store_rows(stand: &Stand, revision: i64) -> Vec<OperationInfo> {
    let (_seen, rows, _) = stand.database.db.read_queue(revision).unwrap();
    crate::owners::write::operation_queue::queue_of(&rows).unwrap().into_iter().map(Into::into).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn dlv_prop_operations_mirror() {
    let stand = Stand::open().await;
    let epoch = stand.database.session_epoch();
    for id in ["m1", "m2"] {
        stand.answer("MomentNode", json!({"node": moment(id, 0)}));
        bound(stand.database.node(epoch, "Moment".into(), "node".into(), id.into(), true)).await.unwrap();
    }
    stand.settle().await;
    let (revision, items) = operations(&stand).await;
    assert_eq!((items.clone(), store_rows(&stand, revision)), (Vec::new(), Vec::new()), "no write, no operation");

    stand.answer("MomentReact", json!({"momentReact": {"moment": moment("m1", 0)}}));
    stand.database.set_online(false).unwrap();
    bound(stand.database.act(epoch, "Moment".into(), "m1".into(), "react".into(), "{}".into(), "{}".into())).await.unwrap();
    stand.settle().await;
    let (revision, items) = operations(&stand).await;
    assert_eq!(items, store_rows(&stand, revision), "the value mirrors the op table at its revision");
    assert_eq!(items.len(), 1, "the write shows one operation before its receipt: {items:?}");
    assert_eq!((items[0].model.as_str(), items[0].action.as_str(), items[0].status), ("Moment", "react", OperationQueueStatus::Pending), "the pending write is in the queue");
    assert_eq!(stand.requests("MomentReact"), 0, "the operation shows before the server received it");

    stand.database.set_online(true).unwrap();
    stand.settle().await;
    let (revision, items) = operations(&stand).await;
    assert_eq!((items.clone(), store_rows(&stand, revision)), (Vec::new(), Vec::new()), "the committed write leaves the queue");

    stand.answer_next("MomentReact", json!({"errors": [{"extensions": {"code": "REFUSED"}}]}));
    stand.database.set_online(false).unwrap();
    bound(stand.database.act(epoch, "Moment".into(), "m2".into(), "react".into(), "{}".into(), "{}".into())).await.unwrap();
    stand.settle().await;
    let (revision, items) = operations(&stand).await;
    assert_eq!((items.len(), items.first().map(|item| item.status)), (1, Some(OperationQueueStatus::Pending)), "the second write is pending: {items:?}");
    assert_eq!(items, store_rows(&stand, revision), "the value still mirrors the op table");

    stand.database.set_online(true).unwrap();
    stand.settle().await;
    let (revision, items) = operations(&stand).await;
    assert_eq!((items.clone(), store_rows(&stand, revision)), (Vec::new(), Vec::new()), "the rolled back write leaves the queue");
    stand.database.close_now();
}
