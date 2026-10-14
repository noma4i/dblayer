use serde_json::json;

use crate::commit::OperationStatus;
use crate::identity::RowId;
use crate::transport::Reply;

use super::super::rig::*;

#[tokio::test(start_paused = true)]
async fn an_answer_that_cannot_be_stored_parks_the_operation_as_unknown_instead_of_leaving_it_sending() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let id = RowId::new("m1").unwrap();
    rig.db.update("moments", &id, params(json!({"text": "new"}))).unwrap();
    rig.db.execute_for_test(&format!("CREATE TRIGGER refuse_updates BEFORE UPDATE ON {} BEGIN SELECT RAISE(ABORT, 'refused'); END", crate::ddl::table_name("moments")));
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "text": "from the server", "userId": null}})));
    assert!(rig.sync.flush().await.is_err());
    let queue = rig.db.outbox().unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!((queue[0].operation.status, queue[0].operation.transport_started), (OperationStatus::DeliveryUnknown, true));
    rig.db.execute_for_test("DROP TRIGGER refuse_updates");
    assert_eq!(rig.db.retry_all().unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn inv_op_06_a_cancelled_send_leaves_the_operation_as_unknown_delivery() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let id = RowId::new("m1").unwrap();
    rig.db.update("moments", &id, params(json!({"text": "new"}))).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "text": "new", "userId": null}})).after(std::time::Duration::from_secs(5)));
    let sync = rig.sync.clone();
    let task = tokio::spawn(async move { sync.flush().await });
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    task.abort();
    let _ = task.await;
    let queue = rig.db.outbox().unwrap();
    assert_eq!((queue[0].operation.status, queue[0].operation.transport_started), (OperationStatus::DeliveryUnknown, true));
}
