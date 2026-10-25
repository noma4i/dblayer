use std::time::Duration;

use super::super::rig::*;
use super::upload_tests::{blob, created, file, input, rid, single_replies, small, upload_rig};
use crate::transport::PutReply;

#[tokio::test(start_paused = true)]
async fn a_step_answered_after_reset_all_sends_nothing_and_leaves_nothing() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob().after(Duration::from_secs(5)));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), "upload", input(&source)).await.unwrap();
    rig.transport.await_calls("CreateBlob", 1).await;
    rig.db.reset_all().unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!((rig.transport.put_calls().len(), rig.transport.call_count("CreateMoment")), (0, 0));
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.sync_info("moments", &rid("m1")).unwrap().uploads.get("upload").cloned(), None);
}

#[tokio::test(start_paused = true)]
async fn the_mutation_answered_after_reset_all_does_not_bring_the_row_back() {
    let rig = upload_rig(small());
    rig.transport.enqueue("CreateBlob", blob());
    rig.transport.enqueue("CreateMoment", created("m9").after(Duration::from_secs(5)));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), "upload", input(&source)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 1);
    rig.db.reset_all().unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(text_of(&rig, "moments", "m9"), None);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn inv_upload_04_a_transfer_in_flight_during_a_reset_does_not_send_its_mutation_or_bring_back_rows() {
    let rig = upload_rig(small());
    single_replies(&rig);
    rig.transport.enqueue_put("https://s3/single", PutReply::etag("e").after(Duration::from_secs(4)));
    let source = file(b"abcdef");
    rig.sync.upload("moments", &rid("m1"), "upload", input(&source)).await.unwrap();
    rig.transport.await_put_calls(1).await;
    rig.db.reset(&["moments"]).unwrap();
    rig.sync.wait_uploads().await;
    assert_eq!(rig.transport.call_count("CreateMoment"), 0);
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(rig.db.count(&crate::query::Query::new("moments")).unwrap(), 0);
}
